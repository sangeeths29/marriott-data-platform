"""Step 3 (PySpark): bronze -> silver.

Runs unchanged locally (spark-submit) and as an AWS Glue 4.0 job (plain PySpark,
no awsglue imports; Glue passes --key value args which argparse reads).

  spark-submit pipeline/jobs/bronze_to_silver.py --date 2026-11-09 --data-root ./data

Produces:
  silver/properties   Marriott-brand properties from OpenStreetMap
  silver/reviews      both review sources conformed to one schema, Marriott brands only,
                      PII masked, VADER sentiment, linked to a property_id
  silver/financials   SEC facts, typed and de-duplicated (latest filing wins)

Each source is a full snapshot, so silver tables are fully overwritten (idempotent).
"""
from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common.brands import match_brand
from pipeline.common.pii import hash_identifier, mask_text
from pipeline.common.spark import resolve_pii_salt

log = logging.getLogger("bronze_to_silver")

MATCH_RADIUS_KM = 0.25

# The 515K dataset covers exactly one city per country; Hotel_Address ends with the
# country name. (Parsing the city from the address fails for UK postcodes.)
EUROPE_COUNTRIES = {  # country name -> (ISO code, city)
    "United Kingdom": ("GB", "London"), "Netherlands": ("NL", "Amsterdam"), "France": ("FR", "Paris"),
    "Spain": ("ES", "Barcelona"), "Italy": ("IT", "Milan"), "Austria": ("AT", "Vienna"),
}

# ---------- UDFs (thin wrappers over tested pure-Python functions) ----------
brand_schema = T.StructType([T.StructField("brand", T.StringType()), T.StructField("tier", T.StringType())])


@F.udf(brand_schema)
def brand_udf(name):
    m = match_brand(name)
    return {"brand": m[0], "tier": m[1]} if m else None


mask_udf = F.udf(mask_text, T.StringType())


_ANALYZER = {}  # one VADER analyzer per Python worker process


@F.udf(T.DoubleType())
def sentiment_udf(text):
    if not text:
        return None
    if "vader" not in _ANALYZER:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        _ANALYZER["vader"] = SentimentIntensityAnalyzer()
    return float(_ANALYZER["vader"].polarity_scores(text)["compound"])


def bronze(spark: SparkSession, root: str, source: str, date: str) -> DataFrame:
    return spark.read.parquet(f"{root}/bronze/source={source}/ingest_date={date}")


def haversine_km(lat1, lon1, lat2, lon2):
    dlat, dlon = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return F.lit(2 * 6371.0) * F.asin(F.sqrt(a))


# ---------- properties ----------
def build_properties(spark, root, date) -> DataFrame:
    osm = bronze(spark, root, "osm_properties", date)
    b = brand_udf(F.coalesce(F.col("brand"), F.col("name")))
    return (
        osm.withColumn("_b", b)
        .where(F.col("_b").isNotNull())
        .select(
            F.concat_ws("_", F.lit("osm"), "osm_type", "osm_id").alias("property_id"),
            F.col("name").alias("property_name"),
            F.col("_b.brand").alias("brand"),
            F.col("_b.tier").alias("tier"),
            F.col("brand_wikidata"),
            F.col("lat").cast("double").alias("latitude"),
            F.col("lon").cast("double").alias("longitude"),
            F.concat_ws(" ", "housenumber", "street").alias("street_address"),
            "city", "postcode", "country_code",
            F.col("stars").cast("double").alias("stars"),
            F.col("rooms").cast("int").alias("rooms"),
            "website",
            F.col("_ingest_date").alias("source_ingest_date"),
        )
        .dropDuplicates(["property_id"])
    )


# ---------- reviews ----------
def conform_datafiniti(df: DataFrame) -> DataFrame:
    return df.select(
        F.lit("datafiniti").alias("source"),
        F.col("name").alias("hotel_name"),
        F.col("address").alias("hotel_address"),
        F.col("city"),
        F.col("country").alias("country_code"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.to_date(F.substring("`reviews.date`", 1, 10), "yyyy-MM-dd").alias("review_date"),
        F.col("`reviews.rating`").cast("double").alias("rating_5"),
        F.col("`reviews.title`").alias("review_title"),
        F.col("`reviews.text`").alias("review_text"),
        F.col("`reviews.username`").alias("reviewer"),
        F.lit(None).cast("string").alias("reviewer_nationality"),
        "_source_file", "_ingest_date",
    )


def conform_europe(df: DataFrame) -> DataFrame:
    addr = F.trim(F.col("Hotel_Address"))
    country_code = F.coalesce(*[F.when(addr.endswith(n), F.lit(cc)) for n, (cc, _) in EUROPE_COUNTRIES.items()])
    city = F.coalesce(*[F.when(addr.endswith(n), F.lit(c)) for n, (_, c) in EUROPE_COUNTRIES.items()])
    pos = F.when(F.trim("Positive_Review") != "No Positive", F.trim("Positive_Review"))
    neg = F.when(F.trim("Negative_Review") != "No Negative", F.trim("Negative_Review"))
    return (
        df.select(
            F.lit("europe_515k").alias("source"),
            F.col("Hotel_Name").alias("hotel_name"),
            F.col("Hotel_Address").alias("hotel_address"),
            city.alias("city"),
            country_code.alias("country_code"),
            F.col("lat").cast("double").alias("latitude"),
            F.col("lng").cast("double").alias("longitude"),
            F.to_date("Review_Date", "M/d/yyyy").alias("review_date"),
            (F.col("Reviewer_Score").cast("double") / 2).alias("rating_5"),  # 10-pt -> 5-pt
            F.lit(None).cast("string").alias("review_title"),
            F.concat_ws(" ", F.concat(F.lit("Liked: "), pos), F.concat(F.lit("Disliked: "), neg)).alias("review_text"),
            F.lit(None).cast("string").alias("reviewer"),
            F.trim("Reviewer_Nationality").alias("reviewer_nationality"),
            "_source_file", "_ingest_date",
        )
    )


def link_properties(reviews: DataFrame, properties: DataFrame) -> DataFrame:
    """Entity resolution v1: same brand + within MATCH_RADIUS_KM -> nearest OSM property.
    Unmatched hotels get a deterministic surrogate id so they still roll up correctly."""
    hotels = reviews.select("source", "hotel_name", "brand", "latitude", "longitude").dropDuplicates()
    p = properties.select("property_id", F.col("brand").alias("p_brand"),
                          F.col("latitude").alias("p_lat"), F.col("longitude").alias("p_lon"))
    cand = (
        hotels.join(F.broadcast(p), hotels.brand == p.p_brand)
        .withColumn("dist_km", haversine_km(F.col("latitude"), F.col("longitude"), F.col("p_lat"), F.col("p_lon")))
        .where(F.col("dist_km") <= MATCH_RADIUS_KM)
    )
    w = Window.partitionBy("source", "hotel_name", "latitude", "longitude").orderBy("dist_km")
    best = cand.withColumn("rn", F.row_number().over(w)).where("rn = 1").select(
        "source", "hotel_name", "latitude", "longitude", "property_id", "dist_km")
    fallback_id = F.concat(F.lit("rev_"), F.substring(F.sha2(F.concat_ws("|",
        F.lower("hotel_name"), F.round("latitude", 3), F.round("longitude", 3)), 256), 1, 16))
    return (
        reviews.join(best, ["source", "hotel_name", "latitude", "longitude"], "left")
        .withColumn("property_match_method",
                    F.when(F.col("property_id").isNotNull(), "osm_geo_brand").otherwise("unmatched"))
        .withColumn("property_id", F.coalesce("property_id", fallback_id))
        .withColumnRenamed("dist_km", "property_match_distance_km")
    )


def build_reviews(spark, root, date, properties: DataFrame, salt: str) -> DataFrame:
    unioned = conform_datafiniti(bronze(spark, root, "datafiniti_reviews", date)).unionByName(
        conform_europe(bronze(spark, root, "europe_reviews", date)))
    hash_udf = F.udf(lambda v: hash_identifier(v, salt), T.StringType())

    reviews = (
        unioned.withColumn("_b", brand_udf("hotel_name"))
        .where(F.col("_b").isNotNull())  # keep Marriott International brands only
        .withColumn("brand", F.col("_b.brand")).withColumn("tier", F.col("_b.tier")).drop("_b")
        .where(F.col("review_text").isNotNull() & (F.length("review_text") > 0))
        .withColumn("review_text", mask_udf("review_text"))
        .withColumn("review_title", mask_udf("review_title"))
        .withColumn("reviewer_hash", hash_udf("reviewer")).drop("reviewer")
        .withColumn("review_id", F.sha2(F.concat_ws("|", "source", "hotel_name", "review_date", "review_text"), 256))
        .dropDuplicates(["review_id"])
        .withColumn("sentiment_compound", sentiment_udf("review_text"))
        .withColumn("sentiment_label",
                    F.when(F.col("sentiment_compound") >= 0.05, "positive")
                    .when(F.col("sentiment_compound") <= -0.05, "negative").otherwise("neutral"))
    )
    return link_properties(reviews, properties)


# ---------- financials ----------
def build_financials(spark, root, date) -> DataFrame:
    df = bronze(spark, root, "sec_financials", date).where(F.col("form").isin("10-K", "10-Q"))
    w = Window.partitionBy("concept", "unit", "period_start", "period_end").orderBy(F.col("filed").desc())
    return (
        df.withColumn("rn", F.row_number().over(w)).where("rn = 1")
        .select(
            "concept", "unit",
            F.col("value").cast("double").alias("value"),
            F.to_date("period_start").alias("period_start"),
            F.to_date("period_end").alias("period_end"),
            F.col("fiscal_year").cast("int").alias("fiscal_year"),
            "fiscal_period", "form",
            F.to_date("filed").alias("filed_date"),
            "frame", "accession",
        )
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--data-root", default=os.environ.get("DATA_ROOT", "./data"))
    p.add_argument("--pii-salt", default=os.environ.get("PII_SALT"))
    p.add_argument("--pii-salt-ssm-param", default=None, help="SSM SecureString name (AWS runs)")
    a, _ = p.parse_known_args()  # Glue injects extra args (--JOB_NAME etc.)
    a.pii_salt = resolve_pii_salt(a.pii_salt, a.pii_salt_ssm_param)
    root = a.data_root.rstrip("/")

    spark = SparkSession.builder.appName("marriott-bronze-to-silver").getOrCreate()
    properties = build_properties(spark, root, a.date).cache()
    properties.write.mode("overwrite").parquet(f"{root}/silver/properties")

    reviews = build_reviews(spark, root, a.date, properties, a.pii_salt)
    reviews.write.mode("overwrite").partitionBy("source").parquet(f"{root}/silver/reviews")

    build_financials(spark, root, a.date).write.mode("overwrite").parquet(f"{root}/silver/financials")
    log.info("silver complete: %s properties", properties.count())
    spark.stop()


if __name__ == "__main__":
    main()
