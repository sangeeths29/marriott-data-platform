# ---------- Code artifacts ----------
# build/pipeline.zip is produced by `make package` (zip keeps the top-level pipeline/
# folder so `import pipeline.common...` works on Glue). Jenkins runs it before plan.
locals {
  pipeline_zip = "${path.module}/../build/pipeline.zip"
}

resource "aws_s3_object" "pipeline_zip" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue/pipeline-${filemd5(local.pipeline_zip)}.zip"
  source = local.pipeline_zip
  etag   = filemd5(local.pipeline_zip)
}

resource "aws_s3_object" "bronze_to_silver" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue/jobs/bronze_to_silver.py"
  source = "${path.module}/../pipeline/jobs/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../pipeline/jobs/bronze_to_silver.py")
}

# ---------- Secrets ----------
resource "random_password" "pii_salt" {
  length  = 48
  special = false
}

resource "aws_ssm_parameter" "pii_salt" {
  name  = "/${local.prefix}/pii-salt"
  type  = "SecureString"
  value = random_password.pii_salt.result
}

# ---------- Catalog ----------
resource "aws_glue_catalog_database" "silver" {
  name = "${local.db_prefix}_silver"
}

resource "aws_glue_catalog_database" "ops" {
  name = "${local.db_prefix}_ops" # pipeline run log + reject tables (Iceberg)
}

resource "aws_glue_catalog_database" "gold" {
  name = "${local.db_prefix}_gold" # dbt (Athena) writes here
}

# ---------- Transform job ----------
resource "aws_glue_job" "bronze_to_silver" {
  name              = "${local.prefix}-bronze-to-silver"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = 60
  max_retries       = 1

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.bronze_to_silver.key}"
  }

  default_arguments = {
    "--extra-py-files"                   = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.pipeline_zip.key}"
    "--additional-python-modules"        = "vaderSentiment==3.3.2"
    "--data-root"                        = "s3://${aws_s3_bucket.lake.id}"
    "--pii-salt-ssm-param"               = aws_ssm_parameter.pii_salt.name
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  execution_property { max_concurrent_runs = 1 }
}

# ---------- Incremental CDC merge into Iceberg (simulated reservation system feed) ----------
resource "aws_s3_object" "reservations_cdc_merge" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue/jobs/reservations_cdc_merge.py"
  source = "${path.module}/../pipeline/jobs/reservations_cdc_merge.py"
  etag   = filemd5("${path.module}/../pipeline/jobs/reservations_cdc_merge.py")
}

locals {
  # Glue only accepts one --conf key, so extra Spark confs are chained inside its value.
  iceberg_spark_conf = join(" --conf ", [
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lake.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.lake.warehouse=s3://${aws_s3_bucket.lake.id}/iceberg",
  ])
}

resource "aws_glue_job" "reservations_cdc_merge" {
  name              = "${local.prefix}-reservations-cdc-merge"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = 60
  max_retries       = 0 # Airflow owns retries; the job is idempotent via its watermark

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.reservations_cdc_merge.key}"
  }

  default_arguments = {
    "--extra-py-files"                   = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.pipeline_zip.key}"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = local.iceberg_spark_conf
    "--data-root"                        = "s3://${aws_s3_bucket.lake.id}"
    "--iceberg-namespace-prefix"         = local.db_prefix
    "--pii-salt-ssm-param"               = aws_ssm_parameter.pii_salt.name
    "--max-reject-rate"                  = "0.01"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-observability-metrics"     = "true"
  }

  execution_property { max_concurrent_runs = 1 } # the watermark assumes one writer
}

# ---------- Crawler: registers silver tables for Athena, runs after a successful job ----------
resource "aws_glue_crawler" "silver" {
  name          = "${local.prefix}-silver"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.silver.name

  dynamic "s3_target" {
    for_each = ["properties", "reviews", "financials"]
    content { path = "s3://${aws_s3_bucket.lake.id}/silver/${s3_target.value}/" }
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}

resource "aws_glue_trigger" "crawl_after_job" {
  name = "${local.prefix}-crawl-after-silver"
  type = "CONDITIONAL"

  predicate {
    conditions {
      job_name = aws_glue_job.bronze_to_silver.name
      state    = "SUCCEEDED"
    }
  }

  actions { crawler_name = aws_glue_crawler.silver.name }
}
