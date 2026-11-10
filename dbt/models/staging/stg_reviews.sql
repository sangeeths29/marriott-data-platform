select
    review_id,
    source,
    property_id,
    property_match_method,
    property_match_distance_km,
    hotel_name,
    brand,
    tier,
    city,
    country_code,
    latitude,
    longitude,
    review_date,
    rating_5,
    review_title,
    review_text,
    reviewer_hash,
    reviewer_nationality,
    sentiment_compound,
    sentiment_label
from {{ source('silver', 'reviews') }}
where review_date is not null
