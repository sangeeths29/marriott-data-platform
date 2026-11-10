select
    review_id,
    property_id,
    brand,
    source,
    review_date,
    country_code,
    reviewer_nationality,
    rating_5,
    sentiment_compound,
    sentiment_label,
    length(review_text) as review_length_chars,
    property_match_method
from {{ ref('stg_reviews') }}
