select
    r.brand,
    b.tier,
    date_trunc('month', r.review_date)                                   as review_month,
    count(*)                                                             as review_count,
    count(distinct r.property_id)                                        as properties_reviewed,
    avg(r.rating_5)                                                      as avg_rating_5,
    avg(r.sentiment_compound)                                            as avg_sentiment,
    avg(case when r.sentiment_label = 'negative' then 1.0 else 0.0 end)  as pct_negative
from {{ ref('fct_review') }} r
left join {{ ref('dim_brand') }} b on r.brand = b.brand
group by r.brand, b.tier, date_trunc('month', r.review_date)
