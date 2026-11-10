-- The analysis question: does guest sentiment move with Marriott's reported results?
with sentiment as (
    select
        extract(year from review_date)    as calendar_year,
        extract(quarter from review_date) as calendar_quarter,
        count(*)                          as review_count,
        avg(rating_5)                     as avg_rating_5,
        avg(sentiment_compound)           as avg_sentiment
    from {{ ref('fct_review') }}
    group by 1, 2
),

fin as (
    select
        *,
        revenue_usd / nullif(lag(revenue_usd, 4) over (order by calendar_year, calendar_quarter), 0) - 1
            as revenue_yoy_growth
    from {{ ref('fct_financials_quarterly') }}
)

select
    s.calendar_year,
    s.calendar_quarter,
    s.review_count,
    s.avg_rating_5,
    s.avg_sentiment,
    f.revenue_usd,
    f.revenue_yoy_growth,
    f.operating_income_usd
from sentiment s
inner join fin f
    on s.calendar_year = f.calendar_year
   and s.calendar_quarter = f.calendar_quarter
