-- Revenue-management view: occupancy already on the books for the next 90 days, by week.
with otb as (
    select f.*, b.bd
    from {{ ref('fct_property_daily') }} f
    cross join (select min(stay_date) as bd from {{ ref('fct_property_daily') }} where is_on_the_books) b
    where f.is_on_the_books
)

select
    property_id,
    brand,
    date_trunc('week', stay_date)                          as stay_week,
    sum(rooms_sold)                                        as rooms_on_the_books,
    sum(rooms_available)                                   as rooms_available,
    cast(sum(rooms_sold) as double) / sum(rooms_available) as otb_occupancy,
    sum(room_revenue_usd)                                  as revenue_on_the_books_usd
from otb
where stay_date < bd + interval '90' day
group by 1, 2, 3
