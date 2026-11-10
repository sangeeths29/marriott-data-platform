-- Property x stay date KPIs: occupancy, ADR, RevPAR. Includes zero-sale days (date spine)
-- and future dates already booked ("on the books"). SIMULATED source data.
with nights as (
    select
        n.property_id,
        n.stay_date,
        sum(n.room_nights)                         as rooms_sold,
        sum(n.room_revenue_local * fx.usd_per_unit) as room_revenue_usd
    from {{ ref('stg_reservation_nights') }} n
    left join {{ ref('fx_rates') }} fx on n.currency = fx.currency
    group by n.property_id, n.stay_date
),

bounds as (
    select property_id, min(stay_date) as d0, max(stay_date) as d1
    from nights
    group by property_id
),

spine as (
    {{ per_key_date_spine('bounds', 'property_id', 'd0', 'd1') }}
),

business_date as (
    -- last day the source system reported activity
    select max(cast(updated_at as date)) as bd from {{ ref('stg_reservations') }}
)

select
    s.property_id,
    s.spine_date                                                          as stay_date,
    p.brand,
    p.tier,
    p.country_code,
    p.rooms_available,
    coalesce(n.rooms_sold, 0)                                             as rooms_sold,
    coalesce(n.room_revenue_usd, 0)                                       as room_revenue_usd,
    cast(coalesce(n.rooms_sold, 0) as double) / p.rooms_available         as occupancy,
    n.room_revenue_usd / nullif(n.rooms_sold, 0)                          as adr_usd,
    coalesce(n.room_revenue_usd, 0) / p.rooms_available                   as revpar_usd,
    s.spine_date < b.bd                                                   as is_actualized,
    s.spine_date >= b.bd                                                  as is_on_the_books
from spine s
cross join business_date b
inner join {{ ref('dim_property') }} p on s.property_id = p.property_id
left join nights n on s.property_id = n.property_id and s.spine_date = n.stay_date
