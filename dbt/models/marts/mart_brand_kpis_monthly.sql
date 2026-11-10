-- Brand x month performance on actualized (past) stay dates. SIMULATED source data.
with daily as (
    select
        brand,
        tier,
        date_trunc('month', stay_date) as stay_month,
        sum(rooms_sold)                as rooms_sold,
        sum(rooms_available)           as rooms_available,
        sum(room_revenue_usd)          as room_revenue_usd
    from {{ ref('fct_property_daily') }}
    where is_actualized
    group by 1, 2, 3
),

bookings as (
    select
        brand,
        date_trunc('month', arrival_date)                                   as stay_month,
        count(*)                                                            as reservations,
        avg(case when is_cancelled then 1.0 else 0.0 end)                   as cancellation_rate,
        avg(case when is_direct_booking then 1.0 else 0.0 end)              as direct_booking_share,
        avg(lead_time_days)                                                 as avg_lead_time_days
    from {{ ref('fct_reservation') }}
    group by 1, 2
)

select
    d.brand,
    d.tier,
    d.stay_month,
    d.rooms_sold,
    d.rooms_available,
    d.room_revenue_usd,
    cast(d.rooms_sold as double) / d.rooms_available   as occupancy,
    d.room_revenue_usd / nullif(d.rooms_sold, 0)       as adr_usd,
    d.room_revenue_usd / d.rooms_available             as revpar_usd,
    b.reservations,
    b.cancellation_rate,
    b.direct_booking_share,
    b.avg_lead_time_days
from daily d
left join bookings b on d.brand = b.brand and d.stay_month = b.stay_month
