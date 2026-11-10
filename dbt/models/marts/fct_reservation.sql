-- One row per reservation (current state). SIMULATED source data.
with r as (
    select * from {{ ref('stg_reservations') }}
),

fx as (
    select * from {{ ref('fx_rates') }}
)

select
    r.reservation_id,
    r.property_id,
    p.brand,
    r.guest_key,
    r.loyalty_tier,
    r.booking_channel,
    r.booking_channel in ('BRAND_WEB', 'MOBILE_APP', 'VOICE')       as is_direct_booking,
    r.rate_plan,
    r.room_type,
    cast(r.booking_ts as date)                                       as booking_date,
    r.arrival_date,
    r.departure_date,
    date_diff('day', cast(r.booking_ts as date), r.arrival_date)    as lead_time_days,
    r.nights,
    r.rooms,
    r.nights * r.rooms                                               as room_nights,
    r.status,
    r.status = 'CANCELLED'                                           as is_cancelled,
    r.status = 'NO_SHOW'                                             as is_no_show,
    r.currency,
    r.adr_local,
    r.adr_local * fx.usd_per_unit                                    as adr_usd,
    r.total_amount_local * fx.usd_per_unit                           as total_amount_usd,
    r.updated_at
from r
left join fx on r.currency = fx.currency
left join {{ ref('dim_property') }} p on r.property_id = p.property_id
