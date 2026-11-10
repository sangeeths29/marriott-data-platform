select
    reservation_id,
    property_id,
    stay_date,
    booking_channel,
    rate_plan,
    room_type,
    loyalty_tier,
    currency,
    room_nights,
    cast(room_revenue_local as double) as room_revenue_local
from {{ source('silver_iceberg', 'reservation_nights') }}
