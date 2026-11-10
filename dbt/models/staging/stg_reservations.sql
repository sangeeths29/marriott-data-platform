-- SIMULATED reservation data (see pipeline/simulator/crs.py)
select
    reservation_id,
    confirmation_number,
    property_id,
    guest_key,
    loyalty_tier,
    booking_channel,
    rate_plan,
    room_type,
    booking_ts,
    arrival_date,
    departure_date,
    nights,
    rooms,
    adults,
    currency,
    cast(adr_local as double)          as adr_local,
    cast(total_amount_local as double) as total_amount_local,
    status,
    cancel_ts,
    updated_at,
    _cdc_seq                           as cdc_seq
from {{ source('silver_iceberg', 'reservations') }}
