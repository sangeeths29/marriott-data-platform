-- Golden property record: current OSM properties plus hotels seen only in reviews
-- (no OSM match within 250 m). record_source tells consumers which is which.
with osm as (
    select
        property_id, property_name, brand, tier, latitude, longitude,
        street_address, city, postcode, country_code, stars, rooms, website,
        'openstreetmap' as record_source
    from {{ ref('properties_snapshot') }}
    where dbt_valid_to is null
),

review_only as (
    select
        property_id,
        min(hotel_name)                as property_name,
        min(brand)                     as brand,
        min(tier)                      as tier,
        avg(latitude)                  as latitude,
        avg(longitude)                 as longitude,
        cast(null as varchar)          as street_address,
        min(city)                      as city,
        cast(null as varchar)          as postcode,
        min(country_code)              as country_code,
        cast(null as double)           as stars,
        cast(null as integer)          as rooms,
        cast(null as varchar)          as website,
        'reviews_only'                 as record_source
    from {{ ref('stg_reviews') }}
    where property_match_method = 'unmatched'
    group by property_id
)

select
    *,
    coalesce(rooms, {{ var('default_room_count') }}) as rooms_available  -- inventory for occupancy KPIs
from (
    select * from osm
    union all
    select * from review_only
) unioned
