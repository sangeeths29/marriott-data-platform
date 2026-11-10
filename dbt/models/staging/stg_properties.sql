select
    property_id,
    property_name,
    brand,
    tier,
    brand_wikidata,
    latitude,
    longitude,
    nullif(trim(street_address), '') as street_address,
    city,
    postcode,
    country_code,
    stars,
    rooms,
    website
from {{ source('silver', 'properties') }}
