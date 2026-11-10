select
    property_id,
    property_name,
    brand,
    stars,
    rooms,
    website,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('properties_snapshot') }}
