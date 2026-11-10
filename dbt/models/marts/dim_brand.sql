select
    brand_name as brand,
    tier
from {{ ref('brands') }}
