-- Generic words ("Courtyard", "Renaissance", "Element") can match non-Marriott hotels.
-- Warn when, for those brands, most review hotels without "marriott" in the name
-- also failed to match an OSM Marriott property. Investigate and tighten brands.py.
{{ config(severity='warn') }}

with hotels as (
    select distinct brand, hotel_name, property_match_method
    from {{ ref('stg_reviews') }}
    where brand in ('Courtyard', 'Renaissance', 'Element', 'Delta Hotels')
      and lower(hotel_name) not like '%marriott%'
)

select
    brand,
    count(*) as hotels,
    avg(case when property_match_method = 'unmatched' then 1.0 else 0.0 end) as unmatched_rate
from hotels
group by brand
having avg(case when property_match_method = 'unmatched' then 1.0 else 0.0 end) > 0.5
