{% snapshot properties_snapshot %}
{#
  SCD Type 2 history of the property master. Each daily run compares tracked
  columns; a change (rebrand, room count update, new website) closes the old row
  (dbt_valid_to) and opens a new one. Feeds dim_property_history.
#}
{{
    config(
        target_schema='snapshots',
        unique_key='property_id',
        strategy='check',
        check_cols=['property_name', 'brand', 'street_address', 'stars', 'rooms', 'website'],
        invalidate_hard_deletes=True
    )
}}
select * from {{ ref('stg_properties') }}
{% endsnapshot %}
