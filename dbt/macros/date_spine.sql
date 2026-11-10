{#
  One row per (key, day) between two date columns of a relation.
  Cross-database: DuckDB (local) and Athena/Trino (AWS) expand date ranges differently.
#}
{% macro per_key_date_spine(relation, key, start_col, end_col) %}
  {{ return(adapter.dispatch('per_key_date_spine')(relation, key, start_col, end_col)) }}
{% endmacro %}

{% macro duckdb__per_key_date_spine(relation, key, start_col, end_col) %}
    select {{ key }}, cast(unnest(generate_series({{ start_col }}, {{ end_col }}, interval 1 day)) as date) as spine_date
    from {{ relation }}
{% endmacro %}

{% macro default__per_key_date_spine(relation, key, start_col, end_col) %}
    select {{ key }}, d as spine_date
    from {{ relation }}
    cross join unnest(sequence({{ start_col }}, {{ end_col }}, interval '1' day)) as t(d)
{% endmacro %}
