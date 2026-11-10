-- Quarterly duration facts only: XBRL frames like CY2023Q1 (instant facts end in 'I').
select
    concept,
    unit,
    value,
    period_start,
    period_end,
    cast(substr(frame, 3, 4) as integer) as calendar_year,
    cast(substr(frame, 8, 1) as integer) as calendar_quarter,
    form,
    filed_date
from {{ source('silver', 'financials') }}
where frame like 'CY%Q_'
  and length(frame) = 8
