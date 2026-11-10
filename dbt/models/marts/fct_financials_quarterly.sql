-- Marriott International quarterly financials (USD), one row per calendar quarter.
with f as (
    select * from {{ ref('stg_financials') }}
)

select
    calendar_year,
    calendar_quarter,
    coalesce(
        max(case when concept = 'Revenues' then value end),
        max(case when concept = 'RevenueFromContractWithCustomerExcludingAssessedTax' then value end)
    )                                                                       as revenue_usd,
    max(case when concept = 'OperatingIncomeLoss' then value end)          as operating_income_usd,
    max(case when concept = 'NetIncomeLoss' then value end)                as net_income_usd,
    max(case when concept = 'EarningsPerShareDiluted' then value end)      as eps_diluted
from f
where unit in ('USD', 'USD/shares')
group by calendar_year, calendar_quarter
