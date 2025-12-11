{{ config(materialized = 'table') }}

with pl as (
    select
        year,
        region_id,      
        country_name,
        region_name,
        city_name,
        category,
        amount
    from {{ ref('int_finance_pl_lines') }}
),

agg as (
    select
        year,
        region_id,       
        country_name,
        region_name,
        city_name,

        sum(case when category = 'Revenue' then amount else 0 end) as revenue_amount,
        sum(case when category = 'Cogs'    then amount else 0 end) as cogs_amount,
        sum(case when category = 'Opex'    then amount else 0 end) as opex_amount

    from pl
    group by
        year,
        region_id,      
        country_name,
        region_name,
        city_name
)

select
    year,
    region_id,   
    country_name,
    region_name,
    city_name,

    revenue_amount,
    cogs_amount,
    opex_amount,

    (revenue_amount + cogs_amount) as gross_profit_amount,

    case
        when nullif(revenue_amount, 0) is null then null
        else (revenue_amount + cogs_amount) / revenue_amount
    end as gross_margin_pct,

    (revenue_amount + cogs_amount + opex_amount) as net_profit_amount,

    case
        when nullif(revenue_amount, 0) is null then null
        else (revenue_amount + cogs_amount + opex_amount) / revenue_amount
    end as net_margin_pct

from agg