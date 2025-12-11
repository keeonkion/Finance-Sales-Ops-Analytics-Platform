{{ config(materialized = 'table') }}

with pl as (
    -- 明细来源：一行 = 一个 GL 账户在某天某地区的一条 P&L 记录
    select
        date_id,
        year,
        month,
        month_name,
        region_id,       
        country_name,
        region_name,
        city_name,
        category,        -- Revenue / Cogs / Opex
        subcategory,
        amount
    from {{ ref('int_finance_pl_lines') }}
),

agg as (
    select
        date_trunc('month', to_date(date_id::text, 'YYYYMMDD')) as period_month,
        year,
        month,
        month_name,
        region_id,        
        country_name,
        region_name,
        city_name,

        -- 假设：Revenue 为正数，Cogs / Opex 为负数
        sum(case when category = 'Revenue' then amount else 0 end) as revenue_amount,
        sum(case when category = 'Cogs'    then amount else 0 end) as cogs_amount,
        sum(case when category = 'Opex'    then amount else 0 end) as opex_amount

    from pl
    group by
        date_trunc('month', to_date(date_id::text, 'YYYYMMDD')),
        year,
        month,
        month_name,
        region_id,        
        country_name,
        region_name,
        city_name
)

select
    period_month,
    year,
    month,
    month_name,
    region_id,
    country_name,
    region_name,
    city_name,

    revenue_amount,
    cogs_amount,
    opex_amount,

    -- 毛利 = 收入 + COGS（COGS 为负数时等于 Revenue - |COGS|）
    (revenue_amount + cogs_amount) as gross_profit_amount,

    case
        when nullif(revenue_amount, 0) is null then null
        else (revenue_amount + cogs_amount) / revenue_amount
    end as gross_margin_pct,

    -- 经营利润 = 毛利 + Opex（Opex 为负数时等于 Gross - |Opex|）
    (revenue_amount + cogs_amount + opex_amount) as operating_profit_amount,

    case
        when nullif(revenue_amount, 0) is null then null
        else (revenue_amount + cogs_amount + opex_amount) / revenue_amount
    end as operating_margin_pct

from agg