{{ config(materialized = 'table') }}

with pl as (
    select
        date_id,
        year,
        month,
        month_name,
        region_id,       
        country_name,
        region_name,
        city_name,
        category,
        subcategory,
        amount
    from {{ ref('int_finance_pl_lines') }}
),

-- 先按期间 & 地区算收入，用来做分母
revenue_per_period_region as (
    select
        date_trunc('month', to_date(date_id::text, 'YYYYMMDD')) as period_month,
        year,
        month,
        month_name,
        region_id,       
        country_name,
        region_name,
        city_name,
        sum(case when category = 'Revenue' then amount else 0 end) as revenue_amount
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
),

-- 再把 COGS + Opex 分解到类别
costs as (
    select
        date_trunc('month', to_date(date_id::text, 'YYYYMMDD')) as period_month,
        year,
        month,
        month_name,
        region_id,       
        country_name,
        region_name,
        city_name,
        category,      -- Cogs / Opex
        subcategory,   -- Snack / Phone / Skincare ... 取决于 GL 设计
        sum(amount) as cost_amount
    from pl
    where category in ('Cogs', 'Opex')
    group by
        date_trunc('month', to_date(date_id::text, 'YYYYMMDD')),
        year,
        month,
        month_name,
        region_id,        
        country_name,
        region_name,
        city_name,
        category,
        subcategory
)

select
    c.period_month,
    c.year,
    c.month,
    c.month_name,
    c.region_id,   
    c.country_name,
    c.region_name,
    c.city_name,

    c.category        as cost_category,
    c.subcategory     as cost_subcategory,
    c.cost_amount,

    r.revenue_amount,

    case
        when nullif(r.revenue_amount, 0) is null then null
        else c.cost_amount / r.revenue_amount
    end as cost_pct_of_revenue

from costs c
left join revenue_per_period_region r
    on  c.period_month  = r.period_month
    and c.region_id     = r.region_id
    and c.country_name  = r.country_name
    and c.region_name  = r.region_name
    and c.city_name     = r.city_name