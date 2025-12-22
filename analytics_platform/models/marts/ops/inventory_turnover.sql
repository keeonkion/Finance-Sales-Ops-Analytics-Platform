{{ config(materialized = 'table') }}

-- 每月 / 地区平均库存金额
with inv as (
    select
        date_id,      
        date_trunc('month', to_date(date_id::text, 'YYYYMMDD')) as period_month,        
        extract(year  from to_date(date_id::text, 'YYYYMMDD'))::int  as year,
        extract(month from to_date(date_id::text, 'YYYYMMDD'))::int  as month,

        --region_id,
        --country_code,
        country_name,
        region_name,

        inventory_value
    from {{ ref('int_inventory_snapshot') }}
),

avg_inv as (
    select
        period_month,
        year,
        month,
        --region_id,
        --country_code,
        country_name,
        region_name,

        avg(inventory_value) as avg_inventory_value
    from inv
    group by
        period_month,
        year,
        month,
        --region_id,
        --country_code,
        country_name,
        region_name
),

-- 每月 / 地区 COGS（金额为负时用绝对值）
cogs as (
    select      
        date_trunc('month', to_date(date_id::text, 'YYYYMMDD')) as period_month,
        extract(year  from to_date(date_id::text, 'YYYYMMDD'))::int  as year,
        extract(month from to_date(date_id::text, 'YYYYMMDD'))::int  as month,

        --region_id,
        --country_code,
        country_name,
        region_name,

        sum(case when category = 'Cogs' then amount else 0 end) as cogs_amount
    from {{ ref('int_finance_pl_lines') }}
    group by
        date_trunc('month', to_date(date_id::text, 'YYYYMMDD')),
        extract(year  from to_date(date_id::text, 'YYYYMMDD')),
        extract(month from to_date(date_id::text, 'YYYYMMDD')),
        --region_id,
        --country_code,
        country_name,
        region_name
)

select
    i.period_month,
    i.year,
    i.month,

    --i.region_id,
    --i.country_code,
    i.country_name,
    i.region_name,

    i.avg_inventory_value,
    abs(c.cogs_amount) as cogs_amount,

    case
        when nullif(i.avg_inventory_value, 0) is null then null
        else abs(c.cogs_amount) / i.avg_inventory_value
    end as inventory_turnover_ratio,          -- 周转次数（每月）

    case
        when
            case
                when nullif(i.avg_inventory_value, 0) is null then null
                else abs(c.cogs_amount) / i.avg_inventory_value
            end = 0
        then null
        else 30
             / (
                 abs(c.cogs_amount) / nullif(i.avg_inventory_value, 0)
               )
    end as days_on_hand_approx                 -- 大约有多少天库存
from avg_inv i
left join cogs c
    on  i.period_month = c.period_month
    --and i.region_id    = c.region_id
    --and i.country_code = c.country_code
    and i.country_name = c.country_name
    and i.region_name = c.region_name