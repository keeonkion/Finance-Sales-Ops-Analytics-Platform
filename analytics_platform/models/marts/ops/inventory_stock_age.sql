{{ config(materialized = 'table') }}

with inv as (
    -- 每天/每月底的库存快照
    select
        date_id,                  -- 快照日期
        date_trunc('month', to_date(date_id::text, 'YYYYMMDD')) as period_month,        
        extract(year  from to_date(date_id::text, 'YYYYMMDD'))::int  as year,
        extract(month from to_date(date_id::text, 'YYYYMMDD'))::int  as month,

        --region_id,
        --country_code,
        country_name,
        region_name,
        warehouse_id,
        warehouse_name,

        product_id,
        --product_code,
        --product_name,
        --category,
        --subcategory,

        closing_qty as on_hand_qty,        -- 当前结存数量
        inventory_value,    -- 当前库存金额（如果你没有金额，就删掉这个字段相关的逻辑）
        average_age_days as stock_age_days      -- 这行库存的平均在库天数（int_inventory_snapshot 里算好的）
    from {{ ref('int_inventory_snapshot') }}
),

agg as (
    select
        period_month,
        year,
        month,

        --region_id,
        --country_code,
        country_name,
        region_name,
        warehouse_id,
        warehouse_name,

        product_id,
       -- product_code,
        --product_name,
        --category,
        --subcategory,

        -- 期末库存数量 & 金额
        sum(on_hand_qty)     as ending_on_hand_qty,
        sum(inventory_value) as ending_inventory_value,

        -- 库存天数：用数量加权平均
        case
            when sum(on_hand_qty) = 0 then null
            else sum(stock_age_days * on_hand_qty) / sum(on_hand_qty)
        end as avg_stock_age_days
    from inv
    group by
        period_month, year, month,
        --region_id, country_code, 
        country_name, region_name,
        warehouse_id, warehouse_name,
        product_id 
        --product_code, product_name,
        --category, subcategory
)

select *
from agg