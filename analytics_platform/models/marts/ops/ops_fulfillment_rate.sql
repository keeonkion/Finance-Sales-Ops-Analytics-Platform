{{ config(materialized = 'table') }}

with o as (
    select
        order_id,
        order_line_number,

        order_date_id,
        date_trunc('month', to_date(order_date_id::text, 'YYYYMMDD')) as period_month,        
        extract(year  from to_date(order_date_id::text, 'YYYYMMDD'))::int  as year,
        extract(month from to_date(order_date_id::text, 'YYYYMMDD'))::int  as month,

        region_id,
        country_code,
        country_name,
        region_name,
        warehouse_code,
        warehouse_name,

        product_id,
        product_code,
        product_name,
        category,
        subcategory,

        ordered_qty,
        shipped_qty,
        cancelled_qty,

        is_on_time,    -- 注意：如果你字段叫 isontime，就改成 isontime
        is_in_full     -- 如果叫 isinfull，也相应改名字
    from {{ ref('int_orders_lines') }}
),

agg as (
    select
        period_month,
        year,
        month,

        region_id,
        country_code,
        country_name,
        region_name,
        warehouse_code,
        warehouse_name,

        count(distinct order_id)                    as order_cnt,
        count(*)                                    as order_line_cnt,

        sum(ordered_qty)                            as ordered_qty,
        sum(shipped_qty)                            as shipped_qty,
        sum(cancelled_qty)                          as cancelled_qty,

        -- On-time / In-full / OTIF 以「行」为单位
        sum(case when is_on_time then 1 else 0 end)                 as on_time_lines,
        sum(case when is_in_full then 1 else 0 end)                 as in_full_lines,
        sum(case when is_on_time and is_in_full then 1 else 0 end)  as otif_lines
    from o
    group by
        period_month,
        year,
        month,
        region_id,
        country_code,
        country_name,
        region_name,
        warehouse_code,
        warehouse_name
)

select
    period_month,
    year,
    month,

    region_id,
    country_code,
    country_name,
    region_name,
    warehouse_code,
    warehouse_name,

    order_cnt,
    order_line_cnt,

    ordered_qty,
    shipped_qty,
    cancelled_qty,

    -- 履约率指标
    case when order_line_cnt = 0 then null
         else on_time_lines::numeric / order_line_cnt end as on_time_rate,

    case when order_line_cnt = 0 then null
         else in_full_lines::numeric / order_line_cnt end as in_full_rate,

    case when order_line_cnt = 0 then null
         else otif_lines::numeric / order_line_cnt end  as otif_rate,

    case when ordered_qty = 0 then null
         else shipped_qty::numeric / ordered_qty end    as ship_fill_rate,

    case when ordered_qty = 0 then null
         else cancelled_qty::numeric / ordered_qty end  as cancel_rate
from agg