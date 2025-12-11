{{ config(
    materialized = 'table'
) }}

with orders as (

    select
        o.order_id,             -- 来自 int_sales_order_lines 的主键（如果叫 orderid 就改成那个）
        o.order_number,
        o.order_date_id,
        o.customer_id,
        o.product_id,
        o.region_id,
        o.ordered_qty,
        o.shipped_qty,
        o.cancelled_qty,
        o.is_on_time,
        o.is_in_full
    from {{ ref('stg_orders') }} o
),

date_dim as (
    select
        d.date_id,
        d.full_date,
        d.year,
        d.month,
        d.month_name,
        d.day_of_week,
        d.day_name,
        d.is_weekday
    from {{ ref('stg_date') }} d
),

region_dim as (
    select
        r.region_id,
        r.country_code,
        r.country_name,
        r.region_name,
        r.city_name
    from {{ ref('stg_region') }} r
),

product_dim as (
    select
        p.product_id,
        p.product_code,
        p.product_name,
        p.category,
        p.subcategory
    from {{ ref('stg_product') }} p
)

select
    d.date_id,
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    r.region_id,
    r.country_code,
    r.country_name,
    r.region_name,
    r.city_name,

    -- 核心 volume 指标
    count(distinct o.order_id)                      as order_count,
    sum(o.ordered_qty)                               as ordered_qty,
    sum(o.shipped_qty)                               as shipped_qty,
    sum(o.cancelled_qty)                             as cancelled_qty,

    -- Revenue：简单按 shippedqty * unitprice 算（之后有更精细价格模型可以再替换）
    --sum(o.shippedqty * p.unitprice)                 as revenue_amount,

    -- 服务表现指标
    sum(case when o.is_on_time  then 1 else 0 end)    as on_time_orders,
    sum(case when o.is_in_full then 1 else 0 end)     as in_full_orders,
    sum(case when o.is_on_time and o.is_in_full then 1 else 0 end) as otif_orders,

    case 
        when count(distinct o.order_id) = 0 
            then 0
        else 1.0 * sum(case when o.is_on_time then 1 else 0 end)
             / count(distinct o.order_id)
    end                                             as on_time_rate,

    case 
        when count(distinct o.order_id) = 0 
            then 0
        else 1.0 * sum(case when o.is_in_full then 1 else 0 end)
             / count(distinct o.order_id)
    end                                             as in_full_rate,

    case 
        when count(distinct o.order_id) = 0 
            then 0
        else 1.0 * sum(case when o.is_on_time and o.is_in_full then 1 else 0 end)
             / count(distinct o.order_id)
    end                                             as otif_rate

from orders o
left join date_dim   d on o.order_date_id   = d.date_id
left join region_dim r on o.region_id = r.region_id
left join product_dim p on o.product_id = p.product_id
group by
    d.date_id, d.full_date, d.year, d.month, d.month_name,
    r.region_id, r.country_code, r.country_name, r.region_name, r.city_name