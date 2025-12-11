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
        date_id,
        year,
        month,
        month_name
    from {{ ref('stg_date') }}
),

region_dim as (
    select
        region_id,
        country_code,
        country_name,
        region_name,
        city_name
    from {{ ref('stg_region') }}
),

customer_dim as (
    select
        c.customer_id,
        c.customer_code,
        c.customer_name,
        c.region_key,
        c.is_active
    from {{ ref('stg_customer') }} c
),

product_dim as (
    select
        product_id,
        product_code,
        product_name
    from {{ ref('stg_product') }}
)

select
    c.customer_id,
    c.customer_code,
    c.customer_name,
    c.is_active,

    r.region_id,
    r.country_code,
    r.country_name,
    r.region_name,
    r.city_name,

    d.year,
    d.month,
    d.month_name,

    -- 订单与数量
    count(distinct o.order_id)                      as order_count,
    sum(o.shipped_qty)                               as shipped_qty,
    sum(o.cancelled_qty)                             as cancelled_qty,

    -- Revenue & AOV
    --sum(o.shipped_qty * p.unitprice)                 as revenue_amount,
    --case 
    --    when count(distinct o.order_id) = 0 
    --        then 0
    --    else 1.0 * sum(o.shipped_qty * p.unitprice)
    --         / count(distinct o.order_id)
    --end                                             as avg_order_value,

    -- OTIF 指标
    sum(case when o.is_on_time then 1 else 0 end)     as on_time_orders,
    sum(case when o.is_in_full then 1 else 0 end)     as in_full_orders,
    sum(case when o.is_on_time and o.is_in_full then 1 else 0 end) as otif_orders,

    case 
        when count(distinct o.order_id) = 0 
            then 0
        else 1.0 * sum(case when o.is_on_time and o.is_in_full then 1 else 0 end)
             / count(distinct o.order_id)
    end                                             as otif_rate,

    -- 取消率（按数量）
    case 
        when coalesce(sum(o.ordered_qty), 0) = 0 
            then 0
        else 1.0 * sum(o.cancelled_qty)
             / sum(o.ordered_qty)
    end                                             as cancel_rate

from orders o
left join customer_dim c on o.customer_id = c.customer_id
left join region_dim   r on c.region_key = r.region_id
left join date_dim     d on o.order_date_id  = d.date_id
left join product_dim  p on o.product_id = p.product_id
group by
    c.customer_id, c.customer_code, c.customer_name, c.is_active,
    r.region_id, r.country_code, r.country_name, r.region_name, r.city_name,
    d.year, d.month, d.month_name