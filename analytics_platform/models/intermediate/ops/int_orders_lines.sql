{{ config(
    materialized = 'view'
) }}

with orders as (
    select *
    from {{ ref('stg_orders') }}
),

dim_date as (
    select *
    from {{ ref('stg_date') }}
),

dim_customer as (
    select *
    from {{ ref('stg_customer') }}
),

dim_product as (
    select *
    from {{ ref('stg_product') }}
),

dim_warehouse as (
    select *
    from {{ ref('stg_warehouse') }}
),

dim_region as (
    select *
    from {{ ref('stg_region') }}
)

select
    -- grain: one row per order line
    o.order_id                 as order_id,
    o.order_number             as order_number,
    o.order_line_number        as order_line_number,
    o.order_date_id            as order_date_id,
    o.customer_id             as customer_id,
    o.product_id              as product_id,
    o.warehouse_id            as warehouse_id,
    o.region_id               as region_id,

    -- order date attributes
    d.full_date                as full_date,
    d.year                    as year,
    d.quarter                 as quarter,
    d.month                   as month,
    d.month_name               as month_name,

    -- customer attributes
    c.customer_code            as customer_code,
    c.customer_name            as customer_name,
    c.customer_type            as customer_type,

    -- product attributes
    p.product_code             as product_code,
    p.product_name             as product_name,
    p.category                as category,
    p.subcategory             as subcategory,

    -- warehouse / region
    w.warehouse_code           as warehouse_code,
    w.warehouse_name           as warehouse_name,
    r.country_code             as country_code,
    r.country_name             as country_name,
    r.region_name              as region_name,
    r.city_name                as city_name,

    -- fulfillment dates & KPIs
    o.requested_delivery_date   as requested_delivery_date,
    o.promised_delivery_date    as promised_delivery_date,
    o.actual_ship_date          as actual_ship_date,

    o.ordered_qty              as ordered_qty,
    o.shipped_qty              as shipped_qty,
    o.cancelled_qty            as cancelled_qty,

    o.is_on_time                as is_on_time,
    o.is_in_full                as is_in_full

from orders o
left join dim_date      d  on o.order_date_id = d.date_id
left join dim_customer  c  on o.customer_id  = c.customer_id
left join dim_product   p  on o.product_id   = p.product_id
left join dim_warehouse w  on o.warehouse_id = w.warehouse_id
left join dim_region    r  on o.region_id    = r.region_id