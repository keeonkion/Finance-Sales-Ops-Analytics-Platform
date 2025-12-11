{{ config(
    materialized = 'table'
) }}

with orders as (

    select
        o.order_id,
        o.order_date_id,
        o.product_id,
        o.region_id,
        o.shipped_qty
    from {{ ref('stg_orders') }} o
),

targets as (

    -- 销售目标来自 int_sales_target
    select
        t.date_id,
        t.product_id,
        t.region_id,
        t.target_revenue,
        t.target_quantity
    from {{ ref('stg_sales_target') }} t
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

product_dim as (
    select
        product_id,
        product_code,
        product_name,
        category,
        subcategory
    from {{ ref('stg_product') }}
),

actuals as (

    select
        o.order_date_id,
        o.product_id,
        o.region_id,
        sum(o.shipped_qty)                        as shipped_qty
        --sum(o.shipped_qty * p.unitprice)          as revenue_amount,
        --sum(o.shipped_qty * p.standardcost)       as cost_amount
    from orders o
    left join product_dim p 
        on o.product_id = p.product_id
    group by
        o.order_date_id,
        o.product_id,
        o.region_id
)

select
    d.year,
    d.month,
    d.month_name,

    r.region_id,
    r.country_code,
    r.country_name,
    r.region_name,
    r.city_name,

    p.product_id,
    p.product_code,
    p.product_name,
    p.category,
    p.subcategory,

    a.shipped_qty,
    --a.revenue_amount,
    --a.cost_amount,
    --(a.revenue_amount - a.cost_amount)          as gross_margin_amount,
    --case 
    --    when a.revenue_amount = 0 
    --        then 0
    --    else 1.0 * (a.revenue_amount - a.cost_amount)
    --         / a.revenue_amount
    --end                                         as gross_margin_pct,

    coalesce(t.target_revenue, 0)                as target_revenue,
    coalesce(t.target_quantity, 0)               as target_quantity,

    --case 
    --    when coalesce(t.target_revenue, 0) = 0 
    --        then 0
    --    else 1.0 * a.revenue_amount
    --         / t.targetrevenue
    --end                                         as revenue_attainment_pct,

    case 
        when coalesce(t.target_quantity, 0) = 0 
            then 0
        else 1.0 * a.shipped_qty
             / t.target_quantity
    end                                         as quantity_attainment_pct

from actuals a
left join targets    t on a.order_date_id    = t.date_id
                     and a.product_id = t.product_id
                     and a.region_id  = t.region_id
left join date_dim   d on a.order_date_id    = d.date_id
left join region_dim r on a.region_id  = r.region_id
left join product_dim p on a.product_id = p.product_id