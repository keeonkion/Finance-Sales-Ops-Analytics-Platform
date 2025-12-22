{{ config(materialized='table') }}

with orders as (
    select
        order_date_id,
        product_id,
        region_id,
        shipped_qty
    from {{ ref('stg_orders') }}
),

date_dim as (
    select
        date_id,
        year,
        month,
        month_name
    from {{ ref('stg_date') }}
),

-- ✅ 月粒度 actuals：year/month + region + product
actuals_month as (
    select
        d.year,
        d.month,
        d.month_name,
        o.region_id,
        o.product_id,
        sum(o.shipped_qty) as shipped_qty
    from orders o
    left join date_dim d
        on o.order_date_id = d.date_id
    group by
        d.year, d.month, d.month_name,
        o.region_id, o.product_id
),

targets as (
    select
        date_id,
        product_id,
        region_id,
        target_revenue,
        target_quantity
    from {{ ref('stg_sales_target') }}
),

-- ✅ 月粒度 targets：同样汇总到 year/month + region + product
targets_month as (
    select
        d.year,
        d.month,
        d.month_name,
        t.region_id,
        t.product_id,
        sum(t.target_revenue)  as target_revenue,
        sum(t.target_quantity) as target_quantity
    from targets t
    left join date_dim d
        on t.date_id = d.date_id
    group by
        d.year, d.month, d.month_name,
        t.region_id, t.product_id
),

region_dim as (
    select
        region_id,
        min(country_code) as country_code,
        min(country_name) as country_name,
        min(region_name)  as region_name,
        min(city_name)    as city_name
    from {{ ref('stg_region') }}
    group by region_id
),

product_dim as (
    select
        product_id,
        min(product_code) as product_code,
        min(product_name) as product_name,
        min(category)     as category,
        min(subcategory)  as subcategory
    from {{ ref('stg_product') }}
    group by product_id
)

select
    a.year,
    a.month,
    a.month_name,

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

    coalesce(t.target_revenue, 0)  as target_revenue,
    coalesce(t.target_quantity, 0) as target_quantity,

    case
        when coalesce(t.target_quantity, 0) = 0 then 0
        else 1.0 * a.shipped_qty / t.target_quantity
    end as quantity_attainment_pct

from actuals_month a
left join targets_month t
    on  a.year = t.year
    and a.month = t.month
    and a.region_id = t.region_id
    and a.product_id = t.product_id
left join region_dim r
    on a.region_id = r.region_id
left join product_dim p
    on a.product_id = p.product_id
