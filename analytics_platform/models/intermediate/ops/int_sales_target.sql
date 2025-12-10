{{ config(
    materialized = 'view'
) }}

with sales_target as (
    select *
    from {{ ref('stg_sales_target') }}
),

dim_date as (
    select *
    from {{ ref('stg_date') }}
),

dim_region as (
    select *
    from {{ ref('stg_region') }}
),

dim_sales_rep as (
    select *
    from {{ ref('stg_sales_rep') }}
),

dim_product as (
    select *
    from {{ ref('stg_product') }}
)

select
    -- grain
    t.sales_target_id           as sales_target_id,
    t.date_id                 as date_id,
    t.region_id               as region_id,
    t.sales_rep_id             as sales_rep_id,
    t.product_id              as product_id,

    -- date
    d.full_date                as full_date,
    d.year                    as year,
    d.quarter                 as quarter,
    d.month                   as month,
    d.month_name               as month_name,

    -- region
    r.country_code             as country_code,
    r.country_name             as country_name,
    r.region_name              as region_name,
    r.city_name                as city_name,

    -- sales rep
    s.employee_code            as employee_code,
    s.full_name                as full_name,
    s.email                   as sales_rep_email,

    -- product
    p.product_code             as product_code,
    p.product_name             as product_name,
    p.category                as category,
    p.subcategory             as subcategory,

    -- target metrics
    t.target_revenue           as target_revenue,
    t.target_quantity          as target_quantity

from sales_target t
left join dim_date      d on t.date_id     = d.date_id
left join dim_region    r on t.region_id   = r.region_id
left join dim_sales_rep s on t.sales_rep_id = s.sales_rep_id
left join dim_product   p on t.product_id  = p.product_id