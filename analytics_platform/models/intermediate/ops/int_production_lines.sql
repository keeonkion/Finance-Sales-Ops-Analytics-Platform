{{ config(
    materialized = 'view'
) }}

with production as (
    select *
    from {{ ref('stg_production') }}
),

dim_date as (
    select *
    from {{ ref('stg_date') }}
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
    -- grain
    p.production_id           as production_id,
    p.date_id,
    p.product_id             as product_id,
    p.warehouse_id           as warehouse_id,

    -- date attributes
    d.full_date               as full_date,
    d.year                   as year,
    d.quarter                as quarter,
    d.month                  as month,
    d.month_name              as month_name,

    -- product attributes  （根据你的 DimProduct 字段命名，这里假设如下）
    pr.product_code           as product_code,
    pr.product_name           as product_name,
    pr.category              as category,
    pr.subcategory           as subcategory,

    -- warehouse / region
    w.warehouse_code          as warehouse_code,
    w.warehouse_name          as warehouse_name,
    r.country_code            as country_code,
    r.country_name            as country_name,
    r.region_name             as region_name,
    r.city_name               as city_name,

    -- metrics
    p.produced_qty            as produced_qty,
    p.scrap_qty               as scrap_qty,
    p.machine_hours           as machine_hours,
    p.downtime_hours          as downtime_hours

from production p
left join dim_date      d  on p.date_id      = d.date_id
left join dim_product   pr on p.product_id   = pr.product_id
left join dim_warehouse w  on p.warehouse_id = w.warehouse_id
left join dim_region    r  on w.region_id    = r.region_id