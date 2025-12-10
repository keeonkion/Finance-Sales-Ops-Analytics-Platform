{{ config(
    materialized = 'view'
) }}

with sales as (
    select *
    from {{ ref('stg_sales') }}
),

dim_date as (
    select date_id, year, month, month_name, quarter
    from {{ ref('stg_date') }}
),

dim_region as (
    select region_id, country_code, country_name, region_name, city_name
    from {{ ref('stg_region') }}
),

dim_warehouse as (
    select warehouse_id, warehouse_name, region_id
    from {{ ref('stg_warehouse') }}
),

dim_sales_rep as (
    select sales_rep_id, full_name, region_id 
    from {{ ref('stg_sales_rep') }}
)

select
    -- 粒度键
    s.sales_id,
    s.order_date,
    s.customer_id,
    s.product_id,
    s.region_id,
    s.salesrep_id,
    s.warehouse_id,

    -- 业务度量
    s.quantity,
    s.list_price,
    s.discount_amount,
    s.net_sales,
    s.cogs,
    s.gross_margin,
    s.currency,

    -- 日期维度
    d.year,
    d.quarter,
    d.month,
    d.month_name,

    -- 区域维度
    r.country_code,
    r.country_name,
    r.region_name,
    r.city_name,

    -- 仓库
    w.warehouse_name,

    -- 销售员
    sr.full_name as full_name

from sales s
left join dim_date      d  on s.order_date      = d.date_id
left join dim_region    r  on s.region_id    = r.region_id
left join dim_warehouse w  on s.warehouse_id = w.warehouse_id
left join dim_sales_rep sr on s.salesrep_id  = sr.sales_rep_id
