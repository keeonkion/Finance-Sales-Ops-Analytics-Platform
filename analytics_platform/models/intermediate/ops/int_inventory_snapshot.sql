{{ config(
    materialized = 'view'
) }}

with fact_inventory as (
    select *
    from {{ ref('stg_inventory') }}
),
dim_date as (
    select date_id, year, month, month_name, quarter
    from {{ ref('stg_date') }}
),
dim_region as (
    select region_id, country_name, region_name, city_name
    from {{ ref('stg_region') }}
),
dim_warehouse as (
    select warehouse_id, warehouse_name, region_id
    from {{ ref('stg_warehouse') }}
)

select
    i.inventory_id,
    i.date_id,
    i.product_id,
    i.warehouse_id,

    i.opening_qty,
    i.inbound_qty,
    i.outbound_qty,
    i.closing_qty,
    i.inventory_value,
    i.average_age_days,
    i.provision_amount,

    d.year,
    d.month,
    d.month_name,
    r.country_name,
    r.region_name,
    r.city_name,
    w.warehouse_name

from fact_inventory i
left join dim_date      d on i.date_id      = d.date_id
left join dim_warehouse w on i.warehouse_id = w.warehouse_id
left join dim_region    r on w.region_id    = r.region_id
