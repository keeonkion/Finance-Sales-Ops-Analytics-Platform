{{ config(
    materialized = 'view'
) }}



    select
        productionid   as production_id,
        datekey        as date_id,
        productkey     as product_id,
        warehousekey   as warehouse_id,
        producedqty    as produced_qty,
        scrapqty       as scrap_qty,
        machinehours   as machine_hours,
        downtimehours  as downtime_hours
    from {{ source('analytics', 'factproduction') }}

