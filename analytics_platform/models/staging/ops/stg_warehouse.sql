{{ config(
    materialized = 'view'
) }}



    select
        warehousekey   as warehouse_id,
        warehousecode  as warehouse_code,
        warehousename  as warehouse_name,
        regionkey      as region_id,
        isactive       as is_active
    from {{ source('analytics', 'dimwarehouse') }}

