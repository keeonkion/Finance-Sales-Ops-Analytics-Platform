{{ config(
    materialized = 'view'
) }}



    select
        inventoryid       as inventory_id,
        datekey           as date_id,
        productkey        as product_id,
        warehousekey      as warehouse_id,
        openingqty        as opening_qty,
        inboundqty        as inbound_qty,
        outboundqty       as outbound_qty,
        closingqty        as closing_qty,
        inventoryvalue    as inventory_value,
        averageagedays    as average_age_days,
        provisionamount   as provision_amount
    from {{ source('analytics', 'factinventory') }}

