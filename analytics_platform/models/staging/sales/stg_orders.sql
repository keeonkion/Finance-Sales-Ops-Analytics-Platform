{{ config(
    materialized = 'view'
) }}


    select
        orderid              as order_id,
        ordernumber          as order_number,
        orderlineno          as order_line_number,
        orderdatekey         as order_date_id,
        customerkey          as customer_id,
        productkey           as product_id,
        regionkey            as region_id,
        warehousekey         as warehouse_id,
        orderedqty           as ordered_qty,
        requesteddeliverydate  as requested_delivery_date,
        promiseddeliverydate   as promised_delivery_date,
        actualshipdate         as actual_ship_date,
        shippedqty             as shipped_qty,
        cancelledqty           as cancelled_qty,
        isontime               as is_on_time,
        isinfull               as is_in_full
    from {{ source('analytics', 'factorders') }}

