{{ config(
    materialized = 'view'
) }}

select
    customerkey     as customer_id,          -- 👈 统一成 customer_id
        customercode    as customer_code,
        customername    as customer_name,
        customertype    as customer_type,
        customersegment as customer_segment,
        regionkey       as region_key,
        channel         as channel,
        isactive        as is_active
from {{ source('analytics', 'dimcustomer') }}