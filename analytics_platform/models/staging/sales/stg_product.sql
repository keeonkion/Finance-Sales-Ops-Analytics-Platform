{{ config(
    materialized = 'view'
) }}

select
   productkey      as product_id,
        productcode     as product_code,
        productname     as product_name,
        brand           as brand,
        category        as category,
        subcategory     as subcategory,
        uom             as uom,
        isactive        as is_active
from {{ source('analytics', 'dimproduct') }}