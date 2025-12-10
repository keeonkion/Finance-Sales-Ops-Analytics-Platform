{{ config(
    materialized = 'view'
) }}



    select
        salestargetid   as sales_target_id,
        datekey         as date_id,
        regionkey       as region_id,
        salesrepkey     as sales_rep_id,
        productkey      as product_id,
        targetrevenue   as target_revenue,
        targetquantity  as target_quantity
    from {{ source('analytics', 'factsalestarget') }}

