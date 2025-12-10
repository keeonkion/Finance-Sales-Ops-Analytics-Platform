{{ config(
    materialized = 'view'
) }}



    select
        financebsid    as finance_bs_id,
        datekey        as date_id,
        glaccountkey   as gl_account_id,
        regionkey      as region_id,
        balanceamount  as balance_amount,
        currency       as currency
    from {{ source('analytics', 'factfinancebs') }}
    where regionkey is not null

