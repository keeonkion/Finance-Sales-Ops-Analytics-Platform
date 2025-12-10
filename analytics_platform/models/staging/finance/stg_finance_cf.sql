{{ config(
    materialized = 'view'
) }}



    select
        financecfid      as finance_cf_id,
        datekey          as date_id,
        glaccountkey     as gl_account_id,
        regionkey        as region_id,
        cashflowamount   as cash_flow_amount,
        currency         as currency
    from {{ source('analytics', 'factfinancecf') }}
    where regionkey is not null

