{{ config(
    materialized = 'view'
) }}



    select
        financeplid   as finance_pl_id,
        datekey       as date_id,
        glaccountkey  as gl_account_id,
        regionkey     as region_id,
        amount        as amount,
        currency      as currency
    from {{ source('analytics', 'factfinancepl') }}
    where regionkey is not null

