{{ config(
    materialized = 'view'
) }}



    select
        glaccountkey   as gl_account_id,
        glaccountcode  as gl_account_code,
        glaccountname  as gl_account_name,
        statementtype  as statement_type,
        category       as category,
        subcategory    as subcategory
    from {{ source('analytics', 'dimglaccount') }}

