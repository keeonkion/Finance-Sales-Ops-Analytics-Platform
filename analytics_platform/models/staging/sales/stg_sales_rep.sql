{{ config(
    materialized = 'view'
) }}


    select
        salesrepkey   as sales_rep_id,
        employeecode  as employee_code,
        fullname      as full_name,
        regionkey     as region_id,
        email         as email,
        isactive      as is_active
    from {{ source('analytics', 'dimsalesrep') }}

