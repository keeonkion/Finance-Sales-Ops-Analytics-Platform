{{ config(
    materialized = 'view'
) }}



    select
        datekey        as date_id,
        fulldate       as full_date,
        year           as year,
        quarter        as quarter,
        month          as month,
        monthname      as month_name,
        dayofmonth     as day_of_month,
        dayofweek      as day_of_week,
        dayname        as day_name,
        isweekday      as is_weekday
    from {{ source('analytics', 'dimdate') }}



