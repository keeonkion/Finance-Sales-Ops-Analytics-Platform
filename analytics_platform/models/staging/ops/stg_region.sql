{{ config(
    materialized = 'view'
) }}



    select
        regionkey    as region_id,
        countrycode  as country_code,
        countryname  as country_name,
        regionname   as region_name,
        cityname     as city_name
    from {{ source('analytics', 'dimregion') }}

