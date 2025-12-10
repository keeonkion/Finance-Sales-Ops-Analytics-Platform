{{ config(
    materialized = 'view'
) }}

with finance_cf as (
    select *
    from {{ ref('stg_finance_cf') }}
),

dim_date as (
    select *
    from {{ ref('stg_date') }}
),

dim_gl_account as (
    select *
    from {{ ref('stg_gl_account') }}
),

dim_region as (
    select *
    from {{ ref('stg_region') }}
)

select
    -- grain
    f.finance_cf_id              as finance_cf_id,
    f.date_id,
    f.gl_account_id             as gl_account_id,
    f.region_id                as region_id,

    -- date attributes
    d.full_date                 as full_date,
    d.year                     as year,
    d.quarter                  as quarter,
    d.month                    as month,
    d.month_name                as month_name,

    -- GL account attributes
    g.gl_account_code            as gl_account_code,
    g.gl_account_name            as gl_account_name,
    g.statement_type            as statement_type,      -- PL / BS / CF
    g.category                 as category,
    g.subcategory              as subcategory,

    -- region attributes
    r.country_code              as country_code,
    r.country_name              as country_name,
    r.region_name               as region_name,
    r.city_name                 as city_name,

    -- metric
    f.cash_flow_amount           as cash_flow_amount,
    f.currency

from finance_cf f
left join dim_date       d on f.date_id      = d.date_id
left join dim_gl_account g on f.gl_account_id = g.gl_account_id
left join dim_region     r on f.region_id    = r.region_id