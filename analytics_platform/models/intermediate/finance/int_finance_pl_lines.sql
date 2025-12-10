{{ config(
    materialized = 'view'
) }}

with pl as (
    select *
    from {{ ref('stg_finance_pl') }}
),
dim_date as (
    select date_id, year, month, month_name, quarter
    from {{ ref('stg_date') }}
),
dim_region as (
    select region_id, country_name, region_name, city_name
    from {{ ref('stg_region') }}
),
dim_gl as (
    select gl_account_id, gl_account_code, gl_account_name, statement_type, category, subcategory
    from {{ ref('stg_gl_account') }}
)

select
    p.finance_pl_id,
    p.date_id,
    p.gl_account_id,
    p.region_id,
    p.amount,
    p.currency,

    d.year,
    d.quarter,
    d.month,
    d.month_name,

    r.country_name,
    r.region_name,
    r.city_name,

    g.gl_account_code,
    g.gl_account_name,
    g.statement_type,
    g.category,
    g.subcategory

from pl p
left join dim_date   d on p.date_id    = d.date_id
left join dim_region r on p.region_id  = r.region_id
left join dim_gl     g on p.gl_account_id = g.gl_account_id
