{{ config(materialized='table') }}

with fin as (

    select
        period_month as ds,
        year,
        month,
        month_name,

        region_id,
        country_name,
        region_name,
        city_name,

        revenue_amount,
        cogs_amount,
        opex_amount,
        gross_profit_amount,
        gross_margin_pct,
        operating_profit_amount,
        operating_margin_pct

    from {{ ref('finance_pl_by_period') }}
),

sales_m as (

    select
        date_trunc('month', full_date::date) as ds,
        region_id,

        sum(order_count)   as order_count,
        sum(shipped_qty)   as shipped_qty,
        sum(cancelled_qty) as cancelled_qty,

        case when sum(order_count) = 0 then null else sum(otif_orders)::float / sum(order_count) end as otif_rate

    from {{ ref('sales_daily_revenue') }}
    group by
        date_trunc('month', full_date::date),
        region_id
),

inv as (

    select
        period_month as ds,
        country_name,
        region_name,
        avg_inventory_value,
        inventory_turnover_ratio,
        days_on_hand_approx
    from {{ ref('inventory_turnover') }}
),

base as (

    select
        f.*,

        -- sales features
        s.order_count,
        s.shipped_qty,
        s.cancelled_qty,
        s.otif_rate,

        -- inventory features (join by names)
        i.avg_inventory_value,
        i.inventory_turnover_ratio,
        i.days_on_hand_approx

    from fin f
    left join sales_m s
        on f.ds = s.ds
       and f.region_id = s.region_id
    left join inv i
        on f.ds = i.ds
       and f.country_name = i.country_name
       and f.region_name  = i.region_name
),

features as (

    select
        *,

        -- lags for profit/margin
        lag(operating_profit_amount, 1) over (partition by region_id order by ds) as op_profit_lag_1,
        lag(operating_margin_pct, 1)    over (partition by region_id order by ds) as op_margin_lag_1,
        lag(gross_margin_pct, 1)        over (partition by region_id order by ds) as gross_margin_lag_1,

        avg(operating_profit_amount) over (
            partition by region_id order by ds rows between 3 preceding and 1 preceding
        ) as op_profit_roll_mean_3,

        -- labels: next month profit & margin
        lead(operating_profit_amount, 1) over (partition by region_id order by ds) as y_operating_profit_1m,
        lead(operating_margin_pct, 1)    over (partition by region_id order by ds) as y_operating_margin_pct_1m

    from base
)

select *
from features
where y_operating_profit_1m is not null