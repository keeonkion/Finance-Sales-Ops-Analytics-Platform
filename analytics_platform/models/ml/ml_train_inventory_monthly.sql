{{ config(materialized='table') }}

with base as (

    select
        period_month as ds,
        year,
        month,
        country_name,
        region_name,

        avg_inventory_value,
        cogs_amount,
        inventory_turnover_ratio,
        days_on_hand_approx

    from {{ ref('inventory_turnover') }}
),

features as (

    select
        ds,
        year,
        month,
        country_name,
        region_name,

        avg_inventory_value,
        cogs_amount,
        inventory_turnover_ratio,
        days_on_hand_approx,

        -- lags
        lag(avg_inventory_value, 1)  over (partition by country_name, region_name order by ds) as avg_inv_lag_1,
        lag(avg_inventory_value, 3)  over (partition by country_name, region_name order by ds) as avg_inv_lag_3,
        lag(avg_inventory_value, 12) over (partition by country_name, region_name order by ds) as avg_inv_lag_12,

        lag(days_on_hand_approx, 1)  over (partition by country_name, region_name order by ds) as doh_lag_1,
        lag(inventory_turnover_ratio, 1) over (partition by country_name, region_name order by ds) as turnover_lag_1,

        -- rolling
        avg(avg_inventory_value) over (
            partition by country_name, region_name
            order by ds rows between 3 preceding and 1 preceding
        ) as avg_inv_roll_mean_3,

        -- labels
        lead(avg_inventory_value, 1) over (partition by country_name, region_name order by ds) as y_avg_inventory_value_1m,
        lead(days_on_hand_approx, 1) over (partition by country_name, region_name order by ds) as y_days_on_hand_1m

    from base
)

select *
from features
where y_avg_inventory_value_1m is not null