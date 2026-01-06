{{ config(materialized='table') }}

with base_monthly as (

    select
        date_trunc('month', full_date::date) as ds,
        year,
        month,
        month_name,

        region_id,
        country_name,
        region_name,
        city_name,

        sum(order_count)      as order_count,
        sum(ordered_qty)      as ordered_qty,
        sum(shipped_qty)      as shipped_qty,
        sum(cancelled_qty)    as cancelled_qty,

        -- 服务类指标：用“加权”方式更合理（用订单数当权重）
        case when sum(order_count) = 0 then null else sum(on_time_orders)::float / sum(order_count) end as on_time_rate,
        case when sum(order_count) = 0 then null else sum(in_full_orders)::float / sum(order_count) end as in_full_rate,
        case when sum(order_count) = 0 then null else sum(otif_orders)::float     / sum(order_count) end as otif_rate

    from {{ ref('sales_daily_revenue') }}
    group by
        date_trunc('month', full_date::date),
        year, month, month_name,
        region_id, country_name, region_name, city_name
),

features as (

    select
        ds,
        year,
        month,
        month_name,

        region_id,
        country_name,
        region_name,
        city_name,

        order_count,
        ordered_qty,
        shipped_qty,
        cancelled_qty,
        on_time_rate,
        in_full_rate,
        otif_rate,

        -- lags (销量/订单)
        lag(shipped_qty, 1)  over (partition by region_id order by ds) as shipped_qty_lag_1,
        lag(shipped_qty, 3)  over (partition by region_id order by ds) as shipped_qty_lag_3,
        lag(shipped_qty, 12) over (partition by region_id order by ds) as shipped_qty_lag_12,

        lag(order_count, 1)  over (partition by region_id order by ds) as order_count_lag_1,
        lag(order_count, 12) over (partition by region_id order by ds) as order_count_lag_12,

        -- rolling mean (exclude current month)
        avg(shipped_qty) over (
            partition by region_id order by ds
            rows between 3 preceding and 1 preceding
        ) as shipped_qty_roll_mean_3,

        avg(order_count) over (
            partition by region_id order by ds
            rows between 3 preceding and 1 preceding
        ) as order_count_roll_mean_3,

        -- MoM / YoY（发货量）
        case
            when lag(shipped_qty, 1) over (partition by region_id order by ds) is null
              or lag(shipped_qty, 1) over (partition by region_id order by ds) = 0
            then null
            else (shipped_qty - lag(shipped_qty, 1) over (partition by region_id order by ds))
                 / lag(shipped_qty, 1) over (partition by region_id order by ds)
        end as shipped_qty_mom,

        case
            when lag(shipped_qty, 12) over (partition by region_id order by ds) is null
              or lag(shipped_qty, 12) over (partition by region_id order by ds) = 0
            then null
            else (shipped_qty - lag(shipped_qty, 12) over (partition by region_id order by ds))
                 / lag(shipped_qty, 12) over (partition by region_id order by ds)
        end as shipped_qty_yoy,

        -- label：下月 shipped_qty / order_count
        lead(shipped_qty, 1)  over (partition by region_id order by ds) as y_shipped_qty_1m,
        lead(order_count, 1)  over (partition by region_id order by ds) as y_order_count_1m

    from base_monthly
)

select *
from features
where y_shipped_qty_1m is not null