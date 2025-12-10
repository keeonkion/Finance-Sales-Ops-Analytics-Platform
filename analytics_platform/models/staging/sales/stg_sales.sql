{{ config(
    materialized = 'view'
) }}

select
    -- 主键
        salesid        as sales_id,

        -- 日期 & 维度外键
        datekey  as order_date,
        customerkey    as customer_id,
        productkey     as product_id,
        regionkey      as region_id,
        salesrepkey    as salesrep_id,
        warehousekey   as warehouse_id,

        -- 发票信息
        invoicenumber  as invoice_number,
        invoicelineno  as invoice_line_number,

        -- 数量 & 金额
        quantity::numeric        as quantity,
        listprice::numeric       as list_price,
        discountamount::numeric  as discount_amount,
        netsales::numeric        as net_sales,
        cogs::numeric            as cogs,
        grossmargin::numeric     as gross_margin,

        -- 货币
        currency        as currency
from {{ source('analytics', 'factsales') }}