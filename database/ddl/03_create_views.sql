CREATE SCHEMA IF NOT EXISTS analytics;

-- 销售日粒度视图
CREATE OR REPLACE VIEW analytics.vw_sales_daily AS
SELECT
    d.fulldate,
    d.year,
    d.month,
    d.monthname,
    r.regionname,
    r.countryname,
    p.category,
    p.subcategory,
    SUM(f.netsales)      AS revenue,
    SUM(f.grossmargin)   AS gross_margin,
    SUM(f.quantity)      AS quantity
FROM analytics.factsales f
JOIN analytics.dimdate     d ON f.datekey = d.datekey
JOIN analytics.dimproduct  p ON f.productkey = p.productkey
JOIN analytics.dimregion   r ON f.regionkey = r.regionkey
GROUP BY
    d.fulldate, d.year, d.month, d.monthname,
    r.regionname, r.countryname,
    p.category, p.subcategory;

-- 按月的 P&L 视图
CREATE OR REPLACE VIEW analytics.vw_finance_pl_monthly AS
SELECT
    d.year,
    d.month,
    d.monthname,
    r.regionname,
    ga.glaccountcode,
    ga.glaccountname,
    SUM(f.amount) AS amount
FROM analytics.factfinancepl f
JOIN analytics.dimdate     d  ON f.datekey = d.datekey
JOIN analytics.dimglaccount ga ON f.glaccountkey = ga.glaccountkey
LEFT JOIN analytics.dimregion  r  ON f.regionkey = r.regionkey
GROUP BY
    d.year, d.month, d.monthname,
    r.regionname,
    ga.glaccountcode, ga.glaccountname;

-- 库存月末快照视图
CREATE OR REPLACE VIEW analytics.vw_inventory_monthend AS
SELECT
    d.year,
    d.month,
    d.monthname,
    w.warehousename,
    p.category,
    p.productname,
    f.closingqty,
    f.inventoryvalue,
    f.averageagedays,
    f.provisionamount
FROM analytics.factinventory f
JOIN analytics.dimdate     d ON f.datekey = d.datekey
JOIN analytics.dimwarehouse w ON f.warehousekey = w.warehousekey
JOIN analytics.dimproduct   p ON f.productkey = p.productkey;
