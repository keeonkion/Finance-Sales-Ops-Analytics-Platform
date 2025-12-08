-- =====================================================
-- 03_constraints_indexes.sql
-- Task 17: Primary Keys, Foreign Keys, Indexes on Facts
-- Schema: analytics
-- =====================================================

SET search_path TO analytics;

--------------------------------------------------------
-- 2. Foreign keys from facts → dimensions
--------------------------------------------------------

-- ===== FactSales =====
ALTER TABLE factsales
    ADD CONSTRAINT fk_factsales_date
        FOREIGN KEY (datekey) REFERENCES dimdate(datekey),
    ADD CONSTRAINT fk_factsales_customer
        FOREIGN KEY (customerkey) REFERENCES dimcustomer(customerkey),
    ADD CONSTRAINT fk_factsales_product
        FOREIGN KEY (productkey) REFERENCES dimproduct(productkey),
    ADD CONSTRAINT fk_factsales_region
        FOREIGN KEY (regionkey) REFERENCES dimregion(regionkey),
    ADD CONSTRAINT fk_factsales_salesrep
        FOREIGN KEY (salesrepkey) REFERENCES dimsalesrep(salesrepkey),
    ADD CONSTRAINT fk_factsales_warehouse
        FOREIGN KEY (warehousekey) REFERENCES dimwarehouse(warehousekey);

-- ===== FactSalesTarget =====
ALTER TABLE factsalestarget
    ADD CONSTRAINT fk_factsalestarget_date
        FOREIGN KEY (datekey) REFERENCES dimdate(datekey),
    ADD CONSTRAINT fk_factsalestarget_region
        FOREIGN KEY (regionkey) REFERENCES dimregion(regionkey),
    ADD CONSTRAINT fk_factsalestarget_salesrep
        FOREIGN KEY (salesrepkey) REFERENCES dimsalesrep(salesrepkey),
    ADD CONSTRAINT fk_factsalestarget_product
        FOREIGN KEY (productkey) REFERENCES dimproduct(productkey);

-- ===== FactOrders =====
ALTER TABLE factorders
    ADD CONSTRAINT fk_factorders_date
        FOREIGN KEY (orderdatekey) REFERENCES dimdate(datekey),
    ADD CONSTRAINT fk_factorders_customer
        FOREIGN KEY (customerkey) REFERENCES dimcustomer(customerkey),
    ADD CONSTRAINT fk_factorders_product
        FOREIGN KEY (productkey) REFERENCES dimproduct(productkey),
    ADD CONSTRAINT fk_factorders_region
        FOREIGN KEY (regionkey) REFERENCES dimregion(regionkey),
    ADD CONSTRAINT fk_factorders_warehouse
        FOREIGN KEY (warehousekey) REFERENCES dimwarehouse(warehousekey);

-- ===== FactInventory =====
ALTER TABLE factinventory
    ADD CONSTRAINT fk_factinventory_date
        FOREIGN KEY (datekey) REFERENCES dimdate(datekey),
    ADD CONSTRAINT fk_factinventory_product
        FOREIGN KEY (productkey) REFERENCES dimproduct(productkey),
    ADD CONSTRAINT fk_factinventory_warehouse
        FOREIGN KEY (warehousekey) REFERENCES dimwarehouse(warehousekey);

-- ===== FactProduction =====
ALTER TABLE factproduction
    ADD CONSTRAINT fk_factproduction_date
        FOREIGN KEY (datekey) REFERENCES dimdate(datekey),
    ADD CONSTRAINT fk_factproduction_product
        FOREIGN KEY (productkey) REFERENCES dimproduct(productkey),
    ADD CONSTRAINT fk_factproduction_warehouse
        FOREIGN KEY (warehousekey) REFERENCES dimwarehouse(warehousekey);

-- ===== Finance PL / BS / CF =====
ALTER TABLE factfinancepl
    ADD CONSTRAINT fk_factfinancepl_date
        FOREIGN KEY (datekey) REFERENCES dimdate(datekey),
    ADD CONSTRAINT fk_factfinancepl_gl
        FOREIGN KEY (glaccountkey) REFERENCES dimglaccount(glaccountkey),
    ADD CONSTRAINT fk_factfinancepl_region
        FOREIGN KEY (regionkey) REFERENCES dimregion(regionkey);

ALTER TABLE factfinancebs
    ADD CONSTRAINT fk_factfinancebs_date
        FOREIGN KEY (datekey) REFERENCES dimdate(datekey),
    ADD CONSTRAINT fk_factfinancebs_gl
        FOREIGN KEY (glaccountkey) REFERENCES dimglaccount(glaccountkey),
    ADD CONSTRAINT fk_factfinancebs_region
        FOREIGN KEY (regionkey) REFERENCES dimregion(regionkey);

ALTER TABLE factfinancecf
    ADD CONSTRAINT fk_factfinancecf_date
        FOREIGN KEY (datekey) REFERENCES dimdate(datekey),
    ADD CONSTRAINT fk_factfinancecf_gl
        FOREIGN KEY (glaccountkey) REFERENCES dimglaccount(glaccountkey),
    ADD CONSTRAINT fk_factfinancecf_region
        FOREIGN KEY (regionkey) REFERENCES dimregion(regionkey);

--------------------------------------------------------
-- 3. Useful indexes for analytics queries
--------------------------------------------------------

-- FactSales: 常见切片 Date / Product / Customer / Region
CREATE INDEX ix_factsales_date
    ON factsales (datekey);

CREATE INDEX ix_factsales_product
    ON factsales (productkey);

CREATE INDEX ix_factsales_customer
    ON factsales (customerkey);

CREATE INDEX ix_factsales_region
    ON factsales (regionkey);

CREATE INDEX ix_factsales_date_region
    ON factsales (datekey, regionkey);

-- FactSalesTarget
CREATE INDEX ix_factsalestarget_date_region
    ON factsalestarget (datekey, regionkey);

-- FactOrders
CREATE INDEX ix_factorders_date
    ON factorders (orderdatekey);

CREATE INDEX ix_factorders_customer
    ON factorders (customerkey);

CREATE INDEX ix_factorders_region
    ON factorders (regionkey);

-- FactInventory
CREATE INDEX ix_factinventory_date
    ON factinventory (datekey);

CREATE INDEX ix_factinventory_product_wh
    ON factinventory (productkey, warehousekey);

-- FactProduction
CREATE INDEX ix_factproduction_date
    ON factproduction (datekey);

CREATE INDEX ix_factproduction_product_wh
    ON factproduction (productkey, warehousekey);

-- Finance PL / BS / CF：常用按月份 + 科目 + 区域分析
CREATE INDEX ix_factfinancepl_date_gl
    ON factfinancepl (datekey, glaccountkey);

CREATE INDEX ix_factfinancepl_date_region
    ON factfinancepl (datekey, regionkey);

CREATE INDEX ix_factfinancebs_date_gl
    ON factfinancebs (datekey, glaccountkey);

CREATE INDEX ix_factfinancebs_date_region
    ON factfinancebs (datekey, regionkey);

CREATE INDEX ix_factfinancecf_date_gl
    ON factfinancecf (datekey, glaccountkey);

CREATE INDEX ix_factfinancecf_date_region
    ON factfinancecf (datekey, regionkey);
