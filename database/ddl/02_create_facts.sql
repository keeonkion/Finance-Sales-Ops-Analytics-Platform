-- 02_create_facts.sql
-- Schema for core fact tables

CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics, public;

-- =========================
-- FactSales (invoice line grain)
-- =========================
CREATE TABLE IF NOT EXISTS FactSales (
    SalesId         BIGSERIAL PRIMARY KEY,
    DateKey         INTEGER     NOT NULL REFERENCES DimDate(DateKey),
    CustomerKey     BIGINT      NOT NULL REFERENCES DimCustomer(CustomerKey),
    ProductKey      BIGINT      NOT NULL REFERENCES DimProduct(ProductKey),
    RegionKey       BIGINT          REFERENCES DimRegion(RegionKey),
    SalesRepKey     BIGINT          REFERENCES DimSalesRep(SalesRepKey),
    WarehouseKey    BIGINT          REFERENCES DimWarehouse(WarehouseKey),

    InvoiceNumber   VARCHAR(50) NOT NULL,
    InvoiceLineNo   INTEGER     NOT NULL,

    Quantity        NUMERIC(18,2) NOT NULL,
    ListPrice       NUMERIC(18,4),          -- per unit
    DiscountAmount  NUMERIC(18,2),          -- total discount
    NetSales        NUMERIC(18,2),          -- after discount
    COGS            NUMERIC(18,2),
    GrossMargin     NUMERIC(18,2),          -- NetSales - COGS

    Currency        VARCHAR(10) DEFAULT 'EUR'
);

-- =========================
-- FactSalesTarget (monthly targets)
-- =========================
CREATE TABLE IF NOT EXISTS FactSalesTarget (
    SalesTargetId   BIGSERIAL PRIMARY KEY,
    DateKey         INTEGER     NOT NULL REFERENCES DimDate(DateKey),  -- usually first day of month
    RegionKey       BIGINT          REFERENCES DimRegion(RegionKey),
    SalesRepKey     BIGINT          REFERENCES DimSalesRep(SalesRepKey),
    ProductKey      BIGINT          REFERENCES DimProduct(ProductKey),

    TargetRevenue   NUMERIC(18,2),
    TargetQuantity  NUMERIC(18,2)
);

-- =========================
-- FactOrders (order line grain)
-- =========================
CREATE TABLE IF NOT EXISTS FactOrders (
    OrderId             BIGSERIAL PRIMARY KEY,
    OrderNumber         VARCHAR(50) NOT NULL,
    OrderLineNo         INTEGER     NOT NULL,

    OrderDateKey        INTEGER     NOT NULL REFERENCES DimDate(DateKey),
    CustomerKey         BIGINT      NOT NULL REFERENCES DimCustomer(CustomerKey),
    ProductKey          BIGINT      NOT NULL REFERENCES DimProduct(ProductKey),
    RegionKey           BIGINT          REFERENCES DimRegion(RegionKey),
    WarehouseKey        BIGINT          REFERENCES DimWarehouse(WarehouseKey),

    OrderedQty          NUMERIC(18,2) NOT NULL,
    RequestedDeliveryDate DATE,
    PromisedDeliveryDate  DATE,
    ActualShipDate        DATE,
    ShippedQty          NUMERIC(18,2),
    CancelledQty        NUMERIC(18,2),

    IsOnTime            BOOLEAN,
    IsInFull            BOOLEAN
);

-- =========================
-- FactInventory (daily snapshot per product & warehouse)
-- =========================
CREATE TABLE IF NOT EXISTS FactInventory (
    InventoryId     BIGSERIAL PRIMARY KEY,
    DateKey         INTEGER     NOT NULL REFERENCES DimDate(DateKey),
    ProductKey      BIGINT      NOT NULL REFERENCES DimProduct(ProductKey),
    WarehouseKey    BIGINT      NOT NULL REFERENCES DimWarehouse(WarehouseKey),

    OpeningQty      NUMERIC(18,2),
    InboundQty      NUMERIC(18,2),
    OutboundQty     NUMERIC(18,2),
    ClosingQty      NUMERIC(18,2),
    InventoryValue  NUMERIC(18,2),
    AverageAgeDays  NUMERIC(10,2),
    ProvisionAmount NUMERIC(18,2)          -- write-down / provision
);

-- =========================
-- FactProduction (daily production per product & warehouse)
-- =========================
CREATE TABLE IF NOT EXISTS FactProduction (
    ProductionId    BIGSERIAL PRIMARY KEY,
    DateKey         INTEGER     NOT NULL REFERENCES DimDate(DateKey),
    ProductKey      BIGINT      NOT NULL REFERENCES DimProduct(ProductKey),
    WarehouseKey    BIGINT          REFERENCES DimWarehouse(WarehouseKey),

    ProducedQty     NUMERIC(18,2),
    ScrapQty        NUMERIC(18,2),
    MachineHours    NUMERIC(18,2),
    DowntimeHours   NUMERIC(18,2)
);

-- =========================
-- Finance Facts (PL, BS, CF)
-- Grain: one row per DateKey x GLAccount (optionally Region)
-- =========================

CREATE TABLE IF NOT EXISTS FactFinancePL (
    FinancePLId     BIGSERIAL PRIMARY KEY,
    DateKey         INTEGER     NOT NULL REFERENCES DimDate(DateKey),
    GLAccountKey    BIGINT      NOT NULL REFERENCES DimGLAccount(GLAccountKey),
    RegionKey       BIGINT          REFERENCES DimRegion(RegionKey),

    Amount          NUMERIC(18,2) NOT NULL,   -- positive/negative as per account sign
    Currency        VARCHAR(10) DEFAULT 'EUR'
);

CREATE TABLE IF NOT EXISTS FactFinanceBS (
    FinanceBSId     BIGSERIAL PRIMARY KEY,
    DateKey         INTEGER     NOT NULL REFERENCES DimDate(DateKey),
    GLAccountKey    BIGINT      NOT NULL REFERENCES DimGLAccount(GLAccountKey),
    RegionKey       BIGINT          REFERENCES DimRegion(RegionKey),

    BalanceAmount   NUMERIC(18,2) NOT NULL,
    Currency        VARCHAR(10) DEFAULT 'EUR'
);

CREATE TABLE IF NOT EXISTS FactFinanceCF (
    FinanceCFId     BIGSERIAL PRIMARY KEY,
    DateKey         INTEGER     NOT NULL REFERENCES DimDate(DateKey),
    GLAccountKey    BIGINT      NOT NULL REFERENCES DimGLAccount(GLAccountKey),
    RegionKey       BIGINT          REFERENCES DimRegion(RegionKey),

    CashFlowAmount  NUMERIC(18,2) NOT NULL,
    Currency        VARCHAR(10) DEFAULT 'EUR'
);
