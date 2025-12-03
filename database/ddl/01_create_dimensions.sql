-- 01_create_dimensions.sql
-- Schema for shared dimension tables

CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics;

-- =========================
-- DimDate
-- =========================
CREATE TABLE IF NOT EXISTS DimDate (
    DateKey         INTEGER PRIMARY KEY,          -- e.g. 20250131
    FullDate        DATE        NOT NULL,
    Year            INTEGER     NOT NULL,
    Quarter         INTEGER     NOT NULL,         -- 1-4
    Month           INTEGER     NOT NULL,         -- 1-12
    MonthName       VARCHAR(20) NOT NULL,
    DayOfMonth      INTEGER     NOT NULL,
    DayOfWeek       INTEGER     NOT NULL,         -- 1=Mon ... 7=Sun
    DayName         VARCHAR(20) NOT NULL,
    IsWeekday       BOOLEAN     NOT NULL
);

-- =========================
-- DimRegion
-- =========================
CREATE TABLE IF NOT EXISTS DimRegion (
    RegionKey       BIGSERIAL PRIMARY KEY,
    CountryCode     VARCHAR(5)  NOT NULL,
    CountryName     VARCHAR(100) NOT NULL,
    RegionName      VARCHAR(100),                 -- e.g. EMEA, APAC
    CityName        VARCHAR(100)
);

-- =========================
-- DimCustomer
-- =========================
CREATE TABLE IF NOT EXISTS DimCustomer (
    CustomerKey     BIGSERIAL PRIMARY KEY,
    CustomerCode    VARCHAR(50)  NOT NULL,
    CustomerName    VARCHAR(200) NOT NULL,
    CustomerType    VARCHAR(50),                  -- e.g. Distributor, Retail, Online
    CustomerSegment VARCHAR(50),                  -- e.g. Gold / Silver
    RegionKey       BIGINT REFERENCES DimRegion(RegionKey),
    Channel         VARCHAR(50),                  -- e.g. B2B, B2C
    IsActive        BOOLEAN DEFAULT TRUE
);

-- =========================
-- DimProduct
-- =========================
CREATE TABLE IF NOT EXISTS DimProduct (
    ProductKey      BIGSERIAL PRIMARY KEY,
    ProductCode     VARCHAR(50)  NOT NULL,
    ProductName     VARCHAR(200) NOT NULL,
    Brand           VARCHAR(100),
    Category        VARCHAR(100),
    SubCategory     VARCHAR(100),
    UoM             VARCHAR(20),                  -- Unit of Measure, e.g. PCS
    IsActive        BOOLEAN DEFAULT TRUE
);

-- =========================
-- DimWarehouse
-- =========================
CREATE TABLE IF NOT EXISTS DimWarehouse (
    WarehouseKey    BIGSERIAL PRIMARY KEY,
    WarehouseCode   VARCHAR(50)  NOT NULL,
    WarehouseName   VARCHAR(200) NOT NULL,
    RegionKey       BIGINT REFERENCES DimRegion(RegionKey),
    IsActive        BOOLEAN DEFAULT TRUE
);

-- =========================
-- DimSalesRep
-- =========================
CREATE TABLE IF NOT EXISTS DimSalesRep (
    SalesRepKey     BIGSERIAL PRIMARY KEY,
    EmployeeCode    VARCHAR(50)  NOT NULL,
    FullName        VARCHAR(200) NOT NULL,
    RegionKey       BIGINT REFERENCES DimRegion(RegionKey),
    Email           VARCHAR(200),
    IsActive        BOOLEAN DEFAULT TRUE
);

-- =========================
-- DimGLAccount
-- =========================
CREATE TABLE IF NOT EXISTS DimGLAccount (
    GLAccountKey    BIGSERIAL PRIMARY KEY,
    GLAccountCode   VARCHAR(50)  NOT NULL,
    GLAccountName   VARCHAR(200) NOT NULL,
    StatementType   VARCHAR(10)  NOT NULL,        -- 'PL', 'BS', 'CF'
    Category        VARCHAR(100),                 -- e.g. Revenue, COGS, OPEX
    SubCategory     VARCHAR(100)
);
