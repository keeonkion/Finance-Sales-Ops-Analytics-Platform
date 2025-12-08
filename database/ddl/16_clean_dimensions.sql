-- 16_clean_dimensions.sql
-- Purpose: Clean and normalize core dimension tables
--   - analytics.dimdate
--   - analytics.dimcustomer
--   - analytics.dimproduct
--   - analytics.dimregion
--   - analytics.dimglaccount
--
-- Note: Task 17 will add PK/FK and indexes on top of these cleaned tables.

BEGIN;

------------------------------------------------------------
-- 1. DimDate: normalize text columns (month / day names)
------------------------------------------------------------
UPDATE analytics.dimdate
SET
    monthname = INITCAP(TRIM(monthname)),
    dayname   = INITCAP(TRIM(dayname));

-- 可选：检查是否有重复 datekey（正常不会有）
-- SELECT datekey, COUNT(*) FROM analytics.dimdate GROUP BY datekey HAVING COUNT(*) > 1;


------------------------------------------------------------
-- 2. DimCustomer: trim/normalize text, fix booleans, dedupe
------------------------------------------------------------
-- 2.1 统一格式
UPDATE analytics.dimcustomer
SET
    customercode     = UPPER(TRIM(customercode)),
    customername     = INITCAP(TRIM(customername)),
    customertype     = INITCAP(TRIM(customertype)),
    customersegment  = INITCAP(TRIM(customersegment)),
    channel          = UPPER(TRIM(channel)),
    isactive         = COALESCE(isactive, TRUE);

-- 2.2 删除重复行：以 customercode 作为自然键，保留最小 customerkey
DELETE FROM analytics.dimcustomer d
USING analytics.dimcustomer other
WHERE d.customercode = other.customercode
  AND d.customerkey > other.customerkey;


------------------------------------------------------------
-- 3. DimProduct: trim/normalize text, fix booleans, dedupe
------------------------------------------------------------
-- 3.1 统一文本格式
UPDATE analytics.dimproduct
SET
    productcode = UPPER(TRIM(productcode)),
    productname = INITCAP(TRIM(productname)),
    brand       = INITCAP(TRIM(brand)),
    category    = INITCAP(TRIM(category)),
    subcategory = INITCAP(TRIM(subcategory)),
    uom         = UPPER(TRIM(uom)),
    isactive    = COALESCE(isactive, TRUE);

-- 3.2 删除重复行：以 productcode 作为自然键
DELETE FROM analytics.dimproduct p
USING analytics.dimproduct other
WHERE p.productcode = other.productcode
  AND p.productkey > other.productkey;


------------------------------------------------------------
-- 4. DimRegion: normalize country/region/city text, dedupe
------------------------------------------------------------
-- 4.1 统一文本格式
UPDATE analytics.dimregion
SET
    countrycode = UPPER(TRIM(countrycode)),
    countryname = INITCAP(TRIM(countryname)),
    regionname  = INITCAP(TRIM(regionname)),
    cityname    = INITCAP(TRIM(cityname));

-- 4.2 删除重复行：
--     以 (countrycode, regionname, cityname) 组合作为自然键
DELETE FROM analytics.dimregion r
USING analytics.dimregion other
WHERE r.countrycode = other.countrycode
  AND COALESCE(r.regionname, '') = COALESCE(other.regionname, '')
  AND COALESCE(r.cityname, '')   = COALESCE(other.cityname, '')
  AND r.regionkey > other.regionkey;


------------------------------------------------------------
-- 5. DimGLAccount: normalize statement type & categories
------------------------------------------------------------
UPDATE analytics.dimglaccount
SET
    statementtype = UPPER(TRIM(statementtype)),   -- PL / BS / CF
    category      = INITCAP(TRIM(category)),
    subcategory   = INITCAP(TRIM(subcategory));

-- 可选：如果有奇怪的 statementtype，可以先检查
-- SELECT DISTINCT statementtype FROM analytics.dimglaccount;

COMMIT;
