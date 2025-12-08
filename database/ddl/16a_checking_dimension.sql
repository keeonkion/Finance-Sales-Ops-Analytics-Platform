-- 1) 维度行数（清洗后应该略少或不变）
SELECT 'dimcustomer'  AS table, COUNT(*) FROM analytics.dimcustomer
UNION ALL
SELECT 'dimproduct', COUNT(*) FROM analytics.dimproduct
UNION ALL
SELECT 'dimregion', COUNT(*) FROM analytics.dimregion;

-- 2) 检查是否还有重复自然键
SELECT customercode, COUNT(*) 
FROM analytics.dimcustomer
GROUP BY customercode
HAVING COUNT(*) > 1;

SELECT productcode, COUNT(*) 
FROM analytics.dimproduct
GROUP BY productcode
HAVING COUNT(*) > 1;

SELECT countrycode, regionname, cityname, COUNT(*)
FROM analytics.dimregion
GROUP BY countrycode, regionname, cityname
HAVING COUNT(*) > 1;
