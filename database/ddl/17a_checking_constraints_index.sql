-- 看某个 fact 的结构
\d analytics.factsales

-- 验证外键有没有生效
SELECT
    conname, contype, confrelid::regclass AS references_table
FROM pg_constraint
WHERE conrelid = 'analytics.factsales'::regclass;
