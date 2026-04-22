-- 模拟仓分析相关SQL查询

-- ==================== 聚源数据库查询 ====================
-- 注意：以下查询需要在聚源数据库中执行

-- 查询股票行业信息（从聚源数据库）
-- SELECT
--     s.SecuCode, 
--     s.SecuAbbr, 
--     s.CompanyCode, 
--     s.InnerCode,
--     i.FirstIndustryName, 
--     i.SecondIndustryName, 
--     i.ThirdIndustryName
-- FROM SecuMain s
-- LEFT JOIN LC_ExgIndustry i ON s.CompanyCode = i.CompanyCode
--     AND i.Standard = '38'
--     AND i.IfPerformed = 1
-- WHERE s.SecuCode IN ('000001', '600000', '000002')  -- 替换为实际股票代码
-- AND s.SecuCategory = 1
-- ORDER BY s.SecuCode;

-- 批量查询股票行业信息（用于预处理）
-- SELECT
--     s.SecuCode, 
--     s.SecuAbbr, 
--     s.CompanyCode, 
--     s.InnerCode,
--     i.FirstIndustryName, 
--     i.SecondIndustryName, 
--     i.ThirdIndustryName
-- FROM SecuMain s
-- LEFT JOIN LC_ExgIndustry i ON s.CompanyCode = i.CompanyCode
--     AND i.Standard = '38'
--     AND i.IfPerformed = 1
-- WHERE s.SecuCode IN (
--     -- 这里放入需要查询的股票代码列表
--     '000001', '600000', '000002', '603352', '002637', '688114'
-- )
-- AND s.SecuCategory = 1
-- ORDER BY s.SecuCode;

-- ==================== SQLite本地数据库查询 ====================

-- 1. 查看所有模拟组合
SELECT DISTINCT portfolio_name 
FROM portfolio_data 
ORDER BY portfolio_name;

-- 2. 查看数据日期范围
SELECT 
    MIN(date) as min_date, 
    MAX(date) as max_date,
    COUNT(DISTINCT date) as date_count
FROM portfolio_data;

-- 3. 查看每个组合的数据统计
SELECT 
    portfolio_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT date) as date_count,
    COUNT(DISTINCT stock_code) as stock_count,
    MIN(date) as start_date,
    MAX(date) as end_date
FROM portfolio_data
GROUP BY portfolio_name
ORDER BY portfolio_name;

-- 4. 查看没有行业信息的股票
SELECT DISTINCT stock_code, stock_name
FROM portfolio_data
WHERE (first_industry_name IS NULL OR first_industry_name = '' OR TRIM(first_industry_name) = '')
   OR (third_industry_name IS NULL OR third_industry_name = '' OR TRIM(third_industry_name) = '')
ORDER BY stock_code;

-- 5. 查看有行业信息的股票数量
SELECT 
    COUNT(DISTINCT stock_code) as stocks_with_industry
FROM portfolio_data
WHERE third_industry_name IS NOT NULL 
  AND third_industry_name != '' 
  AND TRIM(third_industry_name) != '';

-- 6. 查看某个组合的行业分布（最新日期）
SELECT 
    portfolio_name,
    date,
    third_industry_name,
    COUNT(DISTINCT stock_code) as stock_count,
    SUM(position_value) as total_value
FROM portfolio_data
WHERE portfolio_name = '你的组合名称'  -- 替换为实际组合名称
  AND date = (SELECT MAX(date) FROM portfolio_data WHERE portfolio_name = '你的组合名称')
  AND third_industry_name IS NOT NULL 
  AND third_industry_name != ''
  AND position_quantity > 0
GROUP BY portfolio_name, date, third_industry_name
ORDER BY total_value DESC;

-- 7. 修复股票代码格式（如果有浮点数格式）
-- 注意：执行前请备份数据库
UPDATE portfolio_data
SET stock_code = CAST(CAST(stock_code AS REAL) AS INTEGER)
WHERE stock_code LIKE '%.%';

-- 然后格式化为6位
UPDATE portfolio_data
SET stock_code = printf('%06d', CAST(stock_code AS INTEGER))
WHERE LENGTH(stock_code) < 6;

-- 8. 查看预处理后的行业信息统计
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN first_industry_name IS NOT NULL AND first_industry_name != '' THEN 1 END) as has_first_industry,
    COUNT(CASE WHEN second_industry_name IS NOT NULL AND second_industry_name != '' THEN 1 END) as has_second_industry,
    COUNT(CASE WHEN third_industry_name IS NOT NULL AND third_industry_name != '' THEN 1 END) as has_third_industry
FROM portfolio_data;

-- 9. 查看某个日期范围的行业分布
SELECT 
    date,
    third_industry_name,
    COUNT(DISTINCT stock_code) as stock_count,
    SUM(position_value) as total_value
FROM portfolio_data
WHERE date >= '2024-01-01'  -- 替换为实际开始日期
  AND date <= '2024-12-31'  -- 替换为实际结束日期
  AND third_industry_name IS NOT NULL 
  AND third_industry_name != ''
  AND position_quantity > 0
GROUP BY date, third_industry_name
ORDER BY date, total_value DESC;

-- 10. 检查数据完整性
SELECT 
    '总记录数' as metric,
    COUNT(*) as value
FROM portfolio_data
UNION ALL
SELECT 
    '有持仓的记录数',
    COUNT(*)
FROM portfolio_data
WHERE position_quantity > 0
UNION ALL
SELECT 
    '有行业信息的记录数',
    COUNT(*)
FROM portfolio_data
WHERE third_industry_name IS NOT NULL 
  AND third_industry_name != '' 
  AND TRIM(third_industry_name) != ''
UNION ALL
SELECT 
    '有买入记录数',
    COUNT(*)
FROM portfolio_data
WHERE buy_quantity > 0
UNION ALL
SELECT 
    '有卖出记录数',
    COUNT(*)
FROM portfolio_data
WHERE sell_quantity > 0;
