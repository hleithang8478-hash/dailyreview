#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
聚源数据库配置文件
请根据实际的聚源数据库结构调整以下配置
"""

# 数据库连接配置
DATABASE_CONFIG = {
    'server': "172.16.105.192",
    'user': "readonly", 
    'password': "glfd@2022",
    'database': "JYDB",
    'port': '1433'
}

# ODBC驱动配置
ODBC_CONFIG = {
    'driver': "ODBC Driver 18 for SQL Server"  # 根据实际安装的驱动版本调整
}

# 数据库表名配置
# QT_StockPerformance和SecuMain通过InnerCode关联，SecuCode/SecuAbbr只在SecuMain
TABLE_CONFIG = {
    # 日线行情表
    'daily_quote_table': 'QT_StockPerformance',
    # 股票基本信息表
    'stock_info_table': 'SecuMain',
    # 字段名配置
    'date_field': 'TradingDay',            # 交易日期字段（QT_StockPerformance）
    'inner_code_field': 'InnerCode',       # 内部代码字段（两表均有）
    'open_field': 'OpenPrice',             # 开盘价字段（QT_StockPerformance）
    'high_field': 'HighPrice',             # 最高价字段（QT_StockPerformance）
    'low_field': 'LowPrice',               # 最低价字段（QT_StockPerformance）
    'close_field': 'ClosePrice',           # 收盘价字段（QT_StockPerformance）
    'volume_field': 'TurnoverVolume',      # 成交量字段（QT_StockPerformance）
    'turnover_rate_field': 'TurnoverRate', # 换手率字段（QT_StockPerformance）
    'negotiable_mv_field': 'NegotiableMV', # 流通市值字段（QT_StockPerformance）
    'code_field': 'SecuCode',              # 股票代码字段（SecuMain）
    'name_field': 'SecuAbbr'               # 股票名称字段（SecuMain）
}

# 股票筛选条件（仅A股，排除债券等，排除ST股票）
# ListedSector: 1-主板, 2-中小企业板, 3-三板, 4-其他, 5-大宗交易系统, 6-创业板, 7-科创板, 8-北交所股票
STOCK_FILTER = (
    "ListedSector in (1,2,6) AND SecuCategory = 1 AND SecuMarket in (83,90) "
    "AND SecuAbbr NOT LIKE '%ST%' AND SecuAbbr NOT LIKE '%*ST%' AND SecuAbbr NOT LIKE '%PT%'"
)

# 股票代码格式配置
# 请确认聚源数据库中的股票代码格式
CODE_FORMAT = {
    'shenzhen_suffix': '.SZ',    # 深圳股票后缀
    'shanghai_suffix': '.SS',    # 上海股票后缀
    'example_codes': [
        '000001.SZ',  # 平安银行
        '000002.SZ',  # 万科A
        '600000.SS',  # 浦发银行
        '600036.SS',  # 招商银行
    ]
}

# 查询示例SQL
# 这些是示例SQL，用于验证表结构和字段名
EXAMPLE_QUERIES = {
    # 查看日线行情表结构
    'check_daily_quote_structure': """
    SELECT TOP 5 *
    FROM QT_StockPerformance
    """,
    
    # 查看股票基本信息表结构
    'check_stock_info_structure': """
    SELECT TOP 5 *
    FROM SecuMain
    WHERE ListedSector in (1,2,6) AND SecuCategory = 1 AND SecuMarket in (83,90)
    AND SecuAbbr NOT LIKE '%ST%' AND SecuAbbr NOT LIKE '%*ST%' AND SecuAbbr NOT LIKE '%PT%'
    """,
    
    # 获取股票列表（A股筛选，排除ST股票）
    'get_stock_list': """
    SELECT TOP 10 SecuCode, SecuAbbr, InnerCode
    FROM SecuMain
    WHERE ListedSector in (1,2,6) AND SecuCategory = 1 AND SecuMarket in (83,90)
    AND SecuAbbr NOT LIKE '%ST%' AND SecuAbbr NOT LIKE '%*ST%' AND SecuAbbr NOT LIKE '%PT%'
    AND SecuCode LIKE '000001%'
    """,
    
    # 获取某只股票的日线数据（通过InnerCode关联）
    'get_stock_data': """
    SELECT TOP 10 q.TradingDay, q.OpenPrice as [Open], q.HighPrice as [High], q.LowPrice as [Low], q.ClosePrice as [Close], q.TurnoverVolume as [Volume], q.TurnoverRate
    FROM QT_StockPerformance q
    INNER JOIN SecuMain s ON q.InnerCode = s.InnerCode
    WHERE s.SecuCode = '000001'
    ORDER BY q.TradingDay DESC
    """
}

def print_config_info():
    """打印配置信息"""
    logger.info("=" * 60)
    logger.info("聚源数据库配置信息")
    logger.info("=" * 60)
    
    logger.info("\n数据库连接配置:")
    for key, value in DATABASE_CONFIG.items():
        if key == 'password':
            logger.info(f"  {key}: {'*' * len(value)}")
        else:
            logger.info(f"  {key}: {value}")
    
    logger.info("\n表名配置:")
    for key, value in TABLE_CONFIG.items():
        logger.info(f"  {key}: {value}")
    
    logger.info("\n股票代码格式:")
    for key, value in CODE_FORMAT.items():
        logger.info(f"  {key}: {value}")
    
    logger.info("\n请根据实际的聚源数据库结构调整以上配置！")
    logger.info("=" * 60)

if __name__ == "__main__":
    print_config_info() 