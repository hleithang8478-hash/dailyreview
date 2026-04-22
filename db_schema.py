# db_schema.py

TABLES = {
    'daily_quote': {
        'table': 'QT_StockPerformance',
        'fields': [
            'TradingDay', 'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice', 'TurnoverVolume', 'InnerCode','TurnoverRate','TurnoverValue', 'NegotiableMV'
        ],
        'date_field': 'TradingDay',
        'inner_code_field': 'InnerCode'
    },
    'stock_info': {
        'table': 'SecuMain',
        'fields': ['SecuCode', 'SecuAbbr', 'InnerCode', 'ListedSector', 'SecuCategory', 'SecuMarket'],
        'code_field': 'SecuCode',
        'name_field': 'SecuAbbr'
    },
    'main_index': {
        'table': 'LC_MainIndexNew',
        'fields': [
            # 营收增长率
            'ROETTM',                # 净资产收益率_TTM(%)
            'ROICTTM',               # 投入资本回报率_TTM(%)
            # 盈利能力
            'NetProfitRatioTTM',     # 销售净利率_TTM(%)
            'NPToTORTTM',             # 净利润/营业收入_TTM(%)
            'OperatingRevenueCashCover', # 营业收入现金含量(%)
            'NetProfitCashCover',    # 净利润现金含量(%)
            'MainProfitProportion',  # 主营业务比率
            # 财务健康状况（偿债能力）
            'NetOperateCashFlowYOY', # 经营活动产生的现金流量净额同比增长(%)
            'OperCashPSGrowRate',    # 每股经营活动产生的现金流量净额_TTM(%)
            'SuperQuickRatio',       # 超速动比率
            'NOCFToCurrentLiability',# 经营活动产生的现金流量净额/流动负债
            'NetAssetLiabilityRatio',# 净资产负债率
            'InteBearDebtToTL',      # 带息负债率
            # 成本控制
            'OperatingExpenseRateTTM',# 销售费用/营业收入_TTM
            'AdminiExpenseRateTTM',   # 管理费用/营业收入_TTM(%)
            'FinancialExpenseRateTTM',# 财务费用/营业收入_TTM(%)
            # 市占率/营业收入增长
            'OperatingRevenueGrowRate',# 营业收入同比增长(%)
            'TORGrowRate',        # 营业收入同比增长率(%)
            'TotalOperatingRevenuePS', # 每股营业收入
            # 分红项
            'DividendPS',            # 股息股利(元/股)
            'DividendPaidRatio',     # 股利支付率(%)
            'DividendTTM',           # 股息TTM(元)
            'EndDate'
        ],
        'date_field': 'EndDate',
        'company_field': 'CompanyCode'
    },
    'income_statement': {
        'table': 'LC_QIncomeStatementNew',
        'fields': [
            'TotalOperatingRevenue',  # 营业总收入
            'EndDate'
        ],
        'date_field': 'EndDate',
        'company_field': 'CompanyCode'
    },
    'industry': {
        'table': 'LC_ExgIndustry',
        'fields': ['CompanyCode', 'FirstIndustryName', 'SecondIndustryName', 'ThirdIndustryName', 'Standard'],
        'company_field': 'CompanyCode'
    },
    'valuation_index': {
        'table': 'DZ_DIndicesForValuation',
        'fields': [
            'PB',        # 市盈率MRQ
            'PETTMCut',     # 扣非市盈率
            'PCFTTM',    # 流动市现率(经营现金流)
        ],
        'code_field': 'SecuCode'
    },
    # 可继续扩展
} 