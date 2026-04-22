#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
功能14：接近历史最低价筛选模块

功能描述：找到当前价格接近最近3年内价格最低点±10%以内的个股

技术架构（完全复用功能13）：
- 数据获取：批量SQL + 连接池 + 增量缓存
- 数据处理：向量化计算 + 预计算指标
- 多线程并发：高性能线程池 + 批量处理
- 缓存管理：增量缓存 + 智能合并
"""

import os
import time
import logging
from datetime import datetime, timedelta, date
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple, Any
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd

from data_fetcher import JuyuanDataFetcher
from config import STOCK_LIST_LIMIT, MAX_TRADING_DAYS_AGO
from high_performance_threading import HighPerformanceThreadPool
from futures_incremental_cache_manager import futures_incremental_cache_manager
from cache_validator import validate_cache_data
from scipy import stats

logger = logging.getLogger(__name__)


def calculate_percentile(current_value, historical_values):
    """
    计算当前值在历史数据中的分位数
    
    参数：
    - current_value: 当前值
    - historical_values: 历史值列表
    
    返回：
    - 分位数（0-100），如果数据不足则返回None
    """
    if current_value is None:
        return None
    
    # 清理历史数据，过滤掉空值和无效值
    clean_historical = [v for v in historical_values if v is not None and not pd.isna(v) and v > 0]
    
    if len(clean_historical) < 5:  # 至少需要5个数据点
        return None
    
    try:
        # 计算分位数（使用scipy.stats.percentileofscore）
        percentile = stats.percentileofscore(clean_historical, current_value)
        return round(percentile, 1)
    except Exception as e:
        logger.error(f"计算分位数时出错: {e}")
        return None


def get_extended_fundamental_data(
    codes: List[str],
    fetcher: JuyuanDataFetcher,
    stock_data_dict: Dict[str, pd.DataFrame],
    years_for_percentile: int = 5,
    years_for_dividend: int = 3
) -> Dict[str, Dict[str, Any]]:
    """
    扩展的基本面数据获取函数，支持PE/PB分位数和股息率计算
    
    参数：
    - codes: 股票代码列表
    - fetcher: 数据获取器
    - stock_data_dict: 股票行情数据字典 {code: DataFrame}，用于获取当前股价
    - years_for_percentile: 计算分位数的历史年数（默认5年）
    - years_for_dividend: 计算股息率的年数（默认3年）
    
    返回：
    - Dict[str, Dict]: {code: {pb_mrq, pe_ttm, pb_percentile, pe_percentile, 
                                dividend_info, dividend_yield, ...}}
    """
    result = {}
    
    if not codes:
        return result
    
    try:
        # 批量查询基本面数据
        codes_str = ','.join([f"'{code}'" for code in codes])
        
        # 1. 查询最新财务指标（PB、PE、ROE_TTM）
        sql_fundamental = f"""
        SELECT 
            s.SecuCode,
            s.InnerCode,
            m.ROETTM as ROE_TTM,
            v.PB as PB_MRQ,
            v.PETTMCut as PE_TTM
        FROM SecuMain s
        LEFT JOIN (
            SELECT 
                CompanyCode,
                ROETTM,
                ROW_NUMBER() OVER (PARTITION BY CompanyCode ORDER BY EndDate DESC) as rn
            FROM LC_MainIndexNew
        ) m ON s.CompanyCode = m.CompanyCode AND m.rn = 1
        LEFT JOIN (
            SELECT 
                InnerCode,
                PB,
                PETTMCut
            FROM DZ_DIndicesForValuation
        ) v ON s.InnerCode = v.InnerCode
        WHERE s.SecuCode IN ({codes_str})
          AND s.SecuCategory = 1
        """
        
        # 2. 查询近3年分红数据
        sql_dividend = f"""
        SELECT 
            s.SecuCode,
            m.DividendPS,
            m.DividendPaidRatio as DividendPayoutRatio,
            m.EndDate
        FROM SecuMain s
        INNER JOIN LC_MainIndexNew m ON s.CompanyCode = m.CompanyCode
        WHERE s.SecuCode IN ({codes_str})
          AND s.SecuCategory = 1
          AND m.DividendPS > 0
          AND m.EndDate >= DATEADD(YEAR, -{years_for_dividend}, GETDATE())
        ORDER BY s.SecuCode, m.EndDate DESC
        """
        
        # 3. 查询历史估值数据（用于计算分位数）
        # DZ_DIndicesForValuation表包含不同交易日期的历史数据，参考fundamental_analysis.py的实现方式
        # 使用TradingDay字段查询历史数据
        df_historical_valuation = pd.DataFrame()
        
        # 参考fundamental_analysis.py的实现：直接查询历史数据，不包装在try-except中
        # 如果字段名不对，会抛出异常，便于调试
        # 注意：这里不要再瞎猜日期字段名，直接把整张表里该股票的历史PB / PE都拿出来，
        # 只依赖 InnerCode 关联和 PB / PETTMCut 两个字段，后续在 Python 里做年份过滤和清洗。
        # 这样可以最大程度贴近其他模块「先把历史值取全，再在内存里算分位数」的做法。
        sql_historical_valuation = f"""
        SELECT 
            s.SecuCode,
            v.PB,
            v.PETTMCut as PE
        FROM SecuMain s
        INNER JOIN DZ_DIndicesForValuation v ON s.InnerCode = v.InnerCode
        WHERE s.SecuCode IN ({codes_str})
          AND s.SecuCategory = 1
          AND v.PB IS NOT NULL
          AND v.PETTMCut IS NOT NULL
        """
        
        # 并行执行3个SQL查询（参考fundamental_analysis.py的实现方式）
        from concurrent.futures import ThreadPoolExecutor
        logger.info(f"开始并行查询基本面数据（共 {len(codes)} 只股票）...")
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_fundamental = executor.submit(fetcher.query, sql_fundamental)
            future_dividend = executor.submit(fetcher.query, sql_dividend)
            future_historical = executor.submit(fetcher.query, sql_historical_valuation)
            
            df_fundamental = future_fundamental.result()
            df_dividend = future_dividend.result()
            df_historical_valuation = future_historical.result()
            
            # 检查历史估值数据查询结果（参考fundamental_analysis.py的处理方式）
            if df_historical_valuation is None:
                logger.warning("历史估值数据查询返回None，可能SQL查询出错")
                df_historical_valuation = pd.DataFrame()
            elif df_historical_valuation.empty:
                logger.warning(f"历史估值数据查询成功但数据为空，共查询 {len(codes)} 只股票，SQL条件可能过严")
                logger.debug(f"历史估值SQL: {sql_historical_valuation[:200]}...")
            else:
                unique_codes = df_historical_valuation['SecuCode'].nunique()
                logger.info(f"历史估值数据查询成功，获取到 {len(df_historical_valuation)} 条记录，涉及 {unique_codes} 只股票")
                # 统计每只股票的历史数据点数
                if unique_codes > 0:
                    code_counts = df_historical_valuation.groupby('SecuCode').size()
                    logger.debug(f"历史数据点数统计: 平均 {code_counts.mean():.1f} 点/股票，最少 {code_counts.min()} 点，最多 {code_counts.max()} 点")
        
        # 如果查询失败，返回空结果
        if df_fundamental is None:
            df_fundamental = pd.DataFrame()
        if df_dividend is None:
            df_dividend = pd.DataFrame()
        if df_historical_valuation is None:
            df_historical_valuation = pd.DataFrame()
        
        # 处理历史估值数据（用于计算分位数）
        historical_pb_data = {}  # {code: [pb_values]}
        historical_pe_data = {}  # {code: [pe_values]}
        
        if not df_historical_valuation.empty:
            df_historical_valuation['SecuCode'] = df_historical_valuation['SecuCode'].astype(str).str.zfill(6)
            historical_groups = df_historical_valuation.groupby('SecuCode')
            
            logger.info(f"处理历史估值数据，共 {len(historical_groups)} 只股票有历史数据")
            
            for code, group in historical_groups:
                # 提取PB和PE的历史值
                pb_values = group['PB'].dropna().tolist()
                pe_values = group['PE'].dropna().tolist()
                
                if pb_values:
                    # 过滤掉无效值（<=0的值）
                    pb_values = [v for v in pb_values if v is not None and not pd.isna(v) and v > 0]
                    if pb_values:
                        historical_pb_data[code] = pb_values
                
                if pe_values:
                    # 过滤掉无效值（<=0的值）
                    pe_values = [v for v in pe_values if v is not None and not pd.isna(v) and v > 0]
                    if pe_values:
                        historical_pe_data[code] = pe_values
            
            logger.info(f"历史PB数据: {len(historical_pb_data)} 只股票，历史PE数据: {len(historical_pe_data)} 只股票")
        else:
            logger.warning("历史估值数据为空，无法计算分位数")
        
        # 优化：使用向量化处理，避免逐行迭代
        if not df_fundamental.empty:
            df_fundamental['SecuCode'] = df_fundamental['SecuCode'].astype(str).str.zfill(6)
            for code in df_fundamental['SecuCode'].unique():
                row = df_fundamental[df_fundamental['SecuCode'] == code].iloc[0]
                current_pb = float(row['PB_MRQ']) if pd.notna(row['PB_MRQ']) else None
                current_pe = float(row['PE_TTM']) if pd.notna(row['PE_TTM']) else None
                
                # 计算分位数
                pb_percentile = None
                pe_percentile = None
                
                # 修复：使用 is not None 而不是直接判断真值（因为PB/PE可能是0或负数）
                if current_pb is not None and current_pb > 0 and code in historical_pb_data:
                    pb_percentile = calculate_percentile(current_pb, historical_pb_data[code])
                    if pb_percentile is None:
                        logger.debug(f"股票 {code} PB分位数计算失败: current_pb={current_pb}, 历史数据点数={len(historical_pb_data[code])}")
                
                if current_pe is not None and current_pe > 0 and code in historical_pe_data:
                    pe_percentile = calculate_percentile(current_pe, historical_pe_data[code])
                    if pe_percentile is None:
                        logger.debug(f"股票 {code} PE分位数计算失败: current_pe={current_pe}, 历史数据点数={len(historical_pe_data[code])}")
                
                # 调试日志：如果当前值存在但历史数据不存在
                if current_pb is not None and current_pb > 0 and code not in historical_pb_data:
                    logger.debug(f"股票 {code} 有当前PB值({current_pb})但无历史PB数据")
                if current_pe is not None and current_pe > 0 and code not in historical_pe_data:
                    logger.debug(f"股票 {code} 有当前PE值({current_pe})但无历史PE数据")
                
                result[code] = {
                    'pb_mrq': current_pb,
                    'pe_ttm': current_pe,
                    'roe_ttm': float(row['ROE_TTM']) if pd.notna(row['ROE_TTM']) else None,
                    'pb_percentile': pb_percentile,
                    'pe_percentile': pe_percentile,
                    'dividend_info': [],
                    'dividend_yield': None,
                    'dividend_yield_avg_3y': None
                }
        
        # 优化：使用groupby批量处理分红数据，避免逐行迭代
        if not df_dividend.empty:
            df_dividend['SecuCode'] = df_dividend['SecuCode'].astype(str).str.zfill(6)
            dividend_groups = df_dividend.groupby('SecuCode')
            
            for code, group in dividend_groups:
                if code not in result:
                    continue
                
                # 获取当前股价（优化：只获取一次）
                current_price = None
                if code in stock_data_dict and not stock_data_dict[code].empty:
                    stock_df = stock_data_dict[code]
                    if 'Close' in stock_df.columns:
                        latest_date = stock_df.index.max()
                        current_price = float(stock_df.loc[latest_date, 'Close'])
                
                # 处理分红数据（取最近3年）
                dividends = []
                for _, row in group.head(3).iterrows():  # 只取前3条
                    dividends.append({
                        'date': row['EndDate'],
                        'dividend_ps': float(row['DividendPS']) if pd.notna(row['DividendPS']) else 0,
                        'payout_ratio': float(row['DividendPayoutRatio']) if pd.notna(row['DividendPayoutRatio']) else None
                    })
                
                result[code]['dividend_info'] = dividends
                
                # 计算股息率
                if current_price and current_price > 0 and dividends:
                    # 计算近3年平均股息率
                    total_dividend = sum([d['dividend_ps'] for d in dividends])
                    avg_dividend_yield = (total_dividend / current_price) * 100 if total_dividend > 0 else None
                    result[code]['dividend_yield_avg_3y'] = round(avg_dividend_yield, 2) if avg_dividend_yield else None
                    
                    # 计算最新一年的股息率
                    latest_dividend = dividends[0]['dividend_ps']
                    latest_dividend_yield = (latest_dividend / current_price) * 100 if latest_dividend > 0 else None
                    result[code]['dividend_yield'] = round(latest_dividend_yield, 2) if latest_dividend_yield else None
                else:
                    result[code]['dividend_yield'] = None
                    result[code]['dividend_yield_avg_3y'] = None
        
        # 填充缺失的股票（确保所有代码都是6位数字格式）
        for code in codes:
            # 统一格式化为6位数字字符串
            normalized_code = str(code).strip().zfill(6)
            # 如果代码包含非数字字符（如.SZ），提取数字部分
            if not normalized_code.isdigit():
                # 提取前6位数字
                digits = ''.join([c for c in normalized_code if c.isdigit()])[:6]
                normalized_code = digits.zfill(6) if digits else normalized_code
            
            if normalized_code not in result:
                result[normalized_code] = {
                    'pb_mrq': None,
                    'pe_ttm': None,
                    'roe_ttm': None,
                    'pb_percentile': None,
                    'pe_percentile': None,
                    'dividend_info': [],
                    'dividend_yield': None,
                    'dividend_yield_avg_3y': None
                }
        
        return result
        
    except Exception as e:
        logger.error(f"获取扩展基本面数据失败: {e}")
        # 返回空字典
        return {code: {
            'pb_mrq': None,
            'pe_ttm': None,
            'roe_ttm': None,
            'pb_percentile': None,
            'pe_percentile': None,
            'dividend_info': [],
            'dividend_yield': None,
            'dividend_yield_avg_3y': None
        } for code in codes}


def format_stock_code(code: str) -> str:
    """
    格式化股票代码，添加交易所后缀
    - 深圳股票（00、30开头）：添加.SZ
    - 上海股票（60、68开头）：添加.SH
    
    参数：
    - code: 股票代码（6位数字字符串）
    
    返回：
    - 格式化后的股票代码（带后缀）
    """
    if not code:
        return code
    
    code_str = str(code).strip()
    
    # 如果已经有后缀，直接返回
    if '.' in code_str:
        return code_str
    
    # 根据股票代码前缀添加后缀
    if code_str.startswith('00') or code_str.startswith('30'):
        return f"{code_str}.SZ"  # 深圳
    elif code_str.startswith('60') or code_str.startswith('68'):
        return f"{code_str}.SH"  # 上海
    elif code_str.startswith('8'):
        return f"{code_str}.BJ"  # 北交所
    else:
        return code_str  # 未知交易所，保持原样


def analyze_near_lowest_price(
    code: str,
    stock_data: pd.DataFrame,
    lookback_years: int = 3,
    price_tolerance: float = 0.10,
    cutoff_date: Optional[date] = None
) -> Optional[Dict[str, Any]]:
    """
    分析股票是否接近历史最低价
    
    参数：
    - code: 股票代码
    - stock_data: 股票历史数据（DataFrame，索引为日期，包含Close列）
    - lookback_years: 回看年数（默认3年）
    - price_tolerance: 价格容差（默认0.10，即±10%）
    
    返回：
    - 包含分析结果的字典，如果不满足条件或出错则返回None
    """
    try:
        if stock_data is None or stock_data.empty:
            return None
        
        # 确保有Close列
        if 'Close' not in stock_data.columns:
            return None
        
        # 确保索引是日期类型
        if not isinstance(stock_data.index, pd.DatetimeIndex):
            try:
                stock_data.index = pd.to_datetime(stock_data.index)
            except:
                return None
        
        # 按日期排序
        stock_data = stock_data.sort_index()
        
        # 如果提供了cutoff_date，过滤数据只到该日期
        if cutoff_date is not None:
            # 转换cutoff_date为date类型
            if isinstance(cutoff_date, str):
                cutoff_date_obj = pd.to_datetime(cutoff_date).date()
            elif isinstance(cutoff_date, pd.Timestamp):
                cutoff_date_obj = cutoff_date.date()
            elif isinstance(cutoff_date, date):
                cutoff_date_obj = cutoff_date
            else:
                cutoff_date_obj = None
            
            if cutoff_date_obj is not None:
                # 转换cutoff_date为Timestamp以便比较
                cutoff_timestamp = pd.Timestamp(cutoff_date_obj)
                # 过滤数据，只保留到cutoff_date的数据
                stock_data = stock_data[stock_data.index <= cutoff_timestamp]
                if stock_data.empty:
                    return None
                # 找到stock_data中<=cutoff_date的最大日期作为latest_date
                valid_dates = stock_data.index[stock_data.index <= cutoff_timestamp]
                if len(valid_dates) == 0:
                    return None
                latest_date = valid_dates.max()
            else:
                # 转换失败，使用数据中的最大日期
                latest_date = stock_data.index.max()
        else:
            # 没有cutoff_date，使用数据中的最大日期
            latest_date = stock_data.index.max()
        
        # 获取最新交易日的数据
        latest_close = stock_data.loc[latest_date, 'Close']
        
        if pd.isna(latest_close) or latest_close <= 0:
            return None
        
        # 计算回看日期（3年前）
        # 修复：使用更大的日期范围确保有足够的交易日数据（3年约750个交易日，但日历天数是1095天）
        # 多获取100天作为缓冲，确保有足够的交易日数据
        lookback_date = latest_date - pd.Timedelta(days=lookback_years * 365 + 100)
        
        # 筛选3年内的数据
        historical_data = stock_data[stock_data.index >= lookback_date]
        
        # 要求1：如果历史数据天数少于200天，不纳入统计范围
        if len(historical_data) < 200:
            return None  # 直接返回None，不纳入统计
        
        # 确保至少有足够的交易日数据（3年约750个交易日，至少需要500个交易日）
        min_required_trading_days = lookback_years * 250  # 每年约250个交易日
        if len(historical_data) < min_required_trading_days * 0.6:  # 至少需要60%的交易日
            # 数据不足，返回失败
            return {
                '股票代码': code,
                '最新日期': latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date),
                '最新收盘价': latest_close,
                '3年最低价': None,
                '3年最高价': None,
                '当前价相对最低价涨幅': None,
                '当前价相对最高价跌幅': None,
                '是否接近最低价': False,
                '失败原因': f'历史数据不足（实际{len(historical_data)}个交易日，需要至少{int(min_required_trading_days*0.6)}个交易日）'
            }
        
        if historical_data.empty or len(historical_data) < 10:
            return {
                '股票代码': code,
                '最新日期': latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date),
                '最新收盘价': latest_close,
                '3年最低价': None,
                '3年最高价': None,
                '当前价相对最低价涨幅': None,
                '当前价相对最高价跌幅': None,
                '是否接近最低价': False,
                '失败原因': '历史数据不足（少于10个交易日）'
            }
        
        # 计算3年内的最低价和最高价
        min_price = historical_data['Close'].min()
        max_price = historical_data['Close'].max()
        
        if pd.isna(min_price) or min_price <= 0:
            return {
                '股票代码': code,
                '最新日期': latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date),
                '最新收盘价': latest_close,
                '3年最低价': None,
                '3年最高价': None,
                '当前价相对最低价涨幅': None,
                '当前价相对最高价跌幅': None,
                '是否接近最低价': False,
                '失败原因': '无法计算最低价'
            }
        
        # 计算当前价格相对最低价的涨幅（百分比）
        price_increase_from_low = (latest_close - min_price) / min_price
        
        # 计算当前价格相对最高价的跌幅（百分比）
        price_decrease_from_high = (max_price - latest_close) / max_price if max_price > 0 else None
        
        # 判断是否在最低价的±10%范围内
        # 定义：当前价格接近最低价，即当前价格在 [最低价*(1-tolerance), 最低价*(1+tolerance)] 范围内
        # 注意：通常"接近最低价"意味着当前价应该 >= 最低价，不应该低于最低价
        # 但如果允许±10%范围，则可能包括低于最低价的情况（比如最低价10元，当前价9元，在±10%范围内）
        # 这里采用严格定义：当前价必须 >= 最低价，且在最低价的±10%范围内
        price_lower_bound = min_price * (1 - price_tolerance)
        price_upper_bound = min_price * (1 + price_tolerance)
        is_near_lowest = (latest_close >= price_lower_bound) and \
                        (latest_close <= price_upper_bound) and \
                        (latest_close >= min_price)  # 不允许低于最低价（接近最低价应该是高于或等于最低价）
        
        # 找到最低价出现的日期
        min_price_date = historical_data[historical_data['Close'] == min_price].index[0]
        
        # 找到最高价出现的日期
        max_price_date = historical_data[historical_data['Close'] == max_price].index[0]
        
        result = {
            '股票代码': code,
            '最新日期': latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date),
            '最新收盘价': round(latest_close, 2),
            '3年最低价': round(min_price, 2),
            '3年最低价日期': min_price_date.strftime('%Y-%m-%d') if hasattr(min_price_date, 'strftime') else str(min_price_date),
            '3年最高价': round(max_price, 2),
            '3年最高价日期': max_price_date.strftime('%Y-%m-%d') if hasattr(max_price_date, 'strftime') else str(max_price_date),
            '当前价相对最低价涨幅': round(price_increase_from_low * 100, 2),
            '当前价相对最高价跌幅': round(price_decrease_from_high * 100, 2) if price_decrease_from_high is not None else None,
            '价格区间位置': round((latest_close - min_price) / (max_price - min_price) * 100, 2) if max_price > min_price else None,
            '是否接近最低价': is_near_lowest,
            '历史数据天数': len(historical_data),
            '失败原因': None if is_near_lowest else f'当前价相对最低价涨幅{price_increase_from_low*100:.2f}%，超过±{price_tolerance*100:.0f}%范围'
        }
        
        return result
        
    except Exception as e:
        logger.error(f"分析股票 {code} 时出错: {e}", exc_info=True)
        return {
            '股票代码': code,
            '最新日期': None,
            '最新收盘价': None,
            '3年最低价': None,
            '3年最高价': None,
            '当前价相对最低价涨幅': None,
            '当前价相对最高价跌幅': None,
            '是否接近最低价': False,
            '失败原因': f'分析出错: {str(e)}'
        }


def run_near_lowest_price_screening(
    limit: Optional[int] = None,
    max_days_ago: Optional[int] = None,
    lookback_years: int = 3,
    price_tolerance: float = 0.10,
    output_dir: str = "outputs",
    cutoff_date: Optional[date] = None
) -> Optional[str]:
    """
    运行接近历史最低价筛选
    
    参数：
    - limit: 股票数量限制，None 使用配置中的 STOCK_LIST_LIMIT
    - max_days_ago: 最大允许行情滞后天数，None 使用配置中的 MAX_TRADING_DAYS_AGO
    - lookback_years: 回看年数（默认3年）
    - price_tolerance: 价格容差（默认0.10，即±10%）
    - output_dir: 输出目录
    
    返回：
    - 输出文件路径，如果失败则返回None
    """
    from logger_config import init_logger
    
    session_logger = init_logger("logs")
    
    if limit is None:
        limit = STOCK_LIST_LIMIT
    if max_days_ago is None:
        max_days_ago = MAX_TRADING_DAYS_AGO
    
    print("=" * 60)
    print("功能14：接近历史最低价筛选")
    print("=" * 60)
    print(f"股票数量上限: {limit}")
    print(f"最大允许行情滞后天数: {max_days_ago}")
    print(f"回看年数: {lookback_years}年")
    print(f"价格容差: ±{price_tolerance*100:.0f}%")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        fetcher = JuyuanDataFetcher(use_connection_pool=True)
        
        # 确定截止日期
        if cutoff_date is not None:
            end_date = cutoff_date
            print(f"📅 回测模式 - 指定截止日期: {end_date}")
        else:
            end_date = fetcher.get_latest_trading_date()
            print(f"📅 数据库最新交易日: {end_date}")
        print("=" * 60)
        
        # 1. 获取活跃股票列表
        print("📊 获取活跃股票列表（正在查询数据库，请稍候...）...")
        list_start_time = time.time()
        stock_info_list = fetcher.get_stock_list(limit=limit, max_days_ago=max_days_ago, cutoff_date=cutoff_date)
        list_elapsed = time.time() - list_start_time
        print(f"  ✅ 股票列表获取完成（耗时: {list_elapsed:.1f}秒）")
        
        # 兼容两种返回格式：字符串列表 或 字典列表
        if stock_info_list and isinstance(stock_info_list[0], dict):
            codes = [info["code"] for info in stock_info_list]
        else:
            codes = stock_info_list
        
        if not codes:
            print("❌ 未获取到任何活跃股票")
            return None
        
        print(f"✅ 实际股票数量: {len(codes)}")
        
        # 2. 批量获取股票数据（完全复用功能13的逻辑）
        print("📈 批量获取行情数据（增量缓存 + 批量SQL + 多线程并发）...")
        
        # 计算日期范围（需要3年+缓冲期的数据）
        days_needed = lookback_years * 365 + 30  # 3年 + 30天缓冲
        # end_date 已在上面确定（cutoff_date 或最新交易日）
        start_date = end_date - timedelta(days=days_needed + 30)  # 多获取30天作为缓冲
        
        # 尝试从缓存加载（批量）
        print("  🔍 检查增量缓存...")
        print(f"  📅 请求日期范围: {start_date} 至 {end_date}（数据库最新交易日，需要{days_needed}天数据）")
        cached_stock_data, missing_stock_codes = futures_incremental_cache_manager.load_stocks_data(
            codes, start_date, end_date
        )
        
        # 检查缓存数据的完整性（使用统一的缓存验证函数）
        insufficient_data_codes = []  # 数据不足的股票（需要补全）
        partial_data_codes = {}  # 部分命中的股票（缓存有数据但需要补全缺失日期范围）
        valid_cached_data = {}  # 完全有效的缓存数据
        
        for code, data in cached_stock_data.items():
            # 使用统一的缓存验证函数
            validation_result = validate_cache_data(
                data=data,
                start_date=start_date,
                end_date=end_date,
                days_needed=days_needed
            )
            
            if validation_result.is_valid:
                # 缓存数据完全有效
                valid_cached_data[code] = data
            elif validation_result.is_partial:
                # 缓存部分有效，需要增量补全
                partial_data_codes[code] = {
                    'cached_data': data,
                    'missing_start': validation_result.missing_start,
                    'missing_end': validation_result.missing_end,
                    'cache_start': validation_result.cache_start,
                    'cache_end': validation_result.cache_end
                }
            else:
                # 缓存数据无效，需要重新获取全部数据
                insufficient_data_codes.append(code)
        
        # 合并需要重新获取的股票代码（完全缺失的 + 数据太少的）
        all_missing_codes = list(set(missing_stock_codes + insufficient_data_codes))
        cache_hit_count = len(valid_cached_data)
        partial_hit_count = len(partial_data_codes)
        cache_miss_count = len(all_missing_codes)
        
        print(f"  ✅ 缓存完全有效: {cache_hit_count} 只股票")
        if partial_hit_count > 0:
            print(f"  🔄 缓存部分命中: {partial_hit_count} 只股票（将增量补全缺失日期范围）")
        print(f"  ⚠️  需要从数据库获取: {cache_miss_count} 只股票")
        
        # 对缺失或数据不足的股票从数据库获取（批量）
        # 优化：区分完全缺失和部分命中的股票，部分命中的只获取缺失日期范围
        fetch_start_time = time.time()
        fetched_stock_data = {}
        
        # 1. 处理完全缺失的股票：获取全部数据
        if all_missing_codes:
            print(f"  📥 从数据库获取完全缺失的 {len(all_missing_codes)} 只股票数据（全部3年数据）...")
            optimized_time_config = SimpleNamespace(
                crash_start_date=start_date,
                crash_end_date=end_date
            )
            fully_missing_data = fetcher.batch_get_stock_data_with_adjustment(
                all_missing_codes,
                days=days_needed,
                time_config=optimized_time_config
            )
            if fully_missing_data:
                fetched_stock_data.update(fully_missing_data)
                print(f"  ✅ 完全缺失股票数据获取完成: {len(fully_missing_data)} 只")
        
        # 2. 处理部分命中的股票：只获取缺失的日期范围（增量补全）
        if partial_data_codes:
            print(f"  🔄 增量补全部分命中的 {len(partial_data_codes)} 只股票数据（只获取缺失日期范围）...")
            
            # 按缺失日期范围分组，相同范围的股票一起获取
            date_range_groups = {}  # {(missing_start, missing_end): [codes]}
            
            for code, partial_info in partial_data_codes.items():
                missing_start = partial_info.get('missing_start')
                missing_end = partial_info.get('missing_end')
                cache_start = partial_info.get('cache_start')
                cache_end = partial_info.get('cache_end')
                
                # 确定需要获取的日期范围
                if missing_start and missing_end:
                    # 有明确的缺失范围
                    fetch_start = missing_start
                    fetch_end = missing_end
                elif missing_end:
                    # 只缺结束日期（缓存数据不够新）
                    fetch_start = cache_end + timedelta(days=1) if cache_end else start_date
                    fetch_end = missing_end
                elif missing_start:
                    # 只缺开始日期（缓存数据不够旧）
                    fetch_start = missing_start
                    fetch_end = cache_start - timedelta(days=1) if cache_start else end_date
                else:
                    # 没有明确缺失范围，使用缓存数据的边界
                    fetch_start = cache_end + timedelta(days=1) if cache_end else start_date
                    fetch_end = end_date
                
                # 确保日期范围合理
                fetch_start = max(fetch_start, start_date)
                fetch_end = min(fetch_end, end_date)
                
                if fetch_start <= fetch_end:
                    range_key = (fetch_start, fetch_end)
                    if range_key not in date_range_groups:
                        date_range_groups[range_key] = []
                    date_range_groups[range_key].append(code)
            
            # 对每个日期范围组，批量获取数据
            from tqdm import tqdm
            
            total_partial_groups = len(date_range_groups)
            print(f"    分为 {total_partial_groups} 个日期范围组进行增量补全...")
            
            with tqdm(total=total_partial_groups, desc="  增量补全", unit="组", ncols=100) as pbar:
                for (fetch_start, fetch_end), codes_in_group in date_range_groups.items():
                    # 计算需要获取的天数
                    days_to_fetch = (fetch_end - fetch_start).days + 30  # 多获取30天缓冲
                    
                    # 创建时间配置（供 data_fetcher 的 crash_start_date / crash_end_date 使用）
                    partial_time_config = SimpleNamespace(
                        crash_start_date=fetch_start,
                        crash_end_date=fetch_end
                    )
                    
                    # 批量获取这个日期范围的数据
                    partial_fetched = fetcher.batch_get_stock_data_with_adjustment(
                        codes_in_group,
                        days=days_to_fetch,
                        time_config=partial_time_config
                    )
                    
                    if partial_fetched:
                        fetched_stock_data.update(partial_fetched)
                    
                    pbar.update(1)
                    pbar.set_postfix({
                        '已补全': f'{len(fetched_stock_data)}只',
                        '当前组': f'{len(codes_in_group)}只',
                        '日期范围': f'{fetch_start}~{fetch_end}'
                    })
            
            print(f"  ✅ 部分命中股票增量补全完成: {len([c for c in fetched_stock_data.keys() if c in partial_data_codes])} 只")
        
        fetch_elapsed = time.time() - fetch_start_time
        print(f"  ✅ 数据获取完成（总耗时: {fetch_elapsed:.1f}秒）")
        
        # 处理部分命中的股票：合并缓存数据和新获取的增量数据
        # 注意：必须在保存缓存之前合并，这样保存的就是完整数据
        if partial_data_codes:
            print(f"  🔄 合并部分命中的缓存数据...")
            merged_count = 0
            for code, partial_info in partial_data_codes.items():
                cached_data = partial_info['cached_data']
                
                # 如果新获取的数据中包含这只股票，合并数据
                if code in fetched_stock_data:
                    new_data = fetched_stock_data[code]
                    # 合并缓存数据和新数据（去重，保留最新数据）
                    if not new_data.empty:
                        # 使用concat合并，然后去重（保留新数据）
                        combined_data = pd.concat([cached_data, new_data])
                        # 按索引去重，保留最后一个（新数据优先）
                        combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
                        combined_data = combined_data.sort_index()
                        # 筛选到目标日期范围
                        mask = (combined_data.index.date >= start_date) & (combined_data.index.date <= end_date)
                        combined_data = combined_data[mask]
                        if not combined_data.empty:
                            # 将合并后的完整数据更新到 fetched_stock_data，这样保存缓存时就是完整数据
                            fetched_stock_data[code] = combined_data
                            valid_cached_data[code] = combined_data
                            merged_count += 1
                            continue
                
                # 如果没有新数据，但缓存数据足够（部分有效的缓存至少有50个交易日），直接使用缓存
                # 这部分命中的股票已经被validate_cache_data判定为部分有效（至少50个交易日或30%覆盖率）
                if len(cached_data) >= 50:
                    valid_cached_data[code] = cached_data
            
            if merged_count > 0:
                print(f"  ✅ 成功合并 {merged_count} 只部分命中股票的数据")
        
        # 保存新获取的数据到缓存（批量）
        # 注意：此时 fetched_stock_data 中已经包含了合并后的完整数据（对于部分命中的股票）
        if fetched_stock_data:
            print(f"  💾 保存 {len(fetched_stock_data)} 只股票的数据到增量缓存...")
            save_result = futures_incremental_cache_manager.save_stocks_data(
                list(fetched_stock_data.keys()),
                fetched_stock_data,
                start_date,
                end_date
            )
            if save_result:
                print(f"  ✅ 缓存保存完成（成功保存 {len(fetched_stock_data)} 只股票）")
            else:
                print(f"  ⚠️  缓存保存失败，请检查日志")
            
            # 合并有效缓存数据和新增数据
            valid_cached_data.update(fetched_stock_data)
        
        all_stock_data = valid_cached_data
        
        if not all_stock_data:
            print("❌ 未获取到任何股票行情数据")
            return None
        
        # 修复：计算缓存命中率（包括完全命中和部分命中）
        total_stocks = cache_hit_count + partial_hit_count + cache_miss_count
        if total_stocks > 0:
            cache_hit_rate = (cache_hit_count + partial_hit_count) / total_stocks * 100
        else:
            cache_hit_rate = 0.0
        
        print(f"✅ 成功获取 {len(all_stock_data)} 只股票的行情数据（缓存命中率: {cache_hit_rate:.1f}%，完全命中: {cache_hit_count}只，部分命中: {partial_hit_count}只）")
        
        # 3. 并行分析每只股票（完全复用功能13的多线程逻辑）
        print("🚀 并行分析接近历史最低价...")
        
        def _task_analyze(code: str,
                         stock_data_dict: Dict[str, pd.DataFrame],
                         lookback_years: int,
                         price_tolerance: float,
                         cutoff_date: Optional[date] = None) -> Optional[Dict]:
            """供高性能线程池调用的任务函数"""
            df = stock_data_dict.get(code)
            if df is None or df.empty:
                return None
            
            # 调用分析函数
            result = analyze_near_lowest_price(
                code=code,
                stock_data=df,
                lookback_years=lookback_years,
                price_tolerance=price_tolerance,
                cutoff_date=cutoff_date
            )
            
            return result
        
        # 使用高性能线程池（完全复用功能13的逻辑）
        thread_pool = HighPerformanceThreadPool(progress_desc="接近历史最低价分析")
        
        results = thread_pool.execute_batch(
            list(all_stock_data.keys()),
            _task_analyze,
            all_stock_data,
            lookback_years,
            price_tolerance,
            end_date  # 传递end_date作为cutoff_date
        )
        
        # 汇总结果
        valid_results = [r for r in results if r is not None]
        near_lowest_results = [r for r in valid_results if r.get('是否接近最低价', False)]
        not_near_lowest_results = [r for r in valid_results if not r.get('是否接近最低价', False)]
        
        print(f"\n✅ 发现 {len(near_lowest_results)} 只股票接近历史最低价（±{price_tolerance*100:.0f}%）")
        if len(not_near_lowest_results) > 0:
            print(f"📋 另有 {len(not_near_lowest_results)} 只股票不满足条件（将一并导出详细信息）")
        
        # 4. 获取全部有效股票的基本面数据（满足与不满足均导出判断指标明细，含PB/PE、股息率等）
        fundamental_data = {}
        all_codes = [r.get('股票代码') for r in valid_results if r.get('股票代码')]
        if all_codes:
            print("\n📊 获取全部股票基本面数据（PB/PE、股息率，供明细导出）...")
            fundamental_start_time = time.time()
            
            try:
                qualified_codes = all_codes
                
                if qualified_codes:
                    # 统一格式化股票代码为6位数字（确保与缓存格式一致）
                    normalized_qualified_codes = []
                    for code in qualified_codes:
                        code_str = str(code).strip().zfill(6)
                        # 如果代码包含非数字字符（如.SZ），提取数字部分
                        if not code_str.isdigit():
                            digits = ''.join([c for c in code_str if c.isdigit()])[:6]
                            code_str = digits.zfill(6) if digits else code_str
                        normalized_qualified_codes.append(code_str)
                    
                    # 先尝试从缓存加载
                    print(f"  🔍 检查 {len(normalized_qualified_codes)} 只符合条件的股票基本面数据缓存...")
                    cached_fundamental_data, missing_codes = futures_incremental_cache_manager.load_fundamental_data(normalized_qualified_codes)

                    # 二次校验：老版本缓存里没有 PE/PB 分位数字段，本次需要强制重算
                    # 逻辑：凡是缓存中 pb_percentile 或 pe_percentile 为空的股票，统一视为“需要更新”的缺失代码
                    incomplete_codes = []
                    if cached_fundamental_data:
                        for c, data in cached_fundamental_data.items():
                            if not isinstance(data, dict):
                                incomplete_codes.append(c)
                                continue
                            pb_p = data.get('pb_percentile')
                            pe_p = data.get('pe_percentile')
                            # 老缓存（只有 PB / PE / ROE / 分红，但没有分位数）或分位数为 None，都认为需要重算
                            if pb_p is None and pe_p is None:
                                incomplete_codes.append(c)

                    # 合并“物理缺失的代码”和“缓存结构不完整的代码”
                    # 注意：这里都用标准的 6 位代码（与 normalized_qualified_codes 一致）
                    if incomplete_codes:
                        logger.info(f"基本面缓存中有 {len(incomplete_codes)} 只股票缺少分位数信息，本次将强制重新计算这些股票的PE/PB分位数")
                    missing_codes = list(sorted(set(list(missing_codes) + incomplete_codes)))

                    if cached_fundamental_data:
                        # 先把缓存里“看起来完整”的数据放入结果，其余缺失/不完整的待会儿用新结果覆盖
                        fundamental_data.update(cached_fundamental_data)
                        cache_hit_count = len(cached_fundamental_data) - len(incomplete_codes)
                        print(f"  ✅ 缓存命中(完整): {cache_hit_count} 只股票")
                        if missing_codes:
                            preview_codes = missing_codes[:5]
                            print(f"  ⚠️  需要从数据库获取/重算: {len(missing_codes)} 只股票（例: {preview_codes}{'...' if len(missing_codes) > 5 else ''}）")
                    
                    # 如果有缺失/需要重算的股票，从数据库获取
                    if missing_codes:
                        print(f"  📥 从数据库获取 / 重新计算 {len(missing_codes)} 只股票的基本面数据...")
                        batch_size = 500  # 每批500只股票，避免SQL查询过大
                        
                        from tqdm import tqdm
                        total_batches = (len(missing_codes) + batch_size - 1) // batch_size
                        
                        fetched_data = {}
                        with tqdm(total=total_batches, desc="  基本面数据查询", unit="批", ncols=100) as pbar:
                            for i in range(0, len(missing_codes), batch_size):
                                batch_codes = missing_codes[i:i+batch_size]
                                batch_data = get_extended_fundamental_data(
                                    codes=batch_codes,
                                    fetcher=fetcher,
                                    stock_data_dict=all_stock_data,
                                    years_for_percentile=5,
                                    years_for_dividend=3
                                )
                                fetched_data.update(batch_data)
                                pbar.update(1)
                                pbar.set_postfix({
                                    '已获取': f'{len(fetched_data)}只',
                                    '进度': f'{(i+batch_size)*100//len(missing_codes) if missing_codes else 0}%'
                                })
                        
                        # 更新结果
                        fundamental_data.update(fetched_data)
                        
                        # 保存新获取的数据到缓存
                        if fetched_data:
                            print(f"  💾 保存 {len(fetched_data)} 只股票的基本面数据到缓存...")
                            futures_incremental_cache_manager.save_fundamental_data(fetched_data)
                            print(f"  ✅ 缓存保存完成")
                    
                    fundamental_elapsed = time.time() - fundamental_start_time
                    print(f"  ✅ 基本面数据获取完成（耗时: {fundamental_elapsed:.1f}秒，共 {len(fundamental_data)} 只股票）")
                
            except Exception as e:
                logger.error(f"获取基本面数据失败: {e}")
                print(f"  ⚠️  基本面数据获取失败: {e}")
                fundamental_data = {}
        
        # 5. 添加基本面数据到结果中（满足与不满足条件的股票均写入判断指标明细）
        def _apply_fundamental(result: dict, fund_dict: dict) -> None:
            code = result.get('股票代码')
            if not code:
                result['PB'] = result['PE_TTM'] = result['ROE_TTM'] = None
                result['PB分位数(5年)'] = result['PE分位数(5年)'] = result['股息率(%)'] = result['近3年平均股息率(%)'] = None
                result['近3年分红年份'] = '无'
                return
            # 兼容 6 位与带后缀的 code 查找
            fund_data = fund_dict.get(code) or fund_dict.get(str(code).split('.')[0].zfill(6))
            if fund_data:
                result['PB'] = round(fund_data.get('pb_mrq'), 2) if fund_data.get('pb_mrq') is not None else None
                result['PE_TTM'] = round(fund_data.get('pe_ttm'), 2) if fund_data.get('pe_ttm') is not None else None
                result['ROE_TTM'] = round(fund_data.get('roe_ttm'), 2) if fund_data.get('roe_ttm') is not None else None
                result['PB分位数(5年)'] = fund_data.get('pb_percentile')
                result['PE分位数(5年)'] = fund_data.get('pe_percentile')
                result['股息率(%)'] = fund_data.get('dividend_yield')
                result['近3年平均股息率(%)'] = fund_data.get('dividend_yield_avg_3y')
                dividend_info = fund_data.get('dividend_info', [])
                if dividend_info:
                    dividend_years = []
                    for d in dividend_info[:3]:
                        div_date = d.get('date')
                        if div_date:
                            dividend_years.append(str(div_date.year) if hasattr(div_date, 'year') else str(div_date)[:4])
                    result['近3年分红年份'] = ','.join(dividend_years) if dividend_years else '无'
                else:
                    result['近3年分红年份'] = '无'
            else:
                result['PB'] = None
                result['PE_TTM'] = None
                result['ROE_TTM'] = None
                result['PB分位数(5年)'] = None
                result['PE分位数(5年)'] = None
                result['股息率(%)'] = None
                result['近3年平均股息率(%)'] = None
                result['近3年分红年份'] = '无'

        for result in near_lowest_results:
            _apply_fundamental(result, fundamental_data)
        for result in not_near_lowest_results:
            _apply_fundamental(result, fundamental_data)
        
        # 创建输出目录
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # 生成输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"接近历史最低价筛选_{timestamp}.xlsx")
        
        # 导出Excel
        print("📊 导出Excel结果...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: 接近历史最低价的股票
            if near_lowest_results:
                near_lowest_df = pd.DataFrame(near_lowest_results)
                # 要求2：格式化股票代码，添加交易所后缀
                if '股票代码' in near_lowest_df.columns:
                    near_lowest_df['股票代码'] = near_lowest_df['股票代码'].apply(format_stock_code)
                # 按当前价相对最低价涨幅排序（从低到高）
                near_lowest_df = near_lowest_df.sort_values('当前价相对最低价涨幅')
                near_lowest_df.to_excel(writer, sheet_name='接近历史最低价', index=False)
                print(f"  ✅ 接近历史最低价: {len(near_lowest_df)} 只")
            
            # Sheet 2: 不满足条件的股票
            if not_near_lowest_results:
                not_near_lowest_df = pd.DataFrame(not_near_lowest_results)
                # 要求2：格式化股票代码，添加交易所后缀
                if '股票代码' in not_near_lowest_df.columns:
                    not_near_lowest_df['股票代码'] = not_near_lowest_df['股票代码'].apply(format_stock_code)
                # 按当前价相对最低价涨幅排序
                not_near_lowest_df = not_near_lowest_df.sort_values('当前价相对最低价涨幅')
                not_near_lowest_df.to_excel(writer, sheet_name='不满足条件', index=False)
                print(f"  ✅ 不满足条件: {len(not_near_lowest_df)} 只")
        
        elapsed_time = time.time() - start_time
        print("=" * 60)
        print(f"✅ 接近历史最低价筛选完成，共分析 {len(valid_results)} 只股票")
        print(f"输出文件: {output_file}")
        print(f"总耗时: {elapsed_time:.2f} 秒")
        print("=" * 60)
        
        return output_file
        
    except Exception as e:
        logger.error(f"运行接近历史最低价筛选时出错: {e}", exc_info=True)
        print(f"❌ 分析失败: {e}")
        return None

