#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
动量筛选策略模块（功能13）

功能：实现"低位动量+基本面安全垫"的双门槛信号系统
核心逻辑：
1. 动量右侧触发：低位放量综合信号（事件簇合成）
2. 基本面过滤：PB-ROE低估 + 盈利/分红约束
3. 负向剔除：高位放量综合信号排除
4. 执行框架：4通道持有20日，周频选股

技术架构（完全仿照功能12）：
- 数据获取：批量SQL + 连接池 + 增量缓存
- 数据处理：向量化计算 + 预计算指标
- 多线程并发：高性能线程池 + 批量处理
- 缓存管理：增量缓存 + 智能合并
"""

import os
import time
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any, Callable
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd

from data_fetcher import JuyuanDataFetcher
from config import STOCK_LIST_LIMIT, MAX_TRADING_DAYS_AGO
from high_performance_threading import HighPerformanceThreadPool
from futures_incremental_cache_manager import futures_incremental_cache_manager
from cache_validator import validate_cache_with_min_window

logger = logging.getLogger(__name__)

# DolphinDB配置（可选，用于分钟级数据）
try:
    import dolphindb as ddb
    DOLPHINDB_AVAILABLE = True
except ImportError:
    DOLPHINDB_AVAILABLE = False
    logger.warning("DolphinDB未安装，分钟级数据功能将不可用")

# DolphinDB连接配置
DDB_CONFIG = {
    'host': '172.16.134.40',
    'port': 8902,
    'username': 'ddb_test1',
    'password': 'ddb_test1'
}


# ==================== DolphinDB分钟级数据支持 ====================

def get_minute_level_data(
    stock_code: str,
    start_time: str,
    end_time: str,
    ddb_config: Optional[Dict] = None
) -> Optional[pd.DataFrame]:
    """
    从DolphinDB获取分钟级Level2快照数据
    
    参数：
    - stock_code: 证券代码（6位字符串，如'000001'）
    - start_time: 开始时间（格式: '2025.02.14 09:30:00'）
    - end_time: 结束时间（格式: '2025.02.14 15:00:00'）
    - ddb_config: DolphinDB连接配置，默认使用全局配置
    
    返回：
    - DataFrame: 包含TradeTime, LastPrice, TotalVolumeTrade等字段，失败返回None
    """
    if not DOLPHINDB_AVAILABLE:
        return None
    
    config = ddb_config or DDB_CONFIG
    s = ddb.Session()
    
    try:
        conn = s.connect(config['host'], config['port'], config['username'], config['password'])
        if not conn:
            logger.warning(f"DolphinDB连接失败: {stock_code}")
            return None
        
        script = f"""
        select TradeTime, SecurityID, LastPrice, TotalVolumeTrade, 
               TotalValueTrade, HighPrice, LowPrice
        from loadTable('dfs://tonglian_level2','snapshot')
        where SecurityID = '{stock_code}'
          and TradeTime >= {start_time}
          and TradeTime <= {end_time}
        order by TradeTime asc
        """
        
        df = s.run(script)
        
        if df.empty:
            logger.debug(f"DolphinDB查询结果为空: {stock_code}, {start_time} - {end_time}")
            return None
        
        return df
        
    except Exception as e:
        logger.warning(f"获取DolphinDB分钟级数据失败 {stock_code}: {e}")
        return None
    finally:
        try:
            s.close()
        except:
            pass


def batch_get_minute_level_data(
    stock_codes: List[str],
    start_time: str,
    end_time: str,
    ddb_config: Optional[Dict] = None
) -> Dict[str, pd.DataFrame]:
    """
    批量获取多只股票的分钟级数据
    
    参数：
    - stock_codes: 证券代码列表
    - start_time: 开始时间
    - end_time: 结束时间
    - ddb_config: DolphinDB连接配置
    
    返回：
    - Dict[str, DataFrame]: {code: DataFrame}
    """
    if not DOLPHINDB_AVAILABLE or not stock_codes:
        return {}
    
    config = ddb_config or DDB_CONFIG
    s = ddb.Session()
    result = {}
    
    try:
        conn = s.connect(config['host'], config['port'], config['username'], config['password'])
        if not conn:
            logger.warning("DolphinDB批量连接失败")
            return {}
        
        codes_str = ','.join([f"'{code}'" for code in stock_codes])
        script = f"""
        select TradeTime, SecurityID, LastPrice, TotalVolumeTrade, 
               TotalValueTrade, HighPrice, LowPrice
        from loadTable('dfs://tonglian_level2','snapshot')
        where SecurityID in ({codes_str})
          and TradeTime >= {start_time}
          and TradeTime <= {end_time}
        order by SecurityID, TradeTime asc
        """
        
        df = s.run(script)
        
        if df.empty:
            return {}
        
        # 按股票代码分组
        for code in stock_codes:
            code_data = df[df['SecurityID'] == code].copy()
            if not code_data.empty:
                result[code] = code_data
        
        return result
        
    except Exception as e:
        logger.warning(f"批量获取DolphinDB分钟级数据失败: {e}")
        return {}
    finally:
        try:
            s.close()
        except:
            pass


# ==================== 基本面数据获取 ====================

def is_star_market_stock(code: str) -> bool:
    """
    判断是否是科创板股票
    
    参数：
    - code: 股票代码（6位字符串）
    
    返回：
    - bool: 是否是科创板股票（代码以688开头）
    """
    code_str = str(code).zfill(6)
    return code_str.startswith('688')


def get_fundamental_data(
    codes: List[str],
    fetcher: JuyuanDataFetcher,
    progress_callback: Optional[Callable[[int, str], None]] = None
) -> Dict[str, Dict[str, Any]]:
    """
    批量获取基本面数据（PB_MRQ, ROE_TTM, 分红数据）
    
    参数：
    - codes: 股票代码列表
    - fetcher: 数据获取器
    
    返回：
    - Dict[str, Dict]: {code: {pb_mrq, roe_ttm, dividend_paid_last3y, dividend_payout_ratio}}
    """
    result = {}
    
    if not codes:
        return result
    
    try:
        # 批量查询基本面数据
        codes_str = ','.join([f"'{code}'" for code in codes])
        
        # 查询最新财务指标
        # ROETTM 从 LC_MainIndexNew 表，PB 从 DZ_DIndicesForValuation 表
        # DZ_DIndicesForValuation表通过InnerCode关联
        sql_fundamental = f"""
        SELECT 
            s.SecuCode,
            m.ROETTM as ROE_TTM,
            v.PB as PB_MRQ
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
                PB
            FROM DZ_DIndicesForValuation
        ) v ON s.InnerCode = v.InnerCode
        WHERE s.SecuCode IN ({codes_str})
          AND s.SecuCategory = 1
        """
        
        # 查询分红数据（近三年）
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
        ORDER BY s.SecuCode, m.EndDate DESC
        """
        
        # 并行执行两个SQL查询（充分利用硬件性能）
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_fundamental = executor.submit(fetcher.query, sql_fundamental)
            future_dividend = executor.submit(fetcher.query, sql_dividend)
            
            df_fundamental = future_fundamental.result()
            df_dividend = future_dividend.result()
        
        if progress_callback:
            elapsed = time.time() - start_time
            progress_callback(3, f"⚙️  处理财务指标数据... (已耗时: {elapsed:.1f}秒)")
        
        # 处理财务指标数据
        for _, row in df_fundamental.iterrows():
            code = str(row['SecuCode']).zfill(6)
            result[code] = {
                'pb_mrq': float(row['PB_MRQ']) if pd.notna(row['PB_MRQ']) else None,
                'roe_ttm': float(row['ROE_TTM']) if pd.notna(row['ROE_TTM']) else None,
                'dividend_paid_last3y': False,
                'dividend_payout_ratio': None
            }
        
        if progress_callback:
            elapsed = time.time() - start_time
            progress_callback(4, f"⚙️  处理分红数据... (已耗时: {elapsed:.1f}秒)")
        
        # 处理分红数据（检查近三年是否有分红）
        dividend_by_code = {}
        for _, row in df_dividend.iterrows():
            code = str(row['SecuCode']).zfill(6)
            if code not in dividend_by_code:
                dividend_by_code[code] = []
            dividend_by_code[code].append({
                'date': row['EndDate'],
                'payout_ratio': float(row['DividendPayoutRatio']) if pd.notna(row['DividendPayoutRatio']) else None
            })
        
        # 判断近三年是否连续分红
        current_date = date.today()
        for code, dividends in dividend_by_code.items():
            if code not in result:
                continue
            
            # 按日期排序（降序）
            dividends.sort(key=lambda x: x['date'], reverse=True)
            
            # 检查近三年是否有分红记录（简化：检查是否有最近3年的分红）
            years_with_dividend = set()
            for div in dividends:
                if isinstance(div['date'], (datetime, pd.Timestamp)):
                    div_date = div['date'].date() if hasattr(div['date'], 'date') else div['date']
                else:
                    div_date = div['date']
                
                if isinstance(div_date, date):
                    years_with_dividend.add(div_date.year)
            
            # 近三年（当前年、去年、前年）
            recent_years = {current_date.year, current_date.year - 1, current_date.year - 2}
            has_continuous_dividend = len(recent_years & years_with_dividend) >= 2  # 至少2年有分红
            
            result[code]['dividend_paid_last3y'] = has_continuous_dividend
            
            # 获取最近一年的股利支付率
            if dividends:
                latest_payout_ratio = dividends[0]['payout_ratio']
                result[code]['dividend_payout_ratio'] = latest_payout_ratio
        
        if progress_callback:
            elapsed = time.time() - start_time
            progress_callback(5, f"⚙️  填充缺失数据并标记科创板... (已耗时: {elapsed:.1f}秒)")
        
        # 填充缺失的股票（没有基本面数据）
        for code in codes:
            if code not in result:
                # 检查是否是科创板
                is_star_market = is_star_market_stock(code)
                result[code] = {
                    'pb_mrq': None,
                    'roe_ttm': None,
                    'dividend_paid_last3y': False,
                    'dividend_payout_ratio': None,
                    'is_star_market': is_star_market  # 标记是否是科创板
                }
            else:
                # 如果已有数据，也标记是否是科创板
                result[code]['is_star_market'] = is_star_market_stock(code)
        
        return result
        
    except Exception as e:
        logger.error(f"获取基本面数据失败: {e}")
        # 返回空字典，避免后续处理出错，并标记科创板
        return {code: {
            'pb_mrq': None,
            'roe_ttm': None,
            'dividend_paid_last3y': False,
            'dividend_payout_ratio': None,
            'is_star_market': is_star_market_stock(code)
        } for code in codes}


# ==================== 信号计算层 ====================

def detect_daily_events(
    stock_data: pd.DataFrame,
    price_window: int = 120,
    price_quantile_low: float = 0.10,
    price_quantile_high: float = 0.90,
    volume_window: int = 120,
    volume_std_multiplier: float = 1.5
) -> Dict[str, pd.Series]:
    """
    检测日频低位/高位放量事件
    
    参数：
    - stock_data: 股票数据（包含Close, Volume）
    - price_window: 价格滚动窗口（天）
    - price_quantile_low: 低位分位数
    - price_quantile_high: 高位分位数
    - volume_window: 成交量滚动窗口（天）
    - volume_std_multiplier: 成交量标准差倍数
    
    返回：
    - {
        'low_event': Series[bool],  # 低位放量事件
        'high_event': Series[bool],  # 高位放量事件
      }
    """
    if stock_data.empty or 'Close' not in stock_data.columns or 'Volume' not in stock_data.columns:
        return {
            'low_event': pd.Series(dtype=bool),
            'high_event': pd.Series(dtype=bool)
        }
    
    close = stock_data['Close']
    volume = stock_data['Volume']
    
    # 关键修复：确保有足够的数据才计算（至少需要price_window个数据点）
    # 新股/次新股数据不足会导致分位数计算异常，必须提前过滤
    if len(close) < price_window:
        # 数据不足，返回全False的Series
        return {
            'low_event': pd.Series(False, index=close.index),
            'high_event': pd.Series(False, index=close.index)
        }
    
    # 计算滚动价格分位（使用min_periods确保至少有price_window个有效数据点）
    price_quantile_10 = close.rolling(window=price_window, min_periods=price_window).quantile(price_quantile_low)
    price_quantile_90 = close.rolling(window=price_window, min_periods=price_window).quantile(price_quantile_high)
    
    # 计算滚动成交量均值和标准差（使用min_periods确保至少有volume_window个有效数据点）
    volume_mean = volume.rolling(window=volume_window, min_periods=volume_window).mean()
    volume_std = volume.rolling(window=volume_window, min_periods=volume_window).std()
    
    # 识别低位/高位（只使用有足够数据支撑的点）
    # 前price_window-1个数据点会因为min_periods限制而成为NaN，这是正常的
    low_price = close <= price_quantile_10
    high_price = close >= price_quantile_90
    
    # 识别放量（只使用有足够数据支撑的点）
    vol_spike = volume >= (volume_mean + volume_std_multiplier * volume_std)
    
    # 低位放量事件（只有当价格和成交量都有足够数据时才计算）
    # 使用&运算，只有两个条件都为True且都不是NaN时才为True
    low_event = low_price & vol_spike
    
    # 高位放量事件
    high_event = high_price & vol_spike
    
    # 关键修复：确保NaN值被填充为False（数据不足的点不应该触发信号）
    # 只有前price_window-1个数据点可能是NaN，这些点应该被标记为False
    low_event = low_event.fillna(False)
    high_event = high_event.fillna(False)
    
    return {
        'low_event': low_event,
        'high_event': high_event
    }


def detect_minute_level_events(
    minute_data: pd.DataFrame,
    price_window: int = 120,
    volume_std_multiplier: float = 1.5,
    daily_price_quantile_10: Optional[float] = None,
    daily_price_quantile_90: Optional[float] = None
) -> Optional[Dict[str, pd.Series]]:
    """
    基于分钟级数据识别低位/高位放量事件（改进版）
    
    参数：
    - minute_data: 分钟级数据（包含LastPrice, TotalVolumeTrade等）
    - price_window: 滚动窗口（分钟数）
    - volume_std_multiplier: 成交量标准差倍数
    - daily_price_quantile_10: 日频数据的10%分位数（可选，用于参考）
    - daily_price_quantile_90: 日频数据的90%分位数（可选，用于参考）
    
    返回：
    - {
        'low_event': Series[bool],
        'high_event': Series[bool],
      } 或 None（如果数据不足）
    """
    if minute_data.empty or 'LastPrice' not in minute_data.columns:
        return None
    
    try:
        # 改进：使用日频数据的分位数作为参考（更准确）
        # 如果提供了日频分位数，使用日频分位数；否则使用分钟级数据的分位数
        if daily_price_quantile_10 is not None and daily_price_quantile_90 is not None:
            # 使用日频数据的分位数作为参考（标量值）
            price_quantile_10 = daily_price_quantile_10
            price_quantile_90 = daily_price_quantile_90
        else:
            # 降级：使用分钟级数据的分位数（如果日频分位数不可用）
            # 使用min_periods确保至少有price_window个有效数据点
            if len(minute_data) < price_window:
                # 分钟级数据也不足，返回None
                return None
            price_quantile_10 = minute_data['LastPrice'].rolling(window=price_window, min_periods=price_window).quantile(0.10)
            price_quantile_90 = minute_data['LastPrice'].rolling(window=price_window, min_periods=price_window).quantile(0.90)
        
        # 计算成交量（使用累计成交量的差值）
        minute_data = minute_data.copy()
        minute_data['Volume'] = minute_data['TotalVolumeTrade'].diff().fillna(0)
        
        # 计算成交量均值和标准差（使用滚动窗口，min_periods确保数据充足）
        volume_mean = minute_data['Volume'].rolling(window=price_window, min_periods=price_window).mean()
        volume_std = minute_data['Volume'].rolling(window=price_window, min_periods=price_window).std()
        
        # 识别事件
        if isinstance(price_quantile_10, (int, float)):
            # 如果使用日频分位数（标量）
            low_price = minute_data['LastPrice'] <= price_quantile_10
            high_price = minute_data['LastPrice'] >= price_quantile_90
        else:
            # 如果使用分钟级分位数（Series）
            low_price = minute_data['LastPrice'] <= price_quantile_10
            high_price = minute_data['LastPrice'] >= price_quantile_90
        
        vol_spike = minute_data['Volume'] >= (volume_mean + volume_std_multiplier * volume_std)
        
        low_event = low_price & vol_spike
        high_event = high_price & vol_spike
        
        # 确保返回的Series的index与minute_data一致
        low_event = low_event.fillna(False)
        high_event = high_event.fillna(False)
        
        return {
            'low_event': low_event,
            'high_event': high_event
        }
    except Exception as e:
        logger.warning(f"分钟级事件识别失败: {e}")
        return None


def synthesize_cluster_signal(
    events: pd.Series,
    min_trigger_ratio: float = 0.5,
    freq: str = 'W'
) -> pd.Series:
    """
    事件簇合成：按周（或其他频率）合成综合信号
    
    参数：
    - events: 每日事件信号（Series[bool]）
    - min_trigger_ratio: 最小触发比例（默认0.5，即半数以上）
    - freq: 分组频率（默认'W'表示周）
    
    返回：
    - Series[bool]: 每周的综合信号
    """
    if events.empty:
        return pd.Series(dtype=bool)
    
    # 按周分组
    # 确保events的index是DatetimeIndex
    if not isinstance(events.index, pd.DatetimeIndex):
        logger.debug("events的index不是DatetimeIndex，无法按周分组")
        return pd.Series(dtype=bool)
    
    # 确保events是布尔类型
    if not events.dtype == bool:
        events = events.astype(bool)
    
    try:
        weekly_events = events.groupby(pd.Grouper(freq=freq))
        
        # 计算每周触发比例
        weekly_sum = weekly_events.sum()
        weekly_count = weekly_events.count()
        
        # 避免除零错误
        weekly_trigger_ratio = weekly_sum / weekly_count.replace(0, 1)
        
        # 综合信号：触发比例 >= min_trigger_ratio
        cluster_signal = weekly_trigger_ratio >= min_trigger_ratio
        
        return cluster_signal
    except Exception as e:
        logger.warning(f"周频信号合成失败: {e}")
        return pd.Series(dtype=bool)


def synthesize_multi_view_cluster_signal(
    daily_events: pd.Series,
    minute_events: Optional[pd.Series] = None,
    min_trigger_ratio: float = 0.5
) -> bool:
    """
    多视角事件簇合成
    
    参数：
    - daily_events: 日频事件信号
    - minute_events: 分钟级事件信号（可选）
    - min_trigger_ratio: 最小触发比例
    
    返回：
    - bool: 综合信号是否成立
    """
    signals = []
    
    # 日频视角
    if daily_events is not None and not daily_events.empty:
        daily_triggered = daily_events.sum() > 0
        signals.append(daily_triggered)
    
    # 分钟级视角（可选）
    if minute_events is not None and not minute_events.empty:
        minute_triggered = minute_events.sum() > 0
        signals.append(minute_triggered)
    
    # 综合判断：半数以上视角触发
    if len(signals) == 0:
        return False
    
    trigger_count = sum(signals)
    trigger_ratio = trigger_count / len(signals)
    
    return trigger_ratio >= min_trigger_ratio


# ==================== 基本面过滤层 ====================

def filter_by_pb_roe(
    fundamental_data: Dict[str, Dict],
    pb_roe_quantile: float = 0.30
) -> Dict[str, bool]:
    """
    PB-ROE过滤
    
    参数：
    - fundamental_data: 基本面数据字典 {code: {pb_mrq, roe_ttm, ...}}
    - pb_roe_quantile: PB-ROE分位阈值（默认0.30，即30分位）
    
    返回：
    - Dict[str, bool]: {code: 是否通过PB-ROE过滤}
    """
    result = {}
    
    # 计算所有股票的PB-ROE排名差
    pb_roe_values = {}
    for code, data in fundamental_data.items():
        pb_mrq = data.get('pb_mrq')
        roe_ttm = data.get('roe_ttm')
        
        if pb_mrq is not None and roe_ttm is not None:
            # Rank计算（升序：值越小排名越靠前）
            # 这里先收集所有值，然后统一计算排名
            pb_roe_values[code] = (pb_mrq, roe_ttm)
    
    if not pb_roe_values:
        # 没有有效数据，全部不通过
        return {code: False for code in fundamental_data.keys()}
    
    # 计算PB和ROE的排名
    pb_values = [v[0] for v in pb_roe_values.values()]
    roe_values = [v[1] for v in pb_roe_values.values()]
    
    pb_rank = pd.Series(pb_values).rank(ascending=True)
    roe_rank = pd.Series(roe_values).rank(ascending=False)  # ROE越大越好，所以降序
    
    # PB-ROE综合指标（值越低越优：PB更低而ROE更高）
    pb_roe_rank_diff = pb_rank - roe_rank
    
    # 计算阈值
    threshold = pb_roe_rank_diff.quantile(pb_roe_quantile)
    
    # 判断每只股票是否通过
    pb_roe_dict = dict(zip(pb_roe_values.keys(), pb_roe_rank_diff))
    
    for code in fundamental_data.keys():
        if code in pb_roe_dict:
            result[code] = pb_roe_dict[code] <= threshold
        else:
            result[code] = False
    
    return result


def filter_by_profit(
    fundamental_data: Dict[str, Dict]
) -> Dict[str, bool]:
    """
    盈利约束过滤
    
    参数：
    - fundamental_data: 基本面数据字典
    
    返回：
    - Dict[str, bool]: {code: 是否盈利为正}
    """
    result = {}
    for code, data in fundamental_data.items():
        roe_ttm = data.get('roe_ttm')
        result[code] = roe_ttm is not None and roe_ttm > 0
    return result


def filter_by_dividend(
    fundamental_data: Dict[str, Dict],
    require_continuous_dividend: bool = True,
    dividend_ratio_min: float = 0.0,
    dividend_ratio_max: float = 2.0  # 放宽到200%，允许使用留存收益分红
) -> Dict[str, bool]:
    """
    分红约束过滤
    
    参数：
    - fundamental_data: 基本面数据字典
    - require_continuous_dividend: 是否要求连续分红（默认True）
    - dividend_ratio_min: 股利支付率最小值（默认0.0）
    - dividend_ratio_max: 股利支付率最大值（默认2.0，即200%）
                        允许超过100%的情况（如使用留存收益分红）
    
    返回：
    - Dict[str, bool]: {code: 是否通过分红约束}
    """
    result = {}
    for code, data in fundamental_data.items():
        dividend_paid = data.get('dividend_paid_last3y', False)
        payout_ratio = data.get('dividend_payout_ratio')
        
        # 连续分红约束
        pass_continuous = dividend_paid if require_continuous_dividend else True
        
        # 股利支付率约束
        # 允许0-200%：超过100%可能是使用留存收益分红（合理情况）
        # 负数或超过200%可能是数据错误，需要过滤
        if payout_ratio is not None:
            pass_ratio = (dividend_ratio_min <= payout_ratio <= dividend_ratio_max)
        else:
            pass_ratio = True  # 如果没有支付率数据，不强制要求
        
        result[code] = pass_continuous and pass_ratio
    
    return result


# ==================== 核心分析函数 ====================

def analyze_momentum_signal(
    code: str,
    stock_data: pd.DataFrame,
    fundamental_data: Dict[str, Any],
    price_window: int = 120,
    price_quantile_low: float = 0.10,
    price_quantile_high: float = 0.90,
    volume_window: int = 120,
    volume_std_multiplier: float = 1.5,
    min_trigger_ratio: float = 0.5,
    pb_roe_quantile: float = 0.30,
    require_continuous_dividend: bool = True,
    high_signal_lookback_days: int = 5,
    use_minute_data: bool = True,  # 默认启用分钟级数据
    ddb_config: Optional[Dict] = None
) -> Optional[Dict[str, Any]]:
    """
    分析单只股票的动量信号
    
    参数：
    - code: 股票代码
    - stock_data: 股票行情数据（包含Close, Volume）
    - fundamental_data: 基本面数据 {pb_mrq, roe_ttm, dividend_paid_last3y, dividend_payout_ratio}
    - price_window: 价格滚动窗口（天）
    - price_quantile_low: 低位分位数
    - price_quantile_high: 高位分位数
    - volume_window: 成交量滚动窗口（天）
    - volume_std_multiplier: 成交量标准差倍数
    - min_trigger_ratio: 事件簇最小触发比例
    - pb_roe_quantile: PB-ROE分位阈值
    - require_continuous_dividend: 是否要求连续分红
    - high_signal_lookback_days: 高位信号回看天数
    - use_minute_data: 是否使用分钟级数据（默认True）
    - ddb_config: DolphinDB连接配置
    
    返回：
    - Dict: 分析结果，包含信号状态、失败原因等
    """
    # 初始化所有可能用到的变量（避免在异常处理中未定义）
    latest_week_minute_signal = False
    recent_minute_high_signal = False
    minute_low_events = None
    minute_high_events = None
    data_days = 0
    daily_low_event_count = 0
    daily_high_event_count = 0
    weekly_low_signal_count = 0
    weekly_high_signal_count = 0
    latest_week_trigger_ratio = 0.0
    recent_high_trigger_ratio = 0.0
    latest_week_signal = False
    recent_high_signal = False
    
    try:
        if stock_data.empty:
            return {
                '股票代码': code,
                '信号日期': None,
                '最终通过': False,
                '失败原因': '数据为空'
            }
        
        # 关键修复：检查数据是否充足（至少需要price_window个交易日）
        # 新股/次新股数据不足会导致分位数计算异常，必须提前过滤
        data_days = len(stock_data)
        if data_days < price_window:
            return {
                '股票代码': code,
                '信号日期': None,
                '最终通过': False,
                '失败原因': f'数据不足（仅{data_days}天，需要至少{price_window}天）',
                '低位放量信号': False,
                '高位放量剔除': False,
                '盈利通过': False,
                '分红通过': False,
                'PB_MRQ': None,
                'ROE_TTM': None,
                '股利支付率': None,
                '连续分红': False,
                '有PB-ROE数据': False,
                'PB-ROE通过': False,
                '是否科创板': False,
                # 调试信息
                '调试_数据天数': data_days,
                '调试_需要天数': price_window,
                '调试_日频低位事件数': 0,
                '调试_日频高位事件数': 0,
                '调试_周频低位信号': False,
                '调试_周频高位信号': False
            }
        
        # 1. 检测日频低位/高位放量事件
        daily_events = detect_daily_events(
            stock_data,
            price_window=price_window,
            price_quantile_low=price_quantile_low,
            price_quantile_high=price_quantile_high,
            volume_window=volume_window,
            volume_std_multiplier=volume_std_multiplier
        )
        
        daily_low_events = daily_events['low_event']
        daily_high_events = daily_events['high_event']
        
        # 调试信息：统计事件数量
        daily_low_event_count = int(daily_low_events.sum()) if not daily_low_events.empty else 0
        daily_high_event_count = int(daily_high_events.sum()) if not daily_high_events.empty else 0
        
        # 2. 分钟级事件识别（可选，改进版）
        minute_low_events = None
        minute_high_events = None
        daily_minute_low_events = None
        daily_minute_high_events = None
        
        if use_minute_data and DOLPHINDB_AVAILABLE:
            try:
                # 改进：获取更长时间范围的分钟级数据（1个月，用于计算分位数）
                end_date = stock_data.index.max()
                if isinstance(end_date, (datetime, pd.Timestamp)):
                    end_date_dt = end_date.date() if hasattr(end_date, 'date') else end_date
                else:
                    end_date_dt = end_date
                
                # 获取最近1个月的分钟级数据（用于计算分位数）
                start_date_dt = end_date_dt - timedelta(days=30)
                
                start_time = start_date_dt.strftime('%Y.%m.%d 09:30:00')
                end_time = end_date_dt.strftime('%Y.%m.%d 15:00:00')
                
                # 尝试从缓存加载分钟级数据
                minute_data = futures_incremental_cache_manager.load_minute_data(code, start_time, end_time)
                
                if minute_data is None or minute_data.empty:
                    # 缓存未命中，从DolphinDB获取
                    minute_data = get_minute_level_data(code, start_time, end_time, ddb_config)
                    
                    # 保存到缓存
                    if minute_data is not None and not minute_data.empty:
                        futures_incremental_cache_manager.save_minute_data(code, minute_data, start_time, end_time)
                
                if minute_data is not None and not minute_data.empty:
                    # 改进：使用日频数据的分位数作为参考（更准确）
                    # 获取日频数据的10%和90%分位数作为分钟级数据的参考
                    daily_price_quantile_10 = close.rolling(price_window).quantile(price_quantile_low).iloc[-1] if len(close) >= price_window else None
                    daily_price_quantile_90 = close.rolling(price_window).quantile(price_quantile_high).iloc[-1] if len(close) >= price_window else None
                    
                    minute_events = detect_minute_level_events(
                        minute_data,
                        price_window=price_window,
                        volume_std_multiplier=volume_std_multiplier,
                        daily_price_quantile_10=daily_price_quantile_10,
                        daily_price_quantile_90=daily_price_quantile_90
                    )
                    if minute_events:
                        minute_low_events = minute_events['low_event']
                        minute_high_events = minute_events['high_event']
                        
                        # 改进：将分钟级事件转换为日频（每天是否有分钟级事件）
                        if not minute_low_events.empty and isinstance(minute_low_events.index, pd.DatetimeIndex):
                            # 按日期分组，每天只要有任何一个分钟级事件就标记为True
                            daily_minute_low_events_series = minute_low_events.groupby(minute_low_events.index.date).any()
                            # 转换为DatetimeIndex（使用每天的日期）
                            daily_minute_low_events = pd.Series(
                                daily_minute_low_events_series.values,
                                index=pd.to_datetime(daily_minute_low_events_series.index),
                                dtype=bool
                            )
                        
                        if not minute_high_events.empty and isinstance(minute_high_events.index, pd.DatetimeIndex):
                            daily_minute_high_events_series = minute_high_events.groupby(minute_high_events.index.date).any()
                            daily_minute_high_events = pd.Series(
                                daily_minute_high_events_series.values,
                                index=pd.to_datetime(daily_minute_high_events_series.index),
                                dtype=bool
                            )
            except Exception as e:
                logger.debug(f"分钟级数据获取失败 {code}: {e}")
                # 降级到日频，不影响主流程
        
        # 3. 事件簇合成（周频）
        # 低位放量综合信号
        weekly_low_signal_count = 0  # 调试：周频信号数量
        latest_week_signal = False
        latest_week_trigger_ratio = 0.0  # 调试：最近一周触发比例
        
        if not daily_low_events.empty and len(daily_low_events) > 0:
            # 确保daily_low_events的index是DatetimeIndex
            if isinstance(daily_low_events.index, pd.DatetimeIndex):
                weekly_low_signal = synthesize_cluster_signal(
                    daily_low_events,
                    min_trigger_ratio=min_trigger_ratio,
                    freq='W'
                )
                weekly_low_signal_count = len(weekly_low_signal)  # 调试：周频信号数量
                # 检查最近一周是否有信号
                if not weekly_low_signal.empty and len(weekly_low_signal) > 0:
                    latest_week_signal = bool(weekly_low_signal.iloc[-1])
                    # 调试：计算最近一周的触发比例
                    if latest_week_signal:
                        # 获取最近一周的日期范围
                        latest_week_end = daily_low_events.index.max()
                        latest_week_start = latest_week_end - timedelta(days=7)
                        week_events = daily_low_events[(daily_low_events.index >= latest_week_start) & (daily_low_events.index <= latest_week_end)]
                        if len(week_events) > 0:
                            latest_week_trigger_ratio = float(week_events.sum() / len(week_events))
                else:
                    latest_week_signal = False
            else:
                # 如果index不是DatetimeIndex，无法按周分组，返回False
                latest_week_signal = False
        else:
            latest_week_signal = False
        
        # 多视角合成（改进版：对分钟级事件也进行周频聚合）
        latest_week_minute_signal = False  # 初始化分钟级周频信号（必须在if之前初始化）
        if use_minute_data and daily_minute_low_events is not None and not daily_minute_low_events.empty:
            # 改进：对分钟级事件也进行周频聚合（与日频数据保持一致）
            if isinstance(daily_minute_low_events.index, pd.DatetimeIndex):
                weekly_minute_low_signal = synthesize_cluster_signal(
                    daily_minute_low_events,
                    min_trigger_ratio=min_trigger_ratio,
                    freq='W'
                )
                # 检查最近一周是否有分钟级周频信号
                if not weekly_minute_low_signal.empty and len(weekly_minute_low_signal) > 0:
                    latest_week_minute_signal = bool(weekly_minute_low_signal.iloc[-1])
                else:
                    latest_week_minute_signal = False
            else:
                latest_week_minute_signal = False
            
            # 改进：多视角综合 - 日频和分钟级都触发 = 强信号，任一触发 = 弱信号
            # 当前实现：至少一个视角触发即可（保持原有逻辑）
            low_cluster_signal = latest_week_signal or latest_week_minute_signal
        else:
            low_cluster_signal = latest_week_signal
        
        # 高位放量综合信号（用于负向剔除）
        weekly_high_signal_count = 0  # 调试：周频高位信号数量
        recent_high_signal = False
        recent_high_trigger_ratio = 0.0  # 调试：最近高位信号触发比例
        
        if not daily_high_events.empty and len(daily_high_events) > 0:
            # 确保daily_high_events的index是DatetimeIndex
            if isinstance(daily_high_events.index, pd.DatetimeIndex):
                weekly_high_signal = synthesize_cluster_signal(
                    daily_high_events,
                    min_trigger_ratio=min_trigger_ratio,
                    freq='W'
                )
                weekly_high_signal_count = len(weekly_high_signal)  # 调试：周频信号数量
                # 检查近N周内是否有高位信号（high_signal_lookback_days是天数，转换为周数）
                # 简化：如果lookback_days是5天，检查最近1-2周
                lookback_weeks = max(1, (high_signal_lookback_days // 5))  # 5天约等于1周
                if not weekly_high_signal.empty and len(weekly_high_signal) > 0:
                    recent_high_signal = bool(weekly_high_signal.tail(lookback_weeks).sum() > 0)
                    # 调试：计算最近N周的高位信号触发比例
                    if recent_high_signal and not daily_high_events.empty:
                        latest_high_week_end = daily_high_events.index.max()
                        latest_high_week_start = latest_high_week_end - timedelta(days=high_signal_lookback_days)
                        high_week_events = daily_high_events[(daily_high_events.index >= latest_high_week_start) & (daily_high_events.index <= latest_high_week_end)]
                        if len(high_week_events) > 0:
                            recent_high_trigger_ratio = float(high_week_events.sum() / len(high_week_events))
                        else:
                            recent_high_trigger_ratio = 0.0
                else:
                    recent_high_signal = False
            else:
                # 如果index不是DatetimeIndex，无法按周分组，返回False
                recent_high_signal = False
        else:
            recent_high_signal = False
        
        # 多视角高位信号（改进版：对分钟级事件也进行周频聚合）
        recent_minute_high_signal = False  # 初始化分钟级高位信号
        if use_minute_data and daily_minute_high_events is not None and not daily_minute_high_events.empty:
            # 改进：对分钟级高位事件也进行周频聚合
            if isinstance(daily_minute_high_events.index, pd.DatetimeIndex):
                weekly_minute_high_signal = synthesize_cluster_signal(
                    daily_minute_high_events,
                    min_trigger_ratio=min_trigger_ratio,
                    freq='W'
                )
                # 检查近N周内是否有分钟级高位信号
                lookback_weeks = max(1, (high_signal_lookback_days // 5))
                if not weekly_minute_high_signal.empty and len(weekly_minute_high_signal) > 0:
                    recent_minute_high_signal = bool(weekly_minute_high_signal.tail(lookback_weeks).sum() > 0)
                else:
                    recent_minute_high_signal = False
            else:
                recent_minute_high_signal = False
            
            # 如果分钟级有高位信号，也标记为剔除
            if recent_minute_high_signal:
                recent_high_signal = True
        
        # 4. 基本面过滤（需要所有股票的数据来计算排名）
        # 这里先标记，在主函数中统一计算PB-ROE排名
        pb_mrq = fundamental_data.get('pb_mrq')
        roe_ttm = fundamental_data.get('roe_ttm')
        is_star_market = fundamental_data.get('is_star_market', False)
        
        # 检查基本面数据是否缺失
        has_fundamental_data = (pb_mrq is not None) or (roe_ttm is not None)
        
        # 如果是科创板且基本面数据缺失，标记为"科创板数据不全"
        if is_star_market and not has_fundamental_data:
            return {
                '股票代码': code,
                '信号日期': None,
                '最终通过': False,
                '失败原因': '科创板数据不全',
                '低位放量信号': False,
                '高位放量剔除': False,
                '盈利通过': False,
                '分红通过': False,
                'PB_MRQ': None,
                'ROE_TTM': None,
                '股利支付率': None,
                '连续分红': False,
                '有PB-ROE数据': False,
                'PB-ROE通过': False,
                '是否科创板': True
            }
        
        # 盈利约束
        pass_profit_flag = roe_ttm is not None and roe_ttm > 0
        
        # 分红约束
        dividend_paid = fundamental_data.get('dividend_paid_last3y', False)
        payout_ratio = fundamental_data.get('dividend_payout_ratio')
        pass_continuous = dividend_paid if require_continuous_dividend else True
        if payout_ratio is not None:
            # 股利支付率约束：允许0-200%（超过100%可能是使用留存收益分红）
            # 负数或超过200%可能是数据错误，需要过滤
            pass_ratio = (0.0 <= payout_ratio <= 2.0)
        else:
            pass_ratio = True
        pass_dividend_flag = pass_continuous and pass_ratio
        
        # PB-ROE过滤（暂时标记，在主函数中统一计算）
        # 这里先检查数据是否有效
        has_pb_roe_data = pb_mrq is not None and roe_ttm is not None
        
        # 5. 综合判断（PB-ROE在主函数中统一计算后传入）
        # 这里先返回中间结果，主函数会进行最终判断
        
        # 6. 构建结果
        # 确保信号值是布尔类型，避免出现意外的True值
        result = {
            '股票代码': code,
            '信号日期': stock_data.index.max() if bool(low_cluster_signal) else None,
            '低位放量信号': bool(low_cluster_signal),
            '高位放量剔除': bool(recent_high_signal),
            '盈利通过': bool(pass_profit_flag),
            '分红通过': bool(pass_dividend_flag),
            'PB_MRQ': pb_mrq,
            'ROE_TTM': roe_ttm,
            '股利支付率': payout_ratio,
            '连续分红': bool(dividend_paid),
            '有PB-ROE数据': bool(has_pb_roe_data),
            '是否科创板': bool(is_star_market),
            # 这些字段在主函数中填充
            'PB-ROE通过': None,
            '最终通过': False,
            '失败原因': None,
            # 详细调试信息
            '调试_数据天数': data_days,
            '调试_需要天数': price_window,
            '调试_日频低位事件数': daily_low_event_count,
            '调试_日频高位事件数': daily_high_event_count,
            '调试_周频低位信号数': weekly_low_signal_count,
            '调试_周频高位信号数': weekly_high_signal_count,
            '调试_最近一周低位触发比例': round(latest_week_trigger_ratio, 3),
            '调试_最近一周高位触发比例': round(recent_high_trigger_ratio, 3),
            '调试_周频低位信号': bool(latest_week_signal),
            '调试_周频高位信号': bool(recent_high_signal),
            '调试_分钟级低位事件数': int(minute_low_events.sum()) if (minute_low_events is not None and not minute_low_events.empty) else 0,
            '调试_分钟级高位事件数': int(minute_high_events.sum()) if (minute_high_events is not None and not minute_high_events.empty) else 0,
            '调试_使用分钟级数据': bool(use_minute_data),
            '调试_分钟级周频低位信号': bool(latest_week_minute_signal),
            '调试_分钟级周频高位信号': bool(recent_minute_high_signal)
        }
        
        return result
        
    except Exception as e:
        logger.error(f"分析股票 {code} 动量信号时出错: {e}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        return {
            '股票代码': code,
            '信号日期': None,
            '最终通过': False,
            '失败原因': f'分析异常: {str(e)}',
            # 确保所有调试字段都有值（使用已初始化的变量）
            '调试_数据天数': data_days,
            '调试_需要天数': price_window,
            '调试_日频低位事件数': daily_low_event_count,
            '调试_日频高位事件数': daily_high_event_count,
            '调试_周频低位信号数': weekly_low_signal_count,
            '调试_周频高位信号数': weekly_high_signal_count,
            '调试_最近一周低位触发比例': round(latest_week_trigger_ratio, 3),
            '调试_最近一周高位触发比例': round(recent_high_trigger_ratio, 3),
            '调试_周频低位信号': bool(latest_week_signal),
            '调试_周频高位信号': bool(recent_high_signal),
            '调试_分钟级低位事件数': int(minute_low_events.sum()) if (minute_low_events is not None and not minute_low_events.empty) else 0,
            '调试_分钟级高位事件数': int(minute_high_events.sum()) if (minute_high_events is not None and not minute_high_events.empty) else 0,
            '调试_使用分钟级数据': bool(use_minute_data),
            '调试_分钟级周频低位信号': bool(latest_week_minute_signal),
            '调试_分钟级周频高位信号': bool(recent_minute_high_signal)
        }


# ==================== 主函数 ====================

def run_momentum_screening(
    limit: Optional[int] = None,
    max_days_ago: Optional[int] = None,
    price_window: int = 120,
    price_quantile_low: float = 0.10,
    price_quantile_high: float = 0.90,
    volume_window: int = 120,
    volume_std_multiplier: float = 1.5,
    min_trigger_ratio: float = 0.5,
    pb_roe_quantile: float = 0.30,
    require_continuous_dividend: bool = True,
    high_signal_lookback_days: int = 5,
    use_minute_data: bool = True,  # 默认启用分钟级数据
    ddb_config: Optional[Dict] = None,
    output_dir: str = "outputs",
    cutoff_date: Optional[date] = None
) -> Optional[str]:
    """
    运行动量筛选策略
    
    参数：
    - limit: 股票数量限制，None 使用配置中的 STOCK_LIST_LIMIT
    - max_days_ago: 最大允许行情滞后天数，None 使用配置中的 MAX_TRADING_DAYS_AGO
    - price_window: 价格滚动窗口（天，默认120）
    - price_quantile_low: 低位分位数（默认0.10）
    - price_quantile_high: 高位分位数（默认0.90）
    - volume_window: 成交量滚动窗口（天，默认120）
    - volume_std_multiplier: 成交量标准差倍数（默认1.5）
    - min_trigger_ratio: 事件簇最小触发比例（默认0.5）
    - pb_roe_quantile: PB-ROE分位阈值（默认0.30）
    - require_continuous_dividend: 是否要求连续分红（默认True）
    - high_signal_lookback_days: 高位信号回看天数（默认5天）
    - use_minute_data: 是否使用分钟级数据（默认True）
    - ddb_config: DolphinDB连接配置
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
    print("动量筛选策略分析（功能13）")
    print("=" * 60)
    print(f"股票数量上限: {limit}")
    print(f"最大允许行情滞后天数: {max_days_ago}")
    print(f"价格滚动窗口: {price_window}天")
    print(f"成交量滚动窗口: {volume_window}天")
    print(f"PB-ROE分位阈值: {pb_roe_quantile*100:.0f}%")
    print(f"使用分钟级数据: {'是' if use_minute_data else '否'}")
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
        
        # 2. 批量获取股票数据（使用增量缓存，与功能12对齐）
        print("📈 批量获取行情数据（增量缓存 + 批量SQL + 多线程并发）...")
        
        # 计算日期范围（至少需要price_window + 30天的数据）
        days_needed = max(price_window + 30, 150)
        # end_date 已在上面确定（cutoff_date 或最新交易日）
        start_date = end_date - timedelta(days=days_needed + 30)  # 多获取30天作为缓冲
        
        # 尝试从缓存加载（批量）
        print("  🔍 检查增量缓存...")
        print(f"  📅 请求日期范围: {start_date} 至 {end_date}（数据库最新交易日，需要{days_needed}天数据）")
        cached_stock_data, missing_stock_codes = futures_incremental_cache_manager.load_stocks_data(
            codes, start_date, end_date
        )
        
        # 检查缓存数据的完整性（使用统一的缓存验证函数）
        insufficient_data_codes = []
        partial_data_codes = {}  # 部分命中的股票，需要增量补全
        valid_cached_data = {}
        
        for code, data in cached_stock_data.items():
            # 使用统一的缓存验证函数（功能13需要price_window作为最小窗口）
            validation_result = validate_cache_with_min_window(
                data=data,
                start_date=start_date,
                end_date=end_date,
                days_needed=days_needed,
                min_window_days=price_window
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
        print(f"  ⚠️  需要从数据库获取: {cache_miss_count} 只股票（{len(missing_stock_codes)}只未命中，{len(insufficient_data_codes)}只数据不足）")
        
        # 对缺失或数据不足的股票从数据库获取（批量）
        # 优化：区分完全缺失和部分命中的股票，部分命中的只获取缺失日期范围
        fetch_start_time = time.time()
        fetched_stock_data = {}
        
        # 1. 处理完全缺失的股票：获取全部数据
        if all_missing_codes:
            print(f"  📥 从数据库获取完全缺失的 {len(all_missing_codes)} 只股票数据（全部{days_needed}天数据）...")
            fully_missing_data = fetcher.batch_get_stock_data_with_adjustment(
                all_missing_codes,
                days=days_needed,
                time_config=None
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
                    # 两端都缺失（理论上不应该发生，但处理一下）
                    # 优先补全结束部分（更常见），然后补全开始部分
                    fetch_start = cache_end + timedelta(days=1) if cache_end else start_date
                    fetch_end = missing_end
                elif missing_end:
                    # 只缺结束日期（缓存数据不够新）- 这是最常见的情况
                    # 从缓存结束日期的下一天开始，到请求结束日期
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
                if fetch_start > fetch_end:
                    # 日期范围无效，跳过增量补全，改为获取全部数据
                    all_missing_codes.append(code)
                    continue
                
                # 计算需要获取的天数
                fetch_days = (fetch_end - fetch_start).days + 30  # 多获取30天作为缓冲
                
                # 按日期范围分组
                range_key = (fetch_start, fetch_end)
                if range_key not in date_range_groups:
                    date_range_groups[range_key] = []
                date_range_groups[range_key].append(code)
            
            # 对每个日期范围组，批量获取数据
            for (fetch_start, fetch_end), codes in date_range_groups.items():
                # 计算需要获取的天数：从数据库最新日期往前推到fetch_start
                # batch_get_stock_data_with_adjustment是从最新日期往前推N天
                days_from_latest = (end_date - fetch_start).days + 30  # 多获取30天作为缓冲
                print(f"    📥 获取 {len(codes)} 只股票的增量数据: {fetch_start} 至 {fetch_end}（从最新日期往前推{days_from_latest}天）...")
                
                # 使用batch_get_stock_data_with_adjustment获取数据
                # 注意：这个函数从数据库最新日期往前推N天
                incremental_data = fetcher.batch_get_stock_data_with_adjustment(
                    codes,
                    days=days_from_latest,
                    time_config=None
                )
                
                if incremental_data:
                    # 过滤数据到实际需要的日期范围
                    for code in codes:
                        if code in incremental_data and incremental_data[code] is not None:
                            df = incremental_data[code]
                            # 过滤到fetch_start到fetch_end的范围
                            filtered_df = df[(df.index >= pd.Timestamp(fetch_start)) & 
                                            (df.index <= pd.Timestamp(fetch_end))]
                            if not filtered_df.empty:
                                # 合并增量数据和缓存数据
                                cached_df = partial_data_codes[code]['cached_data']
                                merged_df = pd.concat([cached_df, filtered_df])
                                merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
                                merged_df = merged_df.sort_index()
                                
                                # 保存到fetched_stock_data（后续会保存到缓存）
                                fetched_stock_data[code] = merged_df
                                print(f"      ✅ {code}: 合并后数据范围 {merged_df.index.min()} 至 {merged_df.index.max()}（{len(merged_df)}行）")
                            else:
                                # 增量数据为空，使用缓存数据
                                fetched_stock_data[code] = partial_data_codes[code]['cached_data']
                    print(f"    ✅ 增量补全完成: {len(codes)} 只股票")
        
        fetch_elapsed = time.time() - fetch_start_time
        
        if fetched_stock_data:
            # 保存新获取的数据到缓存（批量）
            print(f"  💾 保存 {len(fetched_stock_data)} 只股票的数据到增量缓存...")
            futures_incremental_cache_manager.save_stocks_data(
                list(fetched_stock_data.keys()),
                fetched_stock_data,
                start_date,
                end_date
            )
            print(f"  ✅ 缓存保存完成（总耗时: {fetch_elapsed:.1f}秒）")
            
            # 合并有效缓存数据和新增数据
            valid_cached_data.update(fetched_stock_data)
        
        all_stock_data = valid_cached_data
        
        if not all_stock_data:
            print("❌ 未获取到任何股票行情数据")
            return None
        
        total_stocks = cache_hit_count + cache_miss_count
        if total_stocks > 0:
            cache_hit_rate = cache_hit_count / total_stocks * 100
        else:
            cache_hit_rate = 0.0
        print(f"✅ 成功获取 {len(all_stock_data)} 只股票的行情数据（缓存命中率: {cache_hit_rate:.1f}%）")
        
        # 3. 批量获取基本面数据（使用缓存 + 分批多线程并发）
        print("📊 批量获取基本面数据（增量缓存 + 分批多线程并发）...")
        from tqdm import tqdm
        
        stock_count = len(all_stock_data)
        stock_codes_list = list(all_stock_data.keys())
        fundamental_start_time = time.time()
        
        # 尝试从缓存加载基本面数据
        print("  🔍 检查基本面数据缓存...")
        cached_fundamental_data, missing_fundamental_codes = futures_incremental_cache_manager.load_fundamental_data(
            stock_codes_list
        )
        
        cache_hit_fundamental = len(cached_fundamental_data)
        cache_miss_fundamental = len(missing_fundamental_codes)
        print(f"  ✅ 基本面数据缓存命中: {cache_hit_fundamental} 只股票")
        if cache_miss_fundamental > 0:
            print(f"  ⚠️  需要从数据库获取: {cache_miss_fundamental} 只股票")
        
        all_fundamental_data = cached_fundamental_data.copy()
        
        # 对缺失的基本面数据从数据库获取
        if missing_fundamental_codes:
            # 如果股票数量大，分批并发查询
            batch_size = 2000  # 每批2000只股票
            if len(missing_fundamental_codes) > batch_size:
                batches = [missing_fundamental_codes[i:i+batch_size] for i in range(0, len(missing_fundamental_codes), batch_size)]
                print(f"  📦 分为 {len(batches)} 批并发查询（每批 {batch_size} 只股票）")
                
                with tqdm(total=len(batches), desc="  基本面数据查询", unit="批", ncols=100) as pbar:
                    from concurrent.futures import ThreadPoolExecutor, as_completed
                    
                    with ThreadPoolExecutor(max_workers=min(4, len(batches))) as executor:
                        future_to_batch = {
                            executor.submit(get_fundamental_data, batch, fetcher, None): batch 
                            for batch in batches
                        }
                        
                        for future in as_completed(future_to_batch):
                            batch = future_to_batch[future]
                            try:
                                batch_result = future.result()
                                all_fundamental_data.update(batch_result)
                                pbar.update(1)
                                pbar.set_postfix({
                                    '已获取': len(all_fundamental_data),
                                    '耗时': f"{time.time() - fundamental_start_time:.1f}秒"
                                })
                            except Exception as e:
                                logger.error(f"批次查询失败: {e}")
                                pbar.update(1)
            else:
                # 股票数量少，直接查询
                with tqdm(total=1, desc="  基本面数据查询", unit="批", ncols=100) as pbar:
                    fetched_fundamental_data = get_fundamental_data(
                        missing_fundamental_codes, 
                        fetcher,
                        None
                    )
                    all_fundamental_data.update(fetched_fundamental_data)
                    pbar.update(1)
                    pbar.set_postfix({'耗时': f"{time.time() - fundamental_start_time:.1f}秒"})
            
            # 保存新获取的基本面数据到缓存
            if len(all_fundamental_data) > cache_hit_fundamental:
                fetched_fundamental_data = {k: v for k, v in all_fundamental_data.items() if k not in cached_fundamental_data}
                if fetched_fundamental_data:
                    print(f"  💾 保存 {len(fetched_fundamental_data)} 只股票的基本面数据到缓存...")
                    futures_incremental_cache_manager.save_fundamental_data(fetched_fundamental_data)
                    print(f"  ✅ 基本面数据缓存保存完成")
        
        fundamental_elapsed = time.time() - fundamental_start_time
        total_fundamental = len(all_fundamental_data)
        if total_fundamental > 0:
            cache_hit_rate_fundamental = cache_hit_fundamental / total_fundamental * 100
        else:
            cache_hit_rate_fundamental = 0.0
        print(f"  ✅ 基本面数据获取完成（耗时: {fundamental_elapsed:.1f}秒，共 {total_fundamental} 只股票，缓存命中率: {cache_hit_rate_fundamental:.1f}%）")
        
        # 4. 计算PB-ROE排名（需要所有股票的数据）
        print("📊 计算PB-ROE排名...")
        pb_roe_filter_result = filter_by_pb_roe(all_fundamental_data, pb_roe_quantile=pb_roe_quantile)
        
        # 5. 并行计算动量信号
        print("🚀 并行计算动量信号...")
        
        def _task_momentum(code: str,
                          stock_data_dict: Dict[str, pd.DataFrame],
                          fundamental_data_dict: Dict[str, Dict],
                          pb_roe_filter_dict: Dict[str, bool],
                          price_window: int,
                          price_quantile_low: float,
                          price_quantile_high: float,
                          volume_window: int,
                          volume_std_multiplier: float,
                          min_trigger_ratio: float,
                          require_continuous_dividend: bool,
                          high_signal_lookback_days: int,
                          use_minute_data: bool,
                          ddb_config: Optional[Dict]) -> Optional[Dict]:
            """供高性能线程池调用的任务函数"""
            df = stock_data_dict.get(code)
            if df is None or df.empty:
                return None
            
            fundamental_data = fundamental_data_dict.get(code, {})
            
            # 调用分析函数
            result = analyze_momentum_signal(
                code=code,
                stock_data=df,
                fundamental_data=fundamental_data,
                price_window=price_window,
                price_quantile_low=price_quantile_low,
                price_quantile_high=price_quantile_high,
                volume_window=volume_window,
                volume_std_multiplier=volume_std_multiplier,
                min_trigger_ratio=min_trigger_ratio,
                pb_roe_quantile=pb_roe_quantile,
                require_continuous_dividend=require_continuous_dividend,
                high_signal_lookback_days=high_signal_lookback_days,
                use_minute_data=use_minute_data,
                ddb_config=ddb_config
            )
            
            if result:
                # 填充PB-ROE过滤结果
                result['PB-ROE通过'] = pb_roe_filter_dict.get(code, False)
                
                # 最终判断
                result['最终通过'] = (
                    result.get('低位放量信号', False) and
                    result.get('PB-ROE通过', False) and
                    result.get('盈利通过', False) and
                    result.get('分红通过', False) and
                    not result.get('高位放量剔除', False)
                )
                
                # 失败原因
                if not result['最终通过']:
                    failure_reasons = []
                    if not result.get('低位放量信号', False):
                        failure_reasons.append('低位放量信号未触发')
                    if not result.get('PB-ROE通过', False):
                        failure_reasons.append('PB-ROE过滤未通过')
                    if not result.get('盈利通过', False):
                        failure_reasons.append('盈利约束未通过')
                    if not result.get('分红通过', False):
                        failure_reasons.append('分红约束未通过')
                    if result.get('高位放量剔除', False):
                        failure_reasons.append('高位放量信号剔除')
                    
                    result['失败原因'] = '; '.join(failure_reasons) if failure_reasons else '未知原因'
                else:
                    result['失败原因'] = None
            
            return result
        
        # 使用高性能线程池
        thread_pool = HighPerformanceThreadPool(progress_desc="动量筛选分析")
        
        tasks = list(all_stock_data.keys())
        results = thread_pool.execute_batch(
            tasks,
            _task_momentum,
            all_stock_data,
            all_fundamental_data,
            pb_roe_filter_result,
            price_window,
            price_quantile_low,
            price_quantile_high,
            volume_window,
            volume_std_multiplier,
            min_trigger_ratio,
            require_continuous_dividend,
            high_signal_lookback_days,
            use_minute_data,
            ddb_config
        )
        
        # 汇总结果
        valid_results = [r for r in results if r is not None]
        signal_results = [r for r in valid_results if r.get('最终通过', False)]
        no_signal_results = [r for r in valid_results if not r.get('最终通过', False)]
        
        print(f"\n✅ 发现 {len(signal_results)} 只股票满足条件")
        if len(no_signal_results) > 0:
            print(f"📋 另有 {len(no_signal_results)} 只股票不满足条件（将一并导出失败原因）")
        
        # 创建输出目录
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # 生成输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"动量筛选策略_{timestamp}.xlsx")
        
        # 导出Excel
        print("📊 导出Excel结果...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: 满足条件的信号
            if signal_results:
                signal_df = pd.DataFrame(signal_results)
                signal_df.to_excel(writer, sheet_name='满足条件的信号', index=False)
                print(f"  ✅ 满足条件的信号: {len(signal_df)} 只")
            
            # Sheet 2: 不满足条件的股票
            if no_signal_results:
                no_signal_df = pd.DataFrame(no_signal_results)
                no_signal_df = no_signal_df.sort_values('股票代码')
                no_signal_df.to_excel(writer, sheet_name='不满足条件的股票', index=False)
                print(f"  ✅ 不满足条件的股票: {len(no_signal_df)} 只")
        
        elapsed_time = time.time() - start_time
        print("=" * 60)
        print(f"✅ 动量筛选策略分析完成，共导出 {len(valid_results)} 只股票")
        print(f"输出文件: {output_file}")
        print(f"总耗时: {elapsed_time:.2f} 秒")
        print("=" * 60)
        
        return output_file
        
    except Exception as e:
        logger.error(f"运行动量筛选策略时出错: {e}", exc_info=True)
        print(f"❌ 分析失败: {e}")
        return None

