#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
上升趋势中的反弹识别模块

功能：识别上升趋势中的回调反弹机会
实现思路：
- 使用MASS均线排列打分模型判定上升趋势
- 用偏离度量化回调幅度
- 通过价量共振确认反弹末端
- 复用项目现有数据获取和缓存机制
"""

import os
import time
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from data_fetcher import JuyuanDataFetcher
from config import STOCK_LIST_LIMIT, MAX_TRADING_DAYS_AGO
from high_performance_threading import HighPerformanceThreadPool
from futures_incremental_cache_manager import futures_incremental_cache_manager
from juyuan_config import TABLE_CONFIG
import random

logger = logging.getLogger(__name__)


def calculate_mass_score(prices: pd.Series, n: int) -> float:
    """
    计算MASS均线排列打分（0-100分）- 高性能向量化版本
    
    参数：
    - prices: 价格序列（收盘价）
    - n: 计算周期
    
    返回：
    - MASS得分（0-100），分数越高表示均线排列越健康（多头排列）
    """
    try:
        if len(prices) < n:
            return 0.0
        
        # 计算多条均线（5, 10, 20, 30, 60日）- 向量化操作
        ma5 = prices.rolling(5).mean()
        ma10 = prices.rolling(10).mean()
        ma20 = prices.rolling(20).mean()
        ma30 = prices.rolling(30).mean()
        ma60 = prices.rolling(60).mean() if n >= 60 else None
        
        # 只使用有足够数据的部分
        valid_start = max(5, 10, 20, 30, n if n < 60 else 60)
        if len(prices) < valid_start:
            return 0.0
        
        # 取最近n天的数据（转换为numpy数组，提升性能）
        # 确保转换为float64类型，避免类型不匹配
        recent_prices = np.asarray(prices.iloc[-n:].values, dtype=np.float64)
        recent_ma5 = np.asarray(ma5.iloc[-n:].values, dtype=np.float64)
        recent_ma10 = np.asarray(ma10.iloc[-n:].values, dtype=np.float64)
        recent_ma20 = np.asarray(ma20.iloc[-n:].values, dtype=np.float64)
        recent_ma30 = np.asarray(ma30.iloc[-n:].values, dtype=np.float64)
        recent_ma60 = np.asarray(ma60.iloc[-n:].values, dtype=np.float64) if ma60 is not None else None
        
        # 使用numpy向量化操作计算得分（大幅提升性能）
        # 1. 检查有效数据（向量化）
        valid_mask = ~(np.isnan(recent_ma5) | np.isnan(recent_ma10) | 
                      np.isnan(recent_ma20) | np.isnan(recent_ma30))
        valid_mask = valid_mask.astype(bool)  # 确保是布尔类型
        
        if not np.any(valid_mask):
            return 0.0
        
        # 2. 多头排列：MA5 > MA10 > MA20 > MA30（向量化）
        bull_align = ((recent_ma5 > recent_ma10) & (recent_ma10 > recent_ma20) & (recent_ma20 > recent_ma30))
        bull_align = bull_align.astype(bool)  # 确保是布尔类型
        bull_align_mask = bull_align & valid_mask
        bull_align_score = np.where(bull_align_mask, 25.0, 0.0)
        
        # 3. 价格在均线之上（向量化）
        price_above_ma5 = (recent_prices > recent_ma5).astype(bool)
        price_above_mask = price_above_ma5 & valid_mask
        price_above_score = np.where(price_above_mask, 15.0, 0.0)
        
        # 4. 均线向上倾斜（向量化）
        ma5_up = np.zeros(len(recent_ma5), dtype=bool)
        ma10_up = np.zeros(len(recent_ma10), dtype=bool)
        if len(recent_ma5) > 1:
            ma5_up[1:] = (recent_ma5[1:] > recent_ma5[:-1]).astype(bool)
            ma10_up[1:] = (recent_ma10[1:] > recent_ma10[:-1]).astype(bool)
        ma5_up_mask = ma5_up & valid_mask
        ma10_up_mask = ma10_up & valid_mask
        ma5_up_score = np.where(ma5_up_mask, 10.0, 0.0)
        ma10_up_score = np.where(ma10_up_mask, 10.0, 0.0)
        
        # 5. 长期均线（如果有，向量化）
        ma60_score = np.zeros(len(recent_prices), dtype=np.float64)
        if recent_ma60 is not None:
            ma60_valid = (~np.isnan(recent_ma60)).astype(bool)
            price_above_ma60 = (recent_prices > recent_ma60).astype(bool)
            ma30_above_ma60 = (recent_ma30 > recent_ma60).astype(bool)
            ma60_mask1 = price_above_ma60 & ma60_valid & valid_mask
            ma60_mask2 = ma30_above_ma60 & ma60_valid & valid_mask
            ma60_score = np.where(ma60_mask1, 10.0, 0.0)
            ma60_score += np.where(ma60_mask2, 10.0, 0.0)
        
        # 6. 均线间距合理（向量化）
        # 避免除零错误
        ma_spread = np.zeros_like(recent_ma5, dtype=np.float64)
        spread_mask = (recent_ma30 != 0).astype(bool)
        ma_spread[spread_mask] = (recent_ma5[spread_mask] - recent_ma30[spread_mask]) / recent_ma30[spread_mask]
        spread_ok = ((ma_spread >= 0.02) & (ma_spread <= 0.20)).astype(bool)
        spread_mask_final = spread_ok & valid_mask
        spread_score = np.where(spread_mask_final, 10.0, 0.0)
        
        # 7. 汇总所有得分（向量化求和）
        day_scores = (bull_align_score + price_above_score + ma5_up_score + 
                     ma10_up_score + ma60_score + spread_score)
        
        # 只对有效天数求平均
        valid_scores = day_scores[valid_mask]
        if len(valid_scores) > 0:
            avg_score = np.mean(valid_scores)
            return float(min(100.0, max(0.0, avg_score)))
        else:
            return 0.0
            
    except Exception as e:
        logger.error(f"计算MASS得分失败: {e}")
        return 0.0


def calculate_ma_slope(ma_series: pd.Series, period: int = 5) -> float:
    """
    计算均线斜率
    
    参数：
    - ma_series: 均线序列
    - period: 计算斜率的周期
    
    返回：
    - 斜率（正值表示向上，负值表示向下）
    """
    try:
        if len(ma_series) < period + 1:
            return 0.0
        
        recent = ma_series.iloc[-period-1:].dropna()
        if len(recent) < 2:
            return 0.0
        
        # 使用线性回归计算斜率
        x = np.arange(len(recent))
        y = recent.values
        slope = np.polyfit(x, y, 1)[0]
        
        # 归一化斜率（相对于价格水平）
        if recent.iloc[-1] != 0:
            normalized_slope = slope / recent.iloc[-1]
        else:
            normalized_slope = 0.0
        
        return normalized_slope
        
    except Exception as e:
        logger.error(f"计算均线斜率失败: {e}")
        return 0.0


def calculate_bollinger_bands(prices: pd.Series, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
    """
    计算布林带
    
    参数：
    - prices: 价格序列
    - period: 计算周期（默认20，对应EMA20）
    - std_dev: 标准差倍数（默认2.0）
    
    返回：
    - DataFrame包含：中轨（EMA20）、上轨、下轨、带宽
    """
    try:
        # 中轨 = EMA20
        middle = prices.ewm(span=period, adjust=False).mean()
        
        # 标准差
        std = prices.rolling(period).std()
        
        # 上轨和下轨
        upper = middle + std_dev * std
        lower = middle - std_dev * std
        
        # 带宽（相对宽度）
        bandwidth = (upper - lower) / middle * 100
        
        return pd.DataFrame({
            'middle': middle,
            'upper': upper,
            'lower': lower,
            'bandwidth': bandwidth
        })
        
    except Exception as e:
        logger.error(f"计算布林带失败: {e}")
        return pd.DataFrame()


def calculate_turnover_zscore(turnover_series: pd.Series, period: int = 20) -> float:
    """
    计算换手率20日Z-score
    
    参数：
    - turnover_series: 换手率序列
    - period: 计算周期（默认20）
    
    返回：
    - Z-score值
    """
    try:
        if len(turnover_series) < period:
            return 0.0
        
        recent = turnover_series.iloc[-period:]
        mean_val = recent.mean()
        std_val = recent.std()
        
        if std_val == 0 or pd.isna(std_val):
            return 0.0
        
        current = turnover_series.iloc[-1]
        zscore = (current - mean_val) / std_val
        
        return zscore if not pd.isna(zscore) else 0.0
        
    except Exception as e:
        logger.error(f"计算换手率Z-score失败: {e}")
        return 0.0


def calculate_volume_timing_score(volume_series: pd.Series, turnover_series: pd.Series, 
                                  period: int = 60) -> Dict[str, float]:
    """
    计算成交额/换手率择时打分
    
    参数：
    - volume_series: 成交量序列
    - turnover_series: 换手率序列
    - period: 计算周期（默认60天）
    
    返回：
    - 包含分位数得分和标准差得分的字典
    """
    try:
        if len(volume_series) < period or len(turnover_series) < period:
            return {'percentile_score': 0.5, 'sigma_score': 0.0}
        
        # 计算成交额（假设有价格数据，这里简化处理）
        recent_volume = volume_series.iloc[-period:]
        recent_turnover = turnover_series.iloc[-period:]
        
        # 分位数得分
        current_volume = volume_series.iloc[-1]
        current_turnover = turnover_series.iloc[-1]
        
        volume_percentile = (recent_volume <= current_volume).sum() / len(recent_volume)
        turnover_percentile = (recent_turnover <= current_turnover).sum() / len(recent_turnover)
        percentile_score = (volume_percentile + turnover_percentile) / 2
        
        # 标准差得分（±2σ法）
        volume_mean = recent_volume.mean()
        volume_std = recent_volume.std()
        turnover_mean = recent_turnover.mean()
        turnover_std = recent_turnover.std()
        
        if volume_std > 0 and turnover_std > 0:
            volume_z = (current_volume - volume_mean) / volume_std
            turnover_z = (current_turnover - turnover_mean) / turnover_std
            
            # 在±2σ范围内得分为正
            volume_score = 1.0 if -2 <= volume_z <= 2 else 0.0
            turnover_score = 1.0 if -2 <= turnover_z <= 2 else 0.0
            sigma_score = (volume_score + turnover_score) / 2
        else:
            sigma_score = 0.0
        
        return {
            'percentile_score': percentile_score if not pd.isna(percentile_score) else 0.5,
            'sigma_score': sigma_score if not pd.isna(sigma_score) else 0.0
        }
        
    except Exception as e:
        logger.error(f"计算成交择时打分失败: {e}")
        return {'percentile_score': 0.5, 'sigma_score': 0.0}


def check_liquidity_warning(volume_series: pd.Series, price_series: pd.Series, 
                           period: int = 20) -> bool:
    """
    检查流动性恶化预警
    
    参数：
    - volume_series: 成交量序列
    - price_series: 价格序列
    - period: 计算周期
    
    返回：
    - True表示有流动性恶化预警，False表示正常
    """
    try:
        if len(volume_series) < period + 5 or len(price_series) < period + 5:
            return False
        
        # 计算单位交易额对价格的边际影响（简化版）
        # 如果放量但价格下跌，可能是流动性恶化
        recent_volume = volume_series.iloc[-period:]
        recent_prices = price_series.iloc[-period:]
        
        # 计算价格变化率
        price_change = recent_prices.pct_change().dropna()
        
        # 计算成交量变化率
        volume_change = recent_volume.pct_change().dropna()
        
        # 如果最近放量（成交量增加）但价格下跌，可能是流动性恶化
        if len(price_change) > 0 and len(volume_change) > 0:
            recent_price_change = price_change.iloc[-5:].mean()
            recent_volume_change = volume_change.iloc[-5:].mean()
            
            # 放量下跌可能是流动性恶化
            if recent_volume_change > 0.2 and recent_price_change < -0.02:
                return True
        
        return False
        
    except Exception as e:
        logger.error(f"检查流动性预警失败: {e}")
        return False


def check_single_date_condition(code: str, stock_data: pd.DataFrame,
                                target_date: date,
                                mass_short_period: int = 60,
                                mass_long_period: int = 360,
                                deviation_min: float = -0.10,
                                deviation_max: float = -0.02,
                                debug_mode: bool = False,
                                filter_config: Optional[Dict[str, bool]] = None) -> Optional[Dict]:
    """
    检查单只股票在指定日期是否满足反弹条件（用于随机日期回测）
    
    参数：
    - code: 股票代码
    - stock_data: 股票数据DataFrame（需包含Close, Volume, TurnoverRate等字段）
    - target_date: 目标日期（截止日期）
    - mass_short_period: MASS短期周期（默认60）
    - mass_long_period: MASS长期周期（默认360）
    - deviation_min: 偏离度最小值（默认-10%，即-0.10）
    - deviation_max: 偏离度最大值（默认-2%，即-0.02）
    - debug_mode: 调试模式
    - filter_config: 筛选步骤配置字典，包含：
        - 'trend_filter': bool，是否执行上升趋势过滤
        - 'pullback_filter': bool，是否执行回调识别
        - 'end_confirm_filter': bool，是否执行末端确认
      如果为None，则默认全部执行
    
    返回：
    - 包含分析结果的字典，如果不满足条件则返回None
    """
    # 设置默认配置（全部执行）
    if filter_config is None:
        filter_config = {
            'trend_filter': True,
            'pullback_filter': True,
            'end_confirm_filter': True
        }
    
    try:
        if stock_data is None or stock_data.empty:
            return None
        
        # 过滤数据到目标日期
        # 需要至少mass_long_period天的历史数据来计算趋势指标
        df = stock_data[stock_data.index <= pd.Timestamp(target_date)].copy()
        if df.empty or len(df) < mass_long_period:
            return None
        
        # 提取收盘价等数据
        close = pd.to_numeric(df.get("Close"), errors="coerce")
        close = close.dropna()
        if close.empty:
            return None
        
        # 找到目标日期对应的索引
        target_idx = None
        
        # 尝试精确匹配
        for i, date_val in enumerate(close.index):
            if hasattr(date_val, 'date'):
                date_val_date = date_val.date()
            elif isinstance(date_val, pd.Timestamp):
                date_val_date = date_val.date()
            else:
                date_val_date = date_val
            
            if date_val_date == target_date:
                target_idx = i
                break
        
        # 如果精确匹配失败，尝试找最接近的日期（向前查找，不超过5个交易日）
        if target_idx is None:
            # 找到最接近目标日期且小于等于目标日期的日期
            for i in range(len(close) - 1, -1, -1):
                date_val = close.index[i]
                if hasattr(date_val, 'date'):
                    date_val_date = date_val.date()
                elif isinstance(date_val, pd.Timestamp):
                    date_val_date = date_val.date()
                else:
                    date_val_date = date_val
                
                if date_val_date <= target_date:
                    target_idx = i
                    break
        
        if target_idx is None or target_idx < 1:
            return None
        
        # 只使用到目标日期的数据
        close_up_to_target = close.iloc[:target_idx+1]
        
        # 计算MASS得分
        mass_short = calculate_mass_score(close_up_to_target, mass_short_period)
        mass_long = calculate_mass_score(close_up_to_target, mass_long_period)
        
        # 计算均线
        ma5 = close_up_to_target.rolling(5).mean()
        ma10 = close_up_to_target.rolling(10).mean()
        ema20 = close_up_to_target.ewm(span=20, adjust=False).mean()
        
        # 计算均线斜率（使用Series，类似analyze_uptrend_rebound的方式）
        ma5_slope_all = ma5.diff(5) / ma5  # 简化的斜率计算
        ma10_slope_all = ma10.diff(10) / ma10
        
        # 计算偏离度
        deviation = (close_up_to_target / ema20 - 1.0)
        
        # 获取目标日期的指标值
        mass_short_at_target = mass_short
        mass_long_at_target = mass_long
        ma5_slope_at_target = ma5_slope_all.iloc[target_idx] if target_idx < len(ma5_slope_all) and not pd.isna(ma5_slope_all.iloc[target_idx]) else 0
        ma10_slope_at_target = ma10_slope_all.iloc[target_idx] if target_idx < len(ma10_slope_all) and not pd.isna(ma10_slope_all.iloc[target_idx]) else 0
        current_deviation_at_target = deviation.iloc[target_idx] if target_idx < len(deviation) else 0
        prev_deviation_at_target = deviation.iloc[target_idx-1] if target_idx > 0 and (target_idx-1) < len(deviation) else None
        ma5_at_target = ma5.iloc[target_idx] if target_idx < len(ma5) else 0
        close_at_target = close_up_to_target.iloc[target_idx]
        
        # A. 上升趋势过滤（可选）
        if filter_config.get('trend_filter', True):
            if debug_mode:
                mass_short_ok = mass_short_at_target >= 30
                mass_long_ok = (mass_long_at_target >= 30) if mass_long_at_target > 0 else True
            else:
                mass_short_ok = mass_short_at_target >= 50
                mass_long_ok = mass_long_at_target >= 50
            
            trend_ok = (mass_short_ok and mass_long_ok and 
                       ma5_slope_at_target >= 0 and ma10_slope_at_target >= 0 and 
                       close_at_target >= ma5_at_target)
            
            if not trend_ok:
                return None
        
        # B. 回调识别（宽松版本）：只要求“有一定深度的回调”，不再过度苛刻
        pullback_ok = True  # 默认通过
        deviation_turned_negative = False
        if filter_config.get('pullback_filter', True):
            if prev_deviation_at_target is None:
                return None
            
            # 原始设计区间 [-10%, -2%]
            base_min = deviation_min
            base_max = deviation_max
            # 宽松区间，例如 [-15%, -1%]
            loose_min = base_min * 1.5
            loose_max = base_max * 0.5
            
            deviation_in_loose_range = loose_min <= current_deviation_at_target <= loose_max
            
            # 记录是否由正转负或处于负区间（用于打分参考）
            turned_from_positive = prev_deviation_at_target > 0 and current_deviation_at_target <= 0
            already_in_negative = (prev_deviation_at_target <= 0 and current_deviation_at_target < 0)
            deviation_turned_negative = turned_from_positive or already_in_negative
            
            pullback_ok = deviation_in_loose_range
            if not pullback_ok:
                return None
        
        # C. 末端确认（改为影响打分，而不是一票否决）
        cross_ma5 = False
        cross_mid = False
        band_expand = False
        volume_confirmed = False
        environment_ok = True
        
        if filter_config.get('end_confirm_filter', True):
            # 计算布林带
            bb = calculate_bollinger_bands(close_up_to_target, period=20, std_dev=2.0)
            bb_mid = bb['middle'].iloc[target_idx] if target_idx < len(bb) else 0
            bb_upper = bb['upper'].iloc[target_idx] if target_idx < len(bb) else 0
            
            # 检查是否上穿MA5和中轨
            prev_close = close_up_to_target.iloc[target_idx-1] if target_idx > 0 else close_at_target
            prev_ma5 = ma5.iloc[target_idx-1] if target_idx > 0 and (target_idx-1) < len(ma5) else ma5_at_target
            prev_bb_mid = bb['middle'].iloc[target_idx-1] if target_idx > 0 and (target_idx-1) < len(bb) else bb_mid
            
            cross_ma5 = (prev_close <= prev_ma5 and close_at_target > ma5_at_target)
            cross_mid = (prev_close <= prev_bb_mid and close_at_target > bb_mid)
            
            # 布林带扩张
            bb_width = bb['bandwidth'].iloc[target_idx] if target_idx < len(bb) and 'bandwidth' in bb.columns else 0
            band_expand = bb_width > 0.02  # 带宽>2%
            
            # 成交确认
            volume = pd.to_numeric(df.get("Volume"), errors="coerce")
            turnover = pd.to_numeric(df.get("TurnoverRate"), errors="coerce")
            
            if not volume.empty and not turnover.empty and len(volume) > target_idx:
                volume_up_to_target = volume.iloc[:target_idx+1]
                turnover_up_to_target = turnover.iloc[:target_idx+1]
                volume_timing = calculate_volume_timing_score(
                    volume_up_to_target,
                    turnover_up_to_target,
                    period=min(60, len(volume_up_to_target))
                ) if len(volume_up_to_target) >= 60 else {'percentile_score': 0.5, 'sigma_score': 0.0}
                volume_confirmed = volume_timing['percentile_score'] >= 0.5
            else:
                volume_confirmed = False
            
            # 流动性预警
            liquidity_warning = check_liquidity_warning(volume, close_up_to_target, period=20) if not volume.empty else False
            environment_ok = not liquidity_warning
        
        # 计算综合信号强度（0-100分）
        signal_strength = 0.0
        trend_score = (mass_short_at_target + mass_long_at_target) / 2
        signal_strength += trend_score * 0.4
        
        # 偏离度位置（-5%附近最优）
        optimal_deviation = -0.05
        deviation_score = 1.0 - abs(current_deviation_at_target - optimal_deviation) / 0.05
        deviation_score = max(0.0, min(1.0, deviation_score))
        signal_strength += deviation_score * 20
        
        # 价端确认（20分）
        if cross_ma5 and cross_mid:
            signal_strength += 20
        elif cross_ma5 or cross_mid:
            signal_strength += 10
        
        # 量端确认（10分）
        if volume_confirmed:
            signal_strength += 10
        
        # 环境安全（10分）
        if environment_ok:
            signal_strength += 10
        
        result = {
            '股票代码': code,
            '信号日期': target_date,
            '收盘价': round(float(close_at_target), 2),
            'MASS短期': round(mass_short_at_target, 2),
            'MASS长期': round(mass_long_at_target, 2),
            '偏离度': round(current_deviation_at_target * 100, 2),
            'MA5斜率': round(ma5_slope_at_target * 100, 4),
            'MA10斜率': round(ma10_slope_at_target * 100, 4),
            '是否上穿MA5': cross_ma5,
            '是否上穿中轨': cross_mid,
            '布林带扩张': band_expand,
            '综合信号强度': round(signal_strength, 2),
            '是否满足全部条件': True,
            'MASS条件': '全部满足'
        }
        
        return result
        
    except Exception as e:
        logger.error(f"检查股票 {code} 在日期 {target_date} 的条件时出错: {e}")
        return None


def analyze_uptrend_rebound(code: str, stock_data: pd.DataFrame,
                            mass_short_period: int = 60,
                            mass_long_period: int = 360,
                            deviation_min: float = -0.10,
                            deviation_max: float = -0.02,
                            lookback_days: int = 20,
                            debug_mode: bool = False,
                            filter_config: Optional[Dict[str, bool]] = None) -> Optional[Dict]:
    """
    分析单只股票的上升趋势反弹机会
    
    参数：
    - code: 股票代码
    - stock_data: 股票数据DataFrame（需包含Close, Volume, TurnoverRate等字段）
    - mass_short_period: MASS短期周期（默认60）
    - mass_long_period: MASS长期周期（默认360）
    - deviation_min: 偏离度最小值（默认-10%）
    - deviation_max: 偏离度最大值（默认-2%）
    - lookback_days: 观测窗口期，检查最近多少个交易日（默认20，支持20/30/60）
    
    返回：
    - 包含分析结果的字典，如果没有满足条件的反弹机会则返回None
    """
    try:
        # 添加超时保护：如果处理时间过长，记录日志
        import time as time_module
        start_time = time_module.time()
        
        # 调试模式：统计各阶段通过率（提前初始化，确保即使早期返回也能记录）
        debug_stats = {
            'total_checked': 0,
            'trend_filter_pass': 0,
            'pullback_pass': 0,
            'end_confirm_pass': 0,
            'final_pass': 0,
            'data_insufficient': False,
            'data_days': 0
        }
        
        if stock_data is None or stock_data.empty:
            if debug_mode:
                return {
                    '股票代码': code,
                    '信号日期': None,
                    '调试模式': True,
                    '调试_数据状态': '数据为空',
                    '调试_总检查天数': 0,
                    '调试_趋势过滤通过': 0,
                    '调试_回调识别通过': 0,
                    '调试_末端确认通过': 0,
                    '调试_最终通过': 0,
                }
            else:
                # 正常模式：返回数据为空的失败原因（补全与「满足条件」同名的指标字段，便于导出统一明细）
                return {
                    '股票代码': code,
                    '信号日期': None,
                    '是否满足条件': False,
                    '失败原因汇总': '数据为空',
                    '失败详情数量': 0,
                    '失败详情': [],
                    '收盘价': None, 'MASS短期': None, 'MASS长期': None, '偏离度': None, 'MA5斜率': None, 'MA10斜率': None,
                    '是否上穿MA5': None, '是否上穿中轨': None, '布林带扩张': None, '换手率Z20': None, '成交择时得分': None, '流动性预警': None, '综合信号强度': None,
                }
        
        # 确保按日期升序
        df = stock_data.sort_index().copy()
        debug_stats['data_days'] = len(df)
        
        # 需要至少360天的数据来计算长期MASS（调试模式降低要求）
        # 允许95%的天数满足即可（考虑非交易日、停牌等因素）
        min_required_days_full = max(mass_long_period, 100) if not debug_mode else max(mass_long_period // 2, 60)
        min_required_days = int(min_required_days_full * 0.95)  # 95%的容差
        
        if len(df) < min_required_days:
            # 增强数据不足时的日志记录：即使非调试模式也记录原因
            logger.debug(f"股票 {code} 数据不足：仅{len(df)}天，需要至少{min_required_days}天（{min_required_days_full}天的95%）")
            if debug_mode:
                debug_stats['data_insufficient'] = True
                return {
                    '股票代码': code,
                    '信号日期': None,
                    '调试模式': True,
                    '调试_数据状态': f'数据不足（{len(df)}天 < {min_required_days}天，需要{min_required_days_full}天的95%）',
                    '调试_数据天数': len(df),
                    '调试_需要天数': min_required_days_full,
                    '调试_实际要求天数': min_required_days,
                    '调试_总检查天数': 0,
                    '调试_趋势过滤通过': 0,
                    '调试_回调识别通过': 0,
                    '调试_末端确认通过': 0,
                    '调试_最终通过': 0,
                }
            else:
                # 正常模式：返回数据不足的失败原因（补全与「满足条件」同名的指标字段，便于导出统一明细）
                return {
                    '股票代码': code,
                    '信号日期': None,
                    '是否满足条件': False,
                    '失败原因汇总': f'数据不足（仅{len(df)}天，需要至少{min_required_days}天，即{min_required_days_full}天的95%）',
                    '失败详情数量': 0,
                    '当前数据天数': len(df),
                    '需要数据天数': min_required_days_full,
                    '实际要求天数': min_required_days,
                    '失败详情': [],
                    '收盘价': None, 'MASS短期': None, 'MASS长期': None, '偏离度': None, 'MA5斜率': None, 'MA10斜率': None,
                    '是否上穿MA5': None, '是否上穿中轨': None, '布林带扩张': None, '换手率Z20': None, '成交择时得分': None, '流动性预警': None, '综合信号强度': None,
                }
        
        # 提取关键字段（与功能11对齐：batch_get_stock_data_with_adjustment返回Close, Volume, TurnoverRate）
        close = pd.to_numeric(df.get("Close"), errors="coerce")
        close = close.dropna()
        volume = pd.to_numeric(df.get("Volume", pd.Series()), errors="coerce").dropna()
        turnover = pd.to_numeric(df.get("TurnoverRate", pd.Series()), errors="coerce").dropna()
        
        if close.empty:
            if debug_mode:
                return {
                    '股票代码': code,
                    '信号日期': None,
                    '调试模式': True,
                    '调试_数据状态': '收盘价数据为空',
                    '调试_总检查天数': 0,
                    '调试_趋势过滤通过': 0,
                    '调试_回调识别通过': 0,
                    '调试_末端确认通过': 0,
                    '调试_最终通过': 0,
                }
            else:
                # 正常模式：返回收盘价数据为空的失败原因（指标列占位）
                return {
                    '股票代码': code,
                    '信号日期': None,
                    '是否满足条件': False,
                    '失败原因汇总': '收盘价数据为空',
                    '失败详情数量': 0,
                    '失败详情': [],
                    '收盘价': None, 'MASS短期': None, 'MASS长期': None, '偏离度': None, 'MA5斜率': None, 'MA10斜率': None,
                    '是否上穿MA5': None, '是否上穿中轨': None, '布林带扩张': None, '换手率Z20': None, '成交择时得分': None, '流动性预警': None, '综合信号强度': None,
                }
        
        # 计算技术指标（注意：所有指标都基于历史数据，不使用未来数据）
        # 1. MASS均线排列打分（使用全部历史数据计算，但只取最近n天评估）
        mass_short = calculate_mass_score(close, mass_short_period)
        mass_long = calculate_mass_score(close, mass_long_period)
        
        # 2. 均线计算（使用rolling/ewm，只使用当前日期及之前的数据）
        ema20 = close.ewm(span=20, adjust=False).mean()
        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        
        # 3. 均线斜率（使用最近period天的数据计算，不使用未来数据）
        ma5_slope = calculate_ma_slope(ma5)
        ma10_slope = calculate_ma_slope(ma10)
        
        # 4. 偏离度 = Close/EMA20 - 1（EMA20只使用当前日期及之前的数据）
        deviation = (close / ema20 - 1.0)
        
        # 5. 布林带（使用rolling计算，只使用当前日期及之前的数据）
        bb = calculate_bollinger_bands(close, period=20, std_dev=2.0)
        
        # 6. 换手率Z-score（使用最近20天的历史数据）
        turnover_z20 = calculate_turnover_zscore(turnover, period=20) if not turnover.empty else 0.0
        
        # 7. 成交择时打分（使用最近60天的历史数据）
        volume_timing = calculate_volume_timing_score(volume, turnover, period=60) if not volume.empty and not turnover.empty else {'percentile_score': 0.5, 'sigma_score': 0.0}
        
        # 8. 流动性预警（使用最近20天的历史数据）
        liquidity_warning = check_liquidity_warning(volume, close, period=20) if not volume.empty else False
        
        # 注意：以上所有指标的计算都是基于历史数据，不存在未来函数问题
        # 但是，在循环中判断每个日期时，需要确保只使用该日期及之前的数据
        
        # 性能优化：预先计算所有日期的指标（避免在循环中重复计算）
        # 使用向量化操作，大幅提升性能
        # lookback_days参数控制观测窗口期（默认20天，支持20/30/60天）
        actual_lookback_days = min(lookback_days, len(close))
        max_check_index = len(close) - 1
        min_check_index = max(1, max_check_index - actual_lookback_days + 1)
        
        # 预先计算所有日期的指标（使用向量化操作）
        # 1. 预先计算所有日期的MA5、MA10、EMA20
        ma5_all = close.rolling(5).mean()
        ma10_all = close.rolling(10).mean()
        ema20_all = close.ewm(span=20, adjust=False).mean()
        
        # 2. 预先计算所有日期的偏离度
        deviation_all = (close / ema20_all - 1.0)
        
        # 3. 预先计算所有日期的MA5、MA10斜率（使用rolling窗口）
        ma5_slope_all = ma5_all.diff(5) / ma5_all  # 简化的斜率计算
        ma10_slope_all = ma10_all.diff(10) / ma10_all
        
        # 4. 预先计算所有日期的换手率Z-score（使用rolling窗口）
        turnover_z20_all = pd.Series(index=turnover.index, dtype=float)
        if not turnover.empty and len(turnover) >= 20:
            turnover_mean = turnover.rolling(20).mean()
            turnover_std = turnover.rolling(20).std()
            turnover_z20_all = (turnover - turnover_mean) / turnover_std
            turnover_z20_all = turnover_z20_all.fillna(0.0)
        
        # 5. 预先计算所有日期的布林带宽度
        bb_all = calculate_bollinger_bands(close, period=20, std_dev=2.0)
        bb_width_all = bb_all['bandwidth'] if 'bandwidth' in bb_all.columns else pd.Series(index=close.index, dtype=float)
        
        # 6. 预先计算所有日期的MASS（使用缓存，避免重复计算）
        # 注意：MASS计算较复杂，但我们可以优化为只计算需要的日期
        mass_short_cache = {}
        mass_long_cache = {}
        
        # 分析最近的数据点（最近N个交易日，N=lookback_days）
        results = []
        
        # 正常模式：记录失败原因（用于导出所有股票）
        failure_reasons = []  # 记录每个检查日期的失败原因
        
        for i in range(min_check_index, max_check_index + 1):
            if i < 1:
                continue
            
            if debug_mode:
                debug_stats['total_checked'] += 1
            
            current_date = close.index[i]
            close_up_to_i = close.iloc[:i+1]  # 只使用到日期i的数据
            
            # 正常模式：记录当前检查日期的失败原因
            current_failure_reason = None
            
            # 使用缓存的MASS值（避免重复计算）
            data_len = len(close_up_to_i)
            cache_key_short = min(mass_short_period, data_len)
            cache_key_long = min(mass_long_period, data_len) if data_len >= mass_long_period * 0.8 else 0
            
            if cache_key_short not in mass_short_cache:
                mass_short_cache[cache_key_short] = calculate_mass_score(close_up_to_i, cache_key_short)
            mass_short_at_i = mass_short_cache[cache_key_short]
            
            if debug_mode:
                if cache_key_long > 0 and cache_key_long not in mass_long_cache:
                    mass_long_cache[cache_key_long] = calculate_mass_score(close_up_to_i, cache_key_long)
                mass_long_at_i = mass_long_cache.get(cache_key_long, 0)
            else:
                if cache_key_long > 0 and cache_key_long not in mass_long_cache:
                    mass_long_cache[cache_key_long] = calculate_mass_score(close_up_to_i, cache_key_long)
                mass_long_at_i = mass_long_cache.get(cache_key_long, 0)
            
            # 使用预先计算的指标（避免重复计算）
            ma5_slope_at_i = ma5_slope_all.iloc[i] if i < len(ma5_slope_all) and not pd.isna(ma5_slope_all.iloc[i]) else 0
            ma10_slope_at_i = ma10_slope_all.iloc[i] if i < len(ma10_slope_all) and not pd.isna(ma10_slope_all.iloc[i]) else 0
            current_deviation_at_i = deviation_all.iloc[i] if i < len(deviation_all) else 0
            prev_deviation_at_i = deviation_all.iloc[i-1] if i > 0 and (i-1) < len(deviation_all) else None
            turnover_z20_at_i = turnover_z20_all.iloc[i] if i < len(turnover_z20_all) else 0.0
            
            # 对于成交择时打分，仍然需要按日期计算（因为需要历史数据）
            if not volume.empty and not turnover.empty and len(volume) > i and len(turnover) > i:
                volume_up_to_i = volume.iloc[:i+1]
                turnover_up_to_i = turnover.iloc[:i+1]
                volume_timing_at_i = calculate_volume_timing_score(volume_up_to_i, turnover_up_to_i, period=min(60, len(volume_up_to_i))) if len(volume_up_to_i) >= 60 else {'percentile_score': 0.5, 'sigma_score': 0.0}
            else:
                volume_timing_at_i = {'percentile_score': 0.5, 'sigma_score': 0.0}
            
            # 使用预先计算的MA5、MA10、EMA20
            ma5_at_i = ma5_all.iloc[i] if i < len(ma5_all) else 0
            ma10_at_i = ma10_all.iloc[i] if i < len(ma10_all) else 0
            prev_ma5 = ma5_all.iloc[i-1] if i > 0 and (i-1) < len(ma5_all) else 0
            prev_ema20 = ema20_all.iloc[i-1] if i > 0 and (i-1) < len(ema20_all) else 0
            current_ema20 = ema20_all.iloc[i] if i < len(ema20_all) else 0
            
            # A. 上升趋势过滤（可选）
            if filter_config.get('trend_filter', True):
                # 调试模式：放宽MASS阈值（短期≥30，长期≥30或数据不足时忽略长期）
                if debug_mode:
                    mass_short_ok = mass_short_at_i >= 30
                    mass_long_ok = (mass_long_at_i >= 30) if mass_long_at_i > 0 else True  # 如果长期MASS为0（数据不足），忽略此条件
                else:
                    mass_short_ok = mass_short_at_i >= 50
                    mass_long_ok = mass_long_at_i >= 50
                
                trend_ok = (mass_short_ok and mass_long_ok and 
                           ma5_slope_at_i >= 0 and ma10_slope_at_i >= 0 and 
                           close.iloc[i] >= ma5_at_i)
                
                if not trend_ok:
                    # 正常模式：记录趋势过滤失败原因
                    if not debug_mode:
                        reasons = []
                        if not mass_short_ok:
                            reasons.append(f"MASS短期不足({mass_short_at_i:.2f} < 50)")
                        if not mass_long_ok:
                            reasons.append(f"MASS长期不足({mass_long_at_i:.2f} < 50)")
                        if ma5_slope_at_i < 0:
                            reasons.append(f"MA5斜率<0({ma5_slope_at_i*100:.2f}%)")
                        if ma10_slope_at_i < 0:
                            reasons.append(f"MA10斜率<0({ma10_slope_at_i*100:.2f}%)")
                        if close.iloc[i] < ma5_at_i:
                            reasons.append(f"收盘价<MA5({close.iloc[i]:.2f} < {ma5_at_i:.2f})")
                        current_failure_reason = f"趋势过滤失败: {', '.join(reasons)}"
                        # 记录完整的指标信息（包括所有阶段的指标，便于分析）
                        # 注意：偏离度已经在循环外预先计算，可以直接使用
                        current_deviation_for_record = deviation_all.iloc[i] if i < len(deviation_all) else 0
                        prev_deviation_for_record = deviation_all.iloc[i-1] if i > 0 and (i-1) < len(deviation_all) else None
                        
                        failure_record = {
                            '日期': current_date.date() if hasattr(current_date, 'date') else current_date,
                            '失败阶段': '趋势过滤',
                            '失败原因': current_failure_reason,
                            # 趋势过滤相关指标
                            'MASS短期': round(mass_short_at_i, 2),
                            'MASS长期': round(mass_long_at_i, 2),
                            'MA5斜率': round(ma5_slope_at_i * 100, 4),
                            'MA10斜率': round(ma10_slope_at_i * 100, 4),
                            '收盘价': round(float(close.iloc[i]), 2),
                            'MA5': round(float(ma5_at_i), 2),
                            # 回调识别相关指标（即使失败也记录，便于分析）
                            '偏离度': round(current_deviation_for_record * 100, 2),
                            '前一日偏离度': round(prev_deviation_for_record * 100, 2) if prev_deviation_for_record is not None else None,
                        }
                        failure_reasons.append(failure_record)
                    continue  # 不满足上升趋势条件
                
                if debug_mode:
                    debug_stats['trend_filter_pass'] += 1
            
            # B. 回调识别（可选，改为“宽松过滤”，只要有一次像样回调即可）
            pullback_ok = True  # 默认通过
            deviation_turned_negative = False  # 用于后续打分参考
            if filter_config.get('pullback_filter', True):
                # 使用只到日期i的偏离度（避免未来函数）
                if prev_deviation_at_i is None:
                    continue
                
                # 原始设计区间：[-10%, -2%]
                base_min = deviation_min
                base_max = deviation_max
                # 宽松区间：允许稍微深一点或浅一点的回调，例如 [-15%, -1%]
                loose_min = base_min * 1.5  # -0.10 -> -0.15
                loose_max = base_max * 0.5  # -0.02 -> -0.01
                
                # 是否在“宽松回调区间”内
                deviation_in_loose_range = loose_min <= current_deviation_at_i <= loose_max
                
                # 是否由正转负或持续在负区间，用于打分而不是硬过滤
                turned_from_positive = prev_deviation_at_i > 0 and current_deviation_at_i <= 0
                already_in_negative = (prev_deviation_at_i <= 0 and current_deviation_at_i < 0)
                deviation_turned_negative = turned_from_positive or already_in_negative
                
                # 新的硬门槛：只要“有一定深度的回调”即可，通过与否不再依赖“由正转负”
                pullback_ok = deviation_in_loose_range
                
                if not pullback_ok:
                    # 记录失败原因，但不再过度严苛，只要在窗口内别的日期满足即可
                    if not debug_mode:
                        reasons = []
                        if current_deviation_at_i > 0:
                            reasons.append(f"价格仍在均线之上(偏离度{current_deviation_at_i*100:.2f}%>0，未出现回调)")
                        elif current_deviation_at_i > loose_max:
                            reasons.append(f"回调幅度偏小(偏离度{current_deviation_at_i*100:.2f}% > {loose_max*100:.1f}%)")
                        elif current_deviation_at_i < loose_min:
                            reasons.append(f"回调幅度偏大(偏离度{current_deviation_at_i*100:.2f}% < {loose_min*100:.1f}%)")
                        current_failure_reason = f"回调识别失败: {', '.join(reasons)}"
                        failure_record = {
                            '日期': current_date.date() if hasattr(current_date, 'date') else current_date,
                            '失败阶段': '回调识别',
                            '失败原因': current_failure_reason,
                            'MASS短期': round(mass_short_at_i, 2),
                            'MASS长期': round(mass_long_at_i, 2),
                            'MA5斜率': round(ma5_slope_at_i * 100, 4),
                            'MA10斜率': round(ma10_slope_at_i * 100, 4),
                            '收盘价': round(float(close.iloc[i]), 2),
                            'MA5': round(float(ma5_at_i), 2),
                            '偏离度': round(current_deviation_at_i * 100, 2),
                            '前一日偏离度': round(prev_deviation_at_i * 100, 2) if prev_deviation_at_i is not None else None,
                        }
                        failure_reasons.append(failure_record)
                    # 这一日不算信号，但窗口内还会继续检查其他日期
                    continue
                
                if debug_mode:
                    debug_stats['pullback_pass'] += 1
            
            # C. 末端确认（改为“打分”而非硬过滤）
            # 先设置默认值，确保后续打分逻辑始终有值可用
            price_confirmed = False
            volume_confirmed = True
            environment_ok = True
            
            if filter_config.get('end_confirm_filter', True):
                # 价端：二次上穿MA5或布林中轨（使用预先计算的指标）
                prev_close = close.iloc[i-1]
                current_close = close.iloc[i]
                
                cross_ma5 = (prev_close < prev_ma5) and (current_close >= ma5_at_i)
                cross_mid = (prev_close < prev_ema20) and (current_close >= current_ema20)
                
                # 布林带由收敛转扩张（使用预先计算的布林带，避免重复计算）
                # 预先计算所有日期的布林带宽度（在循环外计算）
                if i >= 20 and i < len(bb_width_all) and i >= 2:
                    current_bb_width = bb_width_all.iloc[i] if not pd.isna(bb_width_all.iloc[i]) else 0
                    prev_bb_width = bb_width_all.iloc[i-1] if not pd.isna(bb_width_all.iloc[i-1]) else 0
                    prev2_bb_width = bb_width_all.iloc[i-2] if not pd.isna(bb_width_all.iloc[i-2]) else 0
                    band_expand = (current_bb_width > prev_bb_width) and (prev_bb_width > prev2_bb_width)
                else:
                    band_expand = False
                
                # 量端：Turnover_z20回归常态（-1至+1）（使用只到日期i的数据，避免未来函数）
                # 调试模式：放宽到[-2, +2]
                tz20_threshold = 2.0 if debug_mode else 1.0
                tz20_ok = -tz20_threshold <= turnover_z20_at_i <= tz20_threshold
                
                # 成交择时得分≥分位50%（调试模式：降低到30%）（使用只到日期i的数据，避免未来函数）
                vol_score_threshold = 0.3 if debug_mode else 0.5
                vol_score_ok = volume_timing_at_i['percentile_score'] >= vol_score_threshold or volume_timing_at_i['sigma_score'] > 0
                
                # 环境：无流动性恶化预警（使用只到日期i的数据，避免未来函数）
                if not volume.empty and len(volume) > i:
                    volume_up_to_i = volume.iloc[:i+1]
                    close_up_to_i_for_liq = close.iloc[:i+1]
                    liquidity_warning_at_i = check_liquidity_warning(volume_up_to_i, close_up_to_i_for_liq, period=min(20, len(volume_up_to_i))) if len(volume_up_to_i) >= 20 else False
                else:
                    liquidity_warning_at_i = False
                
                no_liq_warn = not liquidity_warning_at_i if not debug_mode else True
                
                # 综合判断：末端确认（仅用于打分，不再作为硬性过滤条件）
                price_confirmed = cross_ma5 or cross_mid
                volume_confirmed = tz20_ok and vol_score_ok
                environment_ok = no_liq_warn
                
                if not (price_confirmed and volume_confirmed and environment_ok) and not debug_mode:
                    # 仍然记录失败原因，便于事后分析，但不再直接剔除
                    reasons = []
                    if not price_confirmed:
                        reasons.append("价端未确认(未上穿MA5且未上穿中轨)")
                    if not volume_confirmed:
                        vol_reasons = []
                        if not tz20_ok:
                            vol_reasons.append(f"换手率Z20不在范围({turnover_z20_at_i:.2f}不在[-1,1])")
                        if not vol_score_ok:
                            vol_reasons.append(f"成交择时得分不足({volume_timing_at_i['percentile_score']:.3f} < 0.5)")
                        if vol_reasons:
                            reasons.append(f"量端未确认: {', '.join(vol_reasons)}")
                    if not environment_ok:
                        reasons.append("环境不安全(有流动性预警)")
                    current_failure_reason = f"末端确认未完全满足: {', '.join(reasons)}"
                    failure_record = {
                        '日期': current_date.date() if hasattr(current_date, 'date') else current_date,
                        '失败阶段': '末端确认',
                        '失败原因': current_failure_reason,
                        'MASS短期': round(mass_short_at_i, 2),
                        'MASS长期': round(mass_long_at_i, 2),
                        'MA5斜率': round(ma5_slope_at_i * 100, 4),
                        'MA10斜率': round(ma10_slope_at_i * 100, 4),
                        '收盘价': round(float(close.iloc[i]), 2),
                        'MA5': round(float(ma5_at_i), 2),
                        '偏离度': round(current_deviation_at_i * 100, 2),
                        '前一日偏离度': round(prev_deviation_at_i * 100, 2) if prev_deviation_at_i is not None else None,
                        '是否上穿MA5': cross_ma5,
                        '是否上穿中轨': cross_mid,
                        '换手率Z20': round(turnover_z20_at_i, 2),
                        '成交择时得分': round(volume_timing_at_i['percentile_score'], 3),
                        '流动性预警': liquidity_warning_at_i,
                    }
                    failure_reasons.append(failure_record)
                
                if debug_mode and price_confirmed and volume_confirmed:
                    debug_stats['end_confirm_pass'] += 1
            
            # 记录满足条件的反弹信号
            result = {
                '股票代码': code,
                '信号日期': current_date.date() if hasattr(current_date, 'date') else current_date,
                '收盘价': round(float(current_close), 2),
                'MASS短期': round(mass_short_at_i, 2),  # 使用日期i时的MASS值
                'MASS长期': round(mass_long_at_i, 2),  # 使用日期i时的MASS值
                '偏离度': round(current_deviation_at_i * 100, 2),  # 转换为百分比（使用日期i时的值）
                'MA5斜率': round(ma5_slope_at_i * 100, 4),  # 转换为百分比（使用日期i时的值）
                'MA10斜率': round(ma10_slope_at_i * 100, 4),  # 使用日期i时的值
                '是否上穿MA5': cross_ma5,
                '是否上穿中轨': cross_mid,
                '布林带扩张': band_expand,
                '换手率Z20': round(turnover_z20_at_i, 2),  # 使用日期i时的值
                '成交择时得分': round(volume_timing_at_i['percentile_score'], 3),  # 使用日期i时的值
                '流动性预警': liquidity_warning_at_i,  # 使用日期i时的值
                '综合信号强度': 0.0  # 待计算
            }
            
            # 计算综合信号强度（0-100分）
            signal_strength = 0.0
            
            # 趋势强度（40分）（使用日期i时的值）
            trend_score = (mass_short_at_i + mass_long_at_i) / 2
            signal_strength += trend_score * 0.4
            
            # 偏离度位置（20分，-5%附近最优）（使用日期i时的值）
            optimal_deviation = -0.05
            deviation_score = 1.0 - abs(current_deviation_at_i - optimal_deviation) / 0.05
            deviation_score = max(0.0, min(1.0, deviation_score))
            signal_strength += deviation_score * 20
            
            # 价端确认（20分）
            if cross_ma5 and cross_mid:
                signal_strength += 20
            elif cross_ma5 or cross_mid:
                signal_strength += 10
            
            # 量端确认（10分）
            if volume_confirmed:
                signal_strength += 10
            
            # 环境安全（10分）
            if environment_ok:
                signal_strength += 10
            
            result['综合信号强度'] = round(signal_strength, 2)
            results.append(result)
        
        # 在窗口期内可能有多个候选信号：按“综合信号强度”从高到低排序，取得分最高的那个
        if results:
            # 先按综合信号强度排序，其次按信号日期排序（保证窗口内选“最优一次”）
            results.sort(
                key=lambda x: (
                    x.get('综合信号强度', 0.0),
                    x['信号日期'] if x.get('信号日期') else date.min
                ),
                reverse=True
            )
            result = results[0]
            
            # 添加信号日期信息（用于判断是否是当前日期）
            signal_date = result['信号日期']
            if isinstance(signal_date, date):
                today = date.today()
                days_ago = (today - signal_date).days
                result['信号距今天数'] = days_ago
                if days_ago == 0:
                    result['是否当前日期'] = True
                else:
                    result['是否当前日期'] = False
            
            # 调试模式：添加统计信息
            if debug_mode:
                result['调试模式'] = True
                result['调试_总检查天数'] = debug_stats['total_checked']
                result['调试_趋势过滤通过'] = debug_stats['trend_filter_pass']
                result['调试_回调识别通过'] = debug_stats['pullback_pass']
                result['调试_末端确认通过'] = debug_stats['end_confirm_pass']
                result['调试_最终通过'] = len(results)
            
            return result
        else:
            # 没有信号：返回失败原因（调试模式和正常模式都返回）
            if debug_mode:
                result = {
                    '股票代码': code,
                    '信号日期': None,
                    '调试模式': True,
                    '调试_数据状态': '数据充足但无信号',
                    '调试_数据天数': debug_stats['data_days'],
                    '调试_总检查天数': debug_stats['total_checked'],
                    '调试_趋势过滤通过': debug_stats['trend_filter_pass'],
                    '调试_回调识别通过': debug_stats['pullback_pass'],
                    '调试_末端确认通过': debug_stats['end_confirm_pass'],
                    '调试_最终通过': 0,
                    'MASS短期': round(mass_short, 2) if 'mass_short' in locals() else 0,
                    'MASS长期': round(mass_long, 2) if 'mass_long' in locals() else 0,
                }
                # 添加更多调试信息
                if 'ma5_slope' in locals() and 'ma10_slope' in locals():
                    result['MA5斜率'] = round(ma5_slope * 100, 4)
                    result['MA10斜率'] = round(ma10_slope * 100, 4)
                # 添加趋势过滤失败原因分析
                if debug_stats['total_checked'] > 0 and debug_stats['trend_filter_pass'] == 0:
                    if 'mass_short' in locals() and 'mass_long' in locals():
                        if mass_short < 30:
                            result['调试_失败原因'] = f'MASS短期不足({mass_short:.2f} < 30)'
                        elif mass_long == 0:
                            result['调试_失败原因'] = f'MASS长期为0(数据不足，需要360天)'
                        elif mass_long < 30:
                            result['调试_失败原因'] = f'MASS长期不足({mass_long:.2f} < 30)'
                        else:
                            result['调试_失败原因'] = '均线斜率或价格位置不满足'
                return result
            else:
                # 正常模式：返回失败原因 + 与「满足条件的信号」相同的全部指标明细（最新一日）
                latest_close = float(close.iloc[-1]) if len(close) > 0 else 0
                latest_mass_short = float(mass_short.iloc[-1]) if hasattr(mass_short, 'iloc') and len(mass_short) > 0 and not pd.isna(mass_short.iloc[-1]) else 0
                latest_mass_long = float(mass_long.iloc[-1]) if hasattr(mass_long, 'iloc') and len(mass_long) > 0 and not pd.isna(mass_long.iloc[-1]) else 0
                latest_ma5_slope = float(ma5_slope_all.iloc[-1]) * 100 if len(ma5_slope_all) > 0 and not pd.isna(ma5_slope_all.iloc[-1]) else 0
                latest_ma10_slope = float(ma10_slope_all.iloc[-1]) * 100 if len(ma10_slope_all) > 0 and not pd.isna(ma10_slope_all.iloc[-1]) else 0
                latest_deviation = float(deviation_all.iloc[-1]) * 100 if len(deviation_all) > 0 and not pd.isna(deviation_all.iloc[-1]) else 0
                latest_tz20 = float(turnover_z20_all.iloc[-1]) if len(turnover_z20_all) > 0 and not pd.isna(turnover_z20_all.iloc[-1]) else 0
                # 是否上穿MA5、是否上穿中轨（最新一日相对前一日）
                prev_close = float(close.iloc[-2]) if len(close) >= 2 else latest_close
                prev_ma5 = float(ma5_all.iloc[-2]) if len(ma5_all) >= 2 and not pd.isna(ma5_all.iloc[-2]) else 0
                ma5_end = float(ma5_all.iloc[-1]) if len(ma5_all) > 0 and not pd.isna(ma5_all.iloc[-1]) else 0
                prev_ema20 = float(ema20_all.iloc[-2]) if len(ema20_all) >= 2 and not pd.isna(ema20_all.iloc[-2]) else 0
                ema20_end = float(ema20_all.iloc[-1]) if len(ema20_all) > 0 and not pd.isna(ema20_all.iloc[-1]) else 0
                cross_ma5 = (prev_close < prev_ma5) and (latest_close >= ma5_end)
                cross_mid = (prev_close < prev_ema20) and (latest_close >= ema20_end)
                # 布林带扩张（最近三日宽度递增）
                if len(bb_width_all) >= 3 and not pd.isna(bb_width_all.iloc[-1]) and not pd.isna(bb_width_all.iloc[-2]) and not pd.isna(bb_width_all.iloc[-3]):
                    band_expand = (bb_width_all.iloc[-1] > bb_width_all.iloc[-2]) and (bb_width_all.iloc[-2] > bb_width_all.iloc[-3])
                else:
                    band_expand = False
                # 成交择时得分、流动性预警
                vol_timing = calculate_volume_timing_score(volume, turnover, period=min(60, len(volume))) if not volume.empty and not turnover.empty and len(volume) >= 60 else {'percentile_score': 0.5, 'sigma_score': 0.0}
                vol_score = float(vol_timing.get('percentile_score', 0.5))
                liq_warn = check_liquidity_warning(volume, close, period=min(20, len(volume))) if not volume.empty and len(volume) >= 20 else False
                # 综合信号强度（与满足条件时同口径）
                sig = (latest_mass_short + latest_mass_long) / 2 * 0.4
                opt_d = -0.05
                d_score = 1.0 - abs(latest_deviation / 100 - opt_d) / 0.05
                sig += max(0, min(1, d_score)) * 20
                sig += 20 if (cross_ma5 and cross_mid) else (10 if (cross_ma5 or cross_mid) else 0)
                tz20_ok = -1 <= latest_tz20 <= 1
                vol_ok = vol_score >= 0.5 or vol_timing.get('sigma_score', 0) > 0
                sig += 10 if (tz20_ok and vol_ok) else 0
                sig += 10 if (not liq_warn) else 0
                # 汇总失败原因
                if failure_reasons:
                    stage_counts = {}
                    for fr in failure_reasons:
                        stage = fr.get('失败阶段', '未知')
                        stage_counts[stage] = stage_counts.get(stage, 0) + 1
                    most_common_stage = max(stage_counts.items(), key=lambda x: x[1])[0] if stage_counts else '未知'
                    latest_failure = next((fr for fr in reversed(failure_reasons) if fr.get('失败阶段') == most_common_stage), failure_reasons[-1])
                    failure_summary = f"{most_common_stage}({stage_counts[most_common_stage]}次): {latest_failure.get('失败原因', '未知')}"
                else:
                    failure_summary = "未进入分析循环（可能数据不足或指标计算失败）"
                return {
                    '股票代码': code,
                    '信号日期': None,
                    '是否满足条件': False,
                    '失败原因汇总': failure_summary,
                    '失败详情数量': len(failure_reasons),
                    '失败详情': failure_reasons if failure_reasons else [],
                    # 与「满足条件的信号」一致的全部指标（最新一日）
                    '收盘价': round(latest_close, 2),
                    'MASS短期': round(latest_mass_short, 2),
                    'MASS长期': round(latest_mass_long, 2),
                    '偏离度': round(latest_deviation, 2),
                    'MA5斜率': round(latest_ma5_slope, 4),
                    'MA10斜率': round(latest_ma10_slope, 4),
                    '是否上穿MA5': cross_ma5,
                    '是否上穿中轨': cross_mid,
                    '布林带扩张': band_expand,
                    '换手率Z20': round(latest_tz20, 2),
                    '成交择时得分': round(vol_score, 3),
                    '流动性预警': liq_warn,
                    '综合信号强度': round(sig, 2),
                }
            
    except Exception as e:
        logger.error(f"分析股票 {code} 上升趋势反弹时出错: {e}")
        return {
            '股票代码': code,
            '信号日期': None,
            '是否满足条件': False,
            '失败原因汇总': f'分析异常: {str(e)}',
            '失败详情数量': 0,
            '失败详情': [],
            '收盘价': None, 'MASS短期': None, 'MASS长期': None, '偏离度': None, 'MA5斜率': None, 'MA10斜率': None,
            '是否上穿MA5': None, '是否上穿中轨': None, '布林带扩张': None, '换手率Z20': None, '成交择时得分': None, '流动性预警': None, '综合信号强度': None,
        }
    finally:
        # 记录处理时间（如果超过5秒，记录警告）
        if 'start_time' in locals():
            elapsed = time_module.time() - start_time
            if elapsed > 5.0:
                logger.warning(f"股票 {code} 处理耗时: {elapsed:.2f}秒")


def get_trading_dates_in_range(fetcher: JuyuanDataFetcher, start_date: date, end_date: date) -> List[date]:
    """
    获取指定日期范围内的所有交易日
    
    参数：
    - fetcher: 数据获取器
    - start_date: 开始日期
    - end_date: 结束日期
    
    返回：
    - 交易日列表（按日期升序）
    """
    try:
        sql = f"""
        SELECT DISTINCT {TABLE_CONFIG['date_field']} as trading_date
        FROM {TABLE_CONFIG['daily_quote_table']}
        WHERE {TABLE_CONFIG['date_field']} >= '{start_date}'
        AND {TABLE_CONFIG['date_field']} <= '{end_date}'
        ORDER BY {TABLE_CONFIG['date_field']}
        """
        df = fetcher.query(sql)
        if not df.empty:
            trading_dates = [pd.Timestamp(row['trading_date']).date() for _, row in df.iterrows()]
            return trading_dates
        return []
    except Exception as e:
        logger.error(f"获取交易日列表失败: {e}")
        return []


def get_random_trading_dates(fetcher: JuyuanDataFetcher, years: int = 3, count: int = 3) -> List[date]:
    """
    获取近N年内的随机交易日
    
    参数：
    - fetcher: 数据获取器
    - years: 回看年数（默认3年）
    - count: 随机选择的数量（默认3个）
    
    返回：
    - 随机交易日列表
    """
    try:
        latest_date = fetcher.get_latest_trading_date()
        start_date = latest_date - timedelta(days=years * 365)
        
        trading_dates = get_trading_dates_in_range(fetcher, start_date, latest_date)
        
        if len(trading_dates) < count:
            logger.warning(f"交易日数量不足（{len(trading_dates)} < {count}），返回所有交易日")
            return trading_dates
        
        # 随机选择count个交易日
        random_dates = random.sample(trading_dates, count)
        random_dates.sort()  # 按日期排序
        
        return random_dates
        
    except Exception as e:
        logger.error(f"获取随机交易日失败: {e}")
        return []


def get_stocks_with_mass_condition(stock_data_dict: Dict[str, pd.DataFrame],
                                   cutoff_date: date,
                                   mass_short_period: int = 60,
                                   mass_long_period: int = 360,
                                   mass_threshold: float = 50.0) -> List[Dict]:
    """
    获取满足MASS条件的股票（MASS短期>=50或MASS长期>=50）
    
    参数：
    - stock_data_dict: 股票数据字典（已过滤到cutoff_date）
    - cutoff_date: 截止日期
    - mass_short_period: MASS短期周期
    - mass_long_period: MASS长期周期
    - mass_threshold: MASS阈值（默认50）
    
    返回：
    - 满足条件的股票列表，每个包含股票代码、MASS短期、MASS长期等信息
    """
    qualified_stocks = []
    
    for code, data in stock_data_dict.items():
        if data is None or data.empty:
            continue
        
        # 确保数据按日期升序
        df = data.sort_index().copy()
        
        # 需要足够的数据来计算MASS
        if len(df) < max(mass_short_period, 100):
            continue
        
        # 提取收盘价
        close = pd.to_numeric(df.get("Close"), errors="coerce")
        close = close.dropna()
        
        if close.empty:
            continue
        
        # 找到截止日期对应的索引
        target_idx = None
        for i, date_val in enumerate(close.index):
            if hasattr(date_val, 'date'):
                date_val_date = date_val.date()
            elif isinstance(date_val, pd.Timestamp):
                date_val_date = date_val.date()
            else:
                date_val_date = date_val
            
            if date_val_date == cutoff_date:
                target_idx = i
                break
        
        if target_idx is None or target_idx < 1:
            continue
        
        # 只使用到截止日期的数据计算MASS（避免未来函数，基于截止日期当天）
        close_up_to_cutoff = close.iloc[:target_idx+1]
        
        # 计算MASS得分（基于截止日期当天的数据）
        mass_short = calculate_mass_score(close_up_to_cutoff, mass_short_period)
        mass_long = calculate_mass_score(close_up_to_cutoff, mass_long_period)
        
        # 检查是否满足条件：MASS短期>=50 或 MASS长期>=50
        if mass_short >= mass_threshold or mass_long >= mass_threshold:
            cutoff_close = close_up_to_cutoff.iloc[target_idx]
            
            if pd.notna(cutoff_close) and cutoff_close > 0:
                mass_condition_parts = []
                if mass_short >= mass_threshold:
                    mass_condition_parts.append('短期≥50')
                if mass_long >= mass_threshold:
                    mass_condition_parts.append('长期≥50')
                
                qualified_stocks.append({
                    '股票代码': code,
                    '信号日期': cutoff_date,  # 使用截止日期作为信号日期
                    'MASS短期': round(mass_short, 2),
                    'MASS长期': round(mass_long, 2),
                    '收盘价': round(float(cutoff_close), 2),
                    '是否满足全部条件': False,  # 标记为部分满足条件
                    'MASS条件': '或'.join(mass_condition_parts) if mass_condition_parts else '未知'
                })
    
    return qualified_stocks


def calculate_future_returns(stock_data: pd.DataFrame, signal_date: date, 
                            periods: List[int] = [5, 10, 20, 30, 60]) -> Dict[str, float]:
    """
    计算信号日期后的未来收益率
    
    参数：
    - stock_data: 股票数据DataFrame（索引为日期）
    - signal_date: 信号日期
    - periods: 未来周期列表（交易日）
    
    返回：
    - 包含各周期收益率的字典，如果数据不足则返回None
    """
    try:
        if stock_data.empty:
            return {}
        
        # 确保按日期升序
        df = stock_data.sort_index().copy()
        close = pd.to_numeric(df.get("Close"), errors="coerce")
        close = close.dropna()
        
        if close.empty:
            return {}
        
        # 找到信号日期的索引
        signal_idx = None
        for i, date_val in enumerate(close.index):
            if hasattr(date_val, 'date'):
                date_val_date = date_val.date()
            elif isinstance(date_val, pd.Timestamp):
                date_val_date = date_val.date()
            else:
                date_val_date = date_val
            
            if date_val_date >= signal_date:
                signal_idx = i
                break
        
        if signal_idx is None:
            return {}
        
        # 获取信号日收盘价
        signal_close = close.iloc[signal_idx]
        if pd.isna(signal_close) or signal_close <= 0:
            return {}
        
        return_results = {}
        
        for period in periods:
            target_idx = signal_idx + period
            if target_idx < len(close):
                target_close = close.iloc[target_idx]
                if pd.notna(target_close) and target_close > 0:
                    return_rate = (target_close - signal_close) / signal_close * 100
                    return_results[f'{period}日收益率'] = round(return_rate, 2)
                else:
                    return_results[f'{period}日收益率'] = None
            else:
                return_results[f'{period}日收益率'] = None
        
        return return_results
        
    except Exception as e:
        logger.error(f"计算未来收益率失败: {e}")
        return {}


def calculate_backtest_statistics(results_list: List[Dict], periods: List[int] = [5, 10, 20, 30, 60]) -> Dict:
    """
    计算回测统计信息（平均涨幅和胜率）
    
    参数：
    - results_list: 结果列表，每个结果包含各周期的收益率
    - periods: 周期列表
    
    返回：
    - 包含各周期平均涨幅和胜率的字典
    """
    try:
        stats = {}
        
        for period in periods:
            period_key = f'{period}日收益率'
            returns = []
            
            for result in results_list:
                if period_key in result and result[period_key] is not None:
                    returns.append(result[period_key])
            
            if returns:
                avg_return = np.mean(returns)
                win_rate = sum(1 for r in returns if r > 0) / len(returns) * 100
                stats[f'{period}日平均涨幅'] = round(avg_return, 2)
                stats[f'{period}日胜率'] = round(win_rate, 2)
                stats[f'{period}日样本数'] = len(returns)
            else:
                stats[f'{period}日平均涨幅'] = None
                stats[f'{period}日胜率'] = None
                stats[f'{period}日样本数'] = 0
        
        return stats
        
    except Exception as e:
        logger.error(f"计算回测统计信息失败: {e}")
        return {}


def run_uptrend_rebound_analysis(limit: Optional[int] = None,
                                 max_days_ago: Optional[int] = None,
                                 mass_short_period: int = 60,
                                 mass_long_period: int = 360,
                                 deviation_min: float = -0.10,
                                 deviation_max: float = -0.02,
                                 lookback_days: int = 20,
                                 debug_mode: bool = False,
                                 mode: str = "latest",  # "latest" 或 "random"
                                 random_dates_count: int = 3,  # 随机日期数量
                                 specified_dates: Optional[List[date]] = None,  # 指定的日期列表
                                 output_dir: str = "outputs",
                                 filter_config: Optional[Dict[str, bool]] = None) -> Optional[str]:
    """
    一键执行"上升趋势中的反弹识别"分析
    
    参数：
    - limit: 股票数量限制，None 使用配置中的 STOCK_LIST_LIMIT
    - max_days_ago: 最大允许行情滞后天数，None 使用配置中的 MAX_TRADING_DAYS_AGO
    - mass_short_period: MASS短期周期（默认60）
    - mass_long_period: MASS长期周期（默认360）
    - deviation_min: 偏离度最小值（默认-10%，即-0.10）
    - deviation_max: 偏离度最大值（默认-2%，即-0.02）
    - lookback_days: 观测窗口期，检查最近多少个交易日（默认20，支持20/30/60）
    - debug_mode: 调试模式
    - mode: 分析模式，"latest"（最新日期）或"random"（随机日期回测）
    - random_dates_count: 随机日期数量（默认3个）
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
    print("上升趋势中的反弹识别")
    print("=" * 60)
    print(f"分析模式: {'随机日期回测' if mode == 'random' else '最新日期'}")
    if mode == 'random':
        print(f"随机日期数量: {random_dates_count}个")
    print(f"股票数量上限: {limit}")
    print(f"最大允许行情滞后天数: {max_days_ago}")
    print(f"MASS短期周期: {mass_short_period}天")
    print(f"MASS长期周期: {mass_long_period}天")
    print(f"偏离度范围: {deviation_min*100:.1f}% ~ {deviation_max*100:.1f}%")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        fetcher = JuyuanDataFetcher(use_connection_pool=True)
        
        # 获取数据库最新交易日（用于显示）
        latest_trading_date = fetcher.get_latest_trading_date()
        print(f"📅 数据库最新交易日: {latest_trading_date}")
        print("=" * 60)
        
        # 1. 获取活跃股票列表
        print("📊 获取活跃股票列表（正在查询数据库，请稍候...）...")
        list_start_time = time.time()
        stock_info_list = fetcher.get_stock_list(limit=limit, max_days_ago=max_days_ago)
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
        
        # 2. 根据模式确定数据获取范围
        if mode == "random":
            # 随机日期回测模式：先确定使用的日期，然后确定数据范围
            print("🎲 随机日期回测模式")
            print("=" * 60)
            
            # 确定使用的日期列表
            if specified_dates is not None:
                # 使用用户指定的日期
                random_dates = specified_dates
                print(f"📅 使用指定的日期: {', '.join([str(d) for d in random_dates])}")
                
                # 验证日期是否在数据库范围内
                db_latest_date = fetcher.get_latest_trading_date()
                invalid_dates = [d for d in random_dates if d > db_latest_date]
                if invalid_dates:
                    print(f"  ⚠️  警告: 以下日期超过数据库最新交易日({db_latest_date}): {', '.join([str(d) for d in invalid_dates])}")
                    print(f"  💡 这些日期将被跳过，只分析有效的日期")
                    random_dates = [d for d in random_dates if d <= db_latest_date]
                    
                    if not random_dates:
                        print("❌ 所有指定日期都超过数据库最新交易日，无法进行分析")
                        return None
                    
                    print(f"  ✅ 有效日期: {', '.join([str(d) for d in random_dates])}")
            else:
                # 获取随机交易日
                print(f"📅 获取近3年内的{random_dates_count}个随机交易日...")
                random_dates = get_random_trading_dates(fetcher, years=3, count=random_dates_count)
                
                if not random_dates:
                    print("❌ 无法获取随机交易日")
                    return None
                
                print(f"✅ 随机选择的交易日: {', '.join([str(d) for d in random_dates])}")
            
            print("=" * 60)
            
            # 获取数据库最新交易日
            db_latest_date = fetcher.get_latest_trading_date()
            
            # 对每个随机日期，分别计算需要的数据范围，然后合并
            days_needed = max(mass_long_period + 100, 500)
            required_start_dates = []
            required_end_dates = []
            
            for random_date in random_dates:
                # 每个随机日期需要的数据范围：
                # - 开始日期：该随机日期往前推（mass_long_period + 100天）
                # - 结束日期：该随机日期往后推60天（用于回测），但不超过数据库最新交易日
                date_start = random_date - timedelta(days=days_needed + 30)
                date_end = min(random_date + timedelta(days=60), db_latest_date)
                required_start_dates.append(date_start)
                required_end_dates.append(date_end)
            
            # 合并所有需要的日期范围
            start_date = min(required_start_dates)  # 所有随机日期中最早需要的开始日期
            end_date = max(required_end_dates)      # 所有随机日期中最晚需要的结束日期
            
            # 优化：如果end_date比数据库最新交易日早30天以上，使用数据库最新交易日
            # 这样可以提高缓存命中率（因为之前的缓存可能是基于最新交易日的）
            if end_date < db_latest_date - timedelta(days=30):
                print(f"   💡 优化缓存命中率：将结束日期调整为数据库最新交易日({db_latest_date})")
                end_date = db_latest_date
            
            earliest_random_date = min(random_dates)
            latest_random_date = max(random_dates)
            
            print(f"📅 数据获取范围: {start_date} 至 {end_date}")
            print(f"   - 最早随机日期: {earliest_random_date}")
            print(f"   - 最晚随机日期: {latest_random_date}")
            print(f"   - 数据库最新交易日: {db_latest_date}")
            print(f"   - 每个随机日期需要: 往前推{days_needed}天，往后推60天（用于回测）")
        else:
            # 最新日期模式：使用最新交易日
            print("📈 批量获取行情数据（增量缓存 + 批量SQL + 多线程并发）...")
            
            # 计算日期范围（至少需要mass_long_period + 100天的数据）
            days_needed = max(mass_long_period + 100, 500)
            # 使用数据库中的最新交易日，而不是当前自然日
            end_date = fetcher.get_latest_trading_date()
            start_date = end_date - timedelta(days=days_needed + 30)  # 多获取30天作为缓冲
        
        # 尝试从缓存加载（批量）
        print("  🔍 检查增量缓存...")
        if mode == "random":
            print(f"  📅 请求日期范围: {start_date} 至 {end_date}（随机日期回测模式）")
        else:
            print(f"  📅 请求日期范围: {start_date} 至 {end_date}（数据库最新交易日，需要{days_needed}天数据）")
        cached_stock_data, missing_stock_codes = futures_incremental_cache_manager.load_stocks_data(
            codes, start_date, end_date
        )
        
        # 检查缓存数据的完整性和日期范围（支持部分命中并增量补全）
        insufficient_data_codes = []  # 数据不足的股票（需要补全）
        partial_data_codes = {}  # 部分命中的股票（缓存有数据但需要补全缺失日期范围）
        valid_cached_data = {}  # 完全有效的缓存数据
        
        for code, data in cached_stock_data.items():
            if data is None or data.empty:
                insufficient_data_codes.append(code)
                continue
            
            # 检查数据日期范围
            data_start = data.index.min()
            data_end = data.index.max()
            
            # 转换为date类型进行比较
            if hasattr(data_start, 'date'):
                data_start_date = data_start.date()
            elif isinstance(data_start, pd.Timestamp):
                data_start_date = data_start.date()
            else:
                data_start_date = data_start
            
            if hasattr(data_end, 'date'):
                data_end_date = data_end.date()
            elif isinstance(data_end, pd.Timestamp):
                data_end_date = data_end.date()
            else:
                data_end_date = data_end
            
            # 检查数据是否覆盖了请求的日期范围
            actual_data_days = len(data)
            request_calendar_days = (end_date - start_date).days
            covered_start = max(data_start_date, start_date)
            covered_end = min(data_end_date, end_date)
            covered_calendar_days = (covered_end - covered_start).days if covered_end >= covered_start else 0
            
            # 计算覆盖率
            coverage_ratio = covered_calendar_days / request_calendar_days if request_calendar_days > 0 else 0
            
            # 检查日期范围是否覆盖请求范围
            start_ok = data_start_date <= start_date + timedelta(days=10) or coverage_ratio >= 0.95
            
            if mode == "random":
                # 随机日期回测模式：只要缓存覆盖了随机日期范围，就认为有效
                # 不需要严格检查end_date，因为end_date可能被优化为数据库最新交易日
                # 检查缓存是否覆盖了最晚随机日期（回测只需要到随机日期，不需要到end_date）
                if 'random_dates' in locals() and random_dates:
                    latest_random_date = max(random_dates)
                    # 只要缓存覆盖到最晚随机日期即可（回测起点是随机日期，不需要到end_date）
                    end_ok = data_end_date >= latest_random_date
                else:
                    # 如果没有random_dates，使用宽松检查（允许30天误差）
                    end_ok = data_end_date >= end_date - timedelta(days=30)
            else:
                # 最新日期模式：严格检查end_date，确保数据是最新的
                end_ok = data_end_date >= end_date  # 严格检查：必须包含end_date
            
            date_range_ok = start_ok and end_ok
            
            # 至少需要请求天数的80%，或者覆盖率>=95%且日期范围完整
            min_required_days = int(days_needed * 0.8)  # 允许20%的误差
            
            # 对于随机日期回测模式，放宽覆盖率要求（因为end_date可能被优化为数据库最新交易日）
            if mode == "random":
                # 随机日期回测模式：只要数据量足够且覆盖了随机日期范围，就认为有效
                # 不要求覆盖率>=95%，因为end_date可能比实际需要的日期晚很多
                data_sufficient = actual_data_days >= min_required_days and date_range_ok
            else:
                # 最新日期模式：严格要求覆盖率>=95%
                data_sufficient = actual_data_days >= min_required_days or (coverage_ratio >= 0.95 and date_range_ok)
            
            if data_sufficient and date_range_ok:
                # 缓存数据完全有效
                valid_cached_data[code] = data
            elif actual_data_days >= 100:  # 缓存有部分数据（至少100个交易日），尝试增量补全
                # 记录部分命中的股票，保存其缓存数据和缺失的日期范围
                missing_start = start_date if data_start_date > start_date + timedelta(days=10) else None
                missing_end = end_date if data_end_date < end_date else None
                partial_data_codes[code] = {
                    'cached_data': data,
                    'missing_start': missing_start,
                    'missing_end': missing_end,
                    'cache_start': data_start_date,
                    'cache_end': data_end_date
                }
            else:
                # 缓存数据太少，需要重新获取全部数据
                insufficient_data_codes.append(code)
                # 只在调试模式下输出详细信息，避免刷屏
                if debug_mode:
                    if actual_data_days < min_required_days:
                        print(f"  ⚠️  股票 {code} 缓存数据不足: {actual_data_days}个交易日 < {min_required_days}个（需要{days_needed}个交易日）")
                    else:
                        print(f"  ⚠️  股票 {code} 缓存日期范围不完整: {data_start_date} 至 {data_end_date}（需要 {start_date} 至 {end_date}，覆盖{covered_calendar_days}/{request_calendar_days}天）")
        
        # 合并需要重新获取的股票代码（完全缺失的 + 数据太少的）
        all_missing_codes = list(set(missing_stock_codes + insufficient_data_codes))
        cache_hit_count = len(valid_cached_data)
        partial_hit_count = len(partial_data_codes)
        cache_miss_count = len(all_missing_codes)
        
        print(f"  ✅ 缓存完全有效: {cache_hit_count} 只股票")
        if partial_hit_count > 0:
            print(f"  🔄 缓存部分命中: {partial_hit_count} 只股票（将增量补全缺失日期范围）")
        print(f"  ⚠️  需要从数据库获取: {cache_miss_count} 只股票（{len(missing_stock_codes)}只未命中，{len(insufficient_data_codes)}只数据不足）")
        
        # 检查缓存数据的日期范围（调试信息）
        if debug_mode and valid_cached_data:
            sample_code = list(valid_cached_data.keys())[0]
            sample_data = valid_cached_data[sample_code]
            if not sample_data.empty:
                cache_start = sample_data.index.min()
                cache_end = sample_data.index.max()
                print(f"  📊 有效缓存数据示例（{sample_code}）日期范围: {cache_start} 至 {cache_end}")
                print(f"  📊 有效缓存数据行数: {len(sample_data)}")
        
        # 对缺失或数据不足的股票从数据库获取（批量）
        # 优化：区分完全缺失和部分命中的股票，部分命中的只获取缺失日期范围
        fetch_start_time = time.time()
        fetched_stock_data = {}
        
        # 1. 处理完全缺失的股票：获取全部数据
        if all_missing_codes:
            print(f"  📥 从数据库获取完全缺失的 {len(all_missing_codes)} 只股票数据（全部{days_needed}天数据）...")
            print(f"  📊 开始执行数据库查询...")
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
                    # 有明确的缺失范围
                    fetch_start = missing_start
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
                fetch_start = max(fetch_start, start_date)
                fetch_end = min(fetch_end, end_date)
                
                if fetch_start <= fetch_end:
                    range_key = (fetch_start, fetch_end)
                    if range_key not in date_range_groups:
                        date_range_groups[range_key] = []
                    date_range_groups[range_key].append(code)
            
            # 对每个日期范围组，批量获取数据
            total_partial_groups = len(date_range_groups)
            if total_partial_groups > 0:
                print(f"    分为 {total_partial_groups} 个日期范围组进行增量补全...")
                
                for (fetch_start, fetch_end), codes_in_group in date_range_groups.items():
                    # 计算需要获取的天数（多获取30天缓冲，确保覆盖）
                    days_to_fetch = (fetch_end - fetch_start).days + 30
                    
                    # 批量获取这个日期范围的数据
                    partial_fetched = fetcher.batch_get_stock_data_with_adjustment(
                        codes_in_group,
                        days=days_to_fetch,
                        time_config=None
                    )
                    
                    if partial_fetched:
                        fetched_stock_data.update(partial_fetched)
                        print(f"    ✅ 日期范围 {fetch_start} 至 {fetch_end}: 补全 {len(partial_fetched)} 只股票")
            
            print(f"  ✅ 部分命中股票增量补全完成: {len([c for c in fetched_stock_data.keys() if c in partial_data_codes])} 只")
        
        fetch_elapsed = time.time() - fetch_start_time
        if fetched_stock_data:
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
                        combined_data = combined_data.sort_index()
                        combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
                        # 更新到fetched_stock_data中，后续会保存到缓存
                        fetched_stock_data[code] = combined_data
                        merged_count += 1
                else:
                    # 如果没有获取到新数据，使用缓存数据（虽然不完整，但总比没有好）
                    fetched_stock_data[code] = cached_data
            
            if merged_count > 0:
                print(f"  ✅ 合并完成: {merged_count} 只股票")
        
        if fetched_stock_data:
            # 保存新获取的数据到缓存（批量）
            print(f"  💾 保存 {len(fetched_stock_data)} 只股票的数据到增量缓存...")
            cache_save_start = time.time()
            futures_incremental_cache_manager.save_stocks_data(
                list(fetched_stock_data.keys()),
                fetched_stock_data,
                start_date,
                end_date
            )
            cache_save_elapsed = time.time() - cache_save_start
            print(f"  ✅ 缓存保存完成（耗时: {cache_save_elapsed:.1f}秒）")
            
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
        
        # 初始化变量（用于两种模式）
        all_date_results = {}
        all_backtest_data = []
        overall_stats = {}
        random_dates = None  # 初始化random_dates，用于缓存检查
        
        # 3. 根据模式选择处理逻辑
        if mode == "random":
            # 随机日期回测模式（随机日期已在数据获取阶段选择）
            print("\n🚀 开始随机日期回测分析...")
            print("=" * 60)
            
            # 确定使用的日期列表
            if specified_dates is not None:
                # 使用用户指定的日期
                random_dates = specified_dates
                print(f"📅 使用指定的日期: {', '.join([str(d) for d in random_dates])}")
            else:
                # 获取随机交易日
                print(f"📅 获取近3年内的{random_dates_count}个随机交易日...")
                random_dates = get_random_trading_dates(fetcher, years=3, count=random_dates_count)
                
                if not random_dates:
                    print("❌ 无法获取随机交易日")
                    return None
                
                print(f"✅ 随机选择的交易日: {', '.join([str(d) for d in random_dates])}")
            
            print("=" * 60)
            
            # 对每个随机日期进行分析和回测
            all_date_results = {}  # {日期: [结果列表]}
            all_backtest_data = []  # 所有回测数据（用于汇总统计）
            
            for date_idx, cutoff_date in enumerate(random_dates, 1):
                print(f"\n📊 分析日期 {date_idx}/{len(random_dates)}: {cutoff_date}")
                print("-" * 60)
                
                # 过滤数据到截止日期
                # 对每个随机日期，检查该日期所需的数据范围
                # 需要的数据范围：cutoff_date往前推mass_long_period天 至 cutoff_date
                filtered_stock_data = {}
                sample_code = None
                sample_data_info = None
                
                # 计算该随机日期需要的数据范围
                required_start_for_date = cutoff_date - timedelta(days=days_needed + 30)
                
                for code, data in all_stock_data.items():
                    if data is None or data.empty:
                        continue
                    
                    # 保存第一个有效数据的样本信息（用于调试）
                    if sample_code is None and not data.empty:
                        sample_code = code
                        sample_data_info = {
                            'code': code,
                            'total_rows': len(data),
                            'index_min': data.index.min(),
                            'index_max': data.index.max(),
                            'index_type': type(data.index[0]).__name__ if len(data.index) > 0 else 'unknown'
                        }
                    
                    # 检查数据是否覆盖该随机日期所需的范围
                    # 数据需要覆盖：required_start_for_date 至 cutoff_date
                    data_start_date = None
                    data_end_date = None
                    
                    if not data.empty:
                        data_start = data.index.min()
                        data_end = data.index.max()
                        
                        if hasattr(data_start, 'date'):
                            data_start_date = data_start.date()
                        elif isinstance(data_start, pd.Timestamp):
                            data_start_date = data_start.date()
                        elif isinstance(data_start, date):
                            data_start_date = data_start
                        
                        if hasattr(data_end, 'date'):
                            data_end_date = data_end.date()
                        elif isinstance(data_end, pd.Timestamp):
                            data_end_date = data_end.date()
                        elif isinstance(data_end, date):
                            data_end_date = data_end
                    
                    # 检查数据是否覆盖需要的日期范围
                    if data_start_date is None or data_end_date is None:
                        continue
                    
                    # 数据必须包含cutoff_date（或接近cutoff_date，允许5天误差）
                    if data_end_date < cutoff_date - timedelta(days=5):
                        continue  # 数据结束日期太早，无法覆盖cutoff_date
                    
                    # 直接过滤到截止日期，然后检查数据量是否足够
                    # 不再检查数据开始日期，因为可能数据库中没有更早的数据，但只要过滤后的数据量足够即可
                    cutoff_timestamp = pd.Timestamp(cutoff_date)
                    
                    # 尝试多种过滤方式
                    try:
                        # 方式1: 直接使用 <= 比较
                        filtered_data = data[data.index <= cutoff_timestamp]
                    except Exception:
                        # 方式2: 如果索引是date类型，转换为date比较
                        try:
                            if hasattr(data.index[0], 'date'):
                                filtered_data = data[[idx.date() <= cutoff_date for idx in data.index]]
                            else:
                                continue
                        except Exception:
                            continue
                    
                    # 检查数据是否包含截止日期
                    if filtered_data.empty:
                        continue
                    
                    # 检查数据量是否足够（只要数据量>=mass_long_period即可，不要求历史数据从required_start开始）
                    if len(filtered_data) < mass_long_period:
                        continue
                    
                    # 检查数据是否包含截止日期当天的数据（使用日期比较，不要求精确匹配时间戳）
                    has_cutoff_date = False
                    for idx in filtered_data.index:
                        idx_date = None
                        if hasattr(idx, 'date'):
                            idx_date = idx.date()
                        elif isinstance(idx, pd.Timestamp):
                            idx_date = idx.date()
                        elif isinstance(idx, date):
                            idx_date = idx
                        
                        if idx_date == cutoff_date:
                            has_cutoff_date = True
                            break
                    
                    # 如果没有精确匹配，尝试找最接近的日期（允许5天误差）
                    if not has_cutoff_date:
                        closest_date = None
                        min_diff = None
                        for idx in filtered_data.index:
                            idx_date = None
                            if hasattr(idx, 'date'):
                                idx_date = idx.date()
                            elif isinstance(idx, pd.Timestamp):
                                idx_date = idx.date()
                            elif isinstance(idx, date):
                                idx_date = idx
                            
                            if idx_date:
                                diff = abs((idx_date - cutoff_date).days)
                                if min_diff is None or diff < min_diff:
                                    min_diff = diff
                                    closest_date = idx_date
                        
                        # 如果最接近的日期距离截止日期超过5天，跳过
                        if min_diff is None or min_diff > 5:
                            continue
                    
                    filtered_stock_data[code] = filtered_data
                
                if not filtered_stock_data:
                    print(f"  ⚠️  日期 {cutoff_date} 无足够数据，跳过")
                    if sample_data_info:
                        print(f"     📊 数据样本信息（{sample_code}）:")
                        print(f"        - 总行数: {sample_data_info['total_rows']}")
                        print(f"        - 索引范围: {sample_data_info['index_min']} 至 {sample_data_info['index_max']}")
                        print(f"        - 索引类型: {sample_data_info['index_type']}")
                        # 尝试计算过滤后的数据量
                        try:
                            sample_data = all_stock_data.get(sample_code)
                            if sample_data is not None and not sample_data.empty:
                                cutoff_timestamp = pd.Timestamp(cutoff_date)
                                try:
                                    temp_filtered = sample_data[sample_data.index <= cutoff_timestamp]
                                    print(f"        - 过滤到{cutoff_date}后的数据量: {len(temp_filtered)}行（需要{mass_long_period}行）")
                                    if len(temp_filtered) > 0:
                                        print(f"        - 过滤后日期范围: {temp_filtered.index.min()} 至 {temp_filtered.index.max()}")
                                except Exception as e:
                                    print(f"        - 无法计算过滤后的数据量: {e}")
                        except Exception:
                            pass
                    print(f"     💡 可能原因：1) 该日期不在数据库中 2) 数据量不足{mass_long_period}天 3) 数据索引格式不匹配")
                    continue
                
                print(f"  ✅ 有效股票数: {len(filtered_stock_data)}")
                
                # 并行分析（随机日期回测模式：直接判断截止日期当天是否满足条件，不使用观测窗口期）
                def _task_rebound_with_date(code: str,
                                           stock_data_dict: Dict[str, pd.DataFrame],
                                           cutoff_date: date,
                                           mass_short_period: int,
                                           mass_long_period: int,
                                           deviation_min: float,
                                           deviation_max: float,
                                           debug_mode: bool = False,
                                           filter_config: Optional[Dict[str, bool]] = None) -> Optional[Dict]:
                    """供高性能线程池调用的任务函数（随机日期回测模式：判断截止日期当天）"""
                    df = stock_data_dict.get(code)
                    if df is None or df.empty:
                        return None
                    # 使用单日判断函数，不使用观测窗口期
                    return check_single_date_condition(
                        code=code,
                        stock_data=df,
                        target_date=cutoff_date,
                        mass_short_period=mass_short_period,
                        mass_long_period=mass_long_period,
                        deviation_min=deviation_min,
                        deviation_max=deviation_max,
                        debug_mode=debug_mode,
                        filter_config=filter_config
                    )
                
                thread_pool = HighPerformanceThreadPool(progress_desc=f"分析{cutoff_date}")
                tasks = list(filtered_stock_data.keys())
                results = thread_pool.execute_batch(
                    tasks,
                    _task_rebound_with_date,
                    filtered_stock_data,
                    cutoff_date,  # 传入截止日期
                    mass_short_period,
                    mass_long_period,
                    deviation_min,
                    deviation_max,
                    debug_mode,
                    filter_config,
                )
                
                # 汇总该日期的结果
                valid_results = [r for r in results if r is not None]
                signal_results = [r for r in valid_results if r.get('信号日期') is not None]
                
                print(f"  ✅ 发现 {len(signal_results)} 只股票满足全部条件")
                
                # 获取所有满足MASS条件的股票（MASS短期>=50或MASS长期>=50）
                print(f"  🔍 筛选满足MASS条件的股票（MASS短期>=50或MASS长期>=50）...")
                mass_qualified_stocks = get_stocks_with_mass_condition(
                    filtered_stock_data,
                    cutoff_date,
                    mass_short_period,
                    mass_long_period,
                    mass_threshold=50.0
                )
                
                print(f"  ✅ 发现 {len(mass_qualified_stocks)} 只股票满足MASS条件（短期>=50或长期>=50）")
                
                # 合并满足全部条件的信号和满足MASS条件的股票（去重）
                # 使用股票代码+信号日期作为唯一标识
                all_backtest_candidates = {}
                
                # 先添加满足全部条件的信号
                for signal in signal_results:
                    code = signal['股票代码']
                    signal_date = signal['信号日期']
                    if isinstance(signal_date, str):
                        signal_date = pd.Timestamp(signal_date).date()
                    key = (code, signal_date)
                    signal['是否满足全部条件'] = True
                    signal['MASS条件'] = '全部满足'
                    all_backtest_candidates[key] = signal
                
                # 再添加满足MASS条件的股票（如果不在满足全部条件的列表中）
                for stock in mass_qualified_stocks:
                    code = stock['股票代码']
                    signal_date = stock['信号日期']
                    key = (code, signal_date)
                    if key not in all_backtest_candidates:
                        all_backtest_candidates[key] = stock
                
                print(f"  📊 回测候选股票总数: {len(all_backtest_candidates)} 只")
                print(f"    - 满足全部条件: {len(signal_results)} 只")
                print(f"    - 仅满足MASS条件: {len(all_backtest_candidates) - len(signal_results)} 只")
                
                # 计算回测数据（对所有候选股票计算未来收益率）
                if all_backtest_candidates:
                    print(f"  📈 计算回测数据...")
                    backtest_results = []
                    
                    for key, candidate in all_backtest_candidates.items():
                        code, signal_date = key
                        
                        # 获取该股票的完整数据（用于计算未来收益率）
                        full_data = all_stock_data.get(code)
                        if full_data is not None and not full_data.empty:
                            # 计算未来收益率
                            future_returns = calculate_future_returns(
                                full_data, signal_date, periods=[5, 10, 20, 30, 60]
                            )
                            
                            # 合并候选信息和回测数据
                            backtest_result = candidate.copy()
                            backtest_result.update(future_returns)
                            backtest_result['截止日期'] = cutoff_date
                            backtest_results.append(backtest_result)
                    
                    all_date_results[cutoff_date] = backtest_results
                    all_backtest_data.extend(backtest_results)
                    print(f"  ✅ 回测数据计算完成: {len(backtest_results)} 个标的")
                else:
                    print(f"  ⚠️  无回测候选股票")
            
            # 汇总所有日期的回测统计
            print("\n" + "=" * 60)
            print("📊 回测统计汇总")
            print("=" * 60)
            
            # 计算整体统计（用于导出）
            if all_backtest_data:
                overall_stats = calculate_backtest_statistics(all_backtest_data, periods=[5, 10, 20, 30, 60])
                
                # 区分满足全部条件和仅满足MASS条件的统计
                full_condition_data = [r for r in all_backtest_data if r.get('是否满足全部条件', False)]
                mass_only_data = [r for r in all_backtest_data if not r.get('是否满足全部条件', True)]
                
                print("\n整体回测统计（所有日期汇总）:")
                print(f"  总回测标的数: {len(all_backtest_data)} 只")
                print(f"    - 满足全部条件: {len(full_condition_data)} 只")
                print(f"    - 仅满足MASS条件: {len(mass_only_data)} 只")
                
                for period in [5, 10, 20, 30, 60]:
                    avg_key = f'{period}日平均涨幅'
                    win_key = f'{period}日胜率'
                    sample_key = f'{period}日样本数'
                    if avg_key in overall_stats and overall_stats[avg_key] is not None:
                        print(f"  {period}日: 平均涨幅={overall_stats[avg_key]:.2f}%, "
                              f"胜率={overall_stats[win_key]:.2f}%, "
                              f"样本数={overall_stats[sample_key]}")
                
                # 分别统计满足全部条件和仅满足MASS条件的结果
                if full_condition_data:
                    full_stats = calculate_backtest_statistics(full_condition_data, periods=[5, 10, 20, 30, 60])
                    print("\n满足全部条件的回测统计:")
                    for period in [5, 10, 20, 30, 60]:
                        avg_key = f'{period}日平均涨幅'
                        win_key = f'{period}日胜率'
                        sample_key = f'{period}日样本数'
                        if avg_key in full_stats and full_stats[avg_key] is not None:
                            print(f"  {period}日: 平均涨幅={full_stats[avg_key]:.2f}%, "
                                  f"胜率={full_stats[win_key]:.2f}%, "
                                  f"样本数={full_stats[sample_key]}")
                
                if mass_only_data:
                    mass_stats = calculate_backtest_statistics(mass_only_data, periods=[5, 10, 20, 30, 60])
                    print("\n仅满足MASS条件的回测统计:")
                    for period in [5, 10, 20, 30, 60]:
                        avg_key = f'{period}日平均涨幅'
                        win_key = f'{period}日胜率'
                        sample_key = f'{period}日样本数'
                        if avg_key in mass_stats and mass_stats[avg_key] is not None:
                            print(f"  {period}日: 平均涨幅={mass_stats[avg_key]:.2f}%, "
                                  f"胜率={mass_stats[win_key]:.2f}%, "
                                  f"样本数={mass_stats[sample_key]}")
                
                # 按日期统计
                print("\n各日期回测统计:")
                for cutoff_date, date_results in all_date_results.items():
                    date_stats = calculate_backtest_statistics(date_results, periods=[5, 10, 20, 30, 60])
                    date_full_condition = [r for r in date_results if r.get('是否满足全部条件', False)]
                    date_mass_only = [r for r in date_results if not r.get('是否满足全部条件', True)]
                    
                    print(f"\n  截止日期: {cutoff_date} (总标的数: {len(date_results)})")
                    print(f"    - 满足全部条件: {len(date_full_condition)} 只")
                    print(f"    - 仅满足MASS条件: {len(date_mass_only)} 只")
                    for period in [5, 10, 20, 30, 60]:
                        avg_key = f'{period}日平均涨幅'
                        win_key = f'{period}日胜率'
                        if avg_key in date_stats and date_stats[avg_key] is not None:
                            print(f"    {period}日: 平均涨幅={date_stats[avg_key]:.2f}%, "
                                  f"胜率={date_stats[win_key]:.2f}%")
            else:
                print("❌ 无回测数据")
                overall_stats = {}
            
            # 准备导出数据
            final_results = all_backtest_data
            signal_df = pd.DataFrame(all_backtest_data) if all_backtest_data else pd.DataFrame()
            no_signal_df = pd.DataFrame()
            
        else:
            # 最新日期模式（保持原有逻辑）
            print("🚀 并行计算反弹信号...")
            
            def _task_rebound(code: str,
                             stock_data_dict: Dict[str, pd.DataFrame],
                             mass_short_period: int,
                             mass_long_period: int,
                             deviation_min: float,
                             deviation_max: float,
                             lookback_days: int,
                             debug_mode: bool = False,
                             filter_config: Optional[Dict[str, bool]] = None) -> Optional[Dict]:
                """供高性能线程池调用的任务函数。数据为空时仍返回带失败原因的 dict，以便导出明细。"""
                df = stock_data_dict.get(code)
                if df is None or df.empty:
                    return {
                        '股票代码': code,
                        '信号日期': None,
                        '是否满足条件': False,
                        '失败原因汇总': '数据为空或未获取到',
                        '失败详情数量': 0,
                        '失败详情': [],
                        '收盘价': None, 'MASS短期': None, 'MASS长期': None, '偏离度': None, 'MA5斜率': None, 'MA10斜率': None,
                        '是否上穿MA5': None, '是否上穿中轨': None, '布林带扩张': None, '换手率Z20': None, '成交择时得分': None, '流动性预警': None, '综合信号强度': None,
                    }
                return analyze_uptrend_rebound(
                    code=code,
                    stock_data=df,
                    mass_short_period=mass_short_period,
                    mass_long_period=mass_long_period,
                    deviation_min=deviation_min,
                    deviation_max=deviation_max,
                    lookback_days=lookback_days,
                    debug_mode=debug_mode,
                    filter_config=filter_config
                )
            
            # 使用高性能线程池的execute_batch方法（与功能11对齐）
            thread_pool = HighPerformanceThreadPool(progress_desc="上升趋势反弹识别")
            
            tasks = list(all_stock_data.keys())
            results = thread_pool.execute_batch(
                tasks,
                _task_rebound,
                all_stock_data,
                mass_short_period,
                mass_long_period,
                deviation_min,
                deviation_max,
                lookback_days,
                debug_mode,
                filter_config,
            )
            
            # 汇总结果
            valid_results = [r for r in results if r is not None]
            total_stocks = len(results)  # 总股票数（包括返回None的）
            none_count = total_stocks - len(valid_results)  # 返回None的股票数
            
            # 初始化变量
            signal_results = []
            debug_results = []
            
            # 调试模式：统计和输出调试信息
            if debug_mode:
                debug_results = [r for r in valid_results if r.get('调试模式', False)]
                signal_results = [r for r in valid_results if not r.get('调试模式', False)]
                
                # 统计各类情况
                data_insufficient = [r for r in debug_results if r.get('调试_数据状态', '').startswith('数据不足')]
                data_empty = [r for r in debug_results if '数据为空' in str(r.get('调试_数据状态', ''))]
                no_signal = [r for r in debug_results if r.get('调试_数据状态', '') == '数据充足但无信号']
                
                print("\n📊 调试模式统计信息:")
                print("=" * 60)
                print(f"  总股票数: {total_stocks}")
                if none_count > 0:
                    print(f"  - 返回None（异常或非调试模式数据不足）: {none_count} 只")
                print(f"  - 有效结果: {len(valid_results)} 只")
                print(f"    - 数据不足: {len(data_insufficient)} 只")
                print(f"    - 数据为空: {len(data_empty)} 只")
                print(f"    - 数据充足但无信号: {len(no_signal)} 只")
                print(f"    - 有信号: {len(signal_results)} 只")
                
                if debug_results:
                    total_checked = sum(r.get('调试_总检查天数', 0) for r in debug_results)
                    trend_pass = sum(r.get('调试_趋势过滤通过', 0) for r in debug_results)
                    pullback_pass = sum(r.get('调试_回调识别通过', 0) for r in debug_results)
                    end_confirm_pass = sum(r.get('调试_末端确认通过', 0) for r in debug_results)
                    
                    if total_checked > 0:
                        print(f"\n  总检查天数: {total_checked}")
                        print(f"  趋势过滤通过率: {trend_pass}/{total_checked} ({trend_pass/total_checked*100:.1f}%)")
                        if trend_pass > 0:
                            print(f"  回调识别通过率: {pullback_pass}/{trend_pass} ({pullback_pass/trend_pass*100:.1f}%)")
                        if pullback_pass > 0:
                            print(f"  末端确认通过率: {end_confirm_pass}/{pullback_pass} ({end_confirm_pass/pullback_pass*100:.1f}%)")
                    else:
                        print(f"\n  ⚠️  总检查天数为0（可能所有股票数据不足或未进入分析循环）")
                else:
                    print(f"\n  ⚠️  无调试结果（可能所有股票返回None或数据不足）")
                
                print("=" * 60)
            else:
                # 非调试模式：区分有信号和无信号的结果
                signal_results = [r for r in valid_results if r.get('信号日期') is not None]
                no_signal_results = [r for r in valid_results if r.get('信号日期') is None]
            
            # 非调试模式：即使没有信号也导出所有股票（包含失败原因）
            if not signal_results:
                print("\n📊 分析结果汇总:")
                print("=" * 60)
                print(f"  总股票数: {total_stocks}")
                if none_count > 0:
                    print(f"  - 数据不足或异常: {none_count} 只（已记录日志）")
                print(f"  - 有效结果: {len(valid_results)} 只")
                print(f"  - 满足条件的信号: 0 只")
                print(f"  - 不满足条件的股票: {len(no_signal_results)} 只（将导出失败原因）")
                print("=" * 60)
                print("❌ 未发现满足条件的反弹信号")
                print("💡 将导出所有股票及其不满足条件的原因")
            else:
                print(f"\n✅ 发现 {len(signal_results)} 只股票满足条件")
                if len(no_signal_results) > 0:
                    print(f"📋 另有 {len(no_signal_results)} 只股票不满足条件（将一并导出失败原因）")
        
            # 使用调试结果或正常结果
            if debug_mode:
                # 调试模式：返回所有结果（包括调试信息）
                final_results = valid_results
                signal_df = pd.DataFrame([r for r in final_results if r.get('信号日期') is not None])
                no_signal_df = pd.DataFrame([r for r in final_results if r.get('信号日期') is None])
            else:
                # 正常模式：返回所有结果（包括有信号和无信号的）
                final_results = signal_results + no_signal_results
                signal_df = pd.DataFrame(signal_results) if signal_results else pd.DataFrame()
                no_signal_df = pd.DataFrame(no_signal_results) if no_signal_results else pd.DataFrame()
        
        # 使用统一的导出工具模块
        from export_utils import get_timestamped_output_path, format_stock_code_in_df
        
        # 格式化股票代码
        if not signal_df.empty:
            signal_df = format_stock_code_in_df(signal_df, code_column='股票代码')
        if not no_signal_df.empty:
            no_signal_df = format_stock_code_in_df(no_signal_df, code_column='股票代码')
        
        # 生成输出文件名（自动创建日期文件夹）
        mode_suffix = "_回测" if mode == "random" else ""
        base_filename = f"上升趋势反弹识别{mode_suffix}.xlsx"
        output_file = get_timestamped_output_path(output_dir, base_filename)
        
        # 导出Excel
        print("📊 导出Excel结果...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            if mode == "random":
                # 随机日期回测模式：导出回测数据
                if not signal_df.empty:
                    # 按截止日期、是否满足全部条件、综合信号强度排序
                    sort_columns = ['截止日期']
                    if '是否满足全部条件' in signal_df.columns:
                        sort_columns.append('是否满足全部条件')
                    if '综合信号强度' in signal_df.columns:
                        sort_columns.append('综合信号强度')
                    
                    signal_df = signal_df.sort_values(sort_columns, ascending=[True, False, False])
                    
                    # 统计满足全部条件和仅满足MASS条件的数量
                    if '是否满足全部条件' in signal_df.columns:
                        full_condition_count = signal_df['是否满足全部条件'].sum()
                        mass_only_count = len(signal_df) - full_condition_count
                        print(f"  📊 回测标的分类:")
                        print(f"    - 满足全部条件: {full_condition_count} 只")
                        print(f"    - 仅满足MASS条件: {mass_only_count} 只")
                    
                    signal_df.to_excel(writer, sheet_name='回测数据', index=False)
                    print(f"  ✅ 回测数据: {len(signal_df)} 条记录")
                
                # 导出回测统计
                if overall_stats:
                    # 整体统计
                    stats_data = []
                    for period in [5, 10, 20, 30, 60]:
                        avg_key = f'{period}日平均涨幅'
                        win_key = f'{period}日胜率'
                        sample_key = f'{period}日样本数'
                        if avg_key in overall_stats and overall_stats[avg_key] is not None:
                            stats_data.append({
                                '周期': f'{period}日',
                                '平均涨幅(%)': overall_stats[avg_key],
                                '胜率(%)': overall_stats[win_key],
                                '样本数': overall_stats[sample_key]
                            })
                    
                    if stats_data:
                        stats_df = pd.DataFrame(stats_data)
                        stats_df.to_excel(writer, sheet_name='回测统计_整体', index=False)
                        print(f"  ✅ 回测统计（整体）: {len(stats_df)} 个周期")
                    
                    # 分类统计：满足全部条件
                    full_condition_data = [r for r in all_backtest_data if r.get('是否满足全部条件', False)]
                    if full_condition_data:
                        full_stats = calculate_backtest_statistics(full_condition_data, periods=[5, 10, 20, 30, 60])
                        full_stats_data = []
                        for period in [5, 10, 20, 30, 60]:
                            avg_key = f'{period}日平均涨幅'
                            win_key = f'{period}日胜率'
                            sample_key = f'{period}日样本数'
                            if avg_key in full_stats and full_stats[avg_key] is not None:
                                full_stats_data.append({
                                    '周期': f'{period}日',
                                    '平均涨幅(%)': full_stats[avg_key],
                                    '胜率(%)': full_stats[win_key],
                                    '样本数': full_stats[sample_key]
                                })
                        
                        if full_stats_data:
                            full_stats_df = pd.DataFrame(full_stats_data)
                            full_stats_df.to_excel(writer, sheet_name='回测统计_满足全部条件', index=False)
                            print(f"  ✅ 回测统计（满足全部条件）: {len(full_stats_df)} 个周期，样本数={len(full_condition_data)}")
                    
                    # 分类统计：仅满足MASS条件
                    mass_only_data = [r for r in all_backtest_data if not r.get('是否满足全部条件', True)]
                    if mass_only_data:
                        mass_stats = calculate_backtest_statistics(mass_only_data, periods=[5, 10, 20, 30, 60])
                        mass_stats_data = []
                        for period in [5, 10, 20, 30, 60]:
                            avg_key = f'{period}日平均涨幅'
                            win_key = f'{period}日胜率'
                            sample_key = f'{period}日样本数'
                            if avg_key in mass_stats and mass_stats[avg_key] is not None:
                                mass_stats_data.append({
                                    '周期': f'{period}日',
                                    '平均涨幅(%)': mass_stats[avg_key],
                                    '胜率(%)': mass_stats[win_key],
                                    '样本数': mass_stats[sample_key]
                                })
                        
                        if mass_stats_data:
                            mass_stats_df = pd.DataFrame(mass_stats_data)
                            mass_stats_df.to_excel(writer, sheet_name='回测统计_仅MASS条件', index=False)
                            print(f"  ✅ 回测统计（仅MASS条件）: {len(mass_stats_df)} 个周期，样本数={len(mass_only_data)}")
                
                # 按日期导出各日期的统计
                if all_date_results:
                    date_stats_list = []
                    for cutoff_date, date_results in all_date_results.items():
                        date_stats = calculate_backtest_statistics(date_results, periods=[5, 10, 20, 30, 60])
                        for period in [5, 10, 20, 30, 60]:
                            avg_key = f'{period}日平均涨幅'
                            win_key = f'{period}日胜率'
                            sample_key = f'{period}日样本数'
                            if avg_key in date_stats and date_stats[avg_key] is not None:
                                date_stats_list.append({
                                    '截止日期': cutoff_date,
                                    '周期': f'{period}日',
                                    '平均涨幅(%)': date_stats[avg_key],
                                    '胜率(%)': date_stats[win_key],
                                    '样本数': date_stats[sample_key]
                                })
                    
                    if date_stats_list:
                        date_stats_df = pd.DataFrame(date_stats_list)
                        date_stats_df.to_excel(writer, sheet_name='各日期统计', index=False)
                        print(f"  ✅ 各日期统计: {len(date_stats_df)} 条记录")
            else:
                # 最新日期模式：保持原有导出逻辑
                # Sheet 1: 满足条件的信号（如果有）
                if not signal_df.empty:
                    # 按综合信号强度排序
                    if '综合信号强度' in signal_df.columns:
                        signal_df = signal_df.sort_values('综合信号强度', ascending=False)
                    signal_df.to_excel(writer, sheet_name='满足条件的信号', index=False)
                    print(f"  ✅ 满足条件的信号: {len(signal_df)} 只")
                
                # Sheet 2: 不满足条件的股票（包含失败原因）
                if not no_signal_df.empty:
                    # 按股票代码排序
                    no_signal_df = no_signal_df.sort_values('股票代码')
                    
                    # 展开失败详情（如果有）
                    if '失败详情' in no_signal_df.columns:
                        # 创建详细的失败原因表格
                        failure_details_list = []
                        for idx, row in no_signal_df.iterrows():
                            code = row['股票代码']
                            failure_details = row.get('失败详情', [])
                            if isinstance(failure_details, list) and failure_details:
                                for detail in failure_details:
                                    detail_row = detail.copy()
                                    detail_row['股票代码'] = code
                                    failure_details_list.append(detail_row)
                        
                        if failure_details_list:
                            failure_details_df = pd.DataFrame(failure_details_list)
                            failure_details_df.to_excel(writer, sheet_name='失败详情', index=False)
                            print(f"  ✅ 失败详情: {len(failure_details_df)} 条记录")
                    
                    # 移除失败详情列（因为已经单独导出）
                    export_df = no_signal_df.drop(columns=['失败详情'], errors='ignore')
                    export_df.to_excel(writer, sheet_name='不满足条件的股票', index=False)
                    print(f"  ✅ 不满足条件的股票: {len(export_df)} 只")
            
            # 如果没有信号也没有失败结果，至少导出一个说明表（正常情况下不应走到：有股票则应有明细）
            if signal_df.empty and no_signal_df.empty:
                msgs = ['未找到任何结果']
                if total_stocks == 0:
                    msgs.append('未获取到任何股票数据，请检查股票列表与数据源')
                else:
                    msgs.append(f'共分析 {total_stocks} 只股票，均未得到可导出的明细，请检查股票列表与数据源或查看日志')
                empty_df = pd.DataFrame({'说明': msgs})
                empty_df.to_excel(writer, sheet_name='无结果', index=False)
                print(f"  ⚠️  无结果可导出")
        
        elapsed = time.time() - start_time
        total_exported = len(signal_df) + len(no_signal_df)
        print(f"\n✅ 上升趋势反弹识别完成，共导出 {total_exported} 只股票")
        if not signal_df.empty:
            print(f"  - 满足条件的信号: {len(signal_df)} 只")
        if not no_signal_df.empty:
            print(f"  - 不满足条件的股票: {len(no_signal_df)} 只（包含失败原因）")
        print(f"输出文件: {output_file}")
        print(f"总耗时: {elapsed:.2f} 秒")
        
        # 记录会话
        session_logger.log_session_end(
            {
                "功能": "上升趋势中的反弹识别",
                "股票数量": len(codes),
                "有效结果数": total_exported,
                "满足条件的信号数": len(signal_df) if not signal_df.empty else 0,
                "不满足条件的股票数": len(no_signal_df) if not no_signal_df.empty else 0,
                "MASS短期周期": mass_short_period,
                "MASS长期周期": mass_long_period,
                "偏离度范围": f"{deviation_min*100:.1f}% ~ {deviation_max*100:.1f}%",
                "耗时": elapsed,
                "输出文件": output_file,
            }
        )
        
        return output_file
        
    except Exception as e:
        logger.error(f"执行上升趋势反弹识别时出错: {e}", exc_info=True)
        print(f"❌ 上升趋势反弹识别失败: {e}")
        return None
    finally:
        fetcher.close()

