#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一的缓存验证模块

功能：提供统一的缓存数据有效性判断标准，确保所有功能使用一致的缓存策略

作者：AI Assistant
创建时间：2025年1月
"""

from datetime import date, timedelta
from typing import Dict, Optional, Tuple
import pandas as pd


class CacheValidationResult:
    """缓存验证结果"""
    def __init__(self, is_valid: bool, is_partial: bool, 
                 missing_start: Optional[date] = None, 
                 missing_end: Optional[date] = None,
                 cache_start: Optional[date] = None,
                 cache_end: Optional[date] = None):
        self.is_valid = is_valid  # 缓存是否完全有效
        self.is_partial = is_partial  # 缓存是否部分有效（需要增量补全）
        self.missing_start = missing_start  # 缺失的开始日期
        self.missing_end = missing_end  # 缺失的结束日期
        self.cache_start = cache_start  # 缓存数据的开始日期
        self.cache_end = cache_end  # 缓存数据的结束日期


def validate_cache_data(
    data: pd.DataFrame,
    start_date: date,
    end_date: date,
    days_needed: int,
    min_data_days: Optional[int] = None,
    start_tolerance_days: int = 10,
    end_tolerance_days: int = 5,
    min_coverage_ratio: float = 0.95
) -> CacheValidationResult:
    """
    统一的缓存数据验证函数
    
    参数：
    - data: 缓存的数据（DataFrame，索引为日期）
    - start_date: 请求的开始日期
    - end_date: 请求的结束日期
    - days_needed: 需要的数据天数（日历天）
    - min_data_days: 最少需要的数据天数（交易日），如果为None则使用days_needed * 0.8
    - start_tolerance_days: 开始日期的容差（天），默认10天
    - end_tolerance_days: 结束日期的容差（天），默认5天
    - min_coverage_ratio: 最小覆盖率，默认0.95（95%）
    
    返回：
    - CacheValidationResult: 验证结果
    """
    # 如果数据为空，返回无效
    if data is None or data.empty:
        return CacheValidationResult(is_valid=False, is_partial=False)
    
    # 获取数据的日期范围
    data_start = data.index.min()
    data_end = data.index.max()
    
    # 转换为date类型
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
    
    # 计算实际数据天数（交易日数）
    actual_data_days = len(data)
    
    # 计算请求的日期范围（日历天）
    request_calendar_days = (end_date - start_date).days
    
    # 计算覆盖的日期范围
    covered_start = max(data_start_date, start_date)
    covered_end = min(data_end_date, end_date)
    covered_calendar_days = (covered_end - covered_start).days if covered_end >= covered_start else 0
    
    # 计算覆盖率
    coverage_ratio = covered_calendar_days / request_calendar_days if request_calendar_days > 0 else 0
    
    # 判断开始日期是否满足要求
    # 如果缓存开始日期 <= 请求开始日期 + 容差，或者覆盖率 >= 最小覆盖率，则认为开始日期满足
    start_ok = data_start_date <= start_date + timedelta(days=start_tolerance_days) or coverage_ratio >= min_coverage_ratio
    
    # 判断结束日期是否满足要求
    # 严格检查：缓存结束日期必须 >= 请求结束日期 - 容差（不允许coverage_ratio兜底，确保数据足够新）
    end_ok = data_end_date >= end_date - timedelta(days=end_tolerance_days)
    
    # 日期范围是否满足
    date_range_ok = start_ok and end_ok
    
    # 计算最少需要的数据天数
    if min_data_days is None:
        min_required_days = int(days_needed * 0.8)  # 默认需要80%的数据
    else:
        min_required_days = min_data_days
    
    # 判断数据是否充足
    # 如果实际数据天数 >= 最少需要天数，或者（覆盖率 >= 最小覆盖率 且 日期范围满足），则认为数据充足
    data_sufficient = actual_data_days >= min_required_days or (coverage_ratio >= min_coverage_ratio and date_range_ok)
    
    # 判断结果
    if data_sufficient and date_range_ok:
        # 缓存数据完全有效
        return CacheValidationResult(
            is_valid=True,
            is_partial=False,
            cache_start=data_start_date,
            cache_end=data_end_date
        )
    elif actual_data_days >= 50 or coverage_ratio >= 0.3:  
        # 缓存有部分数据（至少50个交易日，或者覆盖率>=30%），尝试增量补全
        # 这样即使日期范围不完整，只要有足够的数据量或覆盖率，也会被识别为"部分有效"
        # 增量补全比重新获取全部数据要快得多，所以放宽条件
        # 记录缺失的日期范围
        missing_start = start_date if data_start_date > start_date + timedelta(days=start_tolerance_days) else None
        missing_end = end_date if data_end_date < end_date - timedelta(days=end_tolerance_days) else None
        
        return CacheValidationResult(
            is_valid=False,
            is_partial=True,
            missing_start=missing_start,
            missing_end=missing_end,
            cache_start=data_start_date,
            cache_end=data_end_date
        )
    else:
        # 缓存数据太少（<50个交易日 且 覆盖率<30%），需要重新获取全部数据
        return CacheValidationResult(
            is_valid=False,
            is_partial=False,
            cache_start=data_start_date,
            cache_end=data_end_date
        )


def validate_cache_with_min_window(
    data: pd.DataFrame,
    start_date: date,
    end_date: date,
    days_needed: int,
    min_window_days: int,
    start_tolerance_days: int = 10,
    end_tolerance_days: int = 5,
    min_coverage_ratio: float = 0.95
) -> CacheValidationResult:
    """
    带最小窗口要求的缓存数据验证函数（用于需要特定最小数据量的场景，如功能13需要price_window）
    
    参数：
    - data: 缓存的数据（DataFrame，索引为日期）
    - start_date: 请求的开始日期
    - end_date: 请求的结束日期
    - days_needed: 需要的数据天数（日历天）
    - min_window_days: 最小窗口天数（交易日），如price_window，新股/次新股至少需要这么多数据
    - start_tolerance_days: 开始日期的容差（天），默认10天
    - end_tolerance_days: 结束日期的容差（天），默认5天
    - min_coverage_ratio: 最小覆盖率，默认0.95（95%）
    
    返回：
    - CacheValidationResult: 验证结果
    """
    # 计算最少需要的数据天数（取max(min_required_days, min_window_days)）
    min_required_days = max(int(days_needed * 0.8), min_window_days)
    
    # 调用统一的验证函数
    result = validate_cache_data(
        data=data,
        start_date=start_date,
        end_date=end_date,
        days_needed=days_needed,
        min_data_days=min_required_days,
        start_tolerance_days=start_tolerance_days,
        end_tolerance_days=end_tolerance_days,
        min_coverage_ratio=min_coverage_ratio
    )
    
    # 对于部分有效的情况，需要调整判断：至少需要min_window_days或50个交易日，或者覆盖率>=30%
    if not result.is_valid and not result.is_partial:
        actual_data_days = len(data)
        # 重新计算覆盖率（使用原来的逻辑）
        data_start = data.index.min()
        data_end = data.index.max()
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
        request_calendar_days = (end_date - start_date).days
        covered_start = max(data_start_date, start_date)
        covered_end = min(data_end_date, end_date)
        covered_calendar_days = (covered_end - covered_start).days if covered_end >= covered_start else 0
        coverage_ratio = covered_calendar_days / request_calendar_days if request_calendar_days > 0 else 0
        
        # 如果实际数据天数 >= max(min_window_days, 50)或覆盖率>=30%，则认为是部分有效
        if actual_data_days >= max(min_window_days, 50) or coverage_ratio >= 0.3:
            missing_start = start_date if result.cache_start and result.cache_start > start_date + timedelta(days=start_tolerance_days) else None
            missing_end = end_date if result.cache_end and result.cache_end < end_date - timedelta(days=end_tolerance_days) else None
            
            return CacheValidationResult(
                is_valid=False,
                is_partial=True,
                missing_start=missing_start,
                missing_end=missing_end,
                cache_start=result.cache_start,
                cache_end=result.cache_end
            )
    
    return result

