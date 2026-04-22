#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
震荡股票识别模块

功能：识别处于震荡回调状态的股票
实现思路：
- 使用MASS均线排列打分模型判定中等强度（40-60分）
- 用均线斜率绝对值判定横盘状态
- 通过价格围绕MA5频繁穿越判定震荡行为
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
from cache_validator import validate_cache_data
from uptrend_rebound_analysis import calculate_mass_score, calculate_ma_slope

logger = logging.getLogger(__name__)


def count_ma5_crosses(close: pd.Series, ma5: pd.Series, lookback_days: int = 15) -> int:
    """
    统计最近N天内，收盘价对MA5的上下穿越次数
    
    参数：
    - close: 收盘价序列
    - ma5: MA5序列
    - lookback_days: 回看天数
    
    返回：
    - 穿越次数（整数）
    """
    try:
        if len(close) < lookback_days + 1 or len(ma5) < lookback_days + 1:
            return 0
        
        # 取最近N+1天的数据（需要比较前一日）
        recent_close = close.iloc[-(lookback_days+1):]
        recent_ma5 = ma5.iloc[-(lookback_days+1):]
        
        cross_count = 0
        
        for i in range(1, len(recent_close)):
            prev_close = recent_close.iloc[i-1]
            curr_close = recent_close.iloc[i]
            prev_ma5 = recent_ma5.iloc[i-1]
            curr_ma5 = recent_ma5.iloc[i]
            
            # 上穿：前一日 <= MA5，当前 > MA5
            if prev_close <= prev_ma5 and curr_close > curr_ma5:
                cross_count += 1
            # 下穿：前一日 >= MA5，当前 < MA5
            elif prev_close >= prev_ma5 and curr_close < curr_ma5:
                cross_count += 1
        
        return cross_count
        
    except Exception as e:
        logger.error(f"计算MA5穿越次数失败: {e}")
        return 0


def analyze_oscillation_stock(code: str, stock_data: pd.DataFrame,
                             mass_short_period: int = 60,
                             mass_long_period: int = 360,
                             mass_min: float = 40.0,
                             mass_max: float = 60.0,
                             ma5_slope_threshold: float = 0.0055,
                             ma10_slope_threshold: float = 0.0031,
                             lookback_days: int = 15,
                             min_cross_count: int = 2,
                             debug_mode: bool = False) -> Optional[Dict]:
    """
    分析单只股票是否为震荡股票
    
    参数：
    - code: 股票代码
    - stock_data: 股票数据DataFrame（需包含Close等字段）
    - mass_short_period: MASS短期周期（默认60）
    - mass_long_period: MASS长期周期（默认360）
    - mass_min: MASS得分下限（默认40）
    - mass_max: MASS得分上限（默认60）
    - ma5_slope_threshold: MA5斜率绝对值阈值（默认0.0055）
    - ma10_slope_threshold: MA10斜率绝对值阈值（默认0.0031）
    - lookback_days: 统计穿越次数的回看天数（默认15）
    - min_cross_count: 最小穿越次数（默认2）
    - debug_mode: 调试模式
    
    返回：
    - 包含分析结果的字典，如果不满足条件则返回None
    """
    try:
        if stock_data is None or stock_data.empty:
            return {
                '股票代码': code,
                '是否震荡': False,
                'MASS短期': None, 'MASS长期': None, 'MA5斜率': None, 'MA10斜率': None,
                '穿越次数': None, '收盘价': None, 'MA5': None, 'MA10': None,
                'MASS条件满足': False, '斜率条件满足': False, '穿越条件满足': False,
                '失败原因': '数据为空',
                **({'调试模式': True, '调试_数据状态': '数据为空'} if debug_mode else {}),
            }
        
        # 确保按日期升序
        df = stock_data.sort_index().copy()
        
        # 需要至少360天的数据来计算长期MASS
        min_required_days = max(mass_long_period, 100)
        if len(df) < min_required_days:
            return {
                '股票代码': code,
                '是否震荡': False,
                'MASS短期': None, 'MASS长期': None, 'MA5斜率': None, 'MA10斜率': None,
                '穿越次数': None, '收盘价': None, 'MA5': None, 'MA10': None,
                'MASS条件满足': False, '斜率条件满足': False, '穿越条件满足': False,
                '失败原因': f'数据不足（{len(df)}天 < {min_required_days}天）',
                **({'调试模式': True, '调试_数据状态': f'数据不足（{len(df)}天 < {min_required_days}天）', '调试_数据天数': len(df)} if debug_mode else {}),
            }
        
        # 提取关键字段
        close = pd.to_numeric(df.get("Close"), errors="coerce")
        close = close.dropna()
        
        if close.empty:
            return {
                '股票代码': code,
                '是否震荡': False,
                'MASS短期': None, 'MASS长期': None, 'MA5斜率': None, 'MA10斜率': None,
                '穿越次数': None, '收盘价': None, 'MA5': None, 'MA10': None,
                'MASS条件满足': False, '斜率条件满足': False, '穿越条件满足': False,
                '失败原因': '收盘价数据为空',
                **({'调试模式': True, '调试_数据状态': '收盘价数据为空'} if debug_mode else {}),
            }
        
        # 计算技术指标
        # 1. MASS均线排列打分
        mass_short = calculate_mass_score(close, mass_short_period)
        mass_long = calculate_mass_score(close, mass_long_period)
        
        # 2. 均线计算
        ma5 = close.rolling(5).mean()
        ma10 = close.rolling(10).mean()
        
        # 3. 均线斜率
        ma5_slope = calculate_ma_slope(ma5)
        ma10_slope = calculate_ma_slope(ma10)
        
        # 4. 统计价格穿越次数
        cross_count = count_ma5_crosses(close, ma5, lookback_days=lookback_days)
        
        # 判断条件
        # 1. MASS得分在中等强度区间
        mass_ok = (mass_min <= mass_short <= mass_max and 
                  mass_min <= mass_long <= mass_max)
        
        # 2. 均线斜率绝对值小（接近横盘）
        slope_ok = (abs(ma5_slope) <= ma5_slope_threshold and 
                   abs(ma10_slope) <= ma10_slope_threshold)
        
        # 3. 价格围绕MA5频繁穿越
        cross_ok = cross_count >= min_cross_count
        
        # 综合判断
        is_oscillation = mass_ok and slope_ok and cross_ok
        
        # 无论是否震荡，均返回判断指标明细（便于导出、复盘）
        fail_parts = []
        if not mass_ok:
            fail_parts.append('MASS不在区间')
        if not slope_ok:
            fail_parts.append('均线斜率超阈值')
        if not cross_ok:
            fail_parts.append('穿越次数不足')
        fail_reason = '; '.join(fail_parts) if fail_parts else None
        
        result = {
            '股票代码': code,
            '是否震荡': is_oscillation,
            'MASS短期': round(mass_short, 2),
            'MASS长期': round(mass_long, 2),
            'MA5斜率': round(ma5_slope * 100, 4),  # 转换为百分比
            'MA10斜率': round(ma10_slope * 100, 4),
            'MA5斜率绝对值': round(abs(ma5_slope) * 100, 4),
            'MA10斜率绝对值': round(abs(ma10_slope) * 100, 4),
            '穿越次数': cross_count,
            '收盘价': round(float(close.iloc[-1]), 2),
            'MA5': round(float(ma5.iloc[-1]), 2),
            'MA10': round(float(ma10.iloc[-1]), 2),
            'MASS条件满足': mass_ok,
            '斜率条件满足': slope_ok,
            '穿越条件满足': cross_ok,
            '失败原因': fail_reason,
        }
        if debug_mode:
            result['调试模式'] = True
            result['调试_数据天数'] = len(df)
        return result
        
    except Exception as e:
        logger.error(f"分析股票 {code} 震荡状态时出错: {e}")
        return {
            '股票代码': code,
            '是否震荡': False,
            'MASS短期': None, 'MASS长期': None, 'MA5斜率': None, 'MA10斜率': None,
            '穿越次数': None, '收盘价': None, 'MA5': None, 'MA10': None,
            'MASS条件满足': False, '斜率条件满足': False, '穿越条件满足': False,
            '失败原因': f'分析异常: {str(e)}',
        }


def run_oscillation_stock_analysis(limit: Optional[int] = None,
                                  max_days_ago: Optional[int] = None,
                                  mass_short_period: int = 60,
                                  mass_long_period: int = 360,
                                  mass_min: float = 40.0,
                                  mass_max: float = 60.0,
                                  ma5_slope_threshold: float = 0.0055,
                                  ma10_slope_threshold: float = 0.0031,
                                  lookback_days: int = 15,
                                  min_cross_count: int = 2,
                                  debug_mode: bool = False,
                                  output_dir: str = "outputs",
                                  cutoff_date: Optional[date] = None) -> Optional[str]:
    """
    一键执行"震荡股票识别"分析
    
    参数：
    - limit: 股票数量限制，None 使用配置中的 STOCK_LIST_LIMIT
    - max_days_ago: 最大允许行情滞后天数，None 使用配置中的 MAX_TRADING_DAYS_AGO
    - mass_short_period: MASS短期周期（默认60）
    - mass_long_period: MASS长期周期（默认360）
    - mass_min: MASS得分下限（默认40）
    - mass_max: MASS得分上限（默认60）
    - ma5_slope_threshold: MA5斜率绝对值阈值（默认0.0055）
    - ma10_slope_threshold: MA10斜率绝对值阈值（默认0.0031）
    - lookback_days: 统计穿越次数的回看天数（默认15）
    - min_cross_count: 最小穿越次数（默认2）
    - debug_mode: 调试模式
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
    print("震荡股票识别")
    print("=" * 60)
    print(f"股票数量上限: {limit}")
    print(f"最大允许行情滞后天数: {max_days_ago}")
    print(f"MASS短期周期: {mass_short_period}天")
    print(f"MASS长期周期: {mass_long_period}天")
    print(f"MASS得分范围: {mass_min} ~ {mass_max}分")
    print(f"MA5斜率阈值: {ma5_slope_threshold*100:.4f}%")
    print(f"MA10斜率阈值: {ma10_slope_threshold*100:.4f}%")
    print(f"穿越统计天数: {lookback_days}天")
    print(f"最小穿越次数: {min_cross_count}次")
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
        # 如果指定了截止日期，使用cutoff_date参数
        stock_info_list = fetcher.get_stock_list(limit=limit, max_days_ago=max_days_ago, cutoff_date=cutoff_date)
        list_elapsed = time.time() - list_start_time
        print(f"  ✅ 股票列表获取完成（耗时: {list_elapsed:.1f}秒）")
        
        # 兼容两种返回格式
        if stock_info_list and isinstance(stock_info_list[0], dict):
            codes = [info["code"] for info in stock_info_list]
        else:
            codes = stock_info_list
        
        if not codes:
            print("❌ 未获取到任何活跃股票")
            return None
        
        print(f"✅ 实际股票数量: {len(codes)}")
        
        # 2. 批量获取股票数据（使用增量缓存）
        print("📈 批量获取行情数据（增量缓存 + 批量SQL + 多线程并发）...")
        
        # 计算日期范围（至少需要mass_long_period + 100天的数据）
        days_needed = max(mass_long_period + 100, 500)
        # end_date 已在上面确定（cutoff_date 或最新交易日）
        start_date = end_date - timedelta(days=days_needed + 30)
        
        # 尝试从缓存加载（批量）
        print("  🔍 检查增量缓存...")
        print(f"  📅 请求日期范围: {start_date} 至 {end_date}（数据库最新交易日，需要{days_needed}天数据）")
        cached_stock_data, missing_stock_codes = futures_incremental_cache_manager.load_stocks_data(
            codes, start_date, end_date
        )
        
        # 检查缓存数据的完整性和日期范围（支持部分命中并增量补全）
        insufficient_data_codes = []
        partial_data_codes = {}
        valid_cached_data = {}
        
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
        
        # 合并需要重新获取的股票代码
        all_missing_codes = list(set(missing_stock_codes + insufficient_data_codes))
        cache_hit_count = len(valid_cached_data)
        partial_hit_count = len(partial_data_codes)
        cache_miss_count = len(all_missing_codes)
        
        print(f"  ✅ 缓存完全有效: {cache_hit_count} 只股票")
        if partial_hit_count > 0:
            print(f"  🔄 缓存部分命中: {partial_hit_count} 只股票（将增量补全缺失日期范围）")
        print(f"  ⚠️  需要从数据库获取: {cache_miss_count} 只股票")
        
        # 对缺失或数据不足的股票从数据库获取（批量）
        fetch_start_time = time.time()
        fetched_stock_data = {}
        
        # 1. 处理完全缺失的股票
        if all_missing_codes:
            print(f"  📥 从数据库获取完全缺失的 {len(all_missing_codes)} 只股票数据...")
            fully_missing_data = fetcher.batch_get_stock_data_with_adjustment(
                all_missing_codes,
                days=days_needed,
                time_config=None
            )
            if fully_missing_data:
                fetched_stock_data.update(fully_missing_data)
        
        # 2. 处理部分命中的股票（增量补全）
        if partial_data_codes:
            print(f"  🔄 增量补全部分命中的 {len(partial_data_codes)} 只股票数据...")
            date_range_groups = {}
            
            for code, partial_info in partial_data_codes.items():
                missing_start = partial_info.get('missing_start')
                missing_end = partial_info.get('missing_end')
                cache_start = partial_info.get('cache_start')
                cache_end = partial_info.get('cache_end')
                
                if missing_end:
                    fetch_start = cache_end + timedelta(days=1) if cache_end else start_date
                    fetch_end = missing_end
                else:
                    fetch_start = cache_end + timedelta(days=1) if cache_end else start_date
                    fetch_end = end_date
                
                fetch_start = max(fetch_start, start_date)
                fetch_end = min(fetch_end, end_date)
                
                if fetch_start <= fetch_end:
                    range_key = (fetch_start, fetch_end)
                    if range_key not in date_range_groups:
                        date_range_groups[range_key] = []
                    date_range_groups[range_key].append(code)
            
            for (fetch_start, fetch_end), codes_in_group in date_range_groups.items():
                days_to_fetch = (fetch_end - fetch_start).days + 30
                partial_fetched = fetcher.batch_get_stock_data_with_adjustment(
                    codes_in_group,
                    days=days_to_fetch,
                    time_config=None
                )
                if partial_fetched:
                    fetched_stock_data.update(partial_fetched)
        
        # 合并部分命中的缓存数据
        if partial_data_codes:
            for code, partial_info in partial_data_codes.items():
                cached_data = partial_info['cached_data']
                if code in fetched_stock_data:
                    new_data = fetched_stock_data[code]
                    if not new_data.empty:
                        combined_data = pd.concat([cached_data, new_data])
                        combined_data = combined_data.sort_index()
                        combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
                        fetched_stock_data[code] = combined_data
                else:
                    fetched_stock_data[code] = cached_data
        
        if fetched_stock_data:
            fetch_elapsed = time.time() - fetch_start_time
            print(f"  ✅ 数据获取完成（耗时: {fetch_elapsed:.1f}秒）")
            
            # 保存新获取的数据到缓存
            print(f"  💾 保存 {len(fetched_stock_data)} 只股票的数据到增量缓存...")
            futures_incremental_cache_manager.save_stocks_data(
                list(fetched_stock_data.keys()),
                fetched_stock_data,
                start_date,
                end_date
            )
            print(f"  ✅ 缓存保存完成")
            
            # 合并有效缓存数据和新增数据
            valid_cached_data.update(fetched_stock_data)
        
        all_stock_data = valid_cached_data
        
        if not all_stock_data:
            print("❌ 未获取到任何股票行情数据")
            return None
        
        print(f"✅ 成功获取 {len(all_stock_data)} 只股票的行情数据")
        
        # 3. 并行计算震荡信号
        print("🚀 并行计算震荡识别信号...")
        
        def _task_oscillation(code: str,
                              stock_data_dict: Dict[str, pd.DataFrame],
                              mass_short_period: int,
                              mass_long_period: int,
                              mass_min: float,
                              mass_max: float,
                              ma5_slope_threshold: float,
                              ma10_slope_threshold: float,
                              lookback_days: int,
                              min_cross_count: int,
                              debug_mode: bool = False) -> Optional[Dict]:
            """供高性能线程池调用的任务函数"""
            df = stock_data_dict.get(code)
            if df is None or df.empty:
                return None
            return analyze_oscillation_stock(
                code=code,
                stock_data=df,
                mass_short_period=mass_short_period,
                mass_long_period=mass_long_period,
                mass_min=mass_min,
                mass_max=mass_max,
                ma5_slope_threshold=ma5_slope_threshold,
                ma10_slope_threshold=ma10_slope_threshold,
                lookback_days=lookback_days,
                min_cross_count=min_cross_count,
                debug_mode=debug_mode
            )
        
        thread_pool = HighPerformanceThreadPool(progress_desc="震荡股票识别")
        
        tasks = list(all_stock_data.keys())
        results = thread_pool.execute_batch(
            tasks,
            _task_oscillation,
            all_stock_data,
            mass_short_period,
            mass_long_period,
            mass_min,
            mass_max,
            ma5_slope_threshold,
            ma10_slope_threshold,
            lookback_days,
            min_cross_count,
            debug_mode,
        )
        
        # 汇总结果
        valid_results = [r for r in results if r is not None]
        oscillation_results = [r for r in valid_results if r.get('是否震荡', False)]
        debug_results = [r for r in valid_results if r.get('调试模式', False)]
        
        if debug_mode:
            print(f"\n📊 调试模式统计信息:")
            print(f"  总股票数: {len(results)}")
            print(f"  有效结果: {len(valid_results)} 只")
            print(f"  震荡股票: {len(oscillation_results)} 只")
            print(f"  调试结果: {len(debug_results)} 只")
        else:
            print(f"\n✅ 发现 {len(oscillation_results)} 只震荡股票")
            if len(valid_results) > len(oscillation_results):
                print(f"📋 另有 {len(valid_results) - len(oscillation_results)} 只股票不满足条件")
        
        # 使用统一的导出工具模块
        from export_utils import get_timestamped_output_path, format_stock_code_in_df
        
        # 格式化股票代码
        if oscillation_results:
            oscillation_df = pd.DataFrame(oscillation_results)
            if not oscillation_df.empty and '股票代码' in oscillation_df.columns:
                oscillation_df = format_stock_code_in_df(oscillation_df, code_column='股票代码')
        else:
            oscillation_df = pd.DataFrame()
        
        # 始终导出所有股票的判断指标明细（含不符合条件的），便于复盘
        all_df = pd.DataFrame(valid_results) if valid_results else pd.DataFrame()
        if not all_df.empty and '股票代码' in all_df.columns:
            all_df = format_stock_code_in_df(all_df, code_column='股票代码')
        
        # 生成输出文件名（自动创建日期文件夹）
        output_file = get_timestamped_output_path(output_dir, "震荡股票识别.xlsx")
        
        # 导出Excel（openpyxl 要求至少有一个可见 sheet，故在无数据时写入说明页）
        print("📊 导出Excel结果...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: 震荡股票（如果有）
            if not oscillation_df.empty:
                # 按MASS短期得分排序
                if 'MASS短期' in oscillation_df.columns:
                    oscillation_df = oscillation_df.sort_values('MASS短期', ascending=False)
                oscillation_df.to_excel(writer, sheet_name='震荡股票', index=False)
                print(f"  ✅ 震荡股票: {len(oscillation_df)} 只")
            
            # Sheet 2: 所有结果（含不满足条件的判断指标明细）
            if not all_df.empty:
                if 'MASS短期' in all_df.columns:
                    all_df = all_df.sort_values('MASS短期', ascending=False)
                all_df.to_excel(writer, sheet_name='所有结果', index=False)
                print(f"  ✅ 所有结果: {len(all_df)} 只")
            
            # 无任何有效结果时也必须写入至少一页，否则 openpyxl 报错: At least one sheet must be visible
            if oscillation_df.empty and all_df.empty:
                pd.DataFrame({'说明': ['未获取到有效分析结果，请检查股票列表与数据']}).to_excel(
                    writer, sheet_name='说明', index=False)
                print("  ⚠️ 无有效结果，已导出说明页")
        
        elapsed = time.time() - start_time
        print(f"\n✅ 震荡股票识别完成，共分析 {len(valid_results)} 只股票")
        if oscillation_results:
            print(f"  - 震荡股票: {len(oscillation_results)} 只")
        print(f"输出文件: {output_file}")
        print(f"总耗时: {elapsed:.2f} 秒")
        
        # 记录会话
        session_logger.log_session_end(
            {
                "功能": "震荡股票识别",
                "股票数量": len(codes),
                "有效结果数": len(valid_results),
                "震荡股票数": len(oscillation_results),
                "MASS短期周期": mass_short_period,
                "MASS长期周期": mass_long_period,
                "MASS得分范围": f"{mass_min}~{mass_max}",
                "耗时": elapsed,
                "输出文件": output_file,
            }
        )
        
        return output_file
        
    except Exception as e:
        logger.error(f"执行震荡股票识别时出错: {e}", exc_info=True)
        print(f"❌ 震荡股票识别失败: {e}")
        return None
    finally:
        fetcher.close()

