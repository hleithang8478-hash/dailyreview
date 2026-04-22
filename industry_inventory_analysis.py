#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
行业库存分析模块

功能：分析各三级行业的库存情况（Inventories）
实现思路：
- 从LC_ExgIndustry表获取三级行业信息
- 通过CompanyCode关联LC_BalanceSheetAll表获取存货数据
- 按三级行业汇总库存数据
- 绘制近五年趋势图
"""

import os
import time
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import numpy as np
import pandas as pd
# 在模块级别设置matplotlib后端，确保线程安全
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端，线程安全
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager

from data_fetcher import JuyuanDataFetcher
from config import STOCK_LIST_LIMIT, MAX_TRADING_DAYS_AGO, MAX_WORKERS, DB_POOL_SIZE
from high_performance_threading import HighPerformanceThreadPool
from logger_config import init_logger, get_logger_config
from tqdm import tqdm
import concurrent.futures

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

logger = logging.getLogger(__name__)


class IndustryInventoryAnalyzer:
    """行业库存分析器"""
    
    def __init__(self, fetcher: JuyuanDataFetcher):
        """
        初始化分析器
        
        参数:
        - fetcher: 数据获取器
        """
        self.fetcher = fetcher
        
    def get_industry_stock_list(self) -> pd.DataFrame:
        """
        获取所有股票的三级行业信息
        
        返回:
        - DataFrame，包含股票代码、公司代码、三级行业等信息
        """
        try:
            sql = """
            SELECT DISTINCT
                s.SecuCode,
                s.SecuAbbr,
                s.CompanyCode,
                i.ThirdIndustryName,
                i.SecondIndustryName,
                i.FirstIndustryName
            FROM SecuMain s
            INNER JOIN LC_ExgIndustry i ON s.CompanyCode = i.CompanyCode
            WHERE s.SecuCategory = 1
            AND i.Standard = '38'
            AND i.IfPerformed = 1
            AND i.ThirdIndustryName IS NOT NULL
            AND i.ThirdIndustryName != ''
            ORDER BY i.ThirdIndustryName, s.SecuCode
            """
            
            logger.info("开始获取行业股票列表...")
            df = self.fetcher.query(sql)
            logger.info(f"获取到 {len(df)} 只股票的三级行业信息")
            
            return df
            
        except Exception as e:
            logger.error(f"获取行业股票列表失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def get_inventory_data(self, company_codes: List[int], years: int = 10, silent: bool = False) -> pd.DataFrame:
        """
        批量获取存货数据
        
        参数:
        - company_codes: 公司代码列表
        - years: 获取近多少年的数据（默认10年）
        
        返回:
        - DataFrame，包含CompanyCode, EndDate, Inventories等字段
        
        数据获取逻辑:
        - 使用EndDate字段匹配季度末日期（3月31日、6月30日、9月30日、12月31日）
        - 筛选条件：IfMerged=1（合并报表）
        - 根据季度末日期匹配对应的报告类型：
          * 3月31日 -> InfoSource='第一季报'
          * 6月30日 -> InfoSource='半年度报告'
          * 9月30日 -> InfoSource='第三季报'
          * 12月31日 -> InfoSource='年度报告'
        """
        try:
            if not company_codes:
                return pd.DataFrame()
            
            # 计算起始日期（确保包含完整年份）
            end_date = date.today()
            # 从当前日期往前推years年，确保包含所有数据
            # ⚠️ 关键：end_date应该包含到当前日期，以便获取最新的季度数据
            # 如果今天是2026年1月，应该能获取到2025年9月30日（三季度）的数据
            start_date = date(end_date.year - years, 1, 1)
            if not silent:
                logger.info(f"库存数据查询日期范围: {start_date} 至 {end_date}（共{years}年）")
            else:
                logger.debug(f"库存数据查询日期范围: {start_date} 至 {end_date}（共{years}年）")
            
            # ⚠️ 极限优化：使用更小的批次和更多线程，最大化并发度
            # 批次大小：50（从500降低到50，产生更多批次，大幅提高并行度）
            # 线程数：使用配置中的MAX_WORKERS（通常是CPU核心数×16，最多1024）
            # 对于IO密集型任务（数据库查询），更小的批次可以显著提高并发度
            batch_size = 50  # 从500降低到50，产生更多批次，提高并行度
            max_workers = max(MAX_WORKERS, 128)  # 至少128线程，极限压榨硬件性能
            all_data = []
            total_batches = (len(company_codes) + batch_size - 1) // batch_size
            
            if not silent:
                logger.info(f"使用多线程并发获取数据：批次大小={batch_size}，线程数={max_workers}，总批次数={total_batches}")
            else:
                logger.debug(f"使用多线程并发获取数据：批次大小={batch_size}，线程数={max_workers}，总批次数={total_batches}")
            
            # 定义单批次查询函数（使用闭包访问外部变量）
            def query_single_batch(batch_codes: List[int], batch_idx: int, 
                                   fetcher: JuyuanDataFetcher, start_date: date, end_date: date) -> Optional[pd.DataFrame]:
                """
                查询单个批次的数据
                策略：先查询不带IfMerged和IfAdjusted条件的数据，如果有重复再使用这两个条件过滤
                """
                try:
                    codes_str = ','.join([str(code) for code in batch_codes])
                    
                    # 第一步：先查询不带IfMerged和IfAdjusted条件的数据
                    sql_step1 = f"""
                    SELECT 
                        CompanyCode,
                        EndDate,
                        Inventories,
                        InfoPublDate,
                        IfMerged,
                        IfAdjusted
                    FROM LC_BalanceSheetAll
                    WHERE CompanyCode IN ({codes_str})
                    AND EndDate >= '{start_date}'
                    AND EndDate <= '{end_date}'
                    AND Inventories IS NOT NULL
                    -- ⚠️ 注意：end_date是当前日期，应该能查询到当前年份的所有季度数据
                    -- 例如：如果今天是2026年1月，end_date=2026-01-19，应该能查询到2025年9月30日的数据
                    AND (
                        -- 3月31日 -> 第一季报
                        (MONTH(EndDate) = 3 AND DAY(EndDate) = 31 AND InfoSource = '第一季报')
                        OR
                        -- 6月30日 -> 半年度报告
                        (MONTH(EndDate) = 6 AND DAY(EndDate) = 30 AND InfoSource = '半年度报告')
                        OR
                        -- 9月30日 -> 第三季报
                        (MONTH(EndDate) = 9 AND DAY(EndDate) = 30 AND InfoSource = '第三季报')
                        OR
                        -- 12月31日 -> 年度报告
                        (MONTH(EndDate) = 12 AND DAY(EndDate) = 31 AND InfoSource = '年度报告')
                    )
                    ORDER BY CompanyCode, EndDate DESC
                    """
                    
                    df_batch = fetcher.query(sql_step1)
                    
                    if df_batch.empty:
                        # 在Web场景下，即使silent模式也要记录关键错误
                        logger.warning(f"批次 {batch_idx}: SQL查询返回空结果 - 公司代码数量: {len(batch_codes)}，示例: {batch_codes[:5] if len(batch_codes) > 5 else batch_codes}")
                        # 尝试不限制InfoSource，看看是否有数据
                        sql_test = f"""
                        SELECT TOP 10
                            CompanyCode, EndDate, InfoSource, Inventories
                        FROM LC_BalanceSheetAll
                        WHERE CompanyCode IN ({codes_str})
                        AND EndDate >= '{start_date}'
                        AND EndDate <= '{end_date}'
                        AND Inventories IS NOT NULL
                        ORDER BY CompanyCode, EndDate DESC
                        """
                        try:
                            df_test = fetcher.query(sql_test)
                            if not df_test.empty:
                                # 有数据但不符合InfoSource条件
                                unique_sources = df_test['InfoSource'].unique().tolist() if 'InfoSource' in df_test.columns else []
                                # 检查是否是InfoSource值不匹配的问题
                                test_dates = df_test['EndDate'].unique().tolist()[:5] if 'EndDate' in df_test.columns else []
                                logger.warning(f"批次 {batch_idx}: ⚠️ 发现数据但InfoSource不匹配！")
                                logger.warning(f"    查询到的InfoSource值: {unique_sources[:10]}")
                                logger.warning(f"    查询到的日期示例: {test_dates}")
                                logger.warning(f"    当前查询条件期望的InfoSource: '第一季报', '半年度报告', '第三季报', '年度报告'")
                                logger.warning(f"    建议：检查数据库中InfoSource字段的实际值，可能需要调整查询条件")
                            else:
                                # 完全不限制条件也查询不到，可能是日期或公司代码问题
                                logger.warning(f"批次 {batch_idx}: 完全不限制条件也查询不到数据")
                                logger.warning(f"    可能原因：1) 这些公司代码在指定时间范围内没有库存数据；2) 日期范围不正确")
                                # 再试一次，只检查公司代码是否存在
                                sql_check = f"""
                                SELECT TOP 5 CompanyCode, EndDate, InfoSource, Inventories
                                FROM LC_BalanceSheetAll
                                WHERE CompanyCode IN ({codes_str})
                                ORDER BY CompanyCode, EndDate DESC
                                """
                                df_check = fetcher.query(sql_check)
                                if not df_check.empty:
                                    check_dates = df_check['EndDate'].unique().tolist()[:5] if 'EndDate' in df_check.columns else []
                                    logger.warning(f"    这些公司代码在其他时间段有数据，最新日期: {check_dates}")
                                else:
                                    logger.warning(f"    这些公司代码在数据库中可能不存在或没有库存数据")
                        except Exception as e:
                            logger.error(f"批次 {batch_idx}: 诊断查询失败: {e}")
                        return None
                    
                    # ⚠️ 调试：检查批次查询到的数据日期范围
                    if 'EndDate' in df_batch.columns:
                        # 确保EndDate是日期类型
                        if df_batch['EndDate'].dtype == 'object':
                            df_batch['EndDate'] = pd.to_datetime(df_batch['EndDate']).dt.date
                        elif isinstance(df_batch['EndDate'].iloc[0] if len(df_batch) > 0 else None, pd.Timestamp):
                            df_batch['EndDate'] = df_batch['EndDate'].dt.date
                        
                        batch_max_date = df_batch['EndDate'].max()
                        batch_min_date = df_batch['EndDate'].min()
                        batch_unique_dates = sorted(df_batch['EndDate'].unique(), reverse=True)
                        logger.debug(f"批次 {batch_idx}: 查询到 {len(df_batch)} 条数据，日期范围 {batch_min_date} 至 {batch_max_date}")
                        logger.debug(f"批次 {batch_idx}: 最新5个报告期: {batch_unique_dates[:5]}")
                        
                        # ⚠️ 关键检查：如果查询到的最大日期小于查询范围，说明SQL查询可能有问题
                        if batch_max_date < end_date:
                            # 检查是否有2024年或2025年的数据
                            year_2024 = df_batch[df_batch['EndDate'].apply(lambda x: x.year if hasattr(x, 'year') else pd.to_datetime(x).year) == 2024]
                            year_2025 = df_batch[df_batch['EndDate'].apply(lambda x: x.year if hasattr(x, 'year') else pd.to_datetime(x).year) == 2025]
                            if year_2024.empty and year_2025.empty:
                                logger.warning(f"批次 {batch_idx}: ⚠️ SQL查询没有返回2024或2025年数据，最大日期只到 {batch_max_date}")
                                # 检查InfoSource字段，看是否有其他值
                                if 'InfoSource' in df_batch.columns:
                                    unique_info_sources = df_batch['InfoSource'].unique()
                                    logger.warning(f"批次 {batch_idx}: 当前批次InfoSource值: {unique_info_sources}")
                                # 输出SQL查询条件用于调试
                                logger.warning(f"批次 {batch_idx}: SQL查询条件: EndDate >= '{start_date}' AND EndDate <= '{end_date}'")
                                logger.warning(f"批次 {batch_idx}: SQL InfoSource条件: 第一季报/半年度报告/第三季报/年度报告")
                    
                    # 第二步：检查是否有重复（同一个CompanyCode和EndDate有多条记录）
                    duplicates = df_batch.duplicated(subset=['CompanyCode', 'EndDate'], keep=False)
                    has_duplicates = duplicates.any()
                    
                    if has_duplicates:
                        dup_count = duplicates.sum()
                        logger.debug(f"批次 {batch_idx}: 发现 {dup_count} 条重复记录（同一CompanyCode + EndDate）")
                    
                    if has_duplicates:
                        # 如果有重复，使用IfMerged=1和IfAdjusted=1条件过滤
                        # 检查是否有IfMerged和IfAdjusted字段
                        before_filter = len(df_batch)
                        # ⚠️ 关键：保存过滤前的数据，以便在过滤后2025年数据为0时恢复
                        df_batch_before_filter = df_batch.copy()
                        
                        if 'IfMerged' in df_batch.columns and 'IfAdjusted' in df_batch.columns:
                            # 先尝试同时使用两个条件
                            filtered = df_batch[(df_batch['IfMerged'] == 1) & (df_batch['IfAdjusted'] == 1)]
                            if not filtered.empty:
                                df_batch = filtered
                                after_filter = len(df_batch)
                                # ⚠️ 关键修复：检查过滤后2025年数据，如果为0，改为只使用IfMerged=1
                                if 'EndDate' in df_batch.columns:
                                    # 确保EndDate是日期类型
                                    if df_batch['EndDate'].dtype == 'object':
                                        df_batch['EndDate'] = pd.to_datetime(df_batch['EndDate']).dt.date
                                    elif isinstance(df_batch['EndDate'].iloc[0] if len(df_batch) > 0 else None, pd.Timestamp):
                                        df_batch['EndDate'] = df_batch['EndDate'].dt.date
                                    
                                    year_2025_after = df_batch[df_batch['EndDate'].apply(lambda x: x.year if hasattr(x, 'year') else pd.to_datetime(x).year) == 2025]
                                    
                                    if len(year_2025_after) == 0:
                                        # ⚠️ 关键问题：如果过滤后2025年数据为0，说明IfAdjusted=1条件太严格
                                        # 改为只使用IfMerged=1过滤
                                        if 'EndDate' in df_batch_before_filter.columns:
                                            if df_batch_before_filter['EndDate'].dtype == 'object':
                                                df_batch_before_filter['EndDate'] = pd.to_datetime(df_batch_before_filter['EndDate']).dt.date
                                            elif isinstance(df_batch_before_filter['EndDate'].iloc[0] if len(df_batch_before_filter) > 0 else None, pd.Timestamp):
                                                df_batch_before_filter['EndDate'] = df_batch_before_filter['EndDate'].dt.date
                                        
                                        filtered_merged_only = df_batch_before_filter[df_batch_before_filter['IfMerged'] == 1]
                                        if not filtered_merged_only.empty:
                                            year_2025_merged_only = filtered_merged_only[filtered_merged_only['EndDate'].apply(lambda x: x.year if hasattr(x, 'year') else pd.to_datetime(x).year) == 2025]
                                            if len(year_2025_merged_only) > 0:
                                                df_batch = filtered_merged_only
                                                after_filter = len(df_batch)
                            else:
                                # 如果同时使用两个条件没有数据，只使用IfMerged=1
                                filtered = df_batch[df_batch['IfMerged'] == 1]
                                if not filtered.empty:
                                    df_batch = filtered
                                    after_filter = len(df_batch)
                                else:
                                    # 如果IfMerged=1也没有数据，检查是否有IfAdjusted=1的数据
                                    filtered = df_batch[df_batch['IfAdjusted'] == 1]
                                    if not filtered.empty:
                                        df_batch = filtered
                                        after_filter = len(df_batch)
                                        logger.debug(f"批次 {batch_idx}: 发现重复数据，使用IfAdjusted=1过滤（IfMerged=1无数据），过滤前 {before_filter} 条，过滤后 {after_filter} 条")
                                    else:
                                        logger.warning(f"批次 {batch_idx}: 发现重复数据，但使用IfMerged=1和IfAdjusted=1过滤后没有数据，保留原始数据")
                        elif 'IfMerged' in df_batch.columns:
                            # 只有IfMerged字段
                            filtered = df_batch[df_batch['IfMerged'] == 1]
                            if not filtered.empty:
                                df_batch = filtered
                                after_filter = len(df_batch)
                                logger.debug(f"批次 {batch_idx}: 发现重复数据，使用IfMerged=1过滤，过滤前 {before_filter} 条，过滤后 {after_filter} 条")
                        elif 'IfAdjusted' in df_batch.columns:
                            # 只有IfAdjusted字段
                            filtered = df_batch[df_batch['IfAdjusted'] == 1]
                            if not filtered.empty:
                                df_batch = filtered
                                after_filter = len(df_batch)
                                logger.debug(f"批次 {batch_idx}: 发现重复数据，使用IfAdjusted=1过滤，过滤前 {before_filter} 条，过滤后 {after_filter} 条")
                    
                    # 移除临时字段，只返回需要的字段
                    result_cols = ['CompanyCode', 'EndDate', 'Inventories']
                    if 'InfoPublDate' in df_batch.columns:
                        result_cols.append('InfoPublDate')
                    df_batch = df_batch[result_cols]
                    
                    return df_batch if not df_batch.empty else None
                except Exception as e:
                    logger.error(f"批次 {batch_idx} 查询失败: {e}")
                    return None
            
            # 使用线程池并发执行
            with tqdm(total=total_batches, desc="获取存货数据", unit="批", ncols=100) as pbar:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # 提交所有批次任务（传递fetcher和日期参数）
                    future_to_batch = {
                        executor.submit(query_single_batch, company_codes[i:i + batch_size], i // batch_size + 1, 
                                       self.fetcher, start_date, end_date): i // batch_size
                        for i in range(0, len(company_codes), batch_size)
                    }
                    
                    # 收集结果（按顺序收集，避免乱序）
                    results_dict = {}
                    completed = 0
                    
                    for future in concurrent.futures.as_completed(future_to_batch):
                        batch_idx = future_to_batch[future]
                        try:
                            df_batch = future.result()
                            if df_batch is not None:
                                results_dict[batch_idx] = df_batch
                                completed += 1
                        except Exception as e:
                            logger.error(f"批次 {batch_idx + 1} 执行失败: {e}")
                        finally:
                            pbar.update(1)
                            pbar.set_postfix_str(f"已获取 {completed}/{total_batches} 批数据")
                    
                    # 按批次顺序合并结果
                    for batch_idx in sorted(results_dict.keys()):
                        all_data.append(results_dict[batch_idx])
            
            if not all_data:
                return pd.DataFrame()
            
            # ⚠️ 调试：检查合并前的数据情况
            logger.info(f"合并前：共有 {len(all_data)} 个批次的数据")
            if all_data:
                total_before_concat = sum(len(df) for df in all_data if df is not None)
                logger.info(f"合并前：所有批次数据总条数: {total_before_concat}")
                
                # 检查每个批次的最大日期
                batch_max_dates = []
                for idx, df in enumerate(all_data):
                    if df is not None and not df.empty and 'EndDate' in df.columns:
                        if df['EndDate'].dtype == 'object':
                            df['EndDate'] = pd.to_datetime(df['EndDate']).dt.date
                        elif isinstance(df['EndDate'].iloc[0] if len(df) > 0 else None, pd.Timestamp):
                            df['EndDate'] = df['EndDate'].dt.date
                        batch_max = df['EndDate'].max()
                        batch_max_dates.append((idx, batch_max))
                
            result_df = pd.concat(all_data, ignore_index=True)
            
            # 确保EndDate是日期类型
            if not result_df.empty and 'EndDate' in result_df.columns:
                if result_df['EndDate'].dtype == 'object':
                    result_df['EndDate'] = pd.to_datetime(result_df['EndDate']).dt.date
                elif isinstance(result_df['EndDate'].iloc[0] if len(result_df) > 0 else None, pd.Timestamp):
                    result_df['EndDate'] = result_df['EndDate'].dt.date
            
            # ⚠️ 数据验证：检查是否有同一公司同一日期有多条记录
            before_dedup = len(result_df)
            duplicates_check = result_df.duplicated(subset=['CompanyCode', 'EndDate'], keep=False)
            if duplicates_check.any():
                dup_count = duplicates_check.sum()
                logger.warning(f"⚠️  获取的存货数据中发现 {dup_count} 条重复记录（同一CompanyCode + EndDate）")
                # 输出一些重复记录的示例用于调试
                dup_records = result_df[duplicates_check].sort_values(['CompanyCode', 'EndDate'])
                logger.warning(f"   重复记录示例（前5条）:\n{dup_records[['CompanyCode', 'EndDate', 'Inventories']].head(5)}")
                
                # ⚠️ 调试：检查重复记录的InfoPublDate情况
                if 'InfoPublDate' in result_df.columns:
                    dup_records_with_date = result_df[duplicates_check].copy()
                    if not dup_records_with_date.empty:
                        # 检查InfoPublDate是否为空
                        null_info_publ = dup_records_with_date['InfoPublDate'].isna().sum()
                        logger.info(f"  重复记录中InfoPublDate为空的数量: {null_info_publ}/{len(dup_records_with_date)}")
                        # 检查InfoPublDate的日期范围
                        if null_info_publ < len(dup_records_with_date):
                            valid_info_publ = dup_records_with_date['InfoPublDate'].dropna()
                            if not valid_info_publ.empty:
                                if valid_info_publ.dtype == 'object':
                                    valid_info_publ = pd.to_datetime(valid_info_publ, errors='coerce')
                                max_info_publ = valid_info_publ.max()
                                min_info_publ = valid_info_publ.min()
                                logger.info(f"  重复记录中InfoPublDate范围: {min_info_publ} 至 {max_info_publ}")
                
                # ⚠️ 立即去重：按InfoPublDate降序排序，保留最新的记录
                if 'InfoPublDate' in result_df.columns:
                    # 将InfoPublDate转换为日期类型（如果还不是）
                    if result_df['InfoPublDate'].dtype == 'object':
                        result_df['InfoPublDate'] = pd.to_datetime(result_df['InfoPublDate'], errors='coerce')
                    
                    # 按InfoPublDate降序排序，保留最新的记录
                    result_df = result_df.sort_values(
                        ['CompanyCode', 'EndDate', 'InfoPublDate'], 
                        ascending=[True, False, False],
                        na_position='last'  # 空值排在最后
                    )
                    result_df = result_df.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                    logger.info(f"  已使用InfoPublDate去重：保留InfoPublDate最新的记录")
                else:
                    # 如果没有InfoPublDate字段，按EndDate降序保留最新的记录
                    result_df = result_df.sort_values(['CompanyCode', 'EndDate'], ascending=[True, False])
                    result_df = result_df.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                    logger.info(f"  已使用EndDate去重：保留最新的记录（InfoPublDate字段不存在）")
                
                after_dedup = len(result_df)
                logger.info(f"  去重完成：去重前 {before_dedup} 条，去重后 {after_dedup} 条，已去除 {before_dedup - after_dedup} 条重复记录")
                
                # ⚠️ 调试：检查去重后的数据日期范围
                if not result_df.empty and 'EndDate' in result_df.columns:
                    # 确保EndDate是日期类型
                    if result_df['EndDate'].dtype == 'object':
                        result_df['EndDate'] = pd.to_datetime(result_df['EndDate']).dt.date
                    elif isinstance(result_df['EndDate'].iloc[0] if len(result_df) > 0 else None, pd.Timestamp):
                        result_df['EndDate'] = result_df['EndDate'].dt.date
                    
                    dedup_max_date = result_df['EndDate'].max()
                    dedup_unique_dates = sorted(result_df['EndDate'].unique(), reverse=True)
                    logger.info(f"  去重后最新数据日期: {dedup_max_date}")
                    logger.info(f"  去重后最近5个报告期: {dedup_unique_dates[:5]}")
            else:
                # 如果没有重复，也要输出日期范围
                if not result_df.empty and 'EndDate' in result_df.columns:
                    # 确保EndDate是日期类型
                    if result_df['EndDate'].dtype == 'object':
                        result_df['EndDate'] = pd.to_datetime(result_df['EndDate']).dt.date
                    elif isinstance(result_df['EndDate'].iloc[0] if len(result_df) > 0 else None, pd.Timestamp):
                        result_df['EndDate'] = result_df['EndDate'].dt.date
                    
                    no_dup_max_date = result_df['EndDate'].max()
                    no_dup_unique_dates = sorted(result_df['EndDate'].unique(), reverse=True)
                    if not silent:
                        logger.info(f"  无重复数据，最新数据日期: {no_dup_max_date}")
                        logger.info(f"  最近5个报告期: {no_dup_unique_dates[:5]}")
                    else:
                        logger.debug(f"无重复数据，最新数据日期: {no_dup_max_date}")
            
            if not silent:
                logger.info(f"获取到 {len(result_df)} 条存货数据记录（合并报表，按季度末日期和报告类型筛选）")
            else:
                logger.debug(f"获取到 {len(result_df)} 条存货数据记录")
            
            return result_df
            
        except Exception as e:
            logger.error(f"获取存货数据失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def _split_cumulative_revenue(self, revenue_df: pd.DataFrame, silent: bool = False) -> pd.DataFrame:
        """
        拆分累加营收数据为单季度数据
        
        规则：
        - 第一季报（3月31日）：直接使用（已经是单季度数据）
        - 半年报（6月30日）：累加数据（一季度+二季度），计算二季度 = 半年报 - 一季度
        - 第三季报（9月30日）：累加数据（一季度+二季度+三季度），计算三季度 = 第三季报 - 半年报
        - 年度报告（12月31日）：累加数据（一季度+二季度+三季度+四季度），计算四季度 = 年度报告 - 第三季报
        
        参数:
        - revenue_df: 包含CompanyCode, EndDate, OperatingRevenue, InfoSource的DataFrame
        - silent: 是否静默模式（Web场景下减少日志输出）
        
        返回:
        - DataFrame，包含拆分后的单季度数据
        """
        if revenue_df.empty:
            return pd.DataFrame()
        
        # 确保EndDate是日期类型
        if revenue_df['EndDate'].dtype == 'object':
            revenue_df['EndDate'] = pd.to_datetime(revenue_df['EndDate']).dt.date
        elif isinstance(revenue_df['EndDate'].iloc[0] if len(revenue_df) > 0 else None, pd.Timestamp):
            revenue_df['EndDate'] = revenue_df['EndDate'].dt.date
        
        # 按CompanyCode和EndDate排序
        revenue_df = revenue_df.sort_values(['CompanyCode', 'EndDate']).copy()
        
        # 添加年份和月份列，便于处理
        revenue_df['Year'] = revenue_df['EndDate'].apply(lambda x: x.year if hasattr(x, 'year') else pd.to_datetime(x).year)
        revenue_df['Month'] = revenue_df['EndDate'].apply(lambda x: x.month if hasattr(x, 'month') else pd.to_datetime(x).month)
        
        result_rows = []
        
        # 按公司和年份分组处理
        for (company_code, year), company_year_data in revenue_df.groupby(['CompanyCode', 'Year']):
            company_year_data = company_year_data.sort_values('EndDate').copy()
            
            # 存储各季度的数据
            q1_data = None  # 一季度（3月31日）
            q2_data = None  # 二季度（6月30日，需要计算）
            q3_data = None  # 三季度（9月30日，需要计算）
            q4_data = None  # 四季度（12月31日，需要计算）
            
            # 存储累加数据
            h1_data = None  # 半年报（6月30日）
            q3_cumulative_data = None  # 第三季报（9月30日）
            annual_data = None  # 年度报告（12月31日）
            
            for _, row in company_year_data.iterrows():
                month = row['Month']
                end_date = row['EndDate']
                info_source = row.get('InfoSource', '')
                # ⚠️ 关键：去除InfoSource的前后空格，避免匹配失败
                if isinstance(info_source, str):
                    info_source = info_source.strip()
                operating_revenue = row['OperatingRevenue']
                
                if pd.isna(operating_revenue) or operating_revenue <= 0:
                    continue
                
                # ⚠️ 关键修复：不再依赖InfoSource字段，而是根据EndDate的月份来判断报告类型
                # 因为ROW_NUMBER()选择出来的记录，InfoSource可能是任何值
                # 我们只需要根据EndDate的月份来判断是哪个季度的数据
                
                if month == 3:
                    # 3月31日：第一季报（单季度数据）
                    q1_data = row.to_dict()
                    q1_data['Quarter'] = 1
                
                elif month == 6:
                    # 6月30日：半年报（累加数据：一季度+二季度）
                    h1_data = row.to_dict()
                
                elif month == 9:
                    # 9月30日：第三季报（累加数据：一季度+二季度+三季度）
                    q3_cumulative_data = row.to_dict()
                
                elif month == 12:
                    # 12月31日：年度报告（累加数据：一季度+二季度+三季度+四季度）
                    annual_data = row.to_dict()
            
            # 处理同一年的数据，按顺序拆分
            # 1. 第一季报：直接添加
            if q1_data:
                result_rows.append(q1_data)
            
            # 2. 半年报：计算二季度 = 半年报 - 一季度
            # ⚠️ 关键：二季度数据应该使用6月30日的EndDate，但营收是拆分后的单季度数据
            if h1_data and q1_data:
                # ⚠️ 关键：确保使用正确的数值类型进行计算
                h1_revenue = float(h1_data.get('OperatingRevenue', 0))
                q1_revenue = float(q1_data.get('OperatingRevenue', 0))
                q2_revenue = h1_revenue - q1_revenue
                
                if q2_revenue > 0:  # 确保二季度营收为正
                    q2_data = h1_data.copy()
                    q2_data['OperatingRevenue'] = q2_revenue  # ⚠️ 关键：使用计算后的单季度数据
                    # 拆分营业利润和净利润（如果存在，允许负数）
                    if 'OperatingProfit' in h1_data and 'OperatingProfit' in q1_data:
                        q2_operating_profit = float(h1_data.get('OperatingProfit', 0) or 0) - float(q1_data.get('OperatingProfit', 0) or 0)
                        q2_data['OperatingProfit'] = q2_operating_profit
                    if 'NetProfit' in h1_data and 'NetProfit' in q1_data:
                        q2_net_profit = float(h1_data.get('NetProfit', 0) or 0) - float(q1_data.get('NetProfit', 0) or 0)
                        q2_data['NetProfit'] = q2_net_profit
                    q2_data['InfoSource'] = '第二季报（拆分）'
                    q2_data['Quarter'] = 2
                    # ⚠️ 关键：确保EndDate是6月30日（二季度末）
                    q2_data['EndDate'] = date(year, 6, 30)
                    result_rows.append(q2_data)
                else:
                    if not silent:
                        logger.warning(f"公司 {company_code} {year}年：二季度营收计算为负或零（半年报={h1_revenue}, 一季度={q1_revenue}）")
                    else:
                        logger.debug(f"公司 {company_code} {year}年：二季度营收计算为负或零（半年报={h1_revenue}, 一季度={q1_revenue}）")
            elif h1_data:
                if not silent:
                    logger.warning(f"公司 {company_code} {year}年：有半年报但无第一季报，无法拆分二季度数据")
                else:
                    logger.debug(f"公司 {company_code} {year}年：有半年报但无第一季报，无法拆分二季度数据")
                # ⚠️ 关键：如果没有第一季报，半年报数据不应该被使用（因为它是累加数据）
                # 不添加h1_data到结果中
            
            # 3. 第三季报：计算三季度 = 第三季报 - 半年报
            # ⚠️ 关键：三季度数据应该使用9月30日的EndDate，但营收是拆分后的单季度数据
            if q3_cumulative_data and h1_data:
                q3_revenue = q3_cumulative_data['OperatingRevenue'] - h1_data['OperatingRevenue']
                if q3_revenue > 0:  # 确保三季度营收为正
                    q3_data = q3_cumulative_data.copy()
                    q3_data['OperatingRevenue'] = q3_revenue
                    # 拆分营业利润和净利润（如果存在，允许负数）
                    if 'OperatingProfit' in q3_cumulative_data and 'OperatingProfit' in h1_data:
                        q3_operating_profit = float(q3_cumulative_data.get('OperatingProfit', 0) or 0) - float(h1_data.get('OperatingProfit', 0) or 0)
                        q3_data['OperatingProfit'] = q3_operating_profit
                    if 'NetProfit' in q3_cumulative_data and 'NetProfit' in h1_data:
                        q3_net_profit = float(q3_cumulative_data.get('NetProfit', 0) or 0) - float(h1_data.get('NetProfit', 0) or 0)
                        q3_data['NetProfit'] = q3_net_profit
                    q3_data['InfoSource'] = '第三季报（拆分）'
                    q3_data['Quarter'] = 3
                    # ⚠️ 关键：确保EndDate是9月30日（三季度末）
                    q3_data['EndDate'] = date(year, 9, 30)
                    result_rows.append(q3_data)
                else:
                    if not silent:
                        logger.warning(f"公司 {company_code} {year}年：三季度营收计算为负或零（第三季报={q3_cumulative_data['OperatingRevenue']}, 半年报={h1_data['OperatingRevenue']}）")
                    else:
                        logger.debug(f"公司 {company_code} {year}年：三季度营收计算为负或零（第三季报={q3_cumulative_data['OperatingRevenue']}, 半年报={h1_data['OperatingRevenue']}）")
            elif q3_cumulative_data:
                if not silent:
                    logger.warning(f"公司 {company_code} {year}年：有第三季报但无半年报，无法拆分三季度数据")
                else:
                    logger.debug(f"公司 {company_code} {year}年：有第三季报但无半年报，无法拆分三季度数据")
            
            # 4. 年度报告：计算四季度 = 年度报告 - 第三季报
            # ⚠️ 关键：四季度数据应该使用12月31日的EndDate，但营收是拆分后的单季度数据
            if annual_data and q3_cumulative_data:
                q4_revenue = annual_data['OperatingRevenue'] - q3_cumulative_data['OperatingRevenue']
                if q4_revenue > 0:  # 确保四季度营收为正
                    q4_data = annual_data.copy()
                    q4_data['OperatingRevenue'] = q4_revenue
                    # 拆分营业利润和净利润（如果存在，允许负数）
                    if 'OperatingProfit' in annual_data and 'OperatingProfit' in q3_cumulative_data:
                        q4_operating_profit = float(annual_data.get('OperatingProfit', 0) or 0) - float(q3_cumulative_data.get('OperatingProfit', 0) or 0)
                        q4_data['OperatingProfit'] = q4_operating_profit
                    if 'NetProfit' in annual_data and 'NetProfit' in q3_cumulative_data:
                        q4_net_profit = float(annual_data.get('NetProfit', 0) or 0) - float(q3_cumulative_data.get('NetProfit', 0) or 0)
                        q4_data['NetProfit'] = q4_net_profit
                    q4_data['InfoSource'] = '第四季报（拆分）'
                    q4_data['Quarter'] = 4
                    # ⚠️ 关键：确保EndDate是12月31日（四季度末）
                    q4_data['EndDate'] = date(year, 12, 31)
                    result_rows.append(q4_data)
                else:
                    if not silent:
                        logger.warning(f"公司 {company_code} {year}年：四季度营收计算为负或零（年度报告={annual_data['OperatingRevenue']}, 第三季报={q3_cumulative_data['OperatingRevenue']}）")
                    else:
                        logger.debug(f"公司 {company_code} {year}年：四季度营收计算为负或零（年度报告={annual_data['OperatingRevenue']}, 第三季报={q3_cumulative_data['OperatingRevenue']}）")
            elif annual_data:
                if not silent:
                    logger.warning(f"公司 {company_code} {year}年：有年度报告但无第三季报，无法拆分四季度数据")
                else:
                    logger.debug(f"公司 {company_code} {year}年：有年度报告但无第三季报，无法拆分四季度数据")
        
        if not result_rows:
            return pd.DataFrame()
        
        # 转换为DataFrame
        result_df = pd.DataFrame(result_rows)
        
        # ⚠️ 关键验证：检查拆分后的数据，确保没有累加数据被误用
        # 检查是否有InfoSource包含"拆分"的数据，它们的OperatingRevenue应该是单季度数据
        split_data = result_df[result_df['InfoSource'].str.contains('拆分', na=False)]
        if not split_data.empty and not silent:
            logger.info(f"拆分后的数据：{len(split_data)} 条（第二季报、第三季报、第四季报）")
            # 验证：拆分后的数据不应该包含原始累加数据
            for idx, row in split_data.iterrows():
                info_source = row.get('InfoSource', '')
                operating_revenue = row.get('OperatingRevenue', 0)
                end_date = row.get('EndDate', None)
                if '第二季报（拆分）' in info_source:
                    logger.debug(f"  验证：{end_date} 第二季报（拆分）营收={operating_revenue}")
                elif '第三季报（拆分）' in info_source:
                    logger.debug(f"  验证：{end_date} 第三季报（拆分）营收={operating_revenue}")
                elif '第四季报（拆分）' in info_source:
                    logger.debug(f"  验证：{end_date} 第四季报（拆分）营收={operating_revenue}")
        
        # 移除临时列
        if 'Year' in result_df.columns:
            result_df = result_df.drop(columns=['Year'])
        if 'Month' in result_df.columns:
            result_df = result_df.drop(columns=['Month'])
        if 'Quarter' in result_df.columns:
            result_df = result_df.drop(columns=['Quarter'])
        
        if not silent:
            logger.info(f"营收数据拆分完成：原始记录 {len(revenue_df)} 条，拆分后 {len(result_df)} 条")
        else:
            logger.debug(f"营收数据拆分完成：原始记录 {len(revenue_df)} 条，拆分后 {len(result_df)} 条")
        
        # ⚠️ 关键验证：确保没有累加数据（半年报、第三季报、年度报告）被直接使用
        # 只有第一季报和拆分后的数据应该被使用
        invalid_sources = result_df[result_df['InfoSource'].isin(['半年报', '第三季报', '年度报告'])]
        if not invalid_sources.empty:
            if not silent:
                logger.error(f"❌ 错误：发现 {len(invalid_sources)} 条累加数据被直接使用（应该被拆分）！")
                logger.error(f"   这些数据应该被拆分，不应该直接出现在结果中")
                for idx, row in invalid_sources.head(10).iterrows():
                    logger.error(f"   - CompanyCode={row.get('CompanyCode', 'N/A')}, EndDate={row.get('EndDate', 'N/A')}, InfoSource={row.get('InfoSource', 'N/A')}, OperatingRevenue={row.get('OperatingRevenue', 0)}")
                logger.warning(f"   已移除 {len(invalid_sources)} 条累加数据，只保留拆分后的单季度数据")
            else:
                logger.debug(f"发现 {len(invalid_sources)} 条累加数据被直接使用，已移除")
            # ⚠️ 关键：移除这些累加数据，只保留拆分后的数据
            result_df = result_df[~result_df['InfoSource'].isin(['半年报', '第三季报', '年度报告'])]
        
        # ⚠️ 额外验证：检查拆分后的数据，确保二季度、三季度、四季度的数据确实是单季度数据
        # 方法：检查同一公司同一年的数据，确保季度数据是递增的（但不应该超过累加数据）
        if not result_df.empty and 'CompanyCode' in result_df.columns and 'EndDate' in result_df.columns:
            result_df_copy = result_df.copy()
            if result_df_copy['EndDate'].dtype == 'object':
                result_df_copy['EndDate'] = pd.to_datetime(result_df_copy['EndDate']).dt.date
            result_df_copy['Year'] = result_df_copy['EndDate'].apply(lambda x: x.year if hasattr(x, 'year') else pd.to_datetime(x).year)
            result_df_copy['Month'] = result_df_copy['EndDate'].apply(lambda x: x.month if hasattr(x, 'month') else pd.to_datetime(x).month)
            
            # 按公司和年份分组，检查数据合理性
            for (company_code, year), company_year_data in result_df_copy.groupby(['CompanyCode', 'Year']):
                company_year_data = company_year_data.sort_values('Month')
                if len(company_year_data) >= 2:
                    # 检查：如果同时有3月和6月的数据，6月的数据应该大于3月的数据（因为6月是二季度，包含了一季度+二季度）
                    q1_data = company_year_data[company_year_data['Month'] == 3]
                    q2_data = company_year_data[company_year_data['Month'] == 6]
                    if not q1_data.empty and not q2_data.empty:
                        q1_rev = float(q1_data.iloc[0]['OperatingRevenue'])
                        q2_rev = float(q2_data.iloc[0]['OperatingRevenue'])
                        # ⚠️ 关键：如果二季度数据大于等于一季度数据，这是正常的（因为二季度是单季度数据，应该小于半年报）
                        # 但如果二季度数据远大于一季度数据（比如2倍以上），可能有问题
                        if q2_rev > q1_rev * 2:
                            if not silent:
                                logger.warning(f"⚠️  数据异常：公司 {company_code} {year}年，二季度营收({q2_rev})远大于一季度营收({q1_rev})，可能是累加数据未拆分")
                            else:
                                logger.debug(f"数据异常：公司 {company_code} {year}年，二季度营收({q2_rev})远大于一季度营收({q1_rev})")
                        # 如果二季度数据小于一季度数据，也可能有问题
                        elif q2_rev < q1_rev * 0.5:
                            if not silent:
                                logger.warning(f"⚠️  数据异常：公司 {company_code} {year}年，二季度营收({q2_rev})远小于一季度营收({q1_rev})，可能拆分有误")
                            else:
                                logger.debug(f"数据异常：公司 {company_code} {year}年，二季度营收({q2_rev})远小于一季度营收({q1_rev})")
        
        return result_df
    
    def get_revenue_data(self, company_codes: List[int], years: int = 10, silent: bool = False) -> pd.DataFrame:
        """
        批量获取营业收入数据
        
        参数:
        - company_codes: 公司代码列表
        - years: 获取近多少年的数据（默认10年）
        - silent: 是否静默模式，True时不输出详细日志（默认False）
        
        返回:
        - DataFrame，包含CompanyCode, EndDate, OperatingRevenue等字段
        
        数据获取逻辑:
        - 使用LC_MainDataNew表获取营业收入数据（字段：OperatingReenue）
        - 使用EndDate字段匹配季度末日期（3月31日、6月30日、9月30日、12月31日）
        - 根据季度末日期匹配对应的报告类型（与库存数据一致）
        - 如果出现重复，使用UpdateTime排序，取最新的记录
        """
        try:
            if not company_codes:
                return pd.DataFrame()
            
            # 计算起始日期（确保包含完整年份）
            end_date = date.today()
            # 从当前日期往前推years年，确保包含所有数据
            # ⚠️ 关键：end_date应该包含到当前日期，以便获取最新的季度数据
            # 如果今天是2025年1月，应该能获取到2024年12月31日的数据
            start_date = date(end_date.year - years, 1, 1)
            if not silent:
                logger.info(f"营收数据查询日期范围: {start_date} 至 {end_date}（共{years}年）")
                logger.info(f"  当前日期: {end_date}，应该能获取到 {end_date.year - 1}年12月31日的数据（如果存在）")
            else:
                logger.debug(f"营收数据查询日期范围: {start_date} 至 {end_date}")
            
            # ⚠️ 极限优化：使用更小的批次和更多线程，最大化并发度
            # 批次大小：50（从500降低到50，产生更多批次，大幅提高并行度）
            batch_size = 50  # 从500降低到50，产生更多批次，提高并行度
            max_workers = max(MAX_WORKERS, 128)  # 至少128线程，极限压榨硬件性能
            all_data = []
            total_batches = (len(company_codes) + batch_size - 1) // batch_size
            
            if not silent:
                logger.info(f"使用多线程并发获取营收数据：批次大小={batch_size}，线程数={max_workers}，总批次数={total_batches}")
            else:
                logger.debug(f"使用多线程并发获取营收数据：批次大小={batch_size}，线程数={max_workers}，总批次数={total_batches}")
            
            # 定义单批次查询函数（使用闭包访问外部变量）
            def query_single_batch_revenue(batch_codes: List[int], batch_idx: int,
                                          fetcher: JuyuanDataFetcher, start_date: date, end_date: date) -> Optional[pd.DataFrame]:
                """查询单个批次的营收数据（使用LC_MainDataNew表，使用ROW_NUMBER()选择最优记录）"""
                try:
                    codes_str = ','.join([str(code) for code in batch_codes])
                    
                    # ⚠️ 使用ROW_NUMBER()确保每个报告期只取一条最合适的数据
                    # ⚠️ 注意：用户的SQL使用 PARTITION BY md.EndDate，但我们需要按公司和日期分区
                    sql = f"""
                    WITH RankedData AS (
                      SELECT 
                          md.CompanyCode,
                          md.InfoPublDate,
                          md.EndDate,
                          md.UpdateTime,
                          md.OperatingReenue AS OperatingRevenue,
                          md.OperatingProfit,
                          md.NetProfit,
                          md.Mark,
                          md.InfoSource,
                          ROW_NUMBER() OVER (
                            PARTITION BY md.CompanyCode, md.EndDate 
                            ORDER BY 
                              -- 优先级1：按报告期月份对应的推荐Mark值优先
                              CASE 
                                WHEN MONTH(md.EndDate) = 12 AND md.Mark = 1 THEN 1
                                WHEN MONTH(md.EndDate) = 9 AND md.Mark = 8 THEN 1
                                WHEN MONTH(md.EndDate) = 6 AND md.Mark = 7 THEN 1
                                WHEN MONTH(md.EndDate) = 3 AND md.Mark = 6 THEN 1
                                WHEN md.Mark = 1 THEN 2  -- 其次取标准合并调整标志1
                                ELSE 3
                              END,
                              -- 优先级2：更新时间最新的优先
                              md.UpdateTime DESC
                          ) AS rn
                      FROM LC_MainDataNew md 
                      WHERE md.CompanyCode IN ({codes_str})
                        AND md.EndDate >= '{start_date}'
                        AND md.EndDate <= '{end_date}'
                        AND md.OperatingReenue IS NOT NULL
                        AND md.Mark IN (1,2,4,5,6,7,8)  -- 只取有意义的Mark值
                        AND (
                            -- 只限制季度末日期，不限制InfoSource（让ROW_NUMBER()来选择最优记录）
                            (MONTH(md.EndDate) = 3 AND DAY(md.EndDate) = 31)
                            OR
                            (MONTH(md.EndDate) = 6 AND DAY(md.EndDate) = 30)
                            OR
                            (MONTH(md.EndDate) = 9 AND DAY(md.EndDate) = 30)
                            OR
                            (MONTH(md.EndDate) = 12 AND DAY(md.EndDate) = 31)
                        )
                    )
                    SELECT 
                        CompanyCode,
                        InfoPublDate,
                        EndDate,
                        UpdateTime,
                        OperatingRevenue,
                        OperatingProfit,
                        NetProfit,
                        Mark,
                        InfoSource
                    FROM RankedData
                    WHERE rn = 1
                    ORDER BY CompanyCode, EndDate
                    """
                    
                    df_batch = fetcher.query(sql)
                    
                    if df_batch.empty:
                        return None
                    
                    # 确保EndDate是日期类型
                    if 'EndDate' in df_batch.columns:
                        if df_batch['EndDate'].dtype == 'object':
                            df_batch['EndDate'] = pd.to_datetime(df_batch['EndDate']).dt.date
                        elif isinstance(df_batch['EndDate'].iloc[0] if len(df_batch) > 0 else None, pd.Timestamp):
                            df_batch['EndDate'] = df_batch['EndDate'].dt.date
                    
                    # 返回包含所有必要字段的数据（包括InfoSource，用于后续拆分累加数据）
                    return df_batch if not df_batch.empty else None
                except Exception as e:
                    logger.error(f"营收批次 {batch_idx} 查询失败: {e}")
                    return None
            
            # 使用线程池并发执行
            with tqdm(total=total_batches, desc="获取营业收入数据", unit="批", ncols=100) as pbar:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # 提交所有批次任务（传递fetcher和日期参数）
                    future_to_batch = {
                        executor.submit(query_single_batch_revenue, company_codes[i:i + batch_size], i // batch_size + 1,
                                       self.fetcher, start_date, end_date): i // batch_size
                        for i in range(0, len(company_codes), batch_size)
                    }
                    
                    # 收集结果（按顺序收集）
                    results_dict = {}
                    completed = 0
                    
                    for future in concurrent.futures.as_completed(future_to_batch):
                        batch_idx = future_to_batch[future]
                        try:
                            df_batch = future.result()
                            if df_batch is not None:
                                results_dict[batch_idx] = df_batch
                                completed += 1
                        except Exception as e:
                            logger.error(f"营收批次 {batch_idx + 1} 执行失败: {e}")
                        finally:
                            pbar.update(1)
                            pbar.set_postfix_str(f"已获取 {completed}/{total_batches} 批数据")
                    
                    # 按批次顺序合并结果
                    for batch_idx in sorted(results_dict.keys()):
                        all_data.append(results_dict[batch_idx])
            
            if not all_data:
                return pd.DataFrame()
            
            result_df = pd.concat(all_data, ignore_index=True)
            
            # ⚠️ 关键：拆分累加数据为单季度数据
            # 规则：
            # - 第一季报：直接使用（已经是单季度数据）
            # - 半年报：累加数据（一季度+二季度），需要计算二季度 = 半年报 - 一季度
            # - 第三季报：累加数据（一季度+二季度+三季度），需要计算三季度 = 第三季报 - 半年报
            # - 年度报告：累加数据（一季度+二季度+三季度+四季度），需要计算四季度 = 年度报告 - 第三季报
            result_df = self._split_cumulative_revenue(result_df, silent=silent)
            
            # ⚠️ 数据验证：检查是否有同一公司同一日期有多条记录
            before_dedup = len(result_df)
            duplicates_check = result_df.duplicated(subset=['CompanyCode', 'EndDate'], keep=False)
            if duplicates_check.any():
                dup_count = duplicates_check.sum()
                if not silent:
                    logger.warning(f"⚠️  获取的营业收入数据中发现 {dup_count} 条重复记录（同一CompanyCode + EndDate）")
                    # 输出一些重复记录的示例用于调试
                    dup_records = result_df[duplicates_check].sort_values(['CompanyCode', 'EndDate'])
                    logger.warning(f"   重复记录示例（前5条）:\n{dup_records[['CompanyCode', 'EndDate', 'OperatingRevenue', 'Mark']].head(5) if 'Mark' in dup_records.columns else dup_records[['CompanyCode', 'EndDate', 'OperatingRevenue']].head(5)}")
                else:
                    logger.debug(f"获取的营业收入数据中发现 {dup_count} 条重复记录，已处理")
            
            # ⚠️ 立即去重：按InfoPublDate降序排序，保留最新的记录
            if 'InfoPublDate' in result_df.columns:
                # 将InfoPublDate转换为日期类型（如果还不是）
                if result_df['InfoPublDate'].dtype == 'object':
                    result_df['InfoPublDate'] = pd.to_datetime(result_df['InfoPublDate'], errors='coerce')
                # 按InfoPublDate降序排序，保留最新的记录
                result_df = result_df.sort_values(
                    ['CompanyCode', 'EndDate', 'InfoPublDate'], 
                    ascending=[True, True, False],
                    na_position='last'  # 空值排在最后
                )
                result_df = result_df.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                if not silent:
                    logger.info(f"  已使用InfoPublDate去重：保留InfoPublDate最新的记录")
                else:
                    logger.debug("已使用InfoPublDate去重：保留InfoPublDate最新的记录")
            else:
                # 如果没有InfoPublDate字段，按EndDate降序保留最新的记录
                result_df = result_df.sort_values(['CompanyCode', 'EndDate'], ascending=[True, False])
                result_df = result_df.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                if not silent:
                    logger.info(f"  已使用EndDate去重：保留最新的记录（InfoPublDate字段不存在）")
                else:
                    logger.debug("已使用EndDate去重：保留最新的记录（InfoPublDate字段不存在）")
            
            after_dedup = len(result_df)
            if not silent:
                logger.info(f"  去重完成：去重前 {before_dedup} 条，去重后 {after_dedup} 条，已去除 {before_dedup - after_dedup} 条重复记录")
            else:
                logger.debug(f"去重完成：去重前 {before_dedup} 条，去重后 {after_dedup} 条")
            
            if not silent:
                logger.info(f"获取到 {len(result_df)} 条营业收入数据记录（合并报表，按季度末日期和报告类型筛选）")
            else:
                logger.debug(f"获取到 {len(result_df)} 条营业收入数据记录")
            
            return result_df
            
        except Exception as e:
            logger.error(f"获取营业收入数据失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def analyze_industry_inventory(self, years: int = 10, 
                                   industry_stocks: pd.DataFrame = None,
                                   inventory_data: pd.DataFrame = None,
                                   revenue_data: pd.DataFrame = None,
                                   silent: bool = False) -> pd.DataFrame:
        """
        分析行业库存情况（支持Web场景：可传入已过滤的数据）
        
        参数:
        - years: 分析近多少年的数据（默认10年）
        - industry_stocks: 可选的行业股票列表（如果为None，则获取全部）
        - inventory_data: 可选的库存数据（如果为None，则自动获取）
        - revenue_data: 可选的营收数据（如果为None，则自动获取）
        - silent: 是否静默模式（Web场景下减少日志输出）
        
        返回:
        - DataFrame，包含按三级行业汇总的库存数据
        """
        try:
            # 1. 获取行业股票列表（如果未提供）
            if industry_stocks is None:
                if not silent:
                    logger.info("=" * 60)
                    logger.info("步骤1: 获取行业股票列表")
                    logger.info("=" * 60)
                industry_stocks = self.get_industry_stock_list()
            
            if industry_stocks.empty:
                if not silent:
                    logger.error("未获取到任何行业股票信息")
                return pd.DataFrame()
            
            # 2. 获取所有公司代码
            company_codes = industry_stocks['CompanyCode'].unique().tolist()
            if not silent:
                logger.info(f"共 {len(company_codes)} 个公司代码")
            
            # 3. 批量获取存货数据（如果未提供）
            if inventory_data is None:
                if not silent:
                    logger.info("=" * 60)
                    logger.info("步骤2: 获取存货和营业收入数据")
                    logger.info("=" * 60)
                    logger.info("数据筛选规则:")
                    logger.info("  - 库存数据：IfMerged=1（合并报表）")
                    logger.info("  - 营收数据：LC_QIncomeStatementNew表没有IfMerged字段，不使用此条件")
                    logger.info("  - 营收字段：使用OperatingRevenue（营业收入）")
                    logger.info("  - 3月31日 -> InfoSource='第一季度'")
                    logger.info("  - 6月30日 -> InfoSource='第二季度'")
                    logger.info("  - 9月30日 -> InfoSource='第三季度'")
                    logger.info("  - 12月31日 -> InfoSource='第四季度'")
                inventory_data = self.get_inventory_data(company_codes, years=years, silent=silent)
            
            if inventory_data.empty:
                if not silent:
                    logger.error("未获取到任何存货数据")
                return pd.DataFrame()
            
            # ⚠️ 检查获取到的数据日期范围（仅在非静默模式下）
            if not silent and not inventory_data.empty and 'EndDate' in inventory_data.columns:
                max_date = inventory_data['EndDate'].max()
                min_date = inventory_data['EndDate'].min()
                unique_dates = sorted(inventory_data['EndDate'].unique(), reverse=True)
                logger.info(f"库存数据日期范围: {min_date} 至 {max_date}")
                logger.info(f"  最新数据日期: {max_date}")
                logger.info(f"  最近5个报告期: {unique_dates[:5]}")
                current_year = date.today().year
                if max_date.year < current_year:
                    missing_years = list(range(max_date.year + 1, current_year + 1))
                    logger.warning(f"⚠️  警告：最新数据只到 {max_date.year}年，可能缺少 {missing_years} 年的数据")
                else:
                    logger.info(f"✅ 数据日期正常：最新数据到 {max_date.year}年，当前年份 {current_year}年")
            
            # 3.2 批量获取营业收入数据（如果未提供）
            if revenue_data is None:
                revenue_data = self.get_revenue_data(company_codes, years=years, silent=silent)
            
            # ⚠️ 检查获取到的营收数据日期范围（仅在非静默模式下）
            if not silent and not revenue_data.empty and 'EndDate' in revenue_data.columns:
                max_date = revenue_data['EndDate'].max()
                min_date = revenue_data['EndDate'].min()
                unique_dates = sorted(revenue_data['EndDate'].unique(), reverse=True)
                logger.info(f"营收数据日期范围: {min_date} 至 {max_date}")
                logger.info(f"  最新数据日期: {max_date}")
                logger.info(f"  最近5个报告期: {unique_dates[:5]}")
                current_year = date.today().year
                if max_date.year < current_year:
                    missing_years = list(range(max_date.year + 1, current_year + 1))
                    logger.warning(f"⚠️  警告：最新数据只到 {max_date.year}年，可能缺少 {missing_years} 年的数据")
            
            # 4. 合并行业信息和存货数据
            if not silent:
                logger.info("=" * 60)
                logger.info("步骤3: 合并数据并按行业汇总")
                logger.info("=" * 60)
            
            # 将EndDate转换为日期类型
            if 'EndDate' in inventory_data.columns:
                inventory_data['EndDate'] = pd.to_datetime(inventory_data['EndDate']).dt.date
            
            # ⚠️ 关键：对库存数据按(CompanyCode, EndDate)去重，避免同一公司同一日期有重复记录
            # 去重策略：按InfoPublDate降序排序，保留最新的记录（如果InfoPublDate为空，则按EndDate降序）
            if not silent:
                logger.info("检查库存数据重复情况...")
            before_dedup = len(inventory_data)
            
            # 改进去重逻辑：按InfoPublDate排序，保留最新的记录
            if 'InfoPublDate' in inventory_data.columns:
                # 将InfoPublDate转换为日期类型（如果还不是）
                if inventory_data['InfoPublDate'].dtype == 'object':
                    inventory_data['InfoPublDate'] = pd.to_datetime(inventory_data['InfoPublDate'], errors='coerce')
                # 按InfoPublDate降序排序，保留最新的记录
                inventory_data = inventory_data.sort_values(
                    ['CompanyCode', 'EndDate', 'InfoPublDate'], 
                    ascending=[True, False, False],
                    na_position='last'  # 空值排在最后
                )
                inventory_data = inventory_data.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                if not silent:
                    logger.info("  使用InfoPublDate去重：保留InfoPublDate最新的记录")
            else:
                # 如果没有InfoPublDate字段，按EndDate降序保留最新的记录
                inventory_data = inventory_data.sort_values(['CompanyCode', 'EndDate'], ascending=[True, False])
                inventory_data = inventory_data.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                if not silent:
                    logger.info("  使用EndDate去重：保留最新的记录（InfoPublDate字段不存在）")
            
            after_dedup = len(inventory_data)
            if not silent and before_dedup != after_dedup:
                logger.warning(f"⚠️  发现重复数据：去重前 {before_dedup} 条，去重后 {after_dedup} 条，已去除 {before_dedup - after_dedup} 条重复记录")
            
            # ⚠️ 关键：库存数据是公司级别的，一个公司在某个日期只有一条库存记录
            # 但一个公司可能属于多个行业（一个CompanyCode可能对应多个ThirdIndustryName）
            # 解决方案：
            # 1. 先对库存数据按(CompanyCode, EndDate)去重，确保每个公司每个日期只有一条记录
            # 2. 然后与行业信息合并，如果一个公司属于多个行业，会产生多条记录（这是正常的，因为我们要按行业汇总）
            # 3. 在groupby时按(ThirdIndustryName, EndDate)汇总，这样每个行业的库存就是该行业所有公司的库存之和
            
            # 第一步：确保库存数据每个(CompanyCode, EndDate)只有一条记录
            if not silent:
                logger.info("检查库存数据重复情况（合并行业信息前）...")
            before_inv_dedup = len(inventory_data)
            inventory_data = inventory_data.sort_values(['CompanyCode', 'EndDate'], ascending=[True, False])
            inventory_data = inventory_data.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
            after_inv_dedup = len(inventory_data)
            if not silent and before_inv_dedup != after_inv_dedup:
                logger.info(f"  库存数据去重：去重前 {before_inv_dedup} 条，去重后 {after_inv_dedup} 条，已去除 {before_inv_dedup - after_inv_dedup} 条重复记录")
            
            # 第二步：合并行业信息（一个公司可能对应多个行业，这是正常的）
            merged_data = inventory_data.merge(
                industry_stocks[['CompanyCode', 'SecuCode', 'SecuAbbr', 'ThirdIndustryName', 
                                 'SecondIndustryName', 'FirstIndustryName']],
                on='CompanyCode',
                how='left'
            )
            
            # 过滤掉没有行业信息的记录
            merged_data = merged_data[merged_data['ThirdIndustryName'].notna()]
            
            # 第三步：检查合并后的情况（一个公司可能属于多个行业，这是正常的，会在groupby时处理）
            if not silent:
                logger.info("检查合并行业信息后的数据情况...")
                logger.info(f"  合并后记录数: {len(merged_data)}")
                # 检查是否有同一个(CompanyCode, EndDate)对应多个行业的情况
                company_date_industry = merged_data.groupby(['CompanyCode', 'EndDate'])['ThirdIndustryName'].nunique()
                multi_industry_companies = company_date_industry[company_date_industry > 1]
                if len(multi_industry_companies) > 0:
                    logger.info(f"  发现 {len(multi_industry_companies)} 个(CompanyCode, EndDate)对应多个行业（这是正常的，会在groupby时按行业分别汇总）")
                    # 输出一些示例
                    sample_companies = multi_industry_companies.head(5)
                    for (comp_code, end_date), industry_count in sample_companies.items():
                        industries = merged_data[(merged_data['CompanyCode'] == comp_code) & 
                                                (merged_data['EndDate'] == end_date)]['ThirdIndustryName'].unique()
                        logger.debug(f"    示例：CompanyCode={comp_code}, EndDate={end_date}, 行业数={industry_count}, 行业={list(industries)}")
            
            # 4.1 合并营业收入数据
            if not revenue_data.empty:
                # 将EndDate转换为日期类型
                if 'EndDate' in revenue_data.columns:
                    revenue_data['EndDate'] = pd.to_datetime(revenue_data['EndDate']).dt.date
                
                # ⚠️ 关键：检查营业收入数据的重复情况
                # 注意：SQL查询已经过滤了Mark=1，理论上不应该有重复，但为了保险还是检查
                duplicates_rev = revenue_data.duplicated(subset=['CompanyCode', 'EndDate'], keep=False)
                if duplicates_rev.any():
                    dup_count = duplicates_rev.sum()
                    if not silent:
                        logger.warning(f"⚠️  获取的营业收入数据中发现 {dup_count} 条重复记录（同一CompanyCode + EndDate）")
                        # 输出一些重复记录的示例用于调试
                        dup_records = revenue_data[duplicates_rev].sort_values(['CompanyCode', 'EndDate'])
                        logger.warning(f"   重复记录示例（前5条）:\n{dup_records[['CompanyCode', 'EndDate', 'OperatingRevenue', 'Mark']].head(5) if 'Mark' in dup_records.columns else dup_records[['CompanyCode', 'EndDate', 'OperatingRevenue']].head(5)}")
                    else:
                        logger.debug(f"获取的营业收入数据中发现 {dup_count} 条重复记录，已处理")
                
                # 对营业收入数据也按(CompanyCode, EndDate)去重
                # 注意：在批次查询时已经使用UpdateTime去重，这里再次检查确保没有遗漏
                before_rev_dedup = len(revenue_data)
                duplicates_rev_check = revenue_data.duplicated(subset=['CompanyCode', 'EndDate'], keep=False)
                if duplicates_rev_check.any():
                    # 如果仍有重复，使用InfoPublDate排序（如果存在）
                    if 'InfoPublDate' in revenue_data.columns:
                        # 将InfoPublDate转换为日期类型（如果还不是）
                        if revenue_data['InfoPublDate'].dtype == 'object':
                            revenue_data['InfoPublDate'] = pd.to_datetime(revenue_data['InfoPublDate'], errors='coerce')
                        # 按InfoPublDate降序排序，保留最新的记录
                        revenue_data = revenue_data.sort_values(
                            ['CompanyCode', 'EndDate', 'InfoPublDate'], 
                            ascending=[True, True, False],
                            na_position='last'  # 空值排在最后
                        )
                        revenue_data = revenue_data.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                        if not silent:
                            logger.info("  营收数据去重：保留InfoPublDate最新的记录")
                        else:
                            logger.debug("营收数据去重：保留InfoPublDate最新的记录")
                    else:
                        # 如果没有InfoPublDate字段，按EndDate降序保留最新的记录
                        revenue_data = revenue_data.sort_values(['CompanyCode', 'EndDate'], ascending=[True, False])
                        revenue_data = revenue_data.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                        if not silent:
                            logger.info("  营收数据去重：保留最新的记录（InfoPublDate字段不存在）")
                        else:
                            logger.debug("营收数据去重：保留最新的记录（InfoPublDate字段不存在）")
                
                after_rev_dedup = len(revenue_data)
                if before_rev_dedup != after_rev_dedup:
                    if not silent:
                        logger.warning(f"⚠️  营业收入数据去重：去重前 {before_rev_dedup} 条，去重后 {after_rev_dedup} 条")
                    else:
                        logger.debug(f"营业收入数据去重：去重前 {before_rev_dedup} 条，去重后 {after_rev_dedup} 条")
                
                # ⚠️ 关键：确保合并前两个数据集的EndDate类型完全一致，确保日期精确匹配
                # 将merged_data的EndDate也转换为date类型（如果还不是）
                if 'EndDate' in merged_data.columns:
                    if merged_data['EndDate'].dtype != 'object' or not isinstance(merged_data['EndDate'].iloc[0] if len(merged_data) > 0 else None, date):
                        merged_data['EndDate'] = pd.to_datetime(merged_data['EndDate']).dt.date
                
                # 确保revenue_data的EndDate也是date类型
                if 'EndDate' in revenue_data.columns:
                    if revenue_data['EndDate'].dtype != 'object' or not isinstance(revenue_data['EndDate'].iloc[0] if len(revenue_data) > 0 else None, date):
                        revenue_data['EndDate'] = pd.to_datetime(revenue_data['EndDate']).dt.date
                
                # 验证：检查日期范围是否一致
                if len(merged_data) > 0 and len(revenue_data) > 0:
                    inv_dates = set(merged_data['EndDate'].unique())
                    rev_dates = set(revenue_data['EndDate'].unique())
                    common_dates = inv_dates & rev_dates
                    if not silent:
                        logger.info(f"日期匹配情况：库存数据有 {len(inv_dates)} 个唯一日期，营收数据有 {len(rev_dates)} 个唯一日期，共同日期 {len(common_dates)} 个")
                    if len(common_dates) == 0:
                        if not silent:
                            logger.warning("⚠️  警告：库存数据和营收数据没有共同的日期，可能无法匹配！")
                        else:
                            logger.debug("库存数据和营收数据没有共同的日期")
                
                # 合并营业收入数据（通过CompanyCode和EndDate精确匹配）
                # 由于merged_data和revenue_data都已去重，这应该是一对一合并
                # 注意：如果revenue_data有Mark或InfoPublDate字段，也一并合并
                revenue_cols = ['CompanyCode', 'EndDate', 'OperatingRevenue']
                if 'OperatingProfit' in revenue_data.columns:
                    revenue_cols.append('OperatingProfit')
                if 'NetProfit' in revenue_data.columns:
                    revenue_cols.append('NetProfit')
                if 'Mark' in revenue_data.columns:
                    revenue_cols.append('Mark')
                if 'InfoPublDate' in revenue_data.columns:
                    revenue_cols.append('InfoPublDate')
                merged_data = merged_data.merge(
                    revenue_data[revenue_cols],
                    on=['CompanyCode', 'EndDate'],
                    how='left'
                )
                
                # ⚠️ 验证：检查合并后的匹配情况
                matched_count = merged_data['OperatingRevenue'].notna().sum()
                total_count = len(merged_data)
                match_rate = matched_count / total_count * 100 if total_count > 0 else 0
                if not silent:
                    logger.info(f"营收数据匹配情况：总记录数 {total_count}，成功匹配 {matched_count} 条，匹配率 {match_rate:.2f}%")
                else:
                    logger.debug(f"营收数据匹配情况：总记录数 {total_count}，成功匹配 {matched_count} 条，匹配率 {match_rate:.2f}%")
                
                if match_rate < 50:
                    if not silent:
                        logger.warning(f"⚠️  警告：营收数据匹配率较低（{match_rate:.2f}%），可能存在日期不匹配的问题")
                    else:
                        logger.debug(f"营收数据匹配率较低（{match_rate:.2f}%）")
                
                # ⚠️ 先重命名字段以保持一致性（统一使用Total前缀作为汇总字段名）
                merged_data = merged_data.rename(columns={'OperatingRevenue': 'TotalOperatingRevenue'})
                if 'OperatingProfit' in merged_data.columns:
                    merged_data = merged_data.rename(columns={'OperatingProfit': 'TotalOperatingProfit'})
                if 'NetProfit' in merged_data.columns:
                    merged_data = merged_data.rename(columns={'NetProfit': 'TotalNetProfit'})
                
                # ⚠️ 关键问题：合并营收数据后，同一个(CompanyCode, EndDate)可能对应多个ThirdIndustryName
                # 这是因为一个公司可能属于多个行业，但营收数据是公司级别的，不应该重复
                # 解决方案：在合并营收数据时，需要确保每个(CompanyCode, EndDate)只有一条营收记录
                # 但由于一个公司可能属于多个行业，我们需要在合并前先处理营收数据，确保每个(CompanyCode, EndDate)只有一条
                
                # 检查合并后是否有重复的(CompanyCode, EndDate)组合
                duplicates_after_merge = merged_data.duplicated(subset=['CompanyCode', 'EndDate'], keep=False)
                if duplicates_after_merge.any():
                    dup_count = duplicates_after_merge.sum()
                    if not silent:
                        logger.warning(f"⚠️  合并营收数据后，同一个(CompanyCode, EndDate)对应多个行业（共 {dup_count} 条重复记录）")
                        logger.warning("   这是正常的：一个公司可能属于多个行业，营收数据会被分配到各个行业")
                        logger.warning("   在groupby时，每个行业的营收会分别汇总，不会重复计算")
                    # 输出一些重复的记录用于调试（仅在debug级别）
                    dup_records = merged_data[duplicates_after_merge][['CompanyCode', 'EndDate', 'SecuCode', 'ThirdIndustryName', 'TotalOperatingRevenue']].head(10)
                    logger.debug(f"   重复记录示例（同一公司同一日期在不同行业）:\n{dup_records}")
                    
                    # ⚠️ 注意：这里不去重，因为一个公司属于多个行业是正常的
                    # 在groupby时，会按(ThirdIndustryName, EndDate)汇总，每个行业的营收会分别计算
                
                if not silent:
                    logger.info(f"已合并营业收入数据，匹配记录数: {merged_data['TotalOperatingRevenue'].notna().sum()}")
                else:
                    logger.debug(f"已合并营业收入数据，匹配记录数: {merged_data['TotalOperatingRevenue'].notna().sum()}")
            else:
                merged_data['TotalOperatingRevenue'] = None
                if not silent:
                    logger.warning("未获取到营业收入数据")
                else:
                    logger.debug("未获取到营业收入数据")
            
            # 5. 按三级行业和报告期汇总
            logger.info("按三级行业和报告期汇总库存和营收数据...")
            
            # ⚠️ 再次检查：确保在groupby前没有重复的(CompanyCode, EndDate)组合
            # 如果有，说明前面的去重逻辑有问题
            final_check = merged_data.duplicated(subset=['CompanyCode', 'EndDate'], keep=False)
            if final_check.any():
                dup_count = final_check.sum()
                logger.error(f"❌ groupby前仍有 {dup_count} 条重复记录（CompanyCode + EndDate）！")
                # 强制去重：优先按InfoPublDate排序，保留最新的记录；如果没有InfoPublDate，则按SecuCode排序
                if 'InfoPublDate' in merged_data.columns:
                    # 将InfoPublDate转换为日期类型（如果还不是）
                    if merged_data['InfoPublDate'].dtype == 'object':
                        merged_data['InfoPublDate'] = pd.to_datetime(merged_data['InfoPublDate'], errors='coerce')
                    merged_data = merged_data.sort_values(
                        ['CompanyCode', 'EndDate', 'InfoPublDate', 'SecuCode'], 
                        ascending=[True, True, False, True],
                        na_position='last'  # 空值排在最后
                    )
                    merged_data = merged_data.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                    logger.warning("   已强制去重：按InfoPublDate降序排序后保留第一条记录")
                else:
                    # 如果没有InfoPublDate字段，按SecuCode排序后保留第一个
                    merged_data = merged_data.sort_values(['CompanyCode', 'EndDate', 'SecuCode'], ascending=[True, True, True])
                    merged_data = merged_data.drop_duplicates(subset=['CompanyCode', 'EndDate'], keep='first')
                    logger.warning("   已强制去重：按SecuCode排序后保留第一条记录（InfoPublDate字段不存在）")
            
            # 定义聚合函数：收集股票代码列表
            def collect_stock_codes(series):
                """收集股票代码列表，去重并排序"""
                codes = series.dropna().unique().tolist()
                codes = sorted([str(code) for code in codes])
                return ','.join(codes)
            
            # ⚠️ 注意：这里使用'count'统计记录数，应该等于去重后的公司数量
            # 如果发现记录数异常多，说明可能有重复
            # 构建聚合字典
            agg_dict = {
                'Inventories': ['sum', 'mean', 'count'],  # count应该等于该行业该日期的公司数量
                'TotalOperatingRevenue': 'sum',  # 营业收入总额
                'CompanyCode': 'nunique',  # 统计公司数量（这才是正确的公司数）
                'SecuCode': ['nunique', collect_stock_codes]  # 统计股票数量和收集股票代码列表
            }
            # 添加营业利润和净利润（如果存在）
            if 'TotalOperatingProfit' in merged_data.columns:
                agg_dict['TotalOperatingProfit'] = 'sum'
            if 'TotalNetProfit' in merged_data.columns:
                agg_dict['TotalNetProfit'] = 'sum'
            
            industry_summary = merged_data.groupby(['ThirdIndustryName', 'EndDate']).agg(agg_dict).reset_index()
            
            # 展平MultiIndex列名并映射到中文列名
            # groupby().agg() 返回的列名可能是 MultiIndex，需要正确展平
            # ⚠️ 注意：reset_index() 后，分组键列（ThirdIndustryName, EndDate）是普通列，不是 MultiIndex
            if isinstance(industry_summary.columns, pd.MultiIndex):
                # 展平MultiIndex：将 ('列名', '聚合函数') 转换为 '列名_聚合函数'
                # 但分组键列可能是 MultiIndex 的第一层，需要特殊处理
                new_columns = []
                for col_tuple in industry_summary.columns:
                    if isinstance(col_tuple, tuple):
                        if len(col_tuple) == 1:
                            # 单个元素，可能是分组键
                            new_columns.append(str(col_tuple[0]))
                        elif len(col_tuple) == 2:
                            # ('列名', '聚合函数') 或 ('列名', '<function>')
                            new_columns.append('_'.join(str(col) for col in col_tuple).strip())
                        else:
                            new_columns.append('_'.join(str(col) for col in col_tuple).strip())
                    else:
                        new_columns.append(str(col_tuple))
                industry_summary.columns = new_columns
            else:
                # 如果不是 MultiIndex，检查是否有列名带有下划线后缀（说明之前的处理有问题）
                # 先输出当前列名以便调试
                logger.debug(f"当前列名（非MultiIndex）: {list(industry_summary.columns)}")
            
            # ⚠️ 调试：输出当前列名以便排查问题
            logger.debug(f"展平后的列名: {list(industry_summary.columns)}")
            
            # 构建列名映射字典
            column_mapping = {}
            
            # 映射分组键列（这些列不会被聚合，所以列名应该是原始的 'ThirdIndustryName' 和 'EndDate'）
            # 但如果之前处理有问题，可能是 'ThirdIndustryName_' 和 'EndDate_'
            if 'ThirdIndustryName' in industry_summary.columns:
                column_mapping['ThirdIndustryName'] = '三级行业'
            elif 'ThirdIndustryName_' in industry_summary.columns:
                column_mapping['ThirdIndustryName_'] = '三级行业'
            else:
                logger.warning(f"警告：未找到 'ThirdIndustryName' 或 'ThirdIndustryName_' 列，当前列名: {list(industry_summary.columns)}")
            
            if 'EndDate' in industry_summary.columns:
                column_mapping['EndDate'] = '报告期'
            elif 'EndDate_' in industry_summary.columns:
                column_mapping['EndDate_'] = '报告期'
            
            # 映射聚合列
            if 'Inventories_sum' in industry_summary.columns:
                column_mapping['Inventories_sum'] = '库存总额'
            if 'Inventories_mean' in industry_summary.columns:
                column_mapping['Inventories_mean'] = '平均库存'
            if 'Inventories_count' in industry_summary.columns:
                column_mapping['Inventories_count'] = '记录数'
            if 'TotalOperatingRevenue_sum' in industry_summary.columns:
                column_mapping['TotalOperatingRevenue_sum'] = '营业收入总额'
            if 'CompanyCode_nunique' in industry_summary.columns:
                column_mapping['CompanyCode_nunique'] = '公司数量'
            if 'SecuCode_nunique' in industry_summary.columns:
                column_mapping['SecuCode_nunique'] = '股票数量'
            
            # 添加营业利润和净利润（如果存在）
            if 'TotalOperatingProfit_sum' in industry_summary.columns:
                column_mapping['TotalOperatingProfit_sum'] = '营业利润总额'
            if 'TotalNetProfit_sum' in industry_summary.columns:
                column_mapping['TotalNetProfit_sum'] = '净利润总额'
            
            # 处理股票代码列表列（通常是SecuCode的collect_stock_codes函数对应的列）
            # 查找所有SecuCode相关的列
            secu_cols = [col for col in industry_summary.columns if 'SecuCode' in str(col)]
            for col in secu_cols:
                if 'nunique' not in str(col) and col not in column_mapping and col not in column_mapping.values():
                    # 这应该是股票代码列表列（collect_stock_codes函数对应的列）
                    # 列名可能是 'SecuCode_<function collect_stock_codes>' 或类似的格式
                    column_mapping[col] = '股票代码列表'
                    break
            
            # ⚠️ 检查是否所有必要的列都已映射（处理带下划线的列名）
            third_ind_mapped = any('ThirdIndustryName' in str(k) for k in column_mapping.keys())
            if not third_ind_mapped and '三级行业' not in industry_summary.columns:
                logger.error(f"❌ 无法映射 '三级行业' 列！")
                logger.error(f"   当前所有列名: {list(industry_summary.columns)}")
                logger.error(f"   映射字典: {column_mapping}")
                # 尝试查找任何包含 ThirdIndustryName 的列
                third_ind_cols = [col for col in industry_summary.columns if 'ThirdIndustryName' in str(col)]
                if third_ind_cols:
                    logger.warning(f"   发现包含 'ThirdIndustryName' 的列: {third_ind_cols}")
                    column_mapping[third_ind_cols[0]] = '三级行业'
                else:
                    raise ValueError("无法找到或映射 '三级行业' 列，请检查数据结构和列名映射逻辑")
            
            # 应用列名映射
            industry_summary = industry_summary.rename(columns=column_mapping)
            
            # ⚠️ 验证关键列是否存在
            if '三级行业' not in industry_summary.columns:
                logger.error(f"❌ 映射后仍然缺少 '三级行业' 列！")
                logger.error(f"   映射后的列名: {list(industry_summary.columns)}")
                raise ValueError("映射后仍然缺少 '三级行业' 列")
            
            # ⚠️ 检查关键列是否存在
            if '三级行业' not in industry_summary.columns:
                logger.error(f"❌ 映射后仍然缺少 '三级行业' 列！")
                logger.error(f"   映射后的列名: {list(industry_summary.columns)}")
                logger.error(f"   映射字典: {column_mapping}")
                # 尝试从原始列名中查找
                if 'ThirdIndustryName' in industry_summary.columns:
                    logger.warning("   发现 'ThirdIndustryName' 列未被映射，手动映射...")
                    industry_summary = industry_summary.rename(columns={'ThirdIndustryName': '三级行业'})
                else:
                    raise ValueError("映射后仍然缺少 '三级行业' 列，且找不到原始列名 'ThirdIndustryName'")
            
            if '报告期' not in industry_summary.columns and 'EndDate' in industry_summary.columns:
                industry_summary = industry_summary.rename(columns={'EndDate': '报告期'})
            
            # 重新排列列顺序
            desired_order = ['三级行业', '报告期', '库存总额', '营业收入总额']
            if '营业利润总额' in industry_summary.columns:
                desired_order.append('营业利润总额')
            if '净利润总额' in industry_summary.columns:
                desired_order.append('净利润总额')
            desired_order.extend(['库存营收比', '平均库存', '记录数', '公司数量', '股票数量', '股票代码列表'])
            # 只保留存在的列
            desired_order = [col for col in desired_order if col in industry_summary.columns]
            # 添加其他未列出的列（如增长率等，这些会在后面计算）
            other_cols = [col for col in industry_summary.columns if col not in desired_order]
            industry_summary = industry_summary[desired_order + other_cols]
            
            # ⚠️ 最终验证：检查是否有关键列
            if len(industry_summary) == 0:
                logger.warning("⚠️  警告：汇总后的数据为空，可能是没有匹配的数据")
                return pd.DataFrame()
            
            if '三级行业' not in industry_summary.columns or '报告期' not in industry_summary.columns:
                logger.error(f"❌ 最终验证失败：缺少关键列")
                logger.error(f"   当前列名: {list(industry_summary.columns)}")
                raise ValueError("缺少关键列 '三级行业' 或 '报告期'")
            
            # ⚠️ 数据验证：检查记录数和公司数量是否匹配
            # 如果记录数远大于公司数量，说明可能有重复
            # 确保数据类型一致（转换为数值类型）
            if '记录数' in industry_summary.columns and '公司数量' in industry_summary.columns:
                try:
                    record_vs_company = pd.to_numeric(industry_summary['记录数'], errors='coerce') - pd.to_numeric(industry_summary['公司数量'], errors='coerce')
                    if (record_vs_company > 0).any():
                        suspicious_rows = industry_summary[record_vs_company > 0]
                        logger.warning(f"⚠️  发现异常：有 {len(suspicious_rows)} 条记录的记录数大于公司数量")
                        logger.warning("   这可能表示汇总时仍有重复数据")
                        # 输出一些异常记录用于调试
                        if len(suspicious_rows) > 0:
                            logger.warning(f"   异常记录示例:\n{suspicious_rows[['三级行业', '报告期', '记录数', '公司数量']].head(5)}")
                except Exception as e:
                    logger.debug(f"检查记录数和公司数量时出错: {e}")
            
            # 计算库存与营收的剪刀差
            # 方法1：库存/营收比率（库存周转相关）
            # ⚠️ 避免除零错误：当营业收入总额为0或NULL时，库存营收比为NULL
            industry_summary['库存营收比'] = industry_summary.apply(
                lambda row: row['库存总额'] / row['营业收入总额'] 
                if pd.notna(row['营业收入总额']) and row['营业收入总额'] != 0 
                else np.nan, 
                axis=1
            )
            
            # 按行业和日期排序
            industry_summary = industry_summary.sort_values(['三级行业', '报告期'])
            
            # 计算每个行业的库存增长率和营收增长率的剪刀差（同比）
            if not silent:
                logger.info("计算库存与营收剪刀差（同比增长率差异）...")
            industry_summary['库存增长率(%)'] = None
            industry_summary['营收增长率(%)'] = None
            industry_summary['库存营收剪刀差(pp)'] = None  # 百分点差异
            
            for industry in industry_summary['三级行业'].unique():
                industry_data = industry_summary[industry_summary['三级行业'] == industry].copy()
                industry_data = industry_data.sort_values('报告期')
                
                # 计算同比增长率
                for idx, row in industry_data.iterrows():
                    current_date = row['报告期']
                    
                    # 找去年同期（一年前）
                    if hasattr(current_date, 'year'):
                        prev_year = current_date.year - 1
                        prev_date = date(prev_year, current_date.month, current_date.day) if hasattr(current_date, 'month') else None
                    else:
                        current_date_obj = pd.to_datetime(current_date).date() if isinstance(current_date, str) else current_date
                        prev_year = current_date_obj.year - 1
                        prev_date = date(prev_year, current_date_obj.month, current_date_obj.day)
                    
                    # 查找去年同期数据
                    prev_data = industry_data[industry_data['报告期'] == prev_date]
                    
                    if not prev_data.empty:
                        prev_row = prev_data.iloc[0]
                        
                        # 计算库存增长率
                        if pd.notna(row['库存总额']) and pd.notna(prev_row['库存总额']) and prev_row['库存总额'] > 0:
                            inv_growth = (row['库存总额'] - prev_row['库存总额']) / prev_row['库存总额'] * 100
                            industry_summary.loc[idx, '库存增长率(%)'] = inv_growth
                        
                        # 计算营收增长率
                        if pd.notna(row['营业收入总额']) and pd.notna(prev_row['营业收入总额']) and prev_row['营业收入总额'] > 0:
                            rev_growth = (row['营业收入总额'] - prev_row['营业收入总额']) / prev_row['营业收入总额'] * 100
                            industry_summary.loc[idx, '营收增长率(%)'] = rev_growth
                        
                        # 计算剪刀差（库存增长率 - 营收增长率，单位：百分点）
                        if pd.notna(industry_summary.loc[idx, '库存增长率(%)']) and pd.notna(industry_summary.loc[idx, '营收增长率(%)']):
                            scissors_diff = industry_summary.loc[idx, '库存增长率(%)'] - industry_summary.loc[idx, '营收增长率(%)']
                            industry_summary.loc[idx, '库存营收剪刀差(pp)'] = scissors_diff
            
            # 调整列顺序
            column_order = ['三级行业', '报告期', '库存总额', '营业收入总额']
            if '营业利润总额' in industry_summary.columns:
                column_order.append('营业利润总额')
            if '净利润总额' in industry_summary.columns:
                column_order.append('净利润总额')
            column_order.extend(['库存营收比', '库存增长率(%)', '营收增长率(%)', '库存营收剪刀差(pp)',
                          '平均库存', '记录数', '股票数量', '股票代码列表'])
            industry_summary = industry_summary[[col for col in column_order if col in industry_summary.columns]]
            
            if not silent:
                logger.info(f"汇总完成，共 {len(industry_summary)} 条记录，涉及 {industry_summary['三级行业'].nunique()} 个三级行业")
            
            return industry_summary
            
        except Exception as e:
            logger.error(f"分析行业库存失败: {e}", exc_info=True)
            return pd.DataFrame()
    
    def _plot_single_industry(self, industry: str, industry_data: pd.DataFrame, output_dir: str) -> Optional[str]:
        """
        绘制单个行业的趋势图（内部方法，用于多线程并行）
        
        参数:
        - industry: 行业名称
        - industry_data: 该行业的数据
        - output_dir: 输出目录
        
        返回:
        - 图片文件路径，如果失败则返回None
        """
        # ⚠️ 关键：在每个线程中创建独立的figure，避免线程冲突
        fig = None
        try:
            industry_data = industry_data.sort_values('报告期')
            
            if len(industry_data) < 2:
                logger.debug(f"行业 {industry} 数据点不足，跳过绘图")
                return None
            
            # ⚠️ 关键：创建新的figure，确保每个线程使用独立的图形对象
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), dpi=100)
            fig.suptitle(f'{industry} - 库存趋势分析', fontsize=16, fontweight='bold')
            
            # 转换日期为datetime用于绘图
            dates = pd.to_datetime(industry_data['报告期'])
            
            # 检查数据是否有效
            if industry_data['库存总额'].isna().all():
                logger.warning(f"行业 {industry} 的库存总额数据全部为空，跳过绘图")
                if fig is not None:
                    plt.close(fig)
                return None
            
            # 子图1: 库存总额和营业收入总额趋势（使用双Y轴）
            valid_mask = industry_data['库存总额'].notna()
            if valid_mask.sum() == 0:
                logger.warning(f"行业 {industry} 没有有效的库存数据，跳过绘图")
                if fig is not None:
                    plt.close(fig)
                return None
            
            # 库存总额（左轴）
            line_inv = ax1.plot(dates[valid_mask], industry_data.loc[valid_mask, '库存总额'] / 1e8, 
                               marker='o', color='blue', linewidth=2, markersize=6, label='库存总额（亿元）')
            ax1.set_xlabel('报告期', fontsize=10)
            ax1.set_ylabel('库存总额（亿元）', fontsize=10, color='blue')
            ax1.tick_params(axis='y', labelcolor='blue')
            ax1.grid(True, alpha=0.3)
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax1.xaxis.set_major_locator(mdates.YearLocator())
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
            # 营业收入总额（右轴）
            ax1_twin = ax1.twinx()
            lines_rev = []
            if '营业收入总额' in industry_data.columns:
                rev_valid_mask = industry_data['营业收入总额'].notna()
                if rev_valid_mask.sum() > 0:
                    try:
                        lines_rev = ax1_twin.plot(dates[rev_valid_mask], industry_data.loc[rev_valid_mask, '营业收入总额'] / 1e8, 
                                                 marker='s', color='green', linewidth=2, markersize=6, label='营业收入总额（亿元）')
                        ax1_twin.set_ylabel('营业收入总额（亿元）', fontsize=10, color='green')
                        ax1_twin.tick_params(axis='y', labelcolor='green')
                    except Exception as e:
                        logger.debug(f"行业 {industry} 绘制营业收入总额失败: {e}")
            
            # 合并图例
            lines_legend = list(line_inv)
            if lines_rev:
                lines_legend.extend(lines_rev)
            if lines_legend:
                labels = [l.get_label() for l in lines_legend]
                ax1.legend(lines_legend, labels, loc='upper left')
            
            # 设置标题
            ax1.set_title('库存总额与营业收入总额趋势（亿元）', fontsize=12, fontweight='bold')
            
            # 添加数值标签（只标注库存总额，避免图表过于拥挤）
            valid_dates = dates[valid_mask]
            valid_values = industry_data.loc[valid_mask, '库存总额'] / 1e8
            for i, (date, value) in enumerate(zip(valid_dates, valid_values)):
                if i % max(1, len(valid_dates) // 10) == 0:  # 只标注部分点，避免拥挤
                    ax1.annotate(f'{value:.2f}', (date, value), 
                               textcoords="offset points", xytext=(0,10), ha='center', fontsize=8)
            
            # 子图2: 营业利润和净利润（使用双Y轴，因为数量级可能不同）
            ax2_twin = ax2.twinx()
            
            # 营业利润（左轴）
            line_profit = []
            if '营业利润总额' in industry_data.columns:
                profit_valid_mask = industry_data['营业利润总额'].notna()
                if profit_valid_mask.sum() > 0:
                    try:
                        line_profit = ax2.plot(dates[profit_valid_mask], industry_data.loc[profit_valid_mask, '营业利润总额'] / 1e8, 
                                             marker='^', color='orange', linewidth=2, markersize=6, label='营业利润总额（亿元）')
                        ax2.set_ylabel('营业利润总额（亿元）', fontsize=10, color='orange')
                        ax2.tick_params(axis='y', labelcolor='orange')
                    except Exception as e:
                        logger.debug(f"行业 {industry} 绘制营业利润总额失败: {e}")
                        line_profit = []
            
            # 净利润（右轴）
            line_net = []
            if '净利润总额' in industry_data.columns:
                net_valid_mask = industry_data['净利润总额'].notna()
                if net_valid_mask.sum() > 0:
                    try:
                        line_net = ax2_twin.plot(dates[net_valid_mask], industry_data.loc[net_valid_mask, '净利润总额'] / 1e8, 
                                               marker='d', color='purple', linewidth=2, markersize=6, label='净利润总额（亿元）')
                        ax2_twin.set_ylabel('净利润总额（亿元）', fontsize=10, color='purple')
                        ax2_twin.tick_params(axis='y', labelcolor='purple')
                    except Exception as e:
                        logger.debug(f"行业 {industry} 绘制净利润总额失败: {e}")
                        line_net = []
            
            ax2.set_xlabel('报告期', fontsize=10)
            ax2.grid(True, alpha=0.3)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax2.xaxis.set_major_locator(mdates.YearLocator())
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
            # 合并图例（只显示有数据的图例）
            lines = []
            if line_profit:
                lines.extend(line_profit)
            if line_net:
                lines.extend(line_net)
            if lines:
                labels = [l.get_label() for l in lines]
                ax2.legend(lines, labels, loc='upper left')
            
            # 设置标题
            title_parts = []
            if line_profit:
                title_parts.append('营业利润总额')
            if line_net:
                title_parts.append('净利润总额')
            if title_parts:
                ax2.set_title('与'.join(title_parts) + '趋势（亿元）', fontsize=12, fontweight='bold')
            else:
                ax2.set_title('利润趋势（亿元）', fontsize=12, fontweight='bold')
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax2.xaxis.set_major_locator(mdates.YearLocator())
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
            
            plt.tight_layout()
            
            # 保存图片
            safe_industry_name = industry.replace('/', '_').replace('\\', '_').replace(':', '_')
            image_file = os.path.join(output_dir, f'行业库存趋势_{safe_industry_name}.png')
            
            # ⚠️ 关键：使用figure对象保存，而不是plt.savefig，确保线程安全
            try:
                fig.savefig(image_file, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
                # 验证文件是否成功保存（检查文件大小）
                if os.path.exists(image_file):
                    file_size = os.path.getsize(image_file)
                    if file_size < 10240:  # 小于10KB可能有问题
                        logger.warning(f"行业 {industry} 生成的图片文件过小（{file_size} 字节），可能生成失败")
                else:
                    logger.error(f"行业 {industry} 图片文件未生成")
                    return None
            except Exception as save_error:
                logger.error(f"行业 {industry} 保存图片失败: {save_error}")
                return None
            finally:
                # ⚠️ 关键：确保关闭figure，释放资源
                if fig is not None:
                    plt.close(fig)
            
            return image_file
            
        except Exception as e:
            # 获取详细的错误信息
            import traceback
            try:
                error_msg = str(e) if e else repr(e)
                if not error_msg or error_msg == repr(e):
                    # 如果错误消息为空或只是对象表示，尝试获取更详细的信息
                    error_msg = f"{type(e).__name__}: {str(e) if str(e) else '未知错误'}"
            except:
                error_msg = repr(e)
            
            full_traceback = traceback.format_exc()
            logger.error(f"绘制行业 {industry} 趋势图失败: {error_msg}")
            logger.error(f"绘制行业 {industry} 趋势图失败详情:\n{full_traceback}")
            
            # 确保关闭图形，避免资源泄漏
            try:
                if fig is not None:
                    plt.close(fig)
                plt.close('all')
            except:
                pass
            
            return None
    
    def plot_industry_trends(self, industry_summary: pd.DataFrame, output_dir: str = "outputs") -> List[str]:
        """
        绘制行业库存趋势图（多线程并行版本）
        
        参数:
        - industry_summary: 行业汇总数据
        - output_dir: 输出目录
        
        返回:
        - 图片文件路径列表
        """
        try:
            if industry_summary.empty:
                logger.warning("没有数据可绘制")
                return []
            
            # 创建输出目录
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # 获取所有三级行业
            industries = industry_summary['三级行业'].unique()
            total_industries = len(industries)
            logger.info(f"开始绘制 {total_industries} 个行业的趋势图（多线程并行）...")
            
            # ⚠️ 优化：使用多线程并行绘制，但限制线程数避免资源竞争
            # matplotlib在多线程环境下可能不稳定，使用较少的线程数
            max_workers = min(max(MAX_WORKERS, 8), 32)  # 限制在8-32线程之间，避免过多线程导致资源竞争
            logger.info(f"使用 {max_workers} 个线程并行绘制趋势图（限制线程数以确保图片生成稳定性）")
            
            image_files = []
            completed = 0
            
            # 使用线程池并发执行
            with tqdm(total=total_industries, desc="绘制趋势图", unit="行业", ncols=100) as pbar:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # 提交所有任务
                    future_to_industry = {
                        executor.submit(
                            self._plot_single_industry, 
                            industry, 
                            industry_summary[industry_summary['三级行业'] == industry].copy(),
                            output_dir
                        ): industry
                        for industry in industries
                    }
                    
                    # 收集结果
                    for future in concurrent.futures.as_completed(future_to_industry):
                        industry = future_to_industry[future]
                        try:
                            image_file = future.result()
                            if image_file:
                                image_files.append(image_file)
                                completed += 1
                        except Exception as e:
                            # 获取详细的错误信息
                            import traceback
                            error_msg = str(e) if str(e) else repr(e)
                            logger.error(f"绘制行业 {industry} 趋势图时出错: {error_msg}")
                            logger.debug(f"绘制行业 {industry} 趋势图错误详情:\n{traceback.format_exc()}")
                        finally:
                            pbar.update(1)
                            pbar.set_postfix_str(f"已完成 {completed}/{total_industries} 张图片")
            
            logger.info(f"趋势图绘制完成，共生成 {len(image_files)} 张图片")
            return image_files
            
        except Exception as e:
            logger.error(f"绘制趋势图失败: {e}", exc_info=True)
            return []


def run_industry_inventory_analysis(years: int = 10, output_dir: str = "outputs") -> Optional[str]:
    """
    运行行业库存分析
    
    参数:
    - years: 分析近多少年的数据（默认5年）
    - output_dir: 输出目录
    
    返回:
    - 输出文件路径，如果失败则返回None
    """
    from logger_config import init_logger
    
    session_logger = init_logger("logs")
    
    print("=" * 60)
    print("行业库存分析（功能18）")
    print("=" * 60)
    print(f"分析年限: 近{years}年")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        # 初始化数据获取器
        print("正在初始化数据获取器...")
        fetcher = JuyuanDataFetcher(use_connection_pool=True, lazy_init_pool=True)
        print("✅ 数据获取器初始化完成")
        
        # 创建分析器
        analyzer = IndustryInventoryAnalyzer(fetcher)
        
        # 执行分析
        print("\n开始分析...")
        industry_summary = analyzer.analyze_industry_inventory(years=years)
        
        if industry_summary.empty:
            print("\n❌ 未获取到任何数据")
            return None
        
        # 绘制趋势图
        print("\n开始绘制趋势图...")
        image_files = analyzer.plot_industry_trends(industry_summary, output_dir=output_dir)
        
        # 保存汇总数据到Excel
        print("\n保存分析结果...")
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(output_dir, f'行业库存分析_{years}年_{timestamp}.xlsx')
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 调整列顺序：优先显示关键指标（库存、营收、利润、剪刀差）
            column_order = [
                '三级行业', '报告期', 
                '库存总额', '营业收入总额'
            ]
            if '营业利润总额' in industry_summary.columns:
                column_order.append('营业利润总额')
            if '净利润总额' in industry_summary.columns:
                column_order.append('净利润总额')
            column_order.extend([
                '库存营收比',  # 基础指标
                '库存增长率(%)', '营收增长率(%)', '库存营收剪刀差(pp)',  # 剪刀差指标
                '平均库存', '公司数量', '股票数量', '股票代码列表', '记录数'  # 辅助信息
            ])
            # 只保留实际存在的列
            available_columns = [col for col in column_order if col in industry_summary.columns]
            # 添加其他可能存在的列（如果还有遗漏的）
            other_columns = [col for col in industry_summary.columns if col not in available_columns]
            final_column_order = available_columns + other_columns
            
            # 保存汇总数据（按调整后的列顺序）
            industry_summary[final_column_order].to_excel(writer, sheet_name='行业库存汇总', index=False)
            
            # 保存按行业分组的详细数据
            industries = industry_summary['三级行业'].unique()
            for industry in industries:
                industry_data = industry_summary[industry_summary['三级行业'] == industry].copy()
                # 按调整后的列顺序
                industry_data = industry_data[final_column_order]
                safe_industry_name = industry.replace('/', '_').replace('\\', '_').replace(':', '_')[:30]  # 限制长度
                industry_data.to_excel(writer, sheet_name=safe_industry_name, index=False)
        
        elapsed_time = time.time() - start_time
        
        print(f"\n✅ 分析完成!")
        print(f"  耗时: {elapsed_time:.1f}秒")
        print(f"  输出文件: {output_file}")
        print(f"  趋势图数量: {len(image_files)} 张")
        print(f"  涉及行业数: {industry_summary['三级行业'].nunique()} 个")
        
        logger.info(f"行业库存分析完成，输出文件: {output_file}")
        
        return output_file
        
    except Exception as e:
        error_msg = f"行业库存分析失败: {e}"
        print(f"\n❌ {error_msg}")
        logger.error(error_msg, exc_info=True)
        import traceback
        traceback.print_exc()
        return None
    
    finally:
        # 关闭数据获取器
        if 'fetcher' in locals():
            try:
                fetcher.close()
                print("数据获取器已关闭")
            except Exception as e:
                print(f"关闭数据获取器时出错: {e}")


if __name__ == '__main__':
    try:
        run_industry_inventory_analysis()
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        print(f"\n❌ 程序执行失败: {e}")
