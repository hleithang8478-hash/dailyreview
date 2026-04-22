#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
期货相关性分析增量缓存管理器

功能：
1. 支持期货数据的增量缓存
2. 支持股票数据的增量缓存
3. 智能合并历史缓存和新增数据
4. 按日期范围管理缓存
5. 自动清理过期缓存

作者：AI Assistant
创建时间：2024年12月
"""

import os
import pickle
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Set, Any
import logging

logger = logging.getLogger(__name__)

class FuturesIncrementalCacheManager:
    """期货相关性分析增量缓存管理器"""
    
    def __init__(self, cache_dir: str = "cache/futures_correlation"):
        """
        初始化增量缓存管理器
        
        Args:
            cache_dir: 缓存目录
        """
        self.cache_dir = cache_dir
        self.futures_cache_dir = os.path.join(cache_dir, "futures")
        self.stocks_cache_dir = os.path.join(cache_dir, "stocks")
        self.fundamental_cache_dir = os.path.join(cache_dir, "fundamental")
        self.minute_cache_dir = os.path.join(cache_dir, "minute")
        
        # 创建缓存目录
        os.makedirs(self.futures_cache_dir, exist_ok=True)
        os.makedirs(self.stocks_cache_dir, exist_ok=True)
        os.makedirs(self.fundamental_cache_dir, exist_ok=True)
        os.makedirs(self.minute_cache_dir, exist_ok=True)
        
        # 缓存配置
        self.cache_expiry_days = 30  # 缓存过期天数（期货数据相对稳定）
        self.fundamental_cache_expiry_days = 7  # 基本面数据缓存过期天数（7天，因为基本面数据更新频率较低）
        self.minute_cache_expiry_days = 3  # 分钟级数据缓存过期天数（3天，因为分钟级数据量大且更新频繁）
        self.max_cache_size_mb = 2000  # 最大缓存大小（MB）
        
    def _generate_date_range_key(self, data_type: str, identifier: str, 
                                start_date: date, end_date: date) -> str:
        """
        生成基于日期范围的缓存键
        
        Args:
            data_type: 数据类型（futures/stocks）
            identifier: 标识符（期货代码或股票代码）
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            str: 缓存键
        """
        date_range = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        return f"{data_type}_{identifier}_{date_range}"
    
    def _get_cache_file_path(self, cache_type: str, cache_key: str) -> str:
        """
        获取缓存文件路径
        
        Args:
            cache_type: 缓存类型（futures/stocks）
            cache_key: 缓存键
            
        Returns:
            str: 缓存文件路径
        """
        if cache_type == "futures":
            return os.path.join(self.futures_cache_dir, f"{cache_key}.pkl")
        elif cache_type == "stocks":
            return os.path.join(self.stocks_cache_dir, f"{cache_key}.pkl")
        else:
            raise ValueError(f"不支持的缓存类型: {cache_type}")
    
    def _is_cache_valid(self, cache_file_path: str) -> bool:
        """
        检查缓存是否有效
        
        Args:
            cache_file_path: 缓存文件路径
            
        Returns:
            bool: 缓存是否有效
        """
        if not os.path.exists(cache_file_path):
            return False
        
        # 检查文件修改时间
        file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file_path))
        if datetime.now() - file_mtime > timedelta(days=self.cache_expiry_days):
            return False
        
        # 检查缓存数据完整性
        try:
            with open(cache_file_path, 'rb') as f:
                cache_data = pickle.load(f)
            
            # 检查缓存元数据
            if 'metadata' not in cache_data or 'data' not in cache_data:
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"缓存文件损坏: {cache_file_path}, 错误: {e}")
            return False
    
    def _find_overlapping_caches(self, cache_type: str, identifier: str, 
                                target_start: date, target_end: date) -> List[Tuple[str, Dict]]:
        """
        查找与目标日期范围重叠的缓存
        
        Args:
            cache_type: 缓存类型
            identifier: 标识符
            target_start: 目标开始日期
            target_end: 目标结束日期
            
        Returns:
            List[Tuple[str, Dict]]: 重叠的缓存文件路径和元数据
        """
        overlapping_caches = []
        cache_dir = self.futures_cache_dir if cache_type == "futures" else self.stocks_cache_dir
        
        if not os.path.exists(cache_dir):
            return overlapping_caches
        
        # 遍历缓存文件
        for filename in os.listdir(cache_dir):
            if not filename.endswith('.pkl'):
                continue
            
            # 解析文件名获取标识符和日期范围
            try:
                parts = filename.replace('.pkl', '').split('_')
                if len(parts) < 4:
                    continue
                
                file_identifier = parts[1]  # 假设格式为 type_identifier_startdate_enddate
                if file_identifier != identifier:
                    continue
                
                # 直接从文件名解析日期范围（避免打开文件）
                # 文件名格式：stocks_000001_20220516_20251016.pkl
                # parts[2] = '20220516', parts[3] = '20251016'
                try:
                    file_start_str = parts[2]  # YYYYMMDD格式
                    file_end_str = parts[3]    # YYYYMMDD格式
                    file_start_date = datetime.strptime(file_start_str, '%Y%m%d').date()
                    file_end_date = datetime.strptime(file_end_str, '%Y%m%d').date()
                except (ValueError, IndexError) as e:
                    logger.warning(f"从文件名解析日期失败: {filename}, 错误: {e}")
                    continue
                
                # 先检查文件名中的日期范围是否有重叠（快速过滤）
                # 放宽条件：只要缓存数据的结束日期 >= 目标开始日期，就认为有重叠
                # 这样可以处理end_date每天变化的情况（比如今天执行end_date是2025-12-23，明天执行是2025-12-24）
                # 只要缓存数据覆盖了目标开始日期，就可以使用（即使end_date不同）
                if file_end_date < target_start:
                    continue  # 缓存数据太旧，没有重叠
                # 如果缓存数据的开始日期 > 目标结束日期 + 30天，也跳过（缓存数据太新，可能是错误的）
                if file_start_date > target_end + timedelta(days=30):
                    continue  # 缓存数据太新，可能有问题
                
                cache_file_path = os.path.join(cache_dir, filename)
                if not self._is_cache_valid(cache_file_path):
                    continue
                
                # 只有通过快速检查的缓存文件才加载元数据（性能优化）
                try:
                    with open(cache_file_path, 'rb') as f:
                        cache_data = pickle.load(f)
                    
                    metadata = cache_data.get('metadata', {})
                    # 使用元数据中的日期（更准确，因为可能包含合并后的实际日期范围）
                    cache_start = metadata.get('start_date')
                    cache_end = metadata.get('end_date')
                    
                    if cache_start and cache_end:
                        cache_start = datetime.strptime(cache_start, '%Y-%m-%d').date()
                        cache_end = datetime.strptime(cache_end, '%Y-%m-%d').date()
                    else:
                        # 如果元数据中没有日期，使用文件名中的日期
                        cache_start = file_start_date
                        cache_end = file_end_date
                    
                    # 再次检查是否有重叠（使用元数据中的日期，更准确）
                    # 放宽条件：只要缓存数据的结束日期 >= 目标开始日期，就认为有重叠
                    # 这样可以处理end_date每天变化的情况
                    if cache_end >= target_start:
                        overlapping_caches.append((cache_file_path, metadata))
                except Exception as e:
                    logger.warning(f"加载缓存元数据失败: {cache_file_path}, 错误: {e}")
                    # 如果加载元数据失败，使用文件名中的日期范围
                    # 放宽条件：只要缓存数据的结束日期 >= 目标开始日期，就认为有重叠
                    if file_end_date >= target_start:
                        overlapping_caches.append((cache_file_path, {
                            'start_date': file_start_date.strftime('%Y-%m-%d'),
                            'end_date': file_end_date.strftime('%Y-%m-%d')
                        }))
                        
            except Exception as e:
                logger.warning(f"解析缓存文件失败: {filename}, 错误: {e}")
                continue
        
        return overlapping_caches
    
    def _merge_data(self, existing_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
        """
        合并历史数据和新增数据
        
        Args:
            existing_data: 历史数据
            new_data: 新增数据
            
        Returns:
            pd.DataFrame: 合并后的数据
        """
        if existing_data.empty:
            return new_data
        if new_data.empty:
            return existing_data
        
        # 合并数据，新数据优先（避免重复）
        combined_data = pd.concat([existing_data, new_data])
        
        # 按日期排序并去重（保留最新的数据）
        combined_data = combined_data.sort_index()
        combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
        
        return combined_data
    
    def save_futures_data(self, option_code: str, futures_data: pd.DataFrame, 
                         futures_info: Dict, start_date: date, end_date: date) -> bool:
        """
        保存期货数据到增量缓存
        
        Args:
            option_code: 期货代码
            futures_data: 期货数据
            futures_info: 期货信息
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            bool: 是否保存成功
        """
        try:
            # 查找重叠的缓存
            overlapping_caches = self._find_overlapping_caches(
                "futures", option_code, start_date, end_date
            )
            
            # 合并重叠的缓存数据
            merged_data = futures_data.copy()
            merged_info = futures_info.copy()
            
            for cache_path, cache_metadata in overlapping_caches:
                try:
                    with open(cache_path, 'rb') as f:
                        cache_data = pickle.load(f)
                    
                    cached_data = cache_data.get('data')
                    if cached_data is not None and not cached_data.empty:
                        merged_data = self._merge_data(merged_data, cached_data)
                        logger.info(f"合并历史缓存数据: {cache_path}")
                        
                except Exception as e:
                    logger.warning(f"合并缓存数据失败: {cache_path}, 错误: {e}")
            
            # 生成新的缓存键（基于合并后的日期范围）
            actual_start = merged_data.index.min().date() if not merged_data.empty else start_date
            actual_end = merged_data.index.max().date() if not merged_data.empty else end_date
            
            cache_key = self._generate_date_range_key("futures", option_code, actual_start, actual_end)
            cache_file_path = self._get_cache_file_path("futures", cache_key)
            
            # 准备缓存数据
            cache_data = {
                'metadata': {
                    'option_code': option_code,
                    'start_date': actual_start.strftime('%Y-%m-%d'),
                    'end_date': actual_end.strftime('%Y-%m-%d'),
                    'created_at': datetime.now(),
                    'data_length': len(merged_data),
                    'original_request': {
                        'start_date': start_date.strftime('%Y-%m-%d'),
                        'end_date': end_date.strftime('%Y-%m-%d')
                    }
                },
                'data': merged_data,
                'info': merged_info
            }
            
            # 保存缓存
            with open(cache_file_path, 'wb') as f:
                pickle.dump(cache_data, f)
            
            # 删除重叠的旧缓存文件
            for old_cache_path, _ in overlapping_caches:
                if old_cache_path != cache_file_path:
                    try:
                        os.remove(old_cache_path)
                        logger.info(f"删除旧缓存文件: {old_cache_path}")
                    except Exception as e:
                        logger.warning(f"删除旧缓存文件失败: {old_cache_path}, 错误: {e}")
            
            logger.info(f"期货数据增量缓存已保存: {option_code}, {actual_start} 到 {actual_end}, 文件: {cache_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"保存期货数据增量缓存失败: {e}")
            return False
    
    def load_futures_data(self, option_code: str, start_date: date, end_date: date) -> Tuple[Optional[pd.DataFrame], Optional[Dict]]:
        """
        从增量缓存加载期货数据
        
        Args:
            option_code: 期货代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Tuple: (期货数据, 期货信息) 或 (None, None)
        """
        try:
            # 查找重叠的缓存
            overlapping_caches = self._find_overlapping_caches(
                "futures", option_code, start_date, end_date
            )
            
            if not overlapping_caches:
                return None, None
            
            # 合并所有重叠的缓存数据
            merged_data = pd.DataFrame()
            merged_info = {}
            
            for cache_path, cache_metadata in overlapping_caches:
                try:
                    with open(cache_path, 'rb') as f:
                        cache_data = pickle.load(f)
                    
                    cached_data = cache_data.get('data')
                    cached_info = cache_data.get('info', {})
                    
                    if cached_data is not None and not cached_data.empty:
                        merged_data = self._merge_data(merged_data, cached_data)
                        merged_info.update(cached_info)
                        
                except Exception as e:
                    logger.warning(f"加载缓存数据失败: {cache_path}, 错误: {e}")
                    continue
            
            if merged_data.empty:
                return None, None
            
            # 筛选目标日期范围的数据
            target_data = merged_data[
                (merged_data.index.date >= start_date) & 
                (merged_data.index.date <= end_date)
            ]
            
            if target_data.empty:
                return None, None
            
            logger.info(f"期货数据增量缓存已加载: {option_code}, {start_date} 到 {end_date}, 数据量: {len(target_data)}")
            return target_data, merged_info
            
        except Exception as e:
            logger.error(f"加载期货数据增量缓存失败: {e}")
            return None, None
    
    def save_stocks_data(self, stock_codes: List[str], stock_data_dict: Dict[str, pd.DataFrame], 
                        start_date: date, end_date: date) -> bool:
        """
        保存股票数据到增量缓存
        
        Args:
            stock_codes: 股票代码列表
            stock_data_dict: 股票数据字典
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            bool: 是否保存成功
        """
        try:
            # 为每个股票代码保存单独的缓存
            success_count = 0
            
            for stock_code in stock_codes:
                if stock_code not in stock_data_dict:
                    continue
                
                stock_data = stock_data_dict[stock_code]
                if stock_data.empty:
                    continue
                
                # 查找重叠的缓存
                overlapping_caches = self._find_overlapping_caches(
                    "stocks", stock_code, start_date, end_date
                )
                
                # 合并重叠的缓存数据
                merged_data = stock_data.copy()
                
                for cache_path, cache_metadata in overlapping_caches:
                    try:
                        with open(cache_path, 'rb') as f:
                            cache_data = pickle.load(f)
                        
                        cached_data = cache_data.get('data')
                        if cached_data is not None and not cached_data.empty:
                            merged_data = self._merge_data(merged_data, cached_data)
                            
                    except Exception as e:
                        logger.warning(f"合并股票缓存数据失败: {cache_path}, 错误: {e}")
                
                # 生成新的缓存键
                actual_start = merged_data.index.min().date() if not merged_data.empty else start_date
                actual_end = merged_data.index.max().date() if not merged_data.empty else end_date
                
                cache_key = self._generate_date_range_key("stocks", stock_code, actual_start, actual_end)
                cache_file_path = self._get_cache_file_path("stocks", cache_key)
                
                # 准备缓存数据
                cache_data = {
                    'metadata': {
                        'stock_code': stock_code,
                        'start_date': actual_start.strftime('%Y-%m-%d'),
                        'end_date': actual_end.strftime('%Y-%m-%d'),
                        'created_at': datetime.now(),
                        'data_length': len(merged_data),
                        'original_request': {
                            'start_date': start_date.strftime('%Y-%m-%d'),
                            'end_date': end_date.strftime('%Y-%m-%d')
                        }
                    },
                    'data': merged_data
                }
                
                # 保存缓存（增强错误处理和文件权限检查）
                try:
                    # 确保缓存目录存在
                    cache_dir = os.path.dirname(cache_file_path)
                    if not os.path.exists(cache_dir):
                        os.makedirs(cache_dir, exist_ok=True)
                    
                    # 检查目录写入权限
                    if not os.access(cache_dir, os.W_OK):
                        logger.error(f"缓存目录无写入权限: {cache_dir}")
                        continue
                    
                    # 如果文件已存在，检查是否有写入权限
                    if os.path.exists(cache_file_path):
                        if not os.access(cache_file_path, os.W_OK):
                            logger.error(f"缓存文件无写入权限: {cache_file_path}")
                            continue
                    
                    # 使用临时文件写入，然后原子性重命名（避免写入过程中文件损坏）
                    temp_file_path = cache_file_path + '.tmp'
                    try:
                        with open(temp_file_path, 'wb') as f:
                            pickle.dump(cache_data, f)
                            # 确保数据已写入磁盘（必须在 with 块内部调用）
                            f.flush()
                            os.fsync(f.fileno())
                        
                        # 原子性重命名（Windows和Linux都支持）
                        if os.path.exists(cache_file_path):
                            os.remove(cache_file_path)
                        os.rename(temp_file_path, cache_file_path)
                        
                        # 验证文件是否成功保存
                        if os.path.exists(cache_file_path) and os.path.getsize(cache_file_path) > 0:
                            logger.info(f"股票缓存已保存: {stock_code}, 文件: {cache_file_path}, 数据范围: {actual_start} 至 {actual_end}, 数据行数: {len(merged_data)}")
                        else:
                            logger.error(f"缓存文件保存后验证失败: {cache_file_path}")
                            continue
                            
                    except Exception as e:
                        # 清理临时文件
                        if os.path.exists(temp_file_path):
                            try:
                                os.remove(temp_file_path)
                            except:
                                pass
                        raise e
                        
                except PermissionError as e:
                    logger.error(f"保存股票缓存文件权限错误: {stock_code}, 文件: {cache_file_path}, 错误: {e}")
                    continue
                except OSError as e:
                    logger.error(f"保存股票缓存文件系统错误: {stock_code}, 文件: {cache_file_path}, 错误: {e}")
                    continue
                except Exception as e:
                    logger.error(f"保存股票缓存文件失败: {stock_code}, 文件: {cache_file_path}, 错误: {e}", exc_info=True)
                    continue
                
                # 删除重叠的旧缓存文件
                for old_cache_path, _ in overlapping_caches:
                    if old_cache_path != cache_file_path:
                        try:
                            os.remove(old_cache_path)
                            logger.debug(f"删除旧股票缓存文件: {old_cache_path}")
                        except Exception as e:
                            logger.warning(f"删除旧股票缓存文件失败: {old_cache_path}, 错误: {e}")
                
                success_count += 1
            
            logger.info(f"股票数据增量缓存已保存: {success_count}/{len(stock_codes)} 只股票")
            if success_count < len(stock_codes):
                logger.warning(f"部分股票缓存保存失败: {len(stock_codes) - success_count} 只股票未保存")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"保存股票数据增量缓存失败: {e}")
            return False
    
    def load_stocks_data(self, stock_codes: List[str], start_date: date, end_date: date) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
        """
        从增量缓存加载股票数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Tuple: (股票数据字典, 缺失的股票代码列表)
        """
        try:
            cached_data = {}
            missing_codes = []
            
            for stock_code in stock_codes:
                # 查找重叠的缓存
                overlapping_caches = self._find_overlapping_caches(
                    "stocks", stock_code, start_date, end_date
                )
                
                if not overlapping_caches:
                    logger.debug(f"股票 {stock_code} 未找到重叠缓存，请求范围: {start_date} 至 {end_date}")
                    missing_codes.append(stock_code)
                    continue
                
                logger.debug(f"股票 {stock_code} 找到 {len(overlapping_caches)} 个重叠缓存")
                
                # 合并所有重叠的缓存数据
                merged_data = pd.DataFrame()
                
                for cache_path, cache_metadata in overlapping_caches:
                    try:
                        with open(cache_path, 'rb') as f:
                            cache_data = pickle.load(f)
                        
                        cached_stock_data = cache_data.get('data')
                        if cached_stock_data is not None and not cached_stock_data.empty:
                            merged_data = self._merge_data(merged_data, cached_stock_data)
                            
                    except Exception as e:
                        logger.warning(f"加载股票缓存数据失败: {cache_path}, 错误: {e}")
                        continue
                
                if merged_data.empty:
                    missing_codes.append(stock_code)
                    continue
                
                # 返回所有找到的缓存数据，让上层函数统一判断是否需要增量补全
                # 这样可以让统一的缓存验证函数（cache_validator）来处理所有验证逻辑
                if not merged_data.empty:
                    cached_data[stock_code] = merged_data
                else:
                    missing_codes.append(stock_code)
            
            logger.info(f"股票数据增量缓存已加载: {len(cached_data)}/{len(stock_codes)} 只股票")
            return cached_data, missing_codes
            
        except Exception as e:
            logger.error(f"加载股票数据增量缓存失败: {e}")
            return {}, stock_codes
    
    def clear_expired_cache(self) -> int:
        """
        清理过期缓存
        
        Returns:
            int: 清理的缓存文件数量
        """
        try:
            cleared_count = 0
            
            # 清理期货和股票数据缓存（30天过期）
            for cache_dir in [self.futures_cache_dir, self.stocks_cache_dir]:
                if not os.path.exists(cache_dir):
                    continue
                
                for filename in os.listdir(cache_dir):
                    if not filename.endswith('.pkl'):
                        continue
                    
                    cache_file_path = os.path.join(cache_dir, filename)
                    
                    # 检查缓存是否过期
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file_path))
                    if datetime.now() - file_mtime > timedelta(days=self.cache_expiry_days):
                        try:
                            os.remove(cache_file_path)
                            cleared_count += 1
                            logger.info(f"清理过期缓存: {cache_file_path}")
                        except Exception as e:
                            logger.warning(f"清理过期缓存失败: {cache_file_path}, 错误: {e}")
            
            # 清理基本面数据缓存（7天过期）
            if os.path.exists(self.fundamental_cache_dir):
                for filename in os.listdir(self.fundamental_cache_dir):
                    if not filename.endswith('.pkl'):
                        continue
                    
                    cache_file_path = os.path.join(self.fundamental_cache_dir, filename)
                    
                    # 检查缓存是否过期（基本面数据7天过期）
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file_path))
                    if datetime.now() - file_mtime > timedelta(days=self.fundamental_cache_expiry_days):
                        try:
                            os.remove(cache_file_path)
                            cleared_count += 1
                            logger.info(f"清理过期基本面数据缓存: {cache_file_path}")
                        except Exception as e:
                            logger.warning(f"清理过期基本面数据缓存失败: {cache_file_path}, 错误: {e}")
            
            logger.info(f"缓存清理完成，共清理 {cleared_count} 个过期文件")
            return cleared_count
            
        except Exception as e:
            logger.error(f"清理过期缓存失败: {e}")
            return 0
    
    def save_fundamental_data(self, fundamental_data: Dict[str, Dict[str, Any]]) -> bool:
        """
        保存基本面数据到缓存
        
        Args:
            fundamental_data: 基本面数据字典 {code: {pb_mrq, pe_ttm, roe_ttm, ...}}
            
        Returns:
            bool: 是否保存成功
        """
        try:
            if not fundamental_data:
                return False
            
            # 使用当前日期作为缓存键的一部分（基本面数据按日期缓存）
            cache_date = datetime.now().date()
            cache_key = f"fundamental_{cache_date.strftime('%Y%m%d')}"
            cache_file_path = os.path.join(self.fundamental_cache_dir, f"{cache_key}.pkl")
            
            # 尝试加载现有缓存（如果存在），合并数据
            existing_data = {}
            if os.path.exists(cache_file_path):
                try:
                    with open(cache_file_path, 'rb') as f:
                        cache_data = pickle.load(f)
                        existing_data = cache_data.get('data', {})
                        logger.info(f"加载现有基本面数据缓存: {len(existing_data)} 只股票")
                except Exception as e:
                    logger.warning(f"加载现有基本面数据缓存失败: {e}")
            
            # 统一格式化股票代码为6位数字（确保保存和加载时格式一致）
            normalized_fundamental_data = {}
            for code, data in fundamental_data.items():
                # 统一格式化为6位数字字符串
                normalized_code = str(code).strip().zfill(6)
                # 如果代码包含非数字字符（如.SZ），提取数字部分
                if not normalized_code.isdigit():
                    # 提取前6位数字
                    digits = ''.join([c for c in normalized_code if c.isdigit()])[:6]
                    normalized_code = digits.zfill(6) if digits else normalized_code
                normalized_fundamental_data[normalized_code] = data
            
            # 合并新数据和现有数据（新数据优先）
            merged_data = {**existing_data, **normalized_fundamental_data}
            
            # 保存数据
            cache_data = {
                'data': merged_data,
                'date': cache_date,
                'timestamp': datetime.now()
            }
            
            with open(cache_file_path, 'wb') as f:
                pickle.dump(cache_data, f)
            
            logger.info(f"基本面数据已保存到缓存: {len(merged_data)} 只股票（新增: {len(fundamental_data)} 只）")
            return True
            
        except Exception as e:
            logger.error(f"保存基本面数据缓存失败: {e}")
            return False
    
    def load_fundamental_data(self, codes: List[str]) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
        """
        从缓存加载基本面数据
        
        Args:
            codes: 股票代码列表
            
        Returns:
            Tuple[Dict, List]: (缓存数据字典, 缺失的股票代码列表)
        """
        try:
            cached_data = {}
            missing_codes = []
            
            # 查找最新的基本面数据缓存（最近7天内的）
            cache_files = []
            if os.path.exists(self.fundamental_cache_dir):
                for filename in os.listdir(self.fundamental_cache_dir):
                    if filename.startswith('fundamental_') and filename.endswith('.pkl'):
                        cache_file_path = os.path.join(self.fundamental_cache_dir, filename)
                        
                        # 检查缓存是否有效（7天内）
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file_path))
                        if datetime.now() - file_mtime <= timedelta(days=self.fundamental_cache_expiry_days):
                            cache_files.append((cache_file_path, file_mtime))
            
            # 使用最新的缓存文件
            if cache_files:
                # 按修改时间排序，使用最新的
                cache_files.sort(key=lambda x: x[1], reverse=True)
                latest_cache_path = cache_files[0][0]
                
                try:
                    with open(latest_cache_path, 'rb') as f:
                        cache_data = pickle.load(f)
                    
                    cached_fundamental_data = cache_data.get('data', {})
                    cache_date = cache_data.get('date')
                    
                    logger.info(f"从缓存加载基本面数据: {latest_cache_path}, 缓存日期: {cache_date}, 共 {len(cached_fundamental_data)} 只股票")
                    
                    # 检查哪些股票有缓存
                    for code in codes:
                        code_str = str(code).zfill(6)
                        if code_str in cached_fundamental_data:
                            cached_data[code_str] = cached_fundamental_data[code_str]
                        else:
                            missing_codes.append(code_str)
                    
                    logger.info(f"基本面数据缓存命中: {len(cached_data)}/{len(codes)} 只股票")
                    
                except Exception as e:
                    logger.warning(f"加载基本面数据缓存失败: {latest_cache_path}, 错误: {e}")
                    missing_codes = [str(code).zfill(6) for code in codes]
            else:
                # 没有缓存，所有股票都需要重新获取
                missing_codes = [str(code).zfill(6) for code in codes]
                logger.info(f"未找到有效的基本面数据缓存，需要重新获取 {len(missing_codes)} 只股票的数据")
            
            return cached_data, missing_codes
            
        except Exception as e:
            logger.error(f"加载基本面数据缓存失败: {e}")
            return {}, codes
    
    def save_minute_data(self, code: str, minute_data: pd.DataFrame, 
                        start_time: str, end_time: str) -> bool:
        """
        保存分钟级数据到缓存
        
        Args:
            code: 股票代码
            minute_data: 分钟级数据DataFrame
            start_time: 开始时间（格式: '2025.02.14 09:30:00'）
            end_time: 结束时间（格式: '2025.02.14 15:00:00'）
            
        Returns:
            bool: 是否保存成功
        """
        try:
            if minute_data is None or minute_data.empty:
                return False
            
            # 生成缓存键：minute_{code}_{start_time}_{end_time}
            # 简化时间格式用于文件名（去除特殊字符）
            start_key = start_time.replace(' ', '_').replace(':', '').replace('.', '')
            end_key = end_time.replace(' ', '_').replace(':', '').replace('.', '')
            code_str = str(code).strip().zfill(6)
            cache_key = f"minute_{code_str}_{start_key}_{end_key}"
            cache_file_path = os.path.join(self.minute_cache_dir, f"{cache_key}.pkl")
            
            # 保存数据
            cache_data = {
                'code': code_str,
                'data': minute_data,
                'start_time': start_time,
                'end_time': end_time,
                'timestamp': datetime.now(),
                'data_length': len(minute_data)
            }
            
            with open(cache_file_path, 'wb') as f:
                pickle.dump(cache_data, f)
            
            logger.debug(f"分钟级数据已保存到缓存: {code_str}, {len(minute_data)} 条记录")
            return True
            
        except Exception as e:
            logger.warning(f"保存分钟级数据缓存失败 {code}: {e}")
            return False
    
    def load_minute_data(self, code: str, start_time: str, end_time: str) -> Optional[pd.DataFrame]:
        """
        从缓存加载分钟级数据
        
        Args:
            code: 股票代码
            start_time: 开始时间（格式: '2025.02.14 09:30:00'）
            end_time: 结束时间（格式: '2025.02.14 15:00:00'）
            
        Returns:
            Optional[pd.DataFrame]: 缓存数据，如果未命中则返回None
        """
        try:
            code_str = str(code).strip().zfill(6)
            
            # 查找匹配的缓存文件
            if not os.path.exists(self.minute_cache_dir):
                return None
            
            # 生成缓存键用于匹配
            start_key = start_time.replace(' ', '_').replace(':', '').replace('.', '')
            end_key = end_time.replace(' ', '_').replace(':', '').replace('.', '')
            cache_key_prefix = f"minute_{code_str}_{start_key}_{end_key}"
            
            # 查找精确匹配的缓存文件
            cache_file_path = os.path.join(self.minute_cache_dir, f"{cache_key_prefix}.pkl")
            
            if os.path.exists(cache_file_path):
                # 检查缓存是否有效（3天内）
                file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file_path))
                if datetime.now() - file_mtime <= timedelta(days=self.minute_cache_expiry_days):
                    try:
                        with open(cache_file_path, 'rb') as f:
                            cache_data = pickle.load(f)
                        
                        cached_data = cache_data.get('data')
                        cached_start = cache_data.get('start_time')
                        cached_end = cache_data.get('end_time')
                        
                        # 检查时间范围是否匹配（转换为Timestamp进行比较）
                        if cached_data is not None and not cached_data.empty:
                            try:
                                cached_start_ts = pd.Timestamp(cached_start)
                                cached_end_ts = pd.Timestamp(cached_end)
                                start_ts = pd.Timestamp(start_time)
                                end_ts = pd.Timestamp(end_time)
                                
                                # 如果缓存的时间范围覆盖了请求的范围，返回缓存数据
                                if cached_start_ts <= start_ts and cached_end_ts >= end_ts:
                                    logger.debug(f"分钟级数据缓存命中: {code_str}, {len(cached_data)} 条记录")
                                    # 过滤到请求的时间范围
                                    if 'TradeTime' in cached_data.columns:
                                        filtered_data = cached_data[
                                            (cached_data['TradeTime'] >= start_ts) &
                                            (cached_data['TradeTime'] <= end_ts)
                                        ]
                                        if not filtered_data.empty:
                                            return filtered_data
                                    return cached_data
                            except Exception as e:
                                logger.debug(f"时间范围比较失败: {e}")
                                # 如果时间转换失败，直接返回缓存数据（保守策略）
                                return cached_data
                    except Exception as e:
                        logger.warning(f"加载分钟级数据缓存失败 {code_str}: {e}")
            
            # 如果没有精确匹配，尝试查找部分匹配（缓存范围包含请求范围）
            for filename in os.listdir(self.minute_cache_dir):
                if filename.startswith(f"minute_{code_str}_") and filename.endswith('.pkl'):
                    cache_file_path = os.path.join(self.minute_cache_dir, filename)
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file_path))
                    if datetime.now() - file_mtime <= timedelta(days=self.minute_cache_expiry_days):
                        try:
                            with open(cache_file_path, 'rb') as f:
                                cache_data = pickle.load(f)
                            
                            cached_data = cache_data.get('data')
                            cached_start = cache_data.get('start_time')
                            cached_end = cache_data.get('end_time')
                            
                            # 检查缓存范围是否覆盖请求范围（转换为Timestamp进行比较）
                            if cached_data is not None and not cached_data.empty:
                                try:
                                    cached_start_ts = pd.Timestamp(cached_start)
                                    cached_end_ts = pd.Timestamp(cached_end)
                                    start_ts = pd.Timestamp(start_time)
                                    end_ts = pd.Timestamp(end_time)
                                    
                                    if cached_start_ts <= start_ts and cached_end_ts >= end_ts:
                                        logger.debug(f"分钟级数据缓存部分命中: {code_str}, 缓存范围: {cached_start} - {cached_end}")
                                        # 过滤到请求的时间范围
                                        if 'TradeTime' in cached_data.columns:
                                            filtered_data = cached_data[
                                                (cached_data['TradeTime'] >= start_ts) &
                                                (cached_data['TradeTime'] <= end_ts)
                                            ]
                                            if not filtered_data.empty:
                                                return filtered_data
                                except Exception:
                                    # 如果时间转换失败，跳过这个缓存文件
                                    continue
                        except Exception:
                            continue
            
            return None
            
        except Exception as e:
            logger.warning(f"加载分钟级数据缓存失败 {code}: {e}")
            return None

# 创建全局实例
futures_incremental_cache_manager = FuturesIncrementalCacheManager()
