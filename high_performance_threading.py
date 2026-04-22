#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能多线程模块
统一管理所有多线程操作，提供优化的线程池和批处理功能
"""

import concurrent.futures
import threading
import time
import logging
from typing import List, Callable, Any, Dict, Optional, Union, Tuple
from tqdm import tqdm
import multiprocessing
import os
from functools import wraps
import sys

# 获取logger
logger = logging.getLogger(__name__)

# 导入优化配置
from config import MAX_WORKERS, BATCH_SIZE, MAX_CONCURRENT_BATCHES, DB_POOL_SIZE

class HighPerformanceThreadPool:
    """
    高性能线程池管理器
    提供优化的多线程执行能力
    """
    
    def __init__(self, 
                 max_workers: Optional[int] = None,
                 batch_size: Optional[int] = None,
                 enable_progress_bar: bool = True,
                 progress_desc: str = "处理进度"):
        """
        初始化高性能线程池
        
        参数：
        - max_workers: 最大线程数，None则使用全局优化配置
        - batch_size: 批处理大小，None则使用全局优化配置
        - enable_progress_bar: 是否启用进度条
        - progress_desc: 进度条描述
        """
        self.max_workers = max_workers or MAX_WORKERS
        self.batch_size = batch_size or BATCH_SIZE
        self.enable_progress_bar = enable_progress_bar
        self.progress_desc = progress_desc
        
        # 性能统计
        self.stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'start_time': None,
            'end_time': None,
            'total_duration': 0
        }
        
        logger.info(f"初始化高性能线程池: {self.max_workers}线程, 批处理大小: {self.batch_size}")
    
    def execute_batch(self, 
                     tasks: List[Any],
                     task_function: Callable,
                     *args, **kwargs) -> List[Any]:
        """
        批量执行任务（优化版）
        
        参数：
        - tasks: 任务列表
        - task_function: 任务处理函数
        - *args, **kwargs: 传递给任务函数的额外参数
        
        返回：结果列表
        """
        if not tasks:
            return []
        
        logger.info(f"开始批量执行 {len(tasks)} 个任务")
        self.stats['total_tasks'] = len(tasks)
        self.stats['start_time'] = time.time()
        
        # 极限优化：大幅减小批次大小，最大化批次数量，提高并行度
        # 对于IO密集型任务（数据库查询），更小的批次可以大幅提高并发度
        # 策略：批次大小与线程数成正比，但尽量小以增加批次数量
        cpu_count = multiprocessing.cpu_count()
        
        if len(tasks) > 2000:
            # 任务数非常多时，使用极小的批次（10-20），最大化批次数量
            optimized_batch_size = max(10, min(20, cpu_count))
        elif len(tasks) > 1000:
            # 任务数很多时，使用小批次（15-30）
            optimized_batch_size = max(15, min(30, cpu_count * 2))
        elif len(tasks) > 500:
            # 任务数较多时，使用较小批次（20-50）
            optimized_batch_size = max(20, min(50, cpu_count * 4))
        else:
            # 任务数较少时，使用适中的批次（但不超过原批次大小）
            optimized_batch_size = min(self.batch_size, max(30, cpu_count * 4))
        
        batches = [tasks[i:i + optimized_batch_size] for i in range(0, len(tasks), optimized_batch_size)]
        logger.info(f"极限优化：分为 {len(batches)} 批处理，每批最多 {optimized_batch_size} 个任务（原批次大小: {self.batch_size}，CPU核心数: {cpu_count}）")
        
        all_results = []
        
        # 极限优化：最大化批次级并行度，压榨硬件性能
        # Windows系统使用多线程（因为多进程在Windows上有限制）
        cpu_count = multiprocessing.cpu_count()
        if sys.platform == 'win32':
            # Windows系统：使用多线程，极限增加并行度
            # 对于IO密集型任务（数据库查询、网络IO），线程数可以远超CPU核心数
            # 使用MAX_WORKERS和MAX_CONCURRENT_BATCHES的最大值，不设上限（除了硬件限制）
            effective_workers = min(self.max_workers, len(batches), MAX_CONCURRENT_BATCHES, 1024)  # 最多1024线程
            logger.info(f"Windows系统极限优化：使用 {effective_workers} 个线程执行批次任务（CPU核心数: {cpu_count}, MAX_WORKERS: {self.max_workers}, 批次数: {len(batches)}）")
            executor_class = concurrent.futures.ThreadPoolExecutor
        else:
            # 非Windows系统：使用多进程（真正利用多核CPU）
            # 进程数可以设置得更高，充分利用多核CPU
            effective_workers = min(cpu_count * 8, len(batches), MAX_CONCURRENT_BATCHES, 256)  # 最多256进程
            logger.info(f"极限优化：使用 {effective_workers} 个进程执行批次任务（CPU核心数: {cpu_count}, 批次数: {len(batches)}）")
            executor_class = concurrent.futures.ProcessPoolExecutor
        
        with executor_class(max_workers=effective_workers) as executor:
            # 提交所有批次任务
            future_to_batch = {
                executor.submit(self._process_batch, batch, task_function, *args, **kwargs): batch
                for batch in batches
            }
            
            # 收集结果（使用功能2的标准进度条格式，显示任务数量而非批次数量）
            if self.enable_progress_bar:
                completed_batches = 0
                with tqdm(total=len(tasks), desc=self.progress_desc, ncols=100, unit='只') as pbar:
                    for future in concurrent.futures.as_completed(future_to_batch):
                        batch = future_to_batch[future]
                        try:
                            # 添加超时保护（30秒）
                            batch_results = future.result(timeout=30)
                            all_results.extend(batch_results)
                            self.stats['completed_tasks'] += len(batch_results)
                            completed_batches += 1
                            pbar.update(len(batch_results))  # 更新已完成的任务数量
                            # 每完成10个批次，记录一次日志
                            if completed_batches % 10 == 0:
                                logger.info(f"已完成 {completed_batches}/{len(batches)} 个批次，进度: {len(all_results)}/{len(tasks)}")
                        except concurrent.futures.TimeoutError:
                            logger.error(f"批次处理超时（30秒）: {batch[:3]}...")
                            self.stats['failed_tasks'] += len(batch)
                            pbar.update(len(batch))  # 即使失败也更新进度
                        except Exception as e:
                            logger.error(f"批次处理失败: {e}", exc_info=True)
                            self.stats['failed_tasks'] += len(batch)
                            pbar.update(len(batch))  # 即使失败也更新进度
            else:
                for future in concurrent.futures.as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    try:
                        batch_results = future.result()
                        all_results.extend(batch_results)
                        self.stats['completed_tasks'] += len(batch_results)
                    except Exception as e:
                        logger.error(f"批次处理失败: {e}")
                        self.stats['failed_tasks'] += len(batch)
        
        self.stats['end_time'] = time.time()
        self.stats['total_duration'] = self.stats['end_time'] - self.stats['start_time']
        
        logger.info(f"批量执行完成: {self.stats['completed_tasks']}成功, {self.stats['failed_tasks']}失败, "
                   f"耗时: {self.stats['total_duration']:.2f}秒")
        
        return all_results
    
    def _process_batch(self, batch: List[Any], task_function: Callable, *args, **kwargs) -> List[Any]:
        """
        处理单个批次（优化：批次内部也并行处理，提高CPU利用率）
        
        参数：
        - batch: 批次任务列表
        - task_function: 任务处理函数
        - *args, **kwargs: 传递给任务函数的额外参数
        
        返回：批次结果列表
        """
        # 极限优化：最大化批次内并行度，彻底压榨硬件性能
        # 如果批次非常小（<=1），直接串行处理（避免线程创建开销）
        if len(batch) <= 1:
            results = []
            thread_name = threading.current_thread().name
            for task in batch:
                try:
                    result = task_function(task, *args, **kwargs)
                    results.append(result)
                except Exception as e:
                    logger.error(f"[线程 {thread_name}] 任务处理失败: {e}")
                    results.append(None)
            return results
        
        # 批次较大时，使用线程池并行处理批次内的任务
        # 极限优化：批次内线程数 = min(批次大小, MAX_WORKERS/2, CPU核心数*8)
        # 对于IO密集型任务（数据库查询、网络请求），线程数可以远超CPU核心数
        # 这样可以最大化并发度，充分利用网络带宽和数据库连接池
        cpu_count = multiprocessing.cpu_count()
        batch_workers = min(len(batch), max(8, self.max_workers // 2, cpu_count * 8))
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_workers) as batch_executor:
            future_to_task = {
                batch_executor.submit(task_function, task, *args, **kwargs): task
                for task in batch
            }
            
            # 按任务顺序收集结果
            task_to_result = {}
            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    task_to_result[task] = result
                except Exception as e:
                    logger.error(f"批次内任务处理失败: {e}")
                    task_to_result[task] = None
            
            # 按原始顺序返回结果
            for task in batch:
                results.append(task_to_result.get(task, None))
        
        return results
    
    def execute_parallel(self, 
                        tasks: List[Any],
                        task_function: Callable,
                        *args, **kwargs) -> List[Any]:
        """
        并行执行任务（每个任务一个线程）
        
        参数：
        - tasks: 任务列表
        - task_function: 任务处理函数
        - *args, **kwargs: 传递给任务函数的额外参数
        
        返回：结果列表
        """
        if not tasks:
            return []
        
        logger.info(f"开始并行执行 {len(tasks)} 个任务")
        self.stats['total_tasks'] = len(tasks)
        self.stats['start_time'] = time.time()
        
        all_results = []
        
        # 使用线程池执行
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(task_function, task, *args, **kwargs): task
                for task in tasks
            }
            
            # 收集结果（使用功能2的标准进度条格式）
            if self.enable_progress_bar:
                for future in tqdm(concurrent.futures.as_completed(future_to_task),
                                 total=len(future_to_task),
                                 desc=self.progress_desc,
                                 ncols=100,
                                 unit='只'):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        all_results.append(result)
                        self.stats['completed_tasks'] += 1
                    except Exception as e:
                        logger.error(f"任务处理失败: {e}")
                        all_results.append(None)
                        self.stats['failed_tasks'] += 1
            else:
                for future in concurrent.futures.as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        all_results.append(result)
                        self.stats['completed_tasks'] += 1
                    except Exception as e:
                        logger.error(f"任务处理失败: {e}")
                        all_results.append(None)
                        self.stats['failed_tasks'] += 1
        
        self.stats['end_time'] = time.time()
        self.stats['total_duration'] = self.stats['end_time'] - self.stats['start_time']
        
        logger.info(f"并行执行完成: {self.stats['completed_tasks']}成功, {self.stats['failed_tasks']}失败, "
                   f"耗时: {self.stats['total_duration']:.2f}秒")
        
        return all_results
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        获取性能统计信息
        
        返回：性能统计字典
        """
        if self.stats['total_duration'] > 0:
            self.stats['tasks_per_second'] = self.stats['completed_tasks'] / self.stats['total_duration']
            self.stats['success_rate'] = self.stats['completed_tasks'] / self.stats['total_tasks'] if self.stats['total_tasks'] > 0 else 0
        else:
            self.stats['tasks_per_second'] = 0
            self.stats['success_rate'] = 0
        
        return self.stats.copy()


class HighPerformanceDataProcessor:
    """
    高性能数据处理器
    专门用于数据获取和处理任务
    """
    
    def __init__(self, data_fetcher=None):
        """
        初始化高性能数据处理器
        
        参数：
        - data_fetcher: 数据获取器实例
        """
        self.data_fetcher = data_fetcher
        self.thread_pool = HighPerformanceThreadPool(
            progress_desc="数据处理进度"
        )
    
    def batch_fetch_data(self, 
                        items: List[str],
                        fetch_function: Callable,
                        batch_size: Optional[int] = None,
                        *args, **kwargs) -> Dict[str, Any]:
        """
        批量获取数据（优化版）
        
        参数：
        - items: 要获取的项目列表（如股票代码）
        - fetch_function: 数据获取函数
        - batch_size: 批处理大小，None则使用默认值
        - *args, **kwargs: 传递给获取函数的额外参数
        
        返回：数据字典
        """
        if not items:
            return {}
        
        logger.info(f"开始批量获取 {len(items)} 个项目的数据")
        
        # 使用优化的批处理大小
        effective_batch_size = batch_size or (BATCH_SIZE * 2)  # 数据获取使用更大的批次
        
        # 分批处理
        batches = [items[i:i + effective_batch_size] for i in range(0, len(items), effective_batch_size)]
        logger.info(f"分为 {len(batches)} 批处理，每批最多 {effective_batch_size} 个项目")
        
        all_results = {}
        
        # 使用线程池执行
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交所有批次任务
            future_to_batch = {
                executor.submit(self._fetch_batch, batch, fetch_function, *args, **kwargs): batch
                for batch in batches
            }
            
            # 收集结果（使用功能2的标准进度条格式，显示项目数量而非批次数量）
            with tqdm(total=len(items), desc='数据获取进度', ncols=100, unit='只') as pbar:
                for future in concurrent.futures.as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    try:
                        batch_results = future.result()
                        all_results.update(batch_results)
                        pbar.update(len(batch))  # 更新已完成的项目数量
                    except Exception as e:
                        logger.error(f"批次数据获取失败: {e}")
                        pbar.update(len(batch))  # 即使失败也更新进度
        
        logger.info(f"数据获取完成，成功获取 {len(all_results)} 个项目的数据")
        return all_results
    
    def _fetch_batch(self, batch: List[str], fetch_function: Callable, *args, **kwargs) -> Dict[str, Any]:
        """
        获取单个批次的数据
        
        参数：
        - batch: 批次项目列表
        - fetch_function: 数据获取函数
        - *args, **kwargs: 传递给获取函数的额外参数
        
        返回：批次数据字典
        """
        results = {}
        thread_name = threading.current_thread().name
        
        try:
            # 尝试批量获取
            if hasattr(fetch_function, '__self__') and hasattr(fetch_function.__self__, 'batch_get_stock_data'):
                # 如果是批量获取方法，直接调用
                batch_results = fetch_function(batch, *args, **kwargs)
                if isinstance(batch_results, dict):
                    results.update(batch_results)
                else:
                    # 如果不是字典，转换为字典格式
                    for i, item in enumerate(batch):
                        if i < len(batch_results):
                            results[item] = batch_results[i]
            else:
                # 逐个获取
                for item in batch:
                    try:
                        result = fetch_function(item, *args, **kwargs)
                        results[item] = result
                    except Exception as e:
                        logger.error(f"[线程 {thread_name}] 获取 {item} 数据失败: {e}")
                        results[item] = None
        except Exception as e:
            logger.error(f"[线程 {thread_name}] 批次获取失败: {e}")
        
        return results


class HighPerformanceAnalyzer:
    """
    高性能分析器
    专门用于数据分析任务
    """
    
    def __init__(self, 
                 max_workers: Optional[int] = None,
                 batch_size: Optional[int] = None):
        """
        初始化高性能分析器
        
        参数：
        - max_workers: 最大线程数
        - batch_size: 批处理大小
        """
        self.thread_pool = HighPerformanceThreadPool(
            max_workers=max_workers,
            batch_size=batch_size,
            progress_desc="分析进度"
        )
    
    def analyze_batch(self, 
                     data_items: List[Any],
                     analysis_function: Callable,
                     *args, **kwargs) -> List[Any]:
        """
        批量分析数据
        
        参数：
        - data_items: 数据项列表
        - analysis_function: 分析函数
        - *args, **kwargs: 传递给分析函数的额外参数
        
        返回：分析结果列表
        """
        return self.thread_pool.execute_batch(
            data_items, 
            analysis_function, 
            *args, **kwargs
        )
    
    def analyze_parallel(self, 
                        data_items: List[Any],
                        analysis_function: Callable,
                        *args, **kwargs) -> List[Any]:
        """
        并行分析数据
        
        参数：
        - data_items: 数据项列表
        - analysis_function: 分析函数
        - *args, **kwargs: 传递给分析函数的额外参数
        
        返回：分析结果列表
        """
        return self.thread_pool.execute_parallel(
            data_items, 
            analysis_function, 
            *args, **kwargs
        )


# 便捷函数
def high_performance_execute(tasks: List[Any], 
                           task_function: Callable,
                           mode: str = 'batch',
                           max_workers: Optional[int] = None,
                           batch_size: Optional[int] = None,
                           enable_progress_bar: bool = True,
                           progress_desc: str = "处理进度",
                           *args, **kwargs) -> List[Any]:
    """
    高性能执行函数（便捷接口）
    
    参数：
    - tasks: 任务列表
    - task_function: 任务处理函数
    - mode: 执行模式 ('batch' 或 'parallel')
    - max_workers: 最大线程数
    - batch_size: 批处理大小
    - enable_progress_bar: 是否启用进度条
    - progress_desc: 进度条描述
    - *args, **kwargs: 传递给任务函数的额外参数
    
    返回：结果列表
    """
    thread_pool = HighPerformanceThreadPool(
        max_workers=max_workers,
        batch_size=batch_size,
        enable_progress_bar=enable_progress_bar,
        progress_desc=progress_desc
    )
    
    if mode == 'batch':
        return thread_pool.execute_batch(tasks, task_function, *args, **kwargs)
    else:
        return thread_pool.execute_parallel(tasks, task_function, *args, **kwargs)


def high_performance_fetch(items: List[str],
                          fetch_function: Callable,
                          data_fetcher=None,
                          batch_size: Optional[int] = None,
                          *args, **kwargs) -> Dict[str, Any]:
    """
    高性能数据获取函数（便捷接口）
    
    参数：
    - items: 要获取的项目列表
    - fetch_function: 数据获取函数
    - data_fetcher: 数据获取器实例
    - batch_size: 批处理大小
    - *args, **kwargs: 传递给获取函数的额外参数
    
    返回：数据字典
    """
    processor = HighPerformanceDataProcessor(data_fetcher)
    return processor.batch_fetch_data(items, fetch_function, batch_size, *args, **kwargs)


def high_performance_analyze(data_items: List[Any],
                            analysis_function: Callable,
                            mode: str = 'batch',
                            max_workers: Optional[int] = None,
                            batch_size: Optional[int] = None,
                            *args, **kwargs) -> List[Any]:
    """
    高性能数据分析函数（便捷接口）
    
    参数：
    - data_items: 数据项列表
    - analysis_function: 分析函数
    - mode: 分析模式 ('batch' 或 'parallel')
    - max_workers: 最大线程数
    - batch_size: 批处理大小
    - *args, **kwargs: 传递给分析函数的额外参数
    
    返回：分析结果列表
    """
    analyzer = HighPerformanceAnalyzer(max_workers, batch_size)
    
    if mode == 'batch':
        return analyzer.analyze_batch(data_items, analysis_function, *args, **kwargs)
    else:
        return analyzer.analyze_parallel(data_items, analysis_function, *args, **kwargs)


# 装饰器
def high_performance(mode: str = 'batch', 
                    max_workers: Optional[int] = None,
                    batch_size: Optional[int] = None,
                    enable_progress_bar: bool = True,
                    progress_desc: str = "处理进度"):
    """
    高性能装饰器
    
    参数：
    - mode: 执行模式 ('batch' 或 'parallel')
    - max_workers: 最大线程数
    - batch_size: 批处理大小
    - enable_progress_bar: 是否启用进度条
    - progress_desc: 进度条描述
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 如果第一个参数是列表，使用高性能执行
            if args and isinstance(args[0], list):
                tasks = args[0]
                other_args = args[1:]
                
                thread_pool = HighPerformanceThreadPool(
                    max_workers=max_workers,
                    batch_size=batch_size,
                    enable_progress_bar=enable_progress_bar,
                    progress_desc=progress_desc
                )
                
                if mode == 'batch':
                    return thread_pool.execute_batch(tasks, func, *other_args, **kwargs)
                else:
                    return thread_pool.execute_parallel(tasks, func, *other_args, **kwargs)
            else:
                # 否则正常执行
                return func(*args, **kwargs)
        return wrapper
    return decorator


if __name__ == '__main__':
    # 测试代码
    def test_task(item):
        """测试任务"""
        time.sleep(0.01)  # 模拟任务
        return f"处理完成: {item}"
    
    # 测试数据
    test_items = [f"item_{i}" for i in range(100)]
    
    print("测试高性能多线程模块...")
    
    # 测试批量执行
    results = high_performance_execute(
        test_items, 
        test_task, 
        mode='batch',
        progress_desc="批量测试"
    )
    
    print(f"批量执行结果: {len(results)} 个结果")
    
    # 测试并行执行
    results = high_performance_execute(
        test_items, 
        test_task, 
        mode='parallel',
        progress_desc="并行测试"
    )
    
    print(f"并行执行结果: {len(results)} 个结果")
    
    print("测试完成！")
