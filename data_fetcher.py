import pyodbc
import pandas as pd
import numpy as np
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime, timedelta, date
import warnings
from tqdm import tqdm
from typing import List, Dict, Optional
warnings.filterwarnings('ignore')

# 获取logger
logger = logging.getLogger(__name__)

from juyuan_config import ODBC_CONFIG, DATABASE_CONFIG, TABLE_CONFIG, STOCK_FILTER
from db_schema import TABLES
from config import STOCK_LIST_LIMIT, DB_POOL_SIZE, DB_TIMEOUT
import concurrent.futures

# 全局连接池管理（单例模式）
_global_connection_pool = []
_global_pool_lock = threading.Lock()
_global_pool_initialized = False
_last_activity_time = None
_idle_timeout_thread = None
_idle_timeout_lock = threading.Lock()
IDLE_TIMEOUT_SECONDS = 20 * 60  # 20分钟空闲超时

def _start_idle_timeout_monitor():
    """启动空闲超时监控线程"""
    global _idle_timeout_thread
    
    with _idle_timeout_lock:
        if _idle_timeout_thread is not None and _idle_timeout_thread.is_alive():
            return  # 已经启动
        
        def monitor_loop():
            """监控循环：定期检查连接池是否空闲超时"""
            global _last_activity_time, _global_pool_initialized
            while True:
                try:
                    time.sleep(60)  # 每分钟检查一次
                    
                    with _idle_timeout_lock:
                        if _last_activity_time is None:
                            continue
                        
                        idle_time = time.time() - _last_activity_time
                        
                        if idle_time >= IDLE_TIMEOUT_SECONDS:
                            # 超过20分钟没有活动，关闭所有连接
                            with _global_pool_lock:
                                closed_count = 0
                                while _global_connection_pool:
                                    conn = _global_connection_pool.pop()
                                    try:
                                        conn.close()
                                        closed_count += 1
                                    except Exception as e:
                                        logger.debug(f"关闭空闲连接时出错: {e}")
                                
                                if closed_count > 0:
                                    logger.info(f"连接池空闲超时（{idle_time/60:.1f}分钟），已关闭 {closed_count} 个连接")
                                
                                # 重置初始化状态，下次使用时重新创建
                                _global_pool_initialized = False
                                
                                # 重置最后活动时间
                                _last_activity_time = None
                
                except Exception as e:
                    logger.error(f"空闲超时监控线程出错: {e}")
        
        _idle_timeout_thread = threading.Thread(target=monitor_loop, daemon=True, name="ConnectionPoolIdleMonitor")
        _idle_timeout_thread.start()
        logger.debug("数据库连接池空闲超时监控线程已启动")

def _update_activity_time():
    """更新最后活动时间"""
    global _last_activity_time
    _last_activity_time = time.time()

def _get_global_connection_pool():
    """获取全局连接池"""
    return _global_connection_pool

def _get_global_pool_lock():
    """获取全局连接池锁"""
    return _global_pool_lock

class JuyuanDataFetcher:
    def __init__(self, use_connection_pool=True, lazy_init_pool=True):
        self.conn = None
        self.use_connection_pool = use_connection_pool
        # 使用全局连接池（所有实例共享）
        self._connection_pool = _get_global_connection_pool()
        self._pool_lock = _get_global_pool_lock()
        self._lazy_init_pool = lazy_init_pool  # 延迟初始化连接池
        
        # 启动空闲超时监控（只启动一次）
        _start_idle_timeout_monitor()
        
        # 更新活动时间
        _update_activity_time()
        
        # 如果不需要延迟初始化，立即初始化连接池
        if use_connection_pool and not lazy_init_pool:
            self._init_connection_pool()

    def _init_connection_pool(self):
        """初始化连接池（延迟初始化，只在需要时创建连接）"""
        global _global_pool_initialized
        
        if _global_pool_initialized:
            return
        
        # 使用较小的初始连接池大小，按需扩展
        initial_pool_size = min(DB_POOL_SIZE, 10)  # 初始只创建10个连接
        
        logger.info(f"初始化数据库连接池，初始大小: {initial_pool_size}（按需扩展到{DB_POOL_SIZE}）")
        
        from tqdm import tqdm
        with tqdm(total=initial_pool_size, desc="创建数据库连接", unit="个", ncols=80) as pbar:
            for i in range(initial_pool_size):
                conn = self._create_connection()
                if conn:
                    self._connection_pool.append(conn)
                pbar.update(1)
        
        _global_pool_initialized = True
        logger.info(f"连接池初始化完成，可用连接数: {len(self._connection_pool)}")
        
        # 更新活动时间
        _update_activity_time()

    def _create_connection(self):
        """创建数据库连接"""
        try:
            # 尝试多个可能的驱动名称（按优先级）
            possible_drivers = [
                ODBC_CONFIG['driver'],  # 配置的驱动
                "ODBC Driver 18 for SQL Server",  # 标准名称
                "ODBC Driver 17 for SQL Server",  # 旧版本
                "SQL Server",  # 旧版驱动
                "SQL Server Native Client 11.0",  # 另一个可能的驱动
            ]
            
            last_error = None
            for driver_name in possible_drivers:
                try:
                    conn_str = (
                        f"DRIVER={{{driver_name}}};"
                        f"SERVER={DATABASE_CONFIG['server']},{DATABASE_CONFIG['port']};"
                        f"DATABASE={DATABASE_CONFIG['database']};"
                        f"UID={DATABASE_CONFIG['user']};"
                        f"PWD={DATABASE_CONFIG['password']};"
                        f"TrustServerCertificate=yes;"
                        f"Connection Timeout={DB_TIMEOUT};"
                        f"Command Timeout=60;"
                    )
                    conn = pyodbc.connect(conn_str, autocommit=True)
                    logger.info(f"成功使用驱动连接: {driver_name}")
                    return conn
                except Exception as e:
                    last_error = e
                    logger.debug(f"尝试驱动 {driver_name} 失败: {e}")
                    continue
            
            # 所有驱动都失败，抛出最后一个错误
            logger.error(f"所有驱动尝试失败，最后一个错误: {last_error}")
            raise last_error
        except Exception as e:
            logger.error(f"创建数据库连接失败: {e}")
            return None

    def _get_connection(self):
        """从连接池获取连接"""
        # 更新活动时间（每次使用连接时）
        _update_activity_time()
        
        if not self.use_connection_pool:
            return self._create_connection()
        
        # 延迟初始化连接池（只在第一次使用时初始化）
        global _global_pool_initialized
        if self._lazy_init_pool and not _global_pool_initialized:
            self._init_connection_pool()
        
        with self._pool_lock:
            if self._connection_pool:
                conn = self._connection_pool.pop()
                _update_activity_time()  # 获取连接时更新活动时间
                return conn
            else:
                # 连接池为空，创建新连接（按需扩展）
                conn = self._create_connection()
                _update_activity_time()  # 创建连接时更新活动时间
                return conn

    def _return_connection(self, conn):
        """归还连接到连接池"""
        # 更新活动时间（归还连接时）
        _update_activity_time()
        
        if not self.use_connection_pool or not conn:
            return
        
        try:
            # 测试连接是否还有效
            conn.execute("SELECT 1")
            with self._pool_lock:
                self._connection_pool.append(conn)
                _update_activity_time()  # 归还连接时更新活动时间
        except:
            # 连接无效，创建新连接
            new_conn = self._create_connection()
            if new_conn:
                with self._pool_lock:
                    self._connection_pool.append(new_conn)
                    _update_activity_time()  # 创建新连接时更新活动时间

    def connect(self):
        if self.conn is not None:
            return self.conn
        self.conn = self._get_connection()
        return self.conn

    def close(self):
        if self.conn:
            self._return_connection(self.conn)
            self.conn = None
    
    def cleanup(self, force=False):
        """
        清理所有连接池中的连接
        
        Args:
            force: 如果为True，立即关闭所有连接；如果为False，不关闭（由空闲超时机制管理）
        """
        # 如果force=False，不立即关闭连接，让空闲超时机制管理
        if not force:
            # 只关闭当前实例使用的连接
            if self.conn:
                try:
                    self._return_connection(self.conn)
                except:
                    pass
                self.conn = None
            return
        
        # force=True时，立即关闭所有连接
        with self._pool_lock:
            closed_count = 0
            while self._connection_pool:
                conn = self._connection_pool.pop()
                try:
                    conn.close()
                    closed_count += 1
                except Exception as e:
                    logger.warning(f"关闭连接时出错: {e}")
            
            if closed_count > 0:
                logger.info(f"强制清理连接池: 关闭了 {closed_count} 个连接")
            
            # 重置初始化状态
            global _global_pool_initialized
            _global_pool_initialized = False
        
        # 同时关闭当前使用的连接
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None
        
        # 重置最后活动时间
        global _last_activity_time
        _last_activity_time = None
    
    def __del__(self):
        """析构函数：确保实例销毁时关闭所有连接"""
        try:
            self.cleanup()
        except:
            # 忽略析构函数中的错误，避免影响程序正常退出
            pass

    def query(self, sql, params=None, max_retries=None):
        if max_retries is None:
            from config import MAX_RETRIES
            max_retries = MAX_RETRIES
        """优化的查询方法，支持连接池"""
        # 更新活动时间（每次查询时）
        _update_activity_time()
        
        conn = None
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # 获取连接
                conn = self._get_connection()
                if not conn:
                    raise Exception("无法获取数据库连接")
                
                # 参数类型转换，避免numpy类型
                if params is not None:
                    def to_native(x):
                        if isinstance(x, (np.integer,)):
                            return int(x)
                        if isinstance(x, (np.floating,)):
                            return float(x)
                        if isinstance(x, (np.str_,)):
                            return str(x)
                        return x
                    params = [to_native(x) for x in params]
                
                start_time = time.time()
                df = pd.read_sql(sql, conn, params=params)
                query_time = time.time() - start_time
                
                # 只在查询时间超过10秒时记录到debug日志（不显示在控制台，避免干扰进度条）
                if query_time > 10:
                    logger.debug(f"查询耗时较长: {query_time:.2f}秒")
                
                # 查询成功，更新活动时间
                _update_activity_time()
                
                return df
                
            except Exception as e:
                retry_count += 1
                logger.error(f"SQL执行出错，第{retry_count}次重试: {e}")
                
                if retry_count >= max_retries:
                    raise
                else:
                    from config import RETRY_DELAY
                    time.sleep(RETRY_DELAY)
                    
            finally:
                if conn:
                    self._return_connection(conn)

    def get_stock_list(self, limit=None, market=None, max_days_ago=30, cutoff_date=None, use_cache=True):
        """
        获取股票列表，排除停牌或退市股票（高性能多线程版本）
        Args:
            limit: 股票数量限制
            market: 市场筛选（SZ/SS），None表示获取所有市场（会并行查询）
            max_days_ago: 最大允许的行情日期滞后天数，超过此天数的股票将被排除（停牌/退市）
            cutoff_date: 数据截止日期，None表示使用当前日期减去max_days_ago
            use_cache: 是否使用缓存（默认True）
        """
        import time
        from functools import lru_cache
        from datetime import datetime
        
        if limit is None:
            limit = STOCK_LIST_LIMIT
        elif limit == -1:  # 使用-1表示获取全市场
            limit = 99999  # 设置一个很大的数字来获取全市场
        
        # 获取截止日期
        if cutoff_date is None:
            current_date = pd.Timestamp.now().date()
            cutoff_date = current_date - pd.Timedelta(days=max_days_ago)
        else:
            # 使用指定的截止日期
            cutoff_date = pd.Timestamp(cutoff_date).date()
        
        # 缓存键（基于参数）
        cache_key = f"stock_list_{limit}_{market}_{max_days_ago}_{cutoff_date}"
        
        # 简单的内存缓存（可以扩展到文件缓存）
        if not hasattr(self, '_stock_list_cache'):
            self._stock_list_cache = {}
            self._stock_list_cache_time = {}
        
        # 检查缓存（缓存有效期5分钟）
        if use_cache and cache_key in self._stock_list_cache:
            cache_time = self._stock_list_cache_time.get(cache_key, 0)
            if time.time() - cache_time < 300:  # 5分钟缓存
                logger.debug(f"[get_stock_list] 使用缓存: {len(self._stock_list_cache[cache_key])} 只股票")
                return self._stock_list_cache[cache_key]
        
        start_time = time.time()
        
        # 如果指定了市场，直接查询
        if market:
            stock_info = self._get_stock_list_single_market(limit, market, cutoff_date)
        else:
            # 未指定市场，并行查询SZ和SS两个市场
            stock_info = self._get_stock_list_parallel(limit, cutoff_date)
        
        elapsed = time.time() - start_time
        
        # 保存到缓存
        if use_cache:
            self._stock_list_cache[cache_key] = stock_info
            self._stock_list_cache_time[cache_key] = time.time()
        
        logger.info(f"[get_stock_list] 获取完成: {len(stock_info)} 只股票，耗时: {elapsed:.2f}秒")
        
        return stock_info
    
    def _get_stock_list_single_market(self, limit, market, cutoff_date):
        """获取单个市场的股票列表"""
        # 先统计各个条件过滤的股票数量（用于调试）
        market_condition = ""
        if market.upper() == "SZ":
            market_condition = " AND SecuMarket=90"
        elif market.upper() == "SS":
            market_condition = " AND SecuMarket=83"
        
        # 统计1: 该市场的所有股票（在SecuMain中）
        stats_sql1 = f"""
        SELECT COUNT(*) as cnt
        FROM {TABLE_CONFIG['stock_info_table']} s
        WHERE SecuCategory = 1 {market_condition}
        """
        try:
            df_stats1 = self.query(stats_sql1)
            total_in_market = df_stats1.iloc[0]['cnt'] if not df_stats1.empty else 0
        except:
            total_in_market = 0
        
        # 统计2: 应用STOCK_FILTER后的数量
        stats_sql2 = f"""
        SELECT COUNT(*) as cnt
        FROM {TABLE_CONFIG['stock_info_table']} s
        WHERE {STOCK_FILTER} {market_condition}
        """
        try:
            df_stats2 = self.query(stats_sql2)
            after_stock_filter = df_stats2.iloc[0]['cnt'] if not df_stats2.empty else 0
        except:
            after_stock_filter = 0
        
        # 统计3: 排除X开头后的数量
        stats_sql3 = f"""
        SELECT COUNT(*) as cnt
        FROM {TABLE_CONFIG['stock_info_table']} s
        WHERE {STOCK_FILTER}
        AND s.{TABLE_CONFIG['code_field']} NOT LIKE 'X%' {market_condition}
        """
        try:
            df_stats3 = self.query(stats_sql3)
            after_x_filter = df_stats3.iloc[0]['cnt'] if not df_stats3.empty else 0
        except:
            after_x_filter = 0
        
        # 统计4: 有交易记录的股票数量
        stats_sql4 = f"""
        SELECT COUNT(DISTINCT s.{TABLE_CONFIG['inner_code_field']}) as cnt
        FROM {TABLE_CONFIG['stock_info_table']} s
        INNER JOIN (
            SELECT q.{TABLE_CONFIG['inner_code_field']}, MAX(q.{TABLE_CONFIG['date_field']}) as latest_trading_day
            FROM {TABLE_CONFIG['daily_quote_table']} q
            GROUP BY q.{TABLE_CONFIG['inner_code_field']}
        ) latest ON s.{TABLE_CONFIG['inner_code_field']} = latest.{TABLE_CONFIG['inner_code_field']}
        WHERE {STOCK_FILTER}
        AND s.{TABLE_CONFIG['code_field']} NOT LIKE 'X%' {market_condition}
        """
        try:
            df_stats4 = self.query(stats_sql4)
            with_trading_data = df_stats4.iloc[0]['cnt'] if not df_stats4.empty else 0
        except:
            with_trading_data = 0
        
        # 统计5: 活跃度筛选后的数量（最新交易日 >= cutoff_date）
        stats_sql5 = f"""
        SELECT COUNT(DISTINCT s.{TABLE_CONFIG['inner_code_field']}) as cnt
        FROM {TABLE_CONFIG['stock_info_table']} s
        INNER JOIN (
            SELECT q.{TABLE_CONFIG['inner_code_field']}, MAX(q.{TABLE_CONFIG['date_field']}) as latest_trading_day
            FROM {TABLE_CONFIG['daily_quote_table']} q
            GROUP BY q.{TABLE_CONFIG['inner_code_field']}
        ) latest ON s.{TABLE_CONFIG['inner_code_field']} = latest.{TABLE_CONFIG['inner_code_field']}
        WHERE {STOCK_FILTER}
        AND latest.latest_trading_day >= '{cutoff_date}'
        AND s.{TABLE_CONFIG['code_field']} NOT LIKE 'X%' {market_condition}
        """
        try:
            df_stats5 = self.query(stats_sql5)
            after_activity_filter = df_stats5.iloc[0]['cnt'] if not df_stats5.empty else 0
        except:
            after_activity_filter = 0
        
        print(f"  📊 [{market}市场] 股票筛选统计:")
        print(f"     市场总股票数: {total_in_market}")
        print(f"     STOCK_FILTER后: {after_stock_filter} (排除: {total_in_market - after_stock_filter})")
        print(f"     排除X开头后: {after_x_filter} (排除: {after_stock_filter - after_x_filter})")
        print(f"     有交易记录: {with_trading_data} (排除: {after_x_filter - with_trading_data})")
        print(f"     活跃度筛选后(>={cutoff_date}): {after_activity_filter} (排除: {with_trading_data - after_activity_filter})")
        
        # 实际查询
        sql = f"""
        SELECT TOP {limit} s.{TABLE_CONFIG['code_field']}, s.{TABLE_CONFIG['inner_code_field']}, s.SecuMarket
        FROM {TABLE_CONFIG['stock_info_table']} s
        INNER JOIN (
            SELECT q.{TABLE_CONFIG['inner_code_field']}, MAX(q.{TABLE_CONFIG['date_field']}) as latest_trading_day
            FROM {TABLE_CONFIG['daily_quote_table']} q
            GROUP BY q.{TABLE_CONFIG['inner_code_field']}
        ) latest ON s.{TABLE_CONFIG['inner_code_field']} = latest.{TABLE_CONFIG['inner_code_field']}
        WHERE {STOCK_FILTER}
        AND latest.latest_trading_day >= '{cutoff_date}'
        AND s.{TABLE_CONFIG['code_field']} NOT LIKE 'X%'
        """
        
        if market.upper() == "SZ":
            sql += " AND SecuMarket=90"
        elif market.upper() == "SS":
            sql += " AND SecuMarket=83"
        
        sql += f" ORDER BY s.{TABLE_CONFIG['code_field']}"
        
        logger.debug(f"[get_stock_list_single_market SQL]: {sql}")
        
        # 添加查询时间统计
        import time
        query_start = time.time()
        df = self.query(sql)
        query_time = time.time() - query_start
        
        result_count = len(df)
        print(f"     实际获取数量: {result_count} (limit={limit})")
        
        if query_time > 10:  # 如果查询超过10秒，记录警告
            logger.warning(f"[get_stock_list_single_market] 查询耗时: {query_time:.2f}秒")
        
        return self._format_stock_info(df)
    
    def _get_stock_list_parallel(self, limit, cutoff_date):
        """并行获取SZ和SS两个市场的股票列表"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # 计算每个市场的limit（如果limit不是-1，则平均分配）
        if limit == 99999:  # 全市场
            sz_limit = 99999
            ss_limit = 99999
        else:
            # 平均分配，但每个市场至少获取limit只（因为可能一个市场股票少）
            sz_limit = limit
            ss_limit = limit
        
        all_stock_info = []
        
        def query_market(market_name, market_limit):
            """查询单个市场"""
            try:
                return self._get_stock_list_single_market(market_limit, market_name, cutoff_date)
            except Exception as e:
                logger.error(f"查询{market_name}市场失败: {e}")
                return []
        
        # 使用线程池并行查询两个市场
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(query_market, "SZ", sz_limit): "SZ",
                executor.submit(query_market, "SS", ss_limit): "SS"
            }
            
            for future in as_completed(futures):
                market_name = futures[future]
                try:
                    market_stocks = future.result()
                    all_stock_info.extend(market_stocks)
                    logger.debug(f"[get_stock_list_parallel] {market_name}市场: {len(market_stocks)} 只股票")
                except Exception as e:
                    logger.error(f"[get_stock_list_parallel] {market_name}市场查询失败: {e}")
        
        # 按代码排序确保确定性和市场公平性，再截断
        all_stock_info.sort(key=lambda x: x.get('code', ''))
        before_limit = len(all_stock_info)
        if limit != 99999 and len(all_stock_info) > limit:
            all_stock_info = all_stock_info[:limit]
            after_limit = len(all_stock_info)
            print(f"  📊 [汇总] 应用limit限制: {before_limit} -> {after_limit} (排除: {before_limit - after_limit})")
        else:
            print(f"  📊 [汇总] 未应用limit限制，总数: {before_limit}")
        
        return all_stock_info
    
    def _format_stock_info(self, df):
        """格式化股票信息"""
        if df.empty:
            return []
        
        stock_info = []
        for _, row in df.iterrows():
            code = str(row[TABLE_CONFIG['code_field']]).zfill(6)
            market = row['SecuMarket']
            
            # 根据SecuMarket值确定交易所标识
            if market == 90:  # 深圳
                exchange_suffix = '.SZ'
            elif market == 83:  # 上海
                exchange_suffix = '.SH'
            else:
                exchange_suffix = '.UNKNOWN'  # 未知交易所
            
            stock_info.append({
                'code': code,
                'exchange': exchange_suffix,
                'full_code': f"{code}{exchange_suffix}"
            })
        
        return stock_info

    def get_latest_trading_date(self):
        """
        获取数据库中的最新交易日
        Returns:
            date: 最新交易日，如果查询失败则返回当前日期
        """
        try:
            sql = f"""
            SELECT MAX({TABLE_CONFIG['date_field']}) as latest_date
            FROM {TABLE_CONFIG['daily_quote_table']}
            """
            df = self.query(sql)
            if not df.empty and df.iloc[0]['latest_date'] is not None:
                latest_date = pd.Timestamp(df.iloc[0]['latest_date']).date()
                logger.info(f"数据库最新交易日: {latest_date}")
                return latest_date
            else:
                # 如果查询失败，返回当前日期
                current_date = pd.Timestamp.now().date()
                logger.warning(f"无法获取最新交易日，使用当前日期: {current_date}")
                return current_date
        except Exception as e:
            logger.error(f"获取最新交易日失败: {e}")
            # 如果查询失败，返回当前日期
            current_date = pd.Timestamp.now().date()
            logger.warning(f"使用当前日期作为默认值: {current_date}")
            return current_date

    def batch_get_stock_data(self, stock_codes, days=365):
        """批量获取股票数据，使用多线程并行处理"""
        if not stock_codes:
            return {}
        
        logger.info(f"开始批量获取 {len(stock_codes)} 只股票的行情数据...")
        start_time = time.time()
        
        # 分批处理，避免SQL语句过长（优化批处理大小提高网络吞吐量）
        batch_size = 500  # 每批处理500只股票，大幅提高网络利用率
        batches = []
        
        for i in range(0, len(stock_codes), batch_size):
            batch_codes_subset = stock_codes[i:i + batch_size]
            batches.append((i//batch_size + 1, batch_codes_subset))
        
        logger.info(f"将 {len(stock_codes)} 只股票分为 {len(batches)} 批处理，每批 {batch_size} 只")
        
        # 使用多线程并行处理批次
        all_results = {}
        processed_count = 0
        
        # 创建总体进度条
        total_pbar = tqdm(total=len(stock_codes), desc='数据获取总进度', ncols=100, unit='只', position=0)
        
        def process_batch(batch_info):
            """处理单个批次的股票数据"""
            batch_id, batch_codes_subset = batch_info
            thread_name = threading.current_thread().name
            batch_results = {}
            
            try:
                logger.info(f"[线程 {thread_name}] 开始处理批次 {batch_id}，包含 {len(batch_codes_subset)} 只股票")
                
                # 构建批量查询SQL
                codes_str = ','.join([f"'{code}'" for code in batch_codes_subset])
                sql = f"""
                SELECT 
                    s.{TABLE_CONFIG['code_field']},
                    q.{TABLE_CONFIG['date_field']} as [Date],
                    q.{TABLE_CONFIG['open_field']} as [Open],
                    q.{TABLE_CONFIG['high_field']} as [High],
                    q.{TABLE_CONFIG['low_field']} as [Low],
                    q.{TABLE_CONFIG['close_field']} as [Close],
                    q.{TABLE_CONFIG['volume_field']} as [Volume],
                    q.{TABLE_CONFIG['turnover_rate_field']} as [TurnoverRate],
                    q.{TABLE_CONFIG['negotiable_mv_field']} as [NegotiableMV],
                    p.SurgedLimit as SurgedLimit,
                    p.DeclineLimit as DeclineLimit
                FROM {TABLE_CONFIG['stock_info_table']} s
                INNER JOIN {TABLE_CONFIG['daily_quote_table']} q 
                    ON s.{TABLE_CONFIG['inner_code_field']} = q.{TABLE_CONFIG['inner_code_field']}
                LEFT JOIN QT_PerformanceData p 
                    ON q.{TABLE_CONFIG['inner_code_field']} = p.InnerCode 
                    AND q.{TABLE_CONFIG['date_field']} = p.TradingDay
                WHERE s.{TABLE_CONFIG['code_field']} IN ({codes_str})
                AND {STOCK_FILTER}
                ORDER BY s.{TABLE_CONFIG['code_field']}, q.{TABLE_CONFIG['date_field']} DESC
                """
                
                # 调试：打印SQL（仅第一个批次）
                if batch_id == 1:
                    print(f"\n{'='*80}")
                    logger.debug(f"[调试] batch_get_stock_data SQL查询（批次{batch_id}）")
                    logger.debug(f"股票代码示例: {batch_codes_subset[:5]}... (共{len(batch_codes_subset)}只)")
                    logger.debug(f"实际执行的SQL:\n{sql}")
                
                df = self.query(sql)
                
                # 调试：检查查询结果的列（仅第一个批次）
                if batch_id == 1 and not df.empty:
                    logger.debug(f"[调试] 查询结果统计:")
                    logger.debug(f"  列名: {list(df.columns)}")
                    logger.debug(f"  行数: {len(df)}")
                    
                    if 'SurgedLimit' in df.columns:
                        surged_limit_count = df['SurgedLimit'].notna().sum()
                        surged_limit_1_count = (df['SurgedLimit'] == 1).sum()
                        logger.debug(f"  SurgedLimit字段统计: 非空数量={surged_limit_count}/{len(df)}, 值为1数量={surged_limit_1_count}")
                    else:
                        logger.warning(f"查询结果中缺少SurgedLimit列！可用列: {list(df.columns)}")
                
                if df.empty:
                    logger.warning(f"[线程 {thread_name}] 批次 {batch_id} 查询结果为空")
                    return batch_results
                
                # 按股票代码分组
                for code in batch_codes_subset:
                    stock_data = df[df[TABLE_CONFIG['code_field']] == code].copy()
                    if not stock_data.empty:
                        stock_data = stock_data.sort_values('Date')
                        stock_data = stock_data.set_index('Date')
                        # 只保留最近days天的数据（与单独获取保持一致）
                        stock_data = stock_data.tail(days)
                        
                        # 数据类型转换
                        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'TurnoverRate', 'NegotiableMV']
                        for col in numeric_columns:
                            if col in stock_data.columns:
                                stock_data[col] = pd.to_numeric(stock_data[col], errors='coerce')
                        
                        # SurgedLimit和DeclineLimit转换为整数（0或1），保留NULL值
                        # 重要：保留NULL值，不要填充为0，这样可以区分"数据缺失"和"非涨停"
                        if 'SurgedLimit' in stock_data.columns:
                            stock_data['SurgedLimit'] = pd.to_numeric(stock_data['SurgedLimit'], errors='coerce')
                            # 只对有限值（非NaN、非inf）转换为整数，保留NaN和inf为NaN
                            finite_mask = np.isfinite(stock_data['SurgedLimit'])
                            stock_data.loc[finite_mask, 'SurgedLimit'] = stock_data.loc[finite_mask, 'SurgedLimit'].astype(int)
                        else:
                            # 如果列不存在，添加NULL值（表示缺失）
                            stock_data['SurgedLimit'] = pd.NA
                        
                        if 'DeclineLimit' in stock_data.columns:
                            stock_data['DeclineLimit'] = pd.to_numeric(stock_data['DeclineLimit'], errors='coerce')
                            # 只对有限值（非NaN、非inf）转换为整数，保留NaN和inf为NaN
                            finite_mask = np.isfinite(stock_data['DeclineLimit'])
                            stock_data.loc[finite_mask, 'DeclineLimit'] = stock_data.loc[finite_mask, 'DeclineLimit'].astype(int)
                        else:
                            stock_data['DeclineLimit'] = pd.NA
                        
                        # 只保留有完整数据的行
                        stock_data = stock_data.dropna(subset=['Close', 'Volume'])
                        
                        if not stock_data.empty:
                            batch_results[code] = stock_data
                
                logger.info(f"[线程 {thread_name}] 批次 {batch_id} 完成，获取到 {len(batch_results)} 只股票数据")
                return batch_results
                
            except Exception as e:
                logger.error(f"[线程 {thread_name}] 批次 {batch_id} 处理失败: {e}")
                return batch_results
        
        # 使用线程池并行处理
        from config import MAX_WORKERS, MAX_CONCURRENT_BATCHES
        max_workers = min(MAX_WORKERS, len(batches), MAX_CONCURRENT_BATCHES)  # 限制最大并发数
        logger.info(f"使用 {max_workers} 个线程并行处理批次（限制最大并发数）")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有批次任务
            future_to_batch = {executor.submit(process_batch, batch_info): batch_info for batch_info in batches}
            
            # 处理完成的任务
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_info = future_to_batch[future]
                
                try:
                    batch_results = future.result()
                    all_results.update(batch_results)
                    processed_count += len(batch_results)
                    
                    # 更新进度条
                    total_pbar.update(len(batch_results))
                    
                    # 显示详细进度信息
                    elapsed_time = time.time() - start_time
                    if processed_count > 0:
                        avg_time_per_stock = elapsed_time / processed_count
                        remaining_stocks = len(stock_codes) - processed_count
                        estimated_remaining_time = remaining_stocks * avg_time_per_stock
                        
                        total_pbar.set_postfix({
                            '已获取': f'{processed_count}/{len(stock_codes)}',
                            '进度': f'{processed_count/len(stock_codes)*100:.1f}%',
                            '耗时': f'{elapsed_time:.1f}s',
                            '平均': f'{avg_time_per_stock:.3f}s/只',
                            '剩余': f'{estimated_remaining_time:.1f}s',
                            '本批': f'{len(batch_results)}只'
                        })
                    
                except Exception as e:
                    logger.error(f"批次 {batch_info[0]} 处理失败: {e}")
                    total_pbar.update(len(batch_info[1]))
        
        total_pbar.close()
        
        total_time = time.time() - start_time
        logger.info(f"批量获取完成，总耗时: {total_time:.1f}秒")
        logger.info(f"成功获取 {len(all_results)} 只股票的数据")
        logger.info(f"平均每只股票获取时间: {total_time/len(stock_codes):.3f}秒")
        
        return all_results

    def get_capital_flow_data(self, secu_code: str, trading_date: date) -> Optional[pd.DataFrame]:
        """
        获取股票指定日期的资金流向数据（单只股票）
        
        参数:
        - secu_code: 股票代码
        - trading_date: 交易日期
        
        返回:
        - DataFrame包含资金流向数据，如果不存在则返回None
        """
        try:
            # 先查InnerCode
            sql_inner = f"""
            SELECT {TABLE_CONFIG['inner_code_field']} 
            FROM {TABLE_CONFIG['stock_info_table']} 
            WHERE {TABLE_CONFIG['code_field']} = ? AND {STOCK_FILTER}
            """
            df_inner = self.query(sql_inner, params=[secu_code])
            if df_inner.empty:
                return None
            
            inner_code = df_inner[TABLE_CONFIG['inner_code_field']].iloc[0]
            
            # 查询资金流向数据
            sql = """
            SELECT 
                ValueRange,
                BuyValue as 流入金额,
                SellValue as 流出金额,
                BuyVolume as 流入量,
                SellVolume as 流出量
            FROM QT_TradingCapitalFlow
            WHERE InnerCode = ? 
                AND TradingDate = ?
                AND QuoteType = 1
            ORDER BY ValueRange
            """
            
            df = self.query(sql, params=[inner_code, trading_date])
            
            if df.empty:
                return None
            
            return df
            
        except Exception as e:
            logger.debug(f"获取股票 {secu_code} 在 {trading_date} 的资金流向数据失败: {e}")
            return None
    
    def batch_get_capital_flow_data(self, stock_date_pairs: List[tuple]) -> Dict[tuple, pd.DataFrame]:
        """
        批量获取多只股票在多个日期的资金流向数据
        
        参数:
        - stock_date_pairs: [(股票代码, 交易日期), ...] 列表
        
        返回:
        - Dict: {(股票代码, 交易日期): DataFrame, ...}
        """
        if not stock_date_pairs:
            return {}
        
        try:
            # 获取所有股票的InnerCode
            stock_codes = list(set([code for code, _ in stock_date_pairs]))
            codes_str = ','.join([f"'{code}'" for code in stock_codes])
            
            sql_inner = f"""
            SELECT {TABLE_CONFIG['code_field']}, {TABLE_CONFIG['inner_code_field']}
            FROM {TABLE_CONFIG['stock_info_table']}
            WHERE {TABLE_CONFIG['code_field']} IN ({codes_str})
            AND {STOCK_FILTER}
            """
            
            df_inner = self.query(sql_inner)
            if df_inner.empty:
                return {}
            
            code_to_inner = dict(zip(
                df_inner[TABLE_CONFIG['code_field']].astype(str),
                df_inner[TABLE_CONFIG['inner_code_field']]
            ))
            
            # 构建批量查询SQL
            # 使用INNER JOIN批量查询
            inner_codes = list(code_to_inner.values())
            inner_codes_str = ','.join([str(ic) for ic in inner_codes])
            
            # 获取所有日期
            dates = list(set([d for _, d in stock_date_pairs]))
            dates_str = ','.join([f"'{d}'" for d in dates])
            
            sql = f"""
            SELECT 
                s.{TABLE_CONFIG['code_field']} as SecuCode,
                cf.TradingDate,
                cf.ValueRange,
                cf.BuyValue as 流入金额,
                cf.SellValue as 流出金额,
                cf.BuyVolume as 流入量,
                cf.SellVolume as 流出量
            FROM QT_TradingCapitalFlow cf
            INNER JOIN {TABLE_CONFIG['stock_info_table']} s
                ON cf.InnerCode = s.{TABLE_CONFIG['inner_code_field']}
            WHERE cf.InnerCode IN ({inner_codes_str})
                AND cf.TradingDate IN ({dates_str})
                AND cf.QuoteType = 1
            ORDER BY s.{TABLE_CONFIG['code_field']}, cf.TradingDate, cf.ValueRange
            """
            
            df = self.query(sql)
            
            if df.empty:
                return {}
            
            # 按(股票代码, 交易日期)分组
            result = {}
            for (code, date), group_df in df.groupby(['SecuCode', 'TradingDate']):
                # 确保日期类型一致
                if isinstance(date, pd.Timestamp):
                    date_val = date.date()
                else:
                    date_val = date
                
                code_str = str(code).zfill(6)
                result[(code_str, date_val)] = group_df[['ValueRange', '流入金额', '流出金额', '流入量', '流出量']].copy()
            
            return result
            
        except Exception as e:
            logger.debug(f"批量获取资金流向数据失败: {e}")
            return {}
    
    def get_stock_data(self, secu_code, days=365):
        # 先查InnerCode
        sql_inner = f"""
        SELECT {TABLE_CONFIG['inner_code_field']} FROM {TABLE_CONFIG['stock_info_table']} WHERE {TABLE_CONFIG['code_field']} = ? AND {STOCK_FILTER}
        """
        df_inner = self.query(sql_inner, params=[secu_code])
        if df_inner.empty:
            return pd.DataFrame()
        inner_code = df_inner[TABLE_CONFIG['inner_code_field']].iloc[0]
        
        # 同时获取行情数据和涨停跌停数据
        sql = f"""
        SELECT TOP {days}
            q.{TABLE_CONFIG['date_field']} as [Date],
            q.{TABLE_CONFIG['open_field']} as [Open],
            q.{TABLE_CONFIG['high_field']} as [High],
            q.{TABLE_CONFIG['low_field']} as [Low],
            q.{TABLE_CONFIG['close_field']} as [Close],
            q.{TABLE_CONFIG['volume_field']} as [Volume],
            q.{TABLE_CONFIG['turnover_rate_field']} as [TurnoverRate],
            q.{TABLE_CONFIG['negotiable_mv_field']} as [NegotiableMV],
            p.SurgedLimit as SurgedLimit,
            p.DeclineLimit as DeclineLimit
        FROM {TABLE_CONFIG['daily_quote_table']} q
        LEFT JOIN QT_PerformanceData p 
            ON q.{TABLE_CONFIG['inner_code_field']} = p.InnerCode 
            AND q.{TABLE_CONFIG['date_field']} = p.TradingDay
        WHERE q.{TABLE_CONFIG['inner_code_field']} = ?
        ORDER BY q.{TABLE_CONFIG['date_field']} DESC
        """
        df = self.query(sql, params=[inner_code])
        if not df.empty:
            df = df.sort_values('Date')
            df = df.set_index('Date')
            numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'TurnoverRate', 'NegotiableMV']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            # SurgedLimit和DeclineLimit转换为整数（0或1），保留NULL值
            # 重要：只对有限值（非NaN、非inf）转换为整数，保留缺失为NaN
            if 'SurgedLimit' in df.columns:
                df['SurgedLimit'] = pd.to_numeric(df['SurgedLimit'], errors='coerce')
                # 只对有限值进行整数转换，保留 NaN 和 inf 为 NaN
                finite_mask = np.isfinite(df['SurgedLimit'])
                df.loc[finite_mask, 'SurgedLimit'] = df.loc[finite_mask, 'SurgedLimit'].astype(int)
            if 'DeclineLimit' in df.columns:
                df['DeclineLimit'] = pd.to_numeric(df['DeclineLimit'], errors='coerce')
                # 只对有限值进行整数转换，保留 NaN 和 inf 为 NaN
                finite_mask = np.isfinite(df['DeclineLimit'])
                df.loc[finite_mask, 'DeclineLimit'] = df.loc[finite_mask, 'DeclineLimit'].astype(int)
            df = df.dropna(subset=['Close', 'Volume'])
        return df

    def get_fundamental_data(self, start_date, end_date):
        sql = f"""
        SELECT
            s.SecuCode, s.SecuAbbr, i.FirstIndustryName, i.SecondIndustryName, i.ThirdIndustryName,
            m.*,
            m.EndDate
        FROM LC_MainIndexNew m
        LEFT JOIN LC_ExgIndustry i ON m.CompanyCode = i.CompanyCode
        LEFT JOIN SecuMain s ON m.CompanyCode = s.CompanyCode
        WHERE i.Standard = '38'
          AND s.SecuCategory = 1
          AND m.EndDate >= '{start_date}'
          AND m.EndDate <= '{end_date}'
        """
        df = self.query(sql)
        if 'SecuAbbr' in df.columns:
            df = df[~df['SecuAbbr'].str.contains('ST', case=False, na=False)]
        return df

    def query_table(self, table_key, fields=None, where=None, limit=100, order_by=None, params=None):
        schema = TABLES[table_key]
        table = schema['table']
        if fields is None:
            fields = schema['fields']
        fields_str = ', '.join(fields)
        sql = f"SELECT TOP {limit} {fields_str} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        return self.query(sql, params=params)

    def get_stock_basic_info(self, secu_code):
        """
        获取股票基本信息
        Args:
            secu_code: 股票代码
        Returns:
            dict: 股票基本信息，包含SecuCode, SecuAbbr, CompanyCode, InnerCode, FirstIndustryName, SecondIndustryName, ThirdIndustryName
        """
        try:
            sql = f"""
            SELECT TOP 1
                s.SecuCode, s.SecuAbbr, s.CompanyCode, s.InnerCode,
                i.FirstIndustryName, i.SecondIndustryName, i.ThirdIndustryName
            FROM SecuMain s
            LEFT JOIN LC_ExgIndustry i ON s.CompanyCode = i.CompanyCode
                AND i.Standard = '38'
                AND i.IfPerformed = 1
            WHERE s.SecuCode = ? AND s.SecuCategory = 1
            """
            df = self.query(sql, params=[secu_code])
            
            if df.empty:
                return None
            
            # 返回第一行数据作为字典
            return df.iloc[0].to_dict()
            
        except Exception as e:
            logger.error(f"获取股票 {secu_code} 基本信息时出错: {e}")
            return None
    
    def batch_get_stock_basic_info(self, secu_codes: List[str]) -> Dict[str, Dict]:
        """
        批量获取股票基本信息（高性能版本）
        Args:
            secu_codes: 股票代码列表
        Returns:
            dict: {股票代码: 股票基本信息字典}
        """
        if not secu_codes:
            return {}
        
        try:
            # 构建IN子句
            codes_str = ','.join([f"'{code}'" for code in secu_codes])
            
            sql = f"""
            SELECT
                s.SecuCode, s.SecuAbbr, s.CompanyCode, s.InnerCode,
                i.FirstIndustryName, i.SecondIndustryName, i.ThirdIndustryName
            FROM SecuMain s
            LEFT JOIN LC_ExgIndustry i ON s.CompanyCode = i.CompanyCode
                AND i.Standard = '38'
                AND i.IfPerformed = 1
            WHERE s.SecuCode IN ({codes_str}) 
            AND s.SecuCategory = 1
            """
            
            df = self.query(sql)
            
            if df.empty:
                logger.warning(f"批量获取股票信息：查询返回空结果，股票代码: {secu_codes[:10]}")
                return {}
            
            # 转换为字典格式
            result = {}
            for _, row in df.iterrows():
                code = str(row['SecuCode']).strip()
                # 处理浮点数格式
                if '.' in code:
                    code = code.split('.')[0]
                code = code.zfill(6)
                result[code] = row.to_dict()
            
            # 检查是否有股票代码未找到
            found_codes = set(result.keys())
            requested_codes = set([str(c).strip().zfill(6) for c in secu_codes])
            missing_codes = requested_codes - found_codes
            if missing_codes:
                logger.warning(f"批量获取股票信息：{len(missing_codes)} 只股票未找到，示例: {list(missing_codes)[:10]}")
            
            return result
            
        except Exception as e:
            logger.error(f"批量获取股票基本信息时出错: {e}")
            return {}

    def get_adjusting_factors(self, inner_codes):
        """批量获取复权因子数据（优化：增大批次，减少查询次数）"""
        if not inner_codes:
            return {}
        
        logger.info(f"开始获取 {len(inner_codes)} 只股票的复权因子数据...")
        start_time = time.time()
        
        # 性能优化：增大批次大小，减少查询次数，充分利用网络带宽
        batch_size = 500  # 每批处理500只股票（大幅增加批次大小）
        all_results = {}
        
        for i in range(0, len(inner_codes), batch_size):
            batch_codes = inner_codes[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(inner_codes) + batch_size - 1) // batch_size
            logger.info(f"处理复权因子批次 {batch_num}/{total_batches}，包含 {len(batch_codes)} 只股票")
            
            # 构建批量查询SQL
            codes_str = ','.join([str(code) for code in batch_codes])
            sql = f"""
            SELECT 
                InnerCode,
                ExDiviDate,
                AdjustingFactor,
                AdjustingConst,
                RatioAdjustingFactor
            FROM QT_AdjustingFactor
            WHERE InnerCode IN ({codes_str})
            ORDER BY InnerCode, ExDiviDate DESC
            """
            
            try:
                batch_start = time.time()
                df = self.query(sql)
                batch_query_time = time.time() - batch_start
                
                if df.empty:
                    logger.warning(f"复权因子批次 {batch_num} 查询结果为空（耗时: {batch_query_time:.2f}秒）")
                    continue
                
                # 按InnerCode分组
                for inner_code in batch_codes:
                    stock_factors = df[df['InnerCode'] == inner_code].copy()
                    if not stock_factors.empty:
                        # 按日期排序（从新到旧）
                        stock_factors = stock_factors.sort_values('ExDiviDate', ascending=False)
                        all_results[inner_code] = stock_factors
                
                batch_time = time.time() - start_time
                logger.info(f"复权因子批次 {batch_num} 完成（查询耗时: {batch_query_time:.2f}秒），已获取 {len(all_results)} 只股票复权因子")
                
            except Exception as e:
                logger.error(f"复权因子批次 {batch_num} 获取失败: {e}")
                continue
        
        total_time = time.time() - start_time
        logger.info(f"复权因子数据获取完成，总耗时: {total_time:.1f}秒，成功获取 {len(all_results)} 只股票复权因子")
        
        return all_results

    def calculate_adjusted_prices(self, stock_data, adjusting_factors):
        """计算复权价格（前复权）"""
        if stock_data.empty or not adjusting_factors:
            return stock_data
        
        adjusted_data = stock_data.copy()
        inner_code = stock_data.iloc[0]['InnerCode'] if 'InnerCode' in stock_data.columns else None
        
        if not inner_code or inner_code not in adjusting_factors:
            return stock_data
        
        factors = adjusting_factors[inner_code]
        
        # 对每个交易日计算复权价格
        for idx, row in adjusted_data.iterrows():
            trading_date = row.name  # 假设索引是日期
            
            # 找到该日期之前最近的复权因子
            applicable_factors = factors[factors['ExDiviDate'] <= trading_date]
            if not applicable_factors.empty:
                # 使用最新的复权因子
                factor = applicable_factors.iloc[0]
                
                # 前复权价格计算
                if pd.notna(factor['AdjustingFactor']) and pd.notna(factor['AdjustingConst']):
                    adjusted_data.loc[idx, 'Close'] = (
                        row['Close'] * factor['AdjustingFactor'] + factor['AdjustingConst']
                    )
                    adjusted_data.loc[idx, 'Open'] = (
                        row['Open'] * factor['AdjustingFactor'] + factor['AdjustingConst']
                    )
                    adjusted_data.loc[idx, 'High'] = (
                        row['High'] * factor['AdjustingFactor'] + factor['AdjustingConst']
                    )
                    adjusted_data.loc[idx, 'Low'] = (
                        row['Low'] * factor['AdjustingFactor'] + factor['AdjustingConst']
                    )
        
        return adjusted_data

    def batch_get_stock_data_with_adjustment(self, stock_codes, days=365, time_config=None):
        """批量获取股票数据并计算复权价格"""
        if not stock_codes:
            return {}
        
        logger.info(f"开始批量获取 {len(stock_codes)} 只股票的行情数据（含复权价格）...")
        start_time = time.time()
        
        # 根据时间配置调整数据获取范围
        if time_config is not None:
            # 计算需要获取的数据天数
            # 优先使用data_start_date和data_end_date（模块17使用）
            # 如果没有，则使用crash_start_date和crash_end_date（模块2使用，向后兼容）
            if hasattr(time_config, 'data_start_date') and hasattr(time_config, 'data_end_date'):
                start_date = pd.Timestamp(time_config.data_start_date)
                end_date = pd.Timestamp(time_config.data_end_date)
                analysis_date = end_date
                days_needed = (end_date - start_date).days + 100  # 多获取100天作为缓冲
            elif hasattr(time_config, 'crash_start_date') and hasattr(time_config, 'crash_end_date'):
                analysis_date = pd.Timestamp(time_config.analysis_date) if hasattr(time_config, 'analysis_date') else pd.Timestamp(time_config.crash_end_date)
                crash_start_date = pd.Timestamp(time_config.crash_start_date)
                days_needed = (analysis_date - crash_start_date).days + 100  # 多获取100天作为缓冲
            else:
                # 如果没有日期配置，使用analysis_date
                analysis_date = pd.Timestamp(time_config.analysis_date) if hasattr(time_config, 'analysis_date') else pd.Timestamp.now()
                days_needed = days + 100  # 使用传入的days参数，多获取100天作为缓冲
            
            days = max(days, days_needed)
            logger.info(f"根据时间配置调整数据获取范围: {days} 天")
        
        # 首先获取所有股票的InnerCode
        # 注意：这里不应该再次应用STOCK_FILTER，因为传入的stock_codes已经是筛选过的
        codes_str = ','.join([f"'{code}'" for code in stock_codes])
        sql_inner_codes = f"""
        SELECT SecuCode, InnerCode
        FROM SecuMain
        WHERE SecuCode IN ({codes_str})
        """
        
        df_inner_codes = self.query(sql_inner_codes)
        if df_inner_codes.empty:
            logger.warning(f"未找到有效的股票InnerCode（输入的{len(stock_codes)}只股票在SecuMain中都不存在）")
            return {}
        
        # 检查是否有股票在SecuMain中不存在
        found_codes = set(df_inner_codes['SecuCode'].astype(str))
        input_codes = set([str(code).zfill(6) for code in stock_codes])
        missing_codes = input_codes - found_codes
        if missing_codes:
            logger.warning(f"以下{len(missing_codes)}只股票在SecuMain中不存在: {list(missing_codes)[:10]}{'...' if len(missing_codes) > 10 else ''}")
        
        # 创建股票代码到InnerCode的映射
        code_to_inner = dict(zip(df_inner_codes['SecuCode'].astype(str), df_inner_codes['InnerCode']))
        inner_codes = list(code_to_inner.values())
        
        logger.info(f"[batch_get_stock_data_with_adjustment] 输入{len(stock_codes)}只股票，找到{len(code_to_inner)}只股票的InnerCode")
        
        # 获取复权因子数据
        adjusting_factors = self.get_adjusting_factors(inner_codes)
        
        # 分批处理，避免SQL语句过长
        # 极限优化：使用非常小的批次大小，最大化并行度，压榨硬件性能
        # 目标：产生尽可能多的批次，充分利用多线程并行
        total_stocks = len(stock_codes)
        from config import MAX_WORKERS
        
        # 极限优化策略：批次大小 = 10-20只股票，产生大量批次
        # 这样可以让更多线程同时工作，最大化硬件利用率
        if days > 800:  # 超过800天（约3年），使用很小批次
            batch_size = 15  # 极小批次，最大化并行度
        elif days > 400:  # 1-3年数据
            batch_size = 20  # 小批次
        else:  # 1年以内数据
            batch_size = 25  # 小批次，但比长期数据稍大
        batches = []
        # 只处理能获取到InnerCode的股票
        valid_stock_codes = [code for code in stock_codes if str(code).zfill(6) in code_to_inner]
        total_batches = (len(valid_stock_codes) + batch_size - 1) // batch_size
        
        for i in range(0, len(valid_stock_codes), batch_size):
            batch_codes = valid_stock_codes[i:i + batch_size]
            batches.append({
                'batch_id': i // batch_size + 1,
                'total_batches': total_batches,
                'codes': batch_codes,
                'code_to_inner': code_to_inner,
                'adjusting_factors': adjusting_factors,
                'days': days,
                'stock_codes': stock_codes,  # 保存原始输入的股票列表，用于调试输出
                'time_config': time_config
            })
        
        def process_batch(batch_info):
            """处理单个批次"""
            batch_id = batch_info['batch_id']
            total_batches = batch_info['total_batches']
            batch_codes = batch_info['codes']
            code_to_inner = batch_info['code_to_inner']
            adjusting_factors = batch_info['adjusting_factors']
            days = batch_info['days']
            time_config = batch_info.get('time_config')
            
            thread_name = threading.current_thread().name
            logger.info(f"[线程 {thread_name}] 处理批次 {batch_id}/{total_batches}，包含 {len(batch_codes)} 只股票")
            
            batch_results = {}
            
            # 构建批量查询SQL
            codes_str = ','.join([f"'{code}'" for code in batch_codes])
            
            # 根据时间配置调整SQL查询
            if time_config is not None:
                # 优先使用data_start_date和data_end_date（模块17使用）
                # 如果没有，则使用crash_start_date和crash_end_date（模块2使用，向后兼容）
                if hasattr(time_config, 'data_start_date') and hasattr(time_config, 'data_end_date'):
                    start_date = pd.Timestamp(time_config.data_start_date)
                    end_date = pd.Timestamp(time_config.data_end_date)
                elif hasattr(time_config, 'crash_start_date') and hasattr(time_config, 'crash_end_date'):
                    start_date = pd.Timestamp(time_config.crash_start_date)
                    end_date = pd.Timestamp(time_config.crash_end_date)
                else:
                    # 如果没有日期配置，使用analysis_date
                    analysis_date = pd.Timestamp(time_config.analysis_date) if hasattr(time_config, 'analysis_date') else pd.Timestamp.now()
                    start_date = analysis_date - pd.Timedelta(days=days + 30)
                    end_date = analysis_date + pd.Timedelta(days=10)
                
                # 计算查询的日期范围，确保包含所需的时间段
                query_start_date = start_date - pd.Timedelta(days=50)  # 多获取50天作为缓冲
                query_end_date = end_date + pd.Timedelta(days=50)  # 多获取50天作为缓冲
                
                sql = f"""
                SELECT 
                    s.{TABLE_CONFIG['code_field']},
                    s.{TABLE_CONFIG['inner_code_field']},
                    q.{TABLE_CONFIG['date_field']} as [Date],
                    q.{TABLE_CONFIG['open_field']} as [Open],
                    q.{TABLE_CONFIG['high_field']} as [High],
                    q.{TABLE_CONFIG['low_field']} as [Low],
                    q.{TABLE_CONFIG['close_field']} as [Close],
                    q.{TABLE_CONFIG['volume_field']} as [Volume],
                    q.{TABLE_CONFIG['turnover_rate_field']} as [TurnoverRate],
                    q.{TABLE_CONFIG['negotiable_mv_field']} as [NegotiableMV],
                    q.BetaLargeCapIndex as BetaLargeCapIndex,
                    q.BetaCompositeIndex as BetaCompositeIndex,
                    q.BetaSYWGIndustryIndex as BetaSYWGIndustryIndex,
                    q.BetaMidCapIndex as BetaMidCapIndex,
                    q.AlphaLargeCapIndex as AlphaLargeCapIndex,
                    q.AlphaCompositeIndex as AlphaCompositeIndex,
                    q.AlphaSYWGIndustryIndex as AlphaSYWGIndustryIndex,
                    q.AlphMidCapIndex as AlphMidCapIndex,
                    q.YearVolatilityByDay as YearVolatilityByDay,
                    q.YearVolatilityByWeek as YearVolatilityByWeek,
                    q.YearSharpeRatio as YearSharpeRatio,
                    q.MarketIndexRORArithAvg as MarketIndexRORArithAvg,
                    q.MarketIndexRORGeomMean as MarketIndexRORGeomMean,
                    p.SurgedLimit as SurgedLimit,
                    p.DeclineLimit as DeclineLimit
                FROM {TABLE_CONFIG['stock_info_table']} s
                INNER JOIN {TABLE_CONFIG['daily_quote_table']} q 
                    ON s.{TABLE_CONFIG['inner_code_field']} = q.{TABLE_CONFIG['inner_code_field']}
                LEFT JOIN QT_PerformanceData p 
                    ON q.{TABLE_CONFIG['inner_code_field']} = p.InnerCode 
                    AND q.{TABLE_CONFIG['date_field']} = p.TradingDay
                WHERE s.{TABLE_CONFIG['code_field']} IN ({codes_str})
                AND q.{TABLE_CONFIG['date_field']} >= CAST('{query_start_date.date()}' AS DATE)
                AND q.{TABLE_CONFIG['date_field']} <= CAST('{query_end_date.date()}' AS DATE)
                ORDER BY s.{TABLE_CONFIG['code_field']}, q.{TABLE_CONFIG['date_field']} DESC
                """
                
                # 调试：打印SQL（仅第一个批次）
                if batch_id == 1:
                    logger.debug(f"[调试] batch_get_stock_data_with_adjustment SQL查询")
                    # 计算实际传入的股票总数
                    actual_total_stocks = len(code_to_inner)  # 实际能获取到InnerCode的股票数
                    input_total = len(stock_codes) if 'stock_codes' in batch_info else len(batch_codes)
                    logger.debug(f"输入股票数: {input_total}只，找到InnerCode: {actual_total_stocks}只，总批次数: {total_batches}批")
                    print(f"当前批次: {batch_id}/{total_batches}")
                    print(f"本批次股票数: {len(batch_codes)}只")
                    print(f"批次大小: {len(batch_codes)}只/批 (根据数据天数动态调整)")
                    print(f"股票代码: {batch_codes[:5]}... (共{len(batch_codes)}只)")
                    logger.debug(f"日期范围: {query_start_date.date()} 至 {query_end_date.date()}")
                    logger.debug(f"实际执行的SQL:\n{sql}")
                
                df = self.query(sql)
                
                # 调试：检查查询结果（仅第一个批次，即使为空也输出）
                if batch_id == 1:
                    logger.debug(f"[调试] 查询结果统计:")
                    if df.empty:
                        logger.warning(f"查询结果为空！可能的原因: 1. 日期范围问题 2. 股票代码不存在 3. STOCK_FILTER筛选条件太严格")
                        # 测试查询1：检查SecuMain
                        test_sql1 = f"""
                        SELECT TOP 5
                            s.{TABLE_CONFIG['code_field']},
                            COUNT(*) as record_count
                        FROM {TABLE_CONFIG['stock_info_table']} s
                        WHERE s.{TABLE_CONFIG['code_field']} IN ({codes_str})
                        AND {STOCK_FILTER}
                        GROUP BY s.{TABLE_CONFIG['code_field']}
                        """
                        # 测试查询2：检查QT_StockPerformance（去掉日期限制）
                        test_sql2 = f"""
                        SELECT TOP 5
                            s.{TABLE_CONFIG['code_field']},
                            MIN(q.{TABLE_CONFIG['date_field']}) as min_date,
                            MAX(q.{TABLE_CONFIG['date_field']}) as max_date,
                            COUNT(*) as record_count
                        FROM {TABLE_CONFIG['stock_info_table']} s
                        INNER JOIN {TABLE_CONFIG['daily_quote_table']} q 
                            ON s.{TABLE_CONFIG['inner_code_field']} = q.{TABLE_CONFIG['inner_code_field']}
                        WHERE s.{TABLE_CONFIG['code_field']} IN ({codes_str})
                        AND {STOCK_FILTER}
                        GROUP BY s.{TABLE_CONFIG['code_field']}
                        """
                        logger.debug(f"测试1: 检查SecuMain中的股票")
                        try:
                            test_df1 = self.query(test_sql1)
                            if not test_df1.empty:
                                logger.debug(f"测试1结果: 找到 {len(test_df1)} 只股票在SecuMain中")
                            else:
                                logger.warning(f"测试1结果: 这 {len(batch_codes)} 只股票都不满足STOCK_FILTER条件")
                        except Exception as e:
                            logger.warning(f"测试1查询失败: {e}")
                        
                        logger.debug(f"测试2: 检查QT_StockPerformance中的行情数据（无日期限制）")
                        try:
                            test_df2 = self.query(test_sql2)
                            if not test_df2.empty:
                                logger.debug(f"测试2结果: 找到 {len(test_df2)} 只股票有行情数据")
                                # 检查日期范围
                                if 'min_date' in test_df2.columns and 'max_date' in test_df2.columns:
                                    min_date = test_df2['min_date'].min()
                                    max_date = test_df2['max_date'].max()
                                    # 确保日期类型一致（转换为date对象进行比较）
                                    if isinstance(min_date, pd.Timestamp):
                                        min_date = min_date.date()
                                    if isinstance(max_date, pd.Timestamp):
                                        max_date = max_date.date()
                                    logger.debug(f"行情数据日期范围: {min_date} 至 {max_date}")
                                    logger.debug(f"查询日期范围: {query_start_date.date()} 至 {query_end_date.date()}")
                                    if query_end_date.date() > max_date:
                                        logger.warning(f"查询结束日期 {query_end_date.date()} 超过最大行情日期 {max_date}")
                                    if query_start_date.date() < min_date:
                                        logger.warning(f"查询开始日期 {query_start_date.date()} 早于最小行情日期 {min_date}")
                                    # 检查日期范围是否完全不重叠
                                    if query_end_date.date() < min_date or query_start_date.date() > max_date:
                                        logger.warning(f"查询日期范围与数据日期范围完全不重叠！这些可能是新上市股票，在查询日期范围内没有数据。")
                            else:
                                logger.warning(f"测试2结果: 这 {len(batch_codes)} 只股票在QT_StockPerformance中没有数据")
                        except Exception as e:
                            logger.warning(f"测试2查询失败: {e}")
                    else:
                        logger.debug(f"列名: {list(df.columns)}, 行数: {len(df)}")
                        if 'SurgedLimit' in df.columns:
                            surged_limit_count = df['SurgedLimit'].notna().sum()
                            surged_limit_1_count = (df['SurgedLimit'] == 1).sum()
                            logger.debug(f"SurgedLimit字段统计: 非空数量={surged_limit_count}/{len(df)}, 值为1数量={surged_limit_1_count}")
                        else:
                            logger.warning(f"查询结果中缺少SurgedLimit列！可用列: {list(df.columns)}")
            else:
                # 优化：使用窗口函数限制每只股票只获取最近days天的数据，大幅提升查询速度
                # 计算截止日期（最近days天）
                from datetime import date, timedelta
                # 优化：使用数据库最新交易日，而不是当前自然日（提升准确性）
                try:
                    end_date = self.get_latest_trading_date()
                except:
                    end_date = date.today()
                start_date = end_date - timedelta(days=days + 30)  # 多获取30天作为缓冲
                
                # 性能优化：使用更高效的SQL查询
                # 1. 先过滤日期范围，减少窗口函数计算的数据量
                # 2. 使用TOP而不是ROW_NUMBER，在某些情况下更快
                # 3. 优化JOIN顺序，先过滤再JOIN
                sql = f"""
                WITH FilteredQuotes AS (
                    SELECT 
                        q.{TABLE_CONFIG['inner_code_field']},
                        q.{TABLE_CONFIG['date_field']} as [Date],
                        q.{TABLE_CONFIG['open_field']} as [Open],
                        q.{TABLE_CONFIG['high_field']} as [High],
                        q.{TABLE_CONFIG['low_field']} as [Low],
                        q.{TABLE_CONFIG['close_field']} as [Close],
                        q.{TABLE_CONFIG['volume_field']} as [Volume],
                        q.{TABLE_CONFIG['turnover_rate_field']} as [TurnoverRate],
                        q.{TABLE_CONFIG['negotiable_mv_field']} as [NegotiableMV],
                        q.BetaLargeCapIndex as BetaLargeCapIndex,
                        q.BetaCompositeIndex as BetaCompositeIndex,
                        q.BetaSYWGIndustryIndex as BetaSYWGIndustryIndex,
                        q.BetaMidCapIndex as BetaMidCapIndex,
                        q.AlphaLargeCapIndex as AlphaLargeCapIndex,
                        q.AlphaCompositeIndex as AlphaCompositeIndex,
                        q.AlphaSYWGIndustryIndex as AlphaSYWGIndustryIndex,
                        q.AlphMidCapIndex as AlphMidCapIndex,
                        q.YearVolatilityByDay as YearVolatilityByDay,
                        q.YearVolatilityByWeek as YearVolatilityByWeek,
                        q.YearSharpeRatio as YearSharpeRatio,
                        q.MarketIndexRORArithAvg as MarketIndexRORArithAvg,
                        q.MarketIndexRORGeomMean as MarketIndexRORGeomMean
                    FROM {TABLE_CONFIG['daily_quote_table']} q
                    WHERE q.{TABLE_CONFIG['date_field']} >= CAST('{start_date}' AS DATE)
                    AND q.{TABLE_CONFIG['date_field']} <= CAST('{end_date}' AS DATE)
                ),
                RankedData AS (
                    SELECT 
                        s.{TABLE_CONFIG['code_field']} as SecuCode,
                        s.{TABLE_CONFIG['inner_code_field']} as InnerCode,
                        fq.Date,
                        fq.[Open],
                        fq.[High],
                        fq.[Low],
                        fq.[Close],
                        fq.[Volume],
                        fq.[TurnoverRate],
                        fq.[NegotiableMV],
                        fq.BetaLargeCapIndex,
                        fq.BetaCompositeIndex,
                        fq.BetaSYWGIndustryIndex,
                        fq.BetaMidCapIndex,
                        fq.AlphaLargeCapIndex,
                        fq.AlphaCompositeIndex,
                        fq.AlphaSYWGIndustryIndex,
                        fq.AlphMidCapIndex,
                        fq.YearVolatilityByDay,
                        fq.YearVolatilityByWeek,
                        fq.YearSharpeRatio,
                        fq.MarketIndexRORArithAvg,
                        fq.MarketIndexRORGeomMean,
                        p.SurgedLimit as SurgedLimit,
                        p.DeclineLimit as DeclineLimit,
                        ROW_NUMBER() OVER (PARTITION BY s.{TABLE_CONFIG['code_field']} ORDER BY fq.Date DESC) as rn
                    FROM {TABLE_CONFIG['stock_info_table']} s
                    INNER JOIN FilteredQuotes fq 
                        ON s.{TABLE_CONFIG['inner_code_field']} = fq.{TABLE_CONFIG['inner_code_field']}
                    LEFT JOIN QT_PerformanceData p 
                        ON fq.{TABLE_CONFIG['inner_code_field']} = p.InnerCode 
                        AND fq.Date = p.TradingDay
                    WHERE s.{TABLE_CONFIG['code_field']} IN ({codes_str})
                    AND {STOCK_FILTER}
                )
                SELECT 
                    SecuCode,
                    InnerCode,
                    Date,
                    [Open],
                    [High],
                    [Low],
                    [Close],
                    [Volume],
                    [TurnoverRate],
                    [NegotiableMV],
                    BetaLargeCapIndex,
                    BetaCompositeIndex,
                    BetaSYWGIndustryIndex,
                    BetaMidCapIndex,
                    AlphaLargeCapIndex,
                    AlphaCompositeIndex,
                    AlphaSYWGIndustryIndex,
                    AlphMidCapIndex,
                    YearVolatilityByDay,
                    YearVolatilityByWeek,
                    YearSharpeRatio,
                    MarketIndexRORArithAvg,
                    MarketIndexRORGeomMean,
                    SurgedLimit,
                    DeclineLimit
                FROM RankedData
                WHERE rn <= {days + 30}
                ORDER BY SecuCode, Date DESC
                """
            
            try:
                df = self.query(sql)
                if df.empty:
                    return batch_results
                
                # 统一字段名：SQL返回的可能是别名，需要映射回标准字段名
                if 'SecuCode' in df.columns:
                    df = df.rename(columns={'SecuCode': TABLE_CONFIG['code_field']})
                if 'InnerCode' in df.columns:
                    df = df.rename(columns={'InnerCode': TABLE_CONFIG['inner_code_field']})
                
                # 按股票代码分组
                for code in batch_codes:
                    stock_data = df[df[TABLE_CONFIG['code_field']] == code].copy()
                    if not stock_data.empty:
                        stock_data = stock_data.sort_values('Date')
                        stock_data = stock_data.set_index('Date')
                        
                        # 根据时间配置调整数据范围
                        if time_config is not None:
                            # 确保数据包含所需的时间范围
                            # 优先使用data_start_date和data_end_date（模块17使用）
                            # 如果没有，则使用crash_start_date和crash_end_date（模块2使用，向后兼容）
                            if hasattr(time_config, 'data_start_date') and hasattr(time_config, 'data_end_date'):
                                start_date = pd.Timestamp(time_config.data_start_date)
                                end_date = pd.Timestamp(time_config.data_end_date)
                                analysis_date = end_date
                            elif hasattr(time_config, 'crash_start_date') and hasattr(time_config, 'crash_end_date'):
                                analysis_date = pd.Timestamp(time_config.analysis_date) if hasattr(time_config, 'analysis_date') else pd.Timestamp(time_config.crash_end_date)
                                start_date = pd.Timestamp(time_config.crash_start_date)
                            else:
                                # 如果没有日期配置，使用analysis_date
                                analysis_date = pd.Timestamp(time_config.analysis_date) if hasattr(time_config, 'analysis_date') else pd.Timestamp.now()
                                start_date = analysis_date - pd.Timedelta(days=365)
                                end_date = analysis_date
                            
                            # 筛选包含所需时间范围的数据
                            mask = (stock_data.index >= start_date - pd.Timedelta(days=30)) & (stock_data.index <= end_date + pd.Timedelta(days=30))
                            stock_data = stock_data[mask]
                        
                        # 数据类型转换
                        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'TurnoverRate', 'NegotiableMV']
                        for col in numeric_columns:
                            if col in stock_data.columns:
                                stock_data[col] = pd.to_numeric(stock_data[col], errors='coerce')
                        
                        # SurgedLimit和DeclineLimit转换为整数（0或1）
                        # 重要：保留NULL值，不要填充为0，这样可以区分"数据缺失"和"非涨停"
                        # 数据校验：确保SurgedLimit字段存在
                        if 'SurgedLimit' in stock_data.columns:
                            # 转换为数值类型，但保留NULL值（不填充为0）
                            stock_data['SurgedLimit'] = pd.to_numeric(stock_data['SurgedLimit'], errors='coerce')
                            # 只对有限值（非NaN、非inf）转换为整数，保留NaN和inf为NaN
                            try:
                                finite_mask = np.isfinite(stock_data['SurgedLimit'])
                                stock_data.loc[finite_mask, 'SurgedLimit'] = stock_data.loc[finite_mask, 'SurgedLimit'].astype(int)
                            except (ValueError, TypeError) as e:
                                # 如果转换失败，保留为NaN（可能是数据异常）
                                logger.debug(f"股票 {code} 的SurgedLimit转换失败: {e}，保留为NaN")
                                stock_data['SurgedLimit'] = pd.NA
                        else:
                            # 如果缺少SurgedLimit字段，记录警告并跳过（避免浪费时间）
                            logger.debug(f"股票 {code} 的数据缺少SurgedLimit字段，跳过该股票")
                            continue
                        
                        if 'DeclineLimit' in stock_data.columns:
                            # 同样保留NULL值
                            stock_data['DeclineLimit'] = pd.to_numeric(stock_data['DeclineLimit'], errors='coerce')
                            # 只对有限值（非NaN、非inf）转换为整数，保留NaN和inf为NaN
                            try:
                                finite_mask = np.isfinite(stock_data['DeclineLimit'])
                                stock_data.loc[finite_mask, 'DeclineLimit'] = stock_data.loc[finite_mask, 'DeclineLimit'].astype(int)
                            except (ValueError, TypeError) as e:
                                # 如果转换失败，保留为NaN（可能是数据异常）
                                logger.debug(f"股票 {code} 的DeclineLimit转换失败: {e}，保留为NaN")
                                stock_data['DeclineLimit'] = pd.NA
                        else:
                            stock_data['DeclineLimit'] = pd.NA  # 使用pd.NA表示缺失
                        
                        # 只保留有完整数据的行
                        stock_data = stock_data.dropna(subset=['Close', 'Volume'])
                        
                        # 数据校验：确保SurgedLimit字段有有效值（至少有一些非空值）
                        if not stock_data.empty:
                            # 验证SurgedLimit字段是否有效（至少有一些数据有值）
                            surged_limit_valid = stock_data['SurgedLimit'].notna().any()
                            if not surged_limit_valid:
                                # 只记录到日志，不打印到控制台（避免刷屏）
                                logger.debug(f"股票 {code} 的SurgedLimit字段全部为NULL，数据可能不完整")
                            
                            # 计算复权价格
                            inner_code = code_to_inner.get(code)
                            if inner_code and inner_code in adjusting_factors:
                                stock_data = self.calculate_adjusted_prices(stock_data, adjusting_factors)
                            
                            batch_results[code] = stock_data
                
                logger.info(f"[线程 {thread_name}] 批次 {batch_id} 完成，获取 {len(batch_results)} 只股票数据")
                return batch_results
                
            except Exception as e:
                # 只记录到日志，不打印到控制台（避免刷屏）
                logger.error(f"[线程 {thread_name}] 批次 {batch_id} 批量获取股票数据失败: {e}", exc_info=True)
                return batch_results
        
        # 使用多线程并行处理批次
        import concurrent.futures
        import threading
        
        from config import MAX_WORKERS, MAX_CONCURRENT_BATCHES
        # 极限优化：使用极小批次 + 极限高并发，最大化硬件利用率
        # 批次大小已经很小（15-25只），可以大幅提高并发数
        # 目标：让尽可能多的线程同时工作，压榨硬件性能
        max_workers = min(MAX_WORKERS * 5, len(batches), MAX_CONCURRENT_BATCHES, 500)  # 极限提高并发数
        logger.info(f"使用 {max_workers} 个线程并行处理 {len(batches)} 个批次（批次大小: {batch_size}，极限高并发模式）")
        
        all_results = {}
        
        # 使用tqdm显示实时进度条
        from tqdm import tqdm
        import sys
        
        # 确保进度条能够正确显示（刷新输出）
        # 使用print而不是logger，确保进度条前有提示信息
        print(f"\n  📥 开始批量获取数据: {len(batches)} 个批次，{max_workers} 个线程并行处理...", flush=True)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_batch = {executor.submit(process_batch, batch_info): batch_info for batch_info in batches}
            
            # 创建进度条（优化显示格式，显示正在执行的批次）
            # 使用mininterval=0.1确保进度条频繁更新
            pbar = tqdm(
                total=len(batches), 
                desc='📥 批量获取股票数据', 
                ncols=120, 
                unit='批',
                mininterval=0.1,  # 最小更新间隔0.1秒
                maxinterval=1.0,  # 最大更新间隔1秒
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}',
                dynamic_ncols=True,
                file=sys.stdout  # 确保输出到标准输出
            )
            
            # 跟踪正在执行和已完成的任务
            completed_count = 0
            running_batches = set()
            last_update_time = time.time()
            
            # 处理完成的任务
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_info = future_to_batch[future]
                batch_id = batch_info['batch_id']
                
                # 从正在执行列表中移除
                running_batches.discard(batch_id)
                
                try:
                    batch_results = future.result()
                    all_results.update(batch_results)
                    completed_count += 1
                    
                    # 更新进度条
                    pbar.update(1)
                    
                    # 显示更详细的进度信息，包括正在执行的批次数
                    total_stocks = len(all_results)
                    running_count = len([f for f in future_to_batch.keys() if not f.done()])
                    
                    # 计算速度（批次/秒）
                    current_time = time.time()
                    elapsed_time = current_time - start_time
                    if elapsed_time > 0:
                        rate = completed_count / elapsed_time
                    else:
                        rate = 0
                    
                    pbar.set_postfix_str(
                        f'已获取={total_stocks}只 | 已完成={completed_count}/{len(batches)} | '
                        f'执行中={running_count}批 | 进度={completed_count*100//len(batches)}% | '
                        f'速度={rate:.1f}批/秒'
                    )
                    
                    # 强制刷新输出
                    pbar.refresh()
                    
                except Exception as e:
                    logger.error(f"批次 {batch_id} 处理出错: {e}")
                    completed_count += 1
                    pbar.update(1)
                    pbar.refresh()
            
            pbar.close()
            print()  # 换行，确保进度条关闭后输出清晰
        
        total_time = time.time() - start_time
        
        # 最终数据校验：确保所有返回的数据都包含SurgedLimit字段
        validated_results = {}
        invalid_count = 0
        for code, data in all_results.items():
            if data is None or data.empty:
                invalid_count += 1
                continue
            if 'SurgedLimit' not in data.columns:
                logger.warning(f"数据校验失败: 股票 {code} 的数据缺少SurgedLimit字段，已排除")
                invalid_count += 1
                continue
            validated_results[code] = data
        
        if invalid_count > 0:
            logger.warning(f"数据校验: {invalid_count} 只股票的数据无效（缺少SurgedLimit字段），已排除")
        
        logger.info(f"批量数据获取完成，总耗时: {total_time:.1f}秒，成功获取 {len(validated_results)} 只股票数据（校验后）")
        
        return validated_results 

    def get_futures_data(self, option_code: str, days: int = 1250) -> pd.DataFrame:
        """
        获取商品期货数据
        
        Args:
            option_code: 品种代码，如'313'表示黄金
            days: 获取天数，默认1250天（约5年）
            
        Returns:
            pd.DataFrame: 期货数据，包含日期、价格等信息
        """
        try:
            # 计算开始日期
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            sql = """
            SELECT 
                EndDate as TradingDay,
                OptionCode,
                ContractName,
                ClosePrice,
                OpenPrice,
                HighPrice,
                LowPrice,
                Volume,
                Turnover,
                SettlePrice
            FROM Fut_DailyQuote 
            WHERE OptionCode = ?
                AND EndDate >= ?
                AND EndDate <= ?
                AND ClosePrice IS NOT NULL
                AND ClosePrice > 0
            ORDER BY EndDate ASC
            """
            
            params = [option_code, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
            
            conn = self._get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return pd.DataFrame()
            
            df = pd.read_sql(sql, conn, params=params)
            self._return_connection(conn)
            
            if df.empty:
                logger.warning(f"未找到OptionCode={option_code}的期货数据")
                return pd.DataFrame()
            
            # 数据预处理
            df['TradingDay'] = pd.to_datetime(df['TradingDay'])
            df = df.set_index('TradingDay')
            
            # 去重，保留每个交易日的最新数据
            df = df.groupby(df.index).last()
            
            logger.info(f"成功获取OptionCode={option_code}的期货数据，共{len(df)}条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取期货数据失败: {e}")
            return pd.DataFrame()
    
    def get_all_futures_options(self) -> List[str]:
        """
        获取所有期货品种代码
        
        Returns:
            List[str]: 期货品种代码列表
        """
        try:
            sql = """
            SELECT DISTINCT OptionCode
            FROM Fut_DailyQuote 
            WHERE OptionCode IS NOT NULL
                AND OptionCode != ''
            ORDER BY OptionCode
            """
            
            conn = self._get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return []
            
            df = pd.read_sql(sql, conn)
            self._return_connection(conn)
            
            if df.empty:
                logger.warning("未找到任何期货品种")
                return []
            
            option_codes = df['OptionCode'].astype(str).tolist()
            logger.info(f"成功获取 {len(option_codes)} 个期货品种")
            return option_codes
            
        except Exception as e:
            logger.error(f"获取期货品种列表失败: {e}")
            return []

    def get_futures_basic_info(self, option_code: str) -> dict:
        """
        获取期货基本信息
        
        Args:
            option_code: 品种代码
            
        Returns:
            dict: 期货基本信息
        """
        try:
            sql = """
            SELECT TOP 1
                OptionCode,
                ContractName,
                Exchange,
                Term
            FROM Fut_DailyQuote 
            WHERE OptionCode = ?
            """
            
            conn = self._get_connection()
            if not conn:
                return {}
            
            cursor = conn.cursor()
            cursor.execute(sql, (option_code,))
            row = cursor.fetchone()
            self._return_connection(conn)
            
            if row:
                return {
                    'OptionCode': row[0],
                    'ContractName': row[1],
                    'Exchange': row[2],
                    'Term': row[3]
                }
            return {}
            
        except Exception as e:
            logger.error(f"获取期货基本信息失败: {e}")
            return {}

    def get_prev_trading_days(self, end_date, count=1):
        """
        获取指定日期之前的 N 个交易日（不含 end_date 当天）。
        end_date: str 'YYYY-MM-DD' 或 date
        count: 需要的交易日数量
        Returns: list of str ['YYYY-MM-DD', ...] 从近到远
        """
        try:
            if hasattr(end_date, 'strftime'):
                end_date = end_date.strftime('%Y-%m-%d')
            sql = f"""
            SELECT DISTINCT TOP {count + 1} q.TradingDay
            FROM {TABLE_CONFIG['daily_quote_table']} q
            WHERE q.TradingDay < ?
            ORDER BY q.TradingDay DESC
            """
            df = self.query(sql, params=[end_date])
            if df.empty:
                return []
            dates = pd.to_datetime(df['TradingDay']).dt.strftime('%Y-%m-%d').tolist()
            return dates[:count] if count else dates
        except Exception as e:
            logger.error(f"get_prev_trading_days 失败: {e}")
            return []

    def get_trading_days_inclusive(self, end_date, count=60):
        """
        获取截止到 end_date（含）的最近 count 个交易日，从早到晚排序。
        """
        try:
            if hasattr(end_date, 'strftime'):
                end_date = end_date.strftime('%Y-%m-%d')
            sql = f"""
            SELECT DISTINCT TOP {count} q.TradingDay
            FROM {TABLE_CONFIG['daily_quote_table']} q
            WHERE q.TradingDay <= ?
            ORDER BY q.TradingDay DESC
            """
            df = self.query(sql, params=[end_date])
            if df.empty:
                return []
            dates = pd.to_datetime(df['TradingDay']).dt.strftime('%Y-%m-%d').tolist()
            return list(reversed(dates))
        except Exception as e:
            logger.error(f"get_trading_days_inclusive 失败: {e}")
            return []

    def get_all_stocks_daily_with_preclose(self, trading_day, exclude_suspended=True):
        """
        全市场单日行情（含昨收），用于市场情绪等。剔除当天无成交的股票（视为停牌）。
        返回 DataFrame 列: SecuCode, Date, Open, High, Low, Close, PreClose
        """
        try:
            prev_days = self.get_prev_trading_days(trading_day, count=1)
            if not prev_days:
                logger.warning(f"无法获取 {trading_day} 的前一交易日")
                return pd.DataFrame()
            prev_day = prev_days[0]
            sql = f"""
            SELECT
                s.{TABLE_CONFIG['code_field']} AS SecuCode,
                q.{TABLE_CONFIG['date_field']} AS [Date],
                q.{TABLE_CONFIG['open_field']} AS [Open],
                q.{TABLE_CONFIG['high_field']} AS [High],
                q.{TABLE_CONFIG['low_field']} AS [Low],
                q.{TABLE_CONFIG['close_field']} AS [Close]
            FROM {TABLE_CONFIG['stock_info_table']} s
            INNER JOIN {TABLE_CONFIG['daily_quote_table']} q
                ON s.{TABLE_CONFIG['inner_code_field']} = q.{TABLE_CONFIG['inner_code_field']}
            WHERE {STOCK_FILTER}
              AND q.{TABLE_CONFIG['date_field']} IN (?, ?)
              AND q.ClosePrice IS NOT NULL AND q.ClosePrice > 0
            ORDER BY s.{TABLE_CONFIG['code_field']}, q.{TABLE_CONFIG['date_field']}
            """
            df = self.query(sql, params=[prev_day, trading_day])
            if df.empty:
                return pd.DataFrame()
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            # 按股票分组，取昨收与今收
            prev = df[df['Date'] == prev_day][['SecuCode', 'Close']].rename(columns={'Close': 'PreClose'})
            curr = df[df['Date'] == str(trading_day)][['SecuCode', 'Date', 'Open', 'High', 'Low', 'Close']]
            curr = curr.merge(prev, on='SecuCode', how='inner')
            for col in ['Open', 'High', 'Low', 'Close', 'PreClose']:
                curr[col] = pd.to_numeric(curr[col], errors='coerce')
            curr = curr.dropna(subset=['Close', 'PreClose'])
            if exclude_suspended:
                curr = curr[(curr['Open'] > 0) & (curr['Close'].notna())]
            logger.info(f"get_all_stocks_daily_with_preclose: {trading_day} 共 {len(curr)} 只股票")
            return curr
        except Exception as e:
            logger.error(f"get_all_stocks_daily_with_preclose 失败: {e}", exc_info=True)
            return pd.DataFrame()

    def get_all_stocks_returns_for_dates(self, date_list, on_progress=None):
        """
        批量获取多日全市场行情并计算日收益率 R。
        date_list: 已按时间升序排列的交易日列表 ['YYYY-MM-DD', ...]
        on_progress: 可选回调 on_progress(batch_idx, total_batches, rows_so_far, batch_label)
        按 120 天分批 BETWEEN 查询，多线程并行（受连接池大小约束）。
        """
        if not date_list:
            return pd.DataFrame()
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import threading as _th

            date_set = set(date_list)

            base_cols = f"""
                    s.{TABLE_CONFIG['code_field']} AS SecuCode,
                    q.{TABLE_CONFIG['date_field']} AS [Date],
                    q.{TABLE_CONFIG['open_field']} AS [Open],
                    q.{TABLE_CONFIG['high_field']} AS [High],
                    q.{TABLE_CONFIG['low_field']} AS [Low],
                    q.{TABLE_CONFIG['close_field']} AS [Close],
                    q.TurnoverValue AS Amount,
                    q.{TABLE_CONFIG['turnover_rate_field']} AS TurnoverRate,
                    q.{TABLE_CONFIG['negotiable_mv_field']} AS NegotiableMV"""
            base_from = f"""
                FROM {TABLE_CONFIG['stock_info_table']} s
                INNER JOIN {TABLE_CONFIG['daily_quote_table']} q
                    ON s.{TABLE_CONFIG['inner_code_field']} = q.{TABLE_CONFIG['inner_code_field']}
                WHERE {STOCK_FILTER}
                  AND q.ClosePrice IS NOT NULL AND q.ClosePrice > 0"""
            sql_tpl = f"""SELECT {base_cols} {base_from}
                  AND q.{TABLE_CONFIG['date_field']} BETWEEN ? AND ?
                ORDER BY s.{TABLE_CONFIG['code_field']}, q.{TABLE_CONFIG['date_field']}"""

            pool_size = len(self._connection_pool) if self._connection_pool else 4
            usable_conns = min(pool_size, 6)

            if len(date_list) <= 600:
                batches = [(date_list[0], date_list[-1])]
            else:
                target_batches = min(usable_conns, 4)
                BATCH_DAYS = max(200, len(date_list) // target_batches + 1)
                batches = []
                for i in range(0, len(date_list), BATCH_DAYS):
                    chunk = date_list[i:i + BATCH_DAYS]
                    batches.append((chunk[0], chunk[-1]))

            total_batches = len(batches)
            workers = min(usable_conns, total_batches)
            logger.info(f"get_all_stocks_returns_for_dates: {len(date_list)} 天, "
                        f"分 {total_batches} 批, {workers} 线程并行查询")

            _done_count = [0]
            _total_rows = [0]
            _lock = _th.Lock()

            def _fetch_one(idx_batch):
                idx, (d_min, d_max) = idx_batch
                part = self.query(sql_tpl, params=[d_min, d_max])
                part_rows = len(part) if part is not None else 0
                with _lock:
                    _done_count[0] += 1
                    _total_rows[0] += part_rows
                    done = _done_count[0]
                    rows = _total_rows[0]
                label = f'{d_min} ~ {d_max}'
                logger.info(f"  批次完成 {done}/{total_batches}: {label} → {part_rows:,} 行")
                if on_progress:
                    on_progress(done, total_batches, rows, label)
                return part

            results = []
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_fetch_one, (i, b)): i
                           for i, b in enumerate(batches)}
                for future in as_completed(futures):
                    part = future.result()
                    if part is not None and not part.empty:
                        results.append(part)

            if not results:
                return pd.DataFrame()

            df = pd.concat(results, ignore_index=True)
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            for col in ['Open', 'High', 'Low', 'Close', 'Amount', 'TurnoverRate', 'NegotiableMV']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.sort_values(['SecuCode', 'Date'])
            df['PreClose'] = df.groupby('SecuCode')['Close'].shift(1)
            df = df.dropna(subset=['PreClose', 'Close'])
            df['R'] = df['Close'] / df['PreClose'] - 1
            df = df[df['Date'].isin(date_set)]
            logger.info(f"get_all_stocks_returns_for_dates: 完成, 共 {len(df):,} 条有效记录")
            return df
        except Exception as e:
            logger.error(f"get_all_stocks_returns_for_dates 失败: {e}", exc_info=True)
            return pd.DataFrame()

    def get_index_quote_for_dates(self, inner_codes, start_date, end_date):
        """
        获取指数在日期区间内的行情。QT_IndexQuote + SecuMain。
        返回 DataFrame: InnerCode, SecuAbbr, ChiName, TradingDay, PrevClosePrice, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverValue, ChangePCT
        """
        try:
            codes_str = ','.join([str(c) for c in inner_codes])
            sql = f"""
            SELECT
                qti.InnerCode,
                sm.SecuAbbr,
                sm.ChiName,
                qti.TradingDay,
                qti.PrevClosePrice,
                qti.OpenPrice,
                qti.HighPrice,
                qti.LowPrice,
                qti.ClosePrice,
                qti.TurnoverValue,
                qti.ChangePCT
            FROM QT_IndexQuote qti
            LEFT JOIN SecuMain sm ON qti.InnerCode = sm.InnerCode
            WHERE sm.SecuCategory = 4
              AND sm.ListedState = 1
              AND qti.TradingDay >= ?
              AND qti.TradingDay <= ?
              AND qti.InnerCode IN ({codes_str})
            ORDER BY qti.InnerCode, qti.TradingDay
            """
            df = self.query(sql, params=[start_date, end_date])
            if not df.empty:
                df['TradingDay'] = pd.to_datetime(df['TradingDay']).dt.strftime('%Y-%m-%d')
            return df
        except Exception as e:
            logger.error(f"get_index_quote_for_dates 失败: {e}", exc_info=True)
            return pd.DataFrame()

    def get_broad_index_quotes(self, trading_day):
        """
        获取宽基/策略指数当日行情（用户提供的 InnerCode 列表）。
        返回 DataFrame: InnerCode, SecuAbbr, ChiName, TradingDay, PrevClosePrice, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverValue, ChangePCT, Category
        """
        inner_codes = [
            1, 1055, 11089, 3145, 4978,
            46, 30, 4074, 39144, 36324,
            6973, 4078, 3036, 7544, 39376, 31398, 48542, 19475, 217313,
            3469, 3471, 4089, 225892, 303968
        ]
        df = self.get_index_quote_for_dates(inner_codes, trading_day, trading_day)
        if df.empty:
            return df
        def _cat(ic):
            if ic in (1, 1055, 11089, 3145, 4978):
                return '市场综合基准'
            if ic in (46, 30, 4074, 39144, 36324):
                return '规模与风格'
            if ic in (6973, 4078, 3036, 7544, 39376, 31398, 48542, 19475, 217313):
                return '主题与策略'
            if ic in (3469, 3471, 4089, 225892, 303968):
                return '其他常用宽基与策略'
            return '其他'
        df['Category'] = df['InnerCode'].map(_cat)
        return df

    def get_broad_index_series_for_ma(self, trading_day, lookback_days=20):
        """
        获取宽基指数最近 lookback_days 个交易日行情，用于计算均线（如 MA20）。
        返回 DataFrame: InnerCode, SecuAbbr, ChiName, TradingDay, ClosePrice, ...（按 InnerCode, TradingDay 升序）
        """
        inner_codes = [
            1, 1055, 11089, 3145, 4978,
            46, 30, 4074, 39144, 36324,
            6973, 4078, 3036, 7544, 39376, 31398, 48542, 19475, 217313,
            3469, 3471, 4089, 225892, 303968
        ]
        dates = self.get_trading_days_inclusive(trading_day, count=lookback_days)
        if not dates:
            return pd.DataFrame()
        start_date, end_date = dates[0], dates[-1]
        df = self.get_index_quote_for_dates(inner_codes, start_date, end_date)
        if df.empty or 'ClosePrice' not in df.columns:
            return df
        df['ClosePrice'] = pd.to_numeric(df['ClosePrice'], errors='coerce')
        return df