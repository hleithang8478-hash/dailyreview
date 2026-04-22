#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日度动量评分流水线（核心逻辑）

说明：
- 这份实现严格按照你给出的项目设计拆分为 5 个核心模块：
  1）DataLoader：数据加载与预处理
  2）FactorCalculator：单因子计算
  3）FactorProcessor：因子预处理（去极值 / 行业市值中性化 / 标准化）
  4）FactorEvaluator：因子评估与合成
  5）DailyMomentumPipeline：日度生产流水线

集成方式：
- 与现有项目完全解耦，仅依赖 pandas / numpy / sklearn
- 如需直接使用聚源数据，可在 DataLoader 中接入 JuyuanDataFetcher（已预留示例）
- Flask 后端会通过 DailyMomentumPipeline.run_daily_pipeline(date) 来生成当日动量分
"""

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Tuple, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ==================== 一、数据准备模块 ====================


@dataclass
class DataLoaderConfig:
    """
    动量流水线拉取数据的日期区间，供 load_daily_data / load_industry_data 使用。

    - start_date: 取数起始日。prepare_base_data 会截取 [current_date-500, current_date]，
      且单因子要求每只股票至少约 300 个交易日，故 start_date 必须早于「目标日」约 500 个自然日
      （建议 600 日或更早，例如 2022-01-01）。
    - end_date: 取数截止日。须 ≥ 跑分的目标日（run_daily_pipeline 的 current_date），
      否则目标日没有行情，无法产出该日动量分。
    - calc_window_days: 因子计算窗口（天）。prepare_base_data 截取 [current_date - calc_window_days, current_date]。
      须 ≤ (end_date - start_date) 所覆盖的天数；建议 ≤ lookback_days（取数区间）。
    - history_years: 历史数据年数（用于状态分析，默认3年）。状态分析需要更长的历史数据。
    """
    start_date: str = "2023-01-01"
    end_date: str = datetime.today().strftime("%Y-%m-%d")
    calc_window_days: int = 500
    history_years: int = 3  # 状态分析需要三年历史数据


class DataLoader:
    """
    数据加载与预处理模块

    默认实现是“抽象层”，不会直接访问具体库。
    你可以有两种使用方式：
    1）在这里直接接入聚源（示例代码见 load_daily_data_juyuan）
    2）在外部继承 DataLoader，自定义子类并在流水线中替换
    """

    def __init__(self, config: Optional[DataLoaderConfig] = None):
        self.config = config or DataLoaderConfig()

    # ------- 接入聚源实现的3个方法 -------
    def load_daily_data(self) -> pd.DataFrame:
        """
        加载个股日行情数据，返回：code, date, close_adj, open, high, low, volume, amount(可选)。
        数据源：聚源 QT_StockPerformance + SecuMain。close_adj 使用 ClosePrice（若需前复权可后续接 QT_AdjustingFactor）。
        注意：为支持状态分析，会加载 history_years 年的历史数据（即使 calc_window_days 更短）。
        """
        from data_fetcher import JuyuanDataFetcher
        from juyuan_config import TABLE_CONFIG, STOCK_FILTER

        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        # 状态分析需要更长的历史数据，取 max(calc_window_days, history_years*365)
        history_days = max(
            getattr(self.config, 'calc_window_days', 500),
            getattr(self.config, 'history_years', 3) * 365
        )
        # 调整 start_date 以确保有足够历史数据
        end_date_ts = pd.to_datetime(self.config.end_date)
        effective_start = (end_date_ts - timedelta(days=history_days + 30)).strftime('%Y-%m-%d')
        start_date = min(self.config.start_date, effective_start)  # 取更早的日期
        end_date = self.config.end_date
        dq = TABLE_CONFIG["daily_quote_table"]
        si = TABLE_CONFIG["stock_info_table"]
        di = TABLE_CONFIG["date_field"]
        ic = TABLE_CONFIG["inner_code_field"]
        cc = TABLE_CONFIG["code_field"]
        o, h, l, c, v = TABLE_CONFIG["open_field"], TABLE_CONFIG["high_field"], TABLE_CONFIG["low_field"], TABLE_CONFIG["close_field"], TABLE_CONFIG["volume_field"]

        sql = f"""
        SELECT
            s.[{cc}] AS code,
            q.[{di}] AS date,
            q.[{c}] AS close_adj,
            q.[{o}] AS open_price,
            q.[{h}] AS high,
            q.[{l}] AS low,
            q.[{v}] AS volume,
            q.TurnoverValue AS amount
        FROM [{dq}] q
        INNER JOIN [{si}] s ON q.[{ic}] = s.[{ic}]
        WHERE q.[{di}] BETWEEN '{start_date}' AND '{end_date}'
          AND ({STOCK_FILTER})
          AND s.[{cc}] NOT LIKE 'X%'
        """
        df = fetcher.query(sql)
        if df.empty:
            return df
        # 若聚源表无 TurnoverValue 列，请从上面 SQL 中删掉 q.TurnoverValue AS amount 一行
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def load_index_data(self) -> pd.DataFrame:
        """
        加载指数数据（可选，后续扩展用，不影响当前动量分逻辑）。返回字段示例：
        - index_code, date, close, return
        """
        return pd.DataFrame()

    def load_industry_data(self) -> pd.DataFrame:
        """
        加载行业市值数据（用于行业中性化），返回：code, date, industry, total_mv, circ_mv。
        数据源：聚源 QT_StockPerformance + SecuMain + LC_ExgIndustry(申万 Standard='38')。
        市值：total_mv 用 QT_StockPerformance.TotalMV（总市值），circ_mv 用 NegotiableMV（流通市值）。
        """
        from data_fetcher import JuyuanDataFetcher
        from juyuan_config import TABLE_CONFIG, STOCK_FILTER

        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        start_date = self.config.start_date
        end_date = self.config.end_date
        dq = TABLE_CONFIG["daily_quote_table"]
        si = TABLE_CONFIG["stock_info_table"]
        di = TABLE_CONFIG["date_field"]
        ic = TABLE_CONFIG["inner_code_field"]
        cc = TABLE_CONFIG["code_field"]
        nv = TABLE_CONFIG["negotiable_mv_field"]

        sql = f"""
        SELECT
            s.[{cc}] AS code,
            q.[{di}] AS date,
            i.ThirdIndustryName AS industry,
            q.TotalMV AS total_mv,
            q.[{nv}] AS circ_mv
        FROM [{dq}] q
        INNER JOIN [{si}] s ON q.[{ic}] = s.[{ic}]
        LEFT JOIN LC_ExgIndustry i ON s.CompanyCode = i.CompanyCode AND i.Standard = '38'
        WHERE q.[{di}] BETWEEN '{start_date}' AND '{end_date}'
          AND ({STOCK_FILTER})
          AND s.[{cc}] NOT LIKE 'X%'
        """
        df = fetcher.query(sql)
        if df.empty:
            return df
        df = df.drop_duplicates(subset=["code", "date"], keep="first")
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["date"] = pd.to_datetime(df["date"])
        return df

    # ------- 示例：与 load_daily_data 等价的聚源写法（仅供参考，流水线已直接接 load_daily_data） -------
    def load_daily_data_juyuan_example(self) -> pd.DataFrame:
        """
        示例：通过 JuyuanDataFetcher 加载日行情（与 load_daily_data 逻辑一致，表名已改为 TABLE_CONFIG）。
        流水线已直接使用 load_daily_data，此方法仅作参考。
        """
        try:
            from data_fetcher import JuyuanDataFetcher
            from juyuan_config import TABLE_CONFIG, STOCK_FILTER
        except ImportError:
            logger.warning("未找到 JuyuanDataFetcher，load_daily_data_juyuan_example 仅作示例使用")
            return pd.DataFrame()

        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        dq, si = TABLE_CONFIG["daily_quote_table"], TABLE_CONFIG["stock_info_table"]
        di, ic, cc = TABLE_CONFIG["date_field"], TABLE_CONFIG["inner_code_field"], TABLE_CONFIG["code_field"]
        o, h, l, c, v = TABLE_CONFIG["open_field"], TABLE_CONFIG["high_field"], TABLE_CONFIG["low_field"], TABLE_CONFIG["close_field"], TABLE_CONFIG["volume_field"]
        sql = f"""
        SELECT s.[{cc}] AS code, q.[{di}] AS date, q.[{c}] AS close_adj, q.[{o}] AS open_price, q.[{h}] AS high, q.[{l}] AS low, q.[{v}] AS volume, q.TurnoverValue AS amount
        FROM [{dq}] q INNER JOIN [{si}] s ON q.[{ic}] = s.[{ic}]
        WHERE q.[{di}] BETWEEN '{self.config.start_date}' AND '{self.config.end_date}' AND ({STOCK_FILTER}) AND s.[{cc}] NOT LIKE 'X%'
        """
        df = fetcher.query(sql)
        if df.empty:
            return df
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["date"] = pd.to_datetime(df["date"])
        return df

    # ------- 公共预处理入口 -------
    def prepare_base_data(self, current_date: str) -> Dict[str, pd.DataFrame]:
        """
        准备当前日期所需的基础数据：截取 [current_date - calc_window_days, current_date]。
        因此 DataLoaderConfig.start_date 须早于 current_date 至少 calc_window_days 天，end_date ≥ current_date。
        """
        current_date_ts = pd.to_datetime(current_date)
        days = getattr(self.config, 'calc_window_days', 500)
        start_calc_date = current_date_ts - timedelta(days=days)

        daily_data = self.load_daily_data()
        industry_data = self.load_industry_data()
        index_data = self.load_index_data()

        if daily_data.empty:
            logger.warning("daily_data 为空，请检查 DataLoader.load_daily_data 的实现")
            return {"daily_data": daily_data, "industry_data": industry_data, "index_data": index_data}

        # 过滤日期范围（优化：先过滤再转换日期，减少处理量）
        if "date" not in daily_data.columns or not isinstance(daily_data["date"].dtype, pd.DatetimeIndex):
            daily_data["date"] = pd.to_datetime(daily_data["date"])
        
        mask = (daily_data["date"] >= start_calc_date) & (daily_data["date"] <= current_date_ts)
        daily_data = daily_data.loc[mask].copy()

        # 计算日收益率（优化：先排序再分组计算，提高效率）
        daily_data = daily_data.sort_values(["code", "date"])
        # 使用更高效的分组操作
        daily_data["daily_return"] = daily_data.groupby("code", sort=False)["close_adj"].pct_change()

        return {
            "daily_data": daily_data,
            "industry_data": industry_data,
            "index_data": index_data,
        }


# ==================== 二、单因子计算模块 ====================


class FactorCalculator:
    @staticmethod
    def price_momentum(close_prices: np.ndarray, lookback: int, skip: int = 0) -> float:
        """价格动量因子：如 MOM_12M_1M, MOM_6M_1M 等"""
        if len(close_prices) < lookback + skip:
            return np.nan

        start_idx = -(lookback + skip)
        end_idx = -skip if skip > 0 else None

        period_prices = close_prices[start_idx:end_idx]
        if len(period_prices) < 2:
            return np.nan

        cumulative_return = (period_prices[-1] / period_prices[0]) - 1
        return float(cumulative_return)

    @staticmethod
    def sharpe_momentum(returns: np.ndarray, lookback: int) -> float:
        """夏普动量因子"""
        if len(returns) < lookback:
            return np.nan

        period_returns = returns[-lookback:]
        mean_return = np.mean(period_returns)
        std_return = np.std(period_returns)

        if std_return == 0:
            return 0.0

        return float((mean_return / std_return) * np.sqrt(252))

    @staticmethod
    def reversal_factor(returns: np.ndarray, lookback: int = 20) -> float:
        """反转因子：过去 lookback 日收益取负值"""
        if len(returns) < lookback:
            return np.nan

        period_returns = returns[-lookback:]
        cumulative_return = np.prod([1 + r for r in period_returns]) - 1
        return float(-cumulative_return)  # 涨得多 → 反转值低

    @staticmethod
    def realized_momentum(long_return: float, short_return: float, volatility: float) -> float:
        """实现动量：长期收益-短期收益，经波动率调整"""
        if volatility == 0:
            return 0.0
        return float((long_return - short_return) / volatility)
    
    @staticmethod
    def calculate_momentum_metrics(stock_data: pd.DataFrame) -> Dict[str, float]:
        """
        计算全面的动量指标（多周期涨幅、振幅、极值、趋势、成交量）
        
        返回：动量指标字典
        """
        metrics: Dict[str, float] = {}
        
        if stock_data.empty or len(stock_data) < 20:
            return metrics
        
        # 提取数据
        close = stock_data["close_adj"].values
        high = stock_data.get("high", stock_data["close_adj"]).values
        low = stock_data.get("low", stock_data["close_adj"]).values
        returns = stock_data["daily_return"].fillna(0).values
        volume = stock_data.get("amount", pd.Series(0, index=stock_data.index)).values  # 使用成交额
        
        # ========== 1. 多周期涨幅 ==========
        periods = [1, 5, 10, 20, 60, 120, 250]
        for period in periods:
            if len(close) >= period + 1:
                try:
                    return_val = (close[-1] / close[-(period+1)]) - 1
                    metrics[f"return_{period}d"] = float(return_val) if not np.isnan(return_val) else np.nan
                except:
                    metrics[f"return_{period}d"] = np.nan
            else:
                metrics[f"return_{period}d"] = np.nan
        
        # ========== 2. 振幅指标 ==========
        amplitude_periods = [5, 20, 60]
        for period in amplitude_periods:
            if len(close) >= period:
                try:
                    period_high = high[-period:].max()
                    period_low = low[-period:].min()
                    if period_low > 0:
                        amplitude = (period_high - period_low) / period_low
                        metrics[f"amplitude_{period}d"] = float(amplitude) if not np.isnan(amplitude) else np.nan
                    else:
                        metrics[f"amplitude_{period}d"] = np.nan
                except:
                    metrics[f"amplitude_{period}d"] = np.nan
            else:
                metrics[f"amplitude_{period}d"] = np.nan
        
        # 波动率
        if len(returns) >= 20:
            try:
                daily_volatility = float(np.std(returns[-20:]))
                metrics["daily_volatility"] = daily_volatility if not np.isnan(daily_volatility) else np.nan
                metrics["annualized_volatility"] = float(daily_volatility * np.sqrt(252)) if not np.isnan(daily_volatility) else np.nan
            except:
                metrics["daily_volatility"] = np.nan
                metrics["annualized_volatility"] = np.nan
        else:
            metrics["daily_volatility"] = np.nan
            metrics["annualized_volatility"] = np.nan
        
        # ========== 3. 极值指标 ==========
        # 最大单日涨幅
        for period in [20, 60]:
            if len(returns) >= period:
                try:
                    max_gain = float(np.max(returns[-period:]))
                    metrics[f"max_gain_{period}d"] = max_gain if not np.isnan(max_gain) else np.nan
                except:
                    metrics[f"max_gain_{period}d"] = np.nan
            else:
                metrics[f"max_gain_{period}d"] = np.nan
        
        # 最大回撤（路径意义上的回撤：高点在前，低点在后）
        # 标准定义：在最近 period 日内，价格从任一历史高点向后看的最大跌幅
        for period in [20, 60]:
            if len(close) >= period:
                try:
                    period_close = close[-period:]
                    # 累计最大值序列（到当日为止的历史高点）
                    running_max = np.maximum.accumulate(period_close)
                    # 跌幅序列：当前价相对历史高点的跌幅
                    drawdowns = (running_max - period_close) / running_max
                    max_drawdown = np.max(drawdowns) if len(drawdowns) > 0 else 0.0
                    metrics[f"max_drawdown_{period}d"] = float(max_drawdown) if not np.isnan(max_drawdown) else np.nan
                except:
                    metrics[f"max_drawdown_{period}d"] = np.nan
            else:
                metrics[f"max_drawdown_{period}d"] = np.nan
        
        # 近期最高/最低价
        if len(close) >= 60:
            try:
                metrics["recent_high"] = float(np.max(high[-60:])) if not np.isnan(np.max(high[-60:])) else np.nan
                metrics["recent_low"] = float(np.min(low[-60:])) if not np.isnan(np.min(low[-60:])) else np.nan
                if not np.isnan(metrics["recent_high"]) and metrics["recent_high"] > 0:
                    metrics["price_vs_high"] = float((close[-1] / metrics["recent_high"] - 1) * 100)
                else:
                    metrics["price_vs_high"] = np.nan
            except:
                metrics["recent_high"] = np.nan
                metrics["recent_low"] = np.nan
                metrics["price_vs_high"] = np.nan
        else:
            metrics["recent_high"] = np.nan
            metrics["recent_low"] = np.nan
            metrics["price_vs_high"] = np.nan
        
        # ========== 4. 趋势指标 ==========
        # 计算均线
        ma5 = pd.Series(close).rolling(5).mean().values
        ma10 = pd.Series(close).rolling(10).mean().values
        ma20 = pd.Series(close).rolling(20).mean().values
        
        # MA斜率（归一化）
        if len(ma5) >= 6:
            try:
                ma5_slope = (ma5[-1] - ma5[-6]) / ma5[-1] if ma5[-1] > 0 else 0
                metrics["ma5_slope"] = float(ma5_slope) if not np.isnan(ma5_slope) else np.nan
            except:
                metrics["ma5_slope"] = np.nan
        else:
            metrics["ma5_slope"] = np.nan
        
        if len(ma10) >= 6:
            try:
                ma10_slope = (ma10[-1] - ma10[-6]) / ma10[-1] if ma10[-1] > 0 else 0
                metrics["ma10_slope"] = float(ma10_slope) if not np.isnan(ma10_slope) else np.nan
            except:
                metrics["ma10_slope"] = np.nan
        else:
            metrics["ma10_slope"] = np.nan
        
        if len(ma20) >= 6:
            try:
                ma20_slope = (ma20[-1] - ma20[-6]) / ma20[-1] if ma20[-1] > 0 else 0
                metrics["ma20_slope"] = float(ma20_slope) if not np.isnan(ma20_slope) else np.nan
            except:
                metrics["ma20_slope"] = np.nan
        else:
            metrics["ma20_slope"] = np.nan
        
        # 价格相对MA位置
        if len(ma5) > 0 and not np.isnan(ma5[-1]) and ma5[-1] > 0:
            try:
                metrics["price_vs_ma5"] = float((close[-1] / ma5[-1] - 1) * 100)
            except:
                metrics["price_vs_ma5"] = np.nan
        else:
            metrics["price_vs_ma5"] = np.nan
        
        if len(ma20) > 0 and not np.isnan(ma20[-1]) and ma20[-1] > 0:
            try:
                metrics["price_vs_ma20"] = float((close[-1] / ma20[-1] - 1) * 100)
            except:
                metrics["price_vs_ma20"] = np.nan
        else:
            metrics["price_vs_ma20"] = np.nan
        
        if len(ma5) > 0 and len(ma20) > 0 and not np.isnan(ma5[-1]) and not np.isnan(ma20[-1]) and ma20[-1] > 0:
            try:
                metrics["ma5_vs_ma20"] = float((ma5[-1] / ma20[-1] - 1) * 100)
            except:
                metrics["ma5_vs_ma20"] = np.nan
        else:
            metrics["ma5_vs_ma20"] = np.nan
        
        # 趋势强度（均线多头排列天数占比）
        if len(close) >= 20:
            try:
                ma5_series = pd.Series(close).rolling(5).mean()
                ma10_series = pd.Series(close).rolling(10).mean()
                ma20_series = pd.Series(close).rolling(20).mean()
                trend_days = ((ma5_series[-20:] > ma10_series[-20:]) & 
                              (ma10_series[-20:] > ma20_series[-20:])).sum()
                metrics["trend_strength"] = float(trend_days / 20) if not np.isnan(trend_days) else np.nan
            except:
                metrics["trend_strength"] = np.nan
        else:
            metrics["trend_strength"] = np.nan
        
        # ========== 5. 成交量指标 ==========
        if len(volume) >= 20 and np.sum(volume[-20:]) > 0:
            try:
                # 5日量比
                if len(volume) >= 5:
                    vol_5d_avg = np.mean(volume[-5:])
                    vol_20d_avg = np.mean(volume[-20:])
                    if vol_20d_avg > 0:
                        metrics["volume_ratio_5d"] = float(vol_5d_avg / vol_20d_avg)
                    else:
                        metrics["volume_ratio_5d"] = np.nan
                else:
                    metrics["volume_ratio_5d"] = np.nan
                
                # 20日量比
                if len(volume) >= 60:
                    vol_20d_avg = np.mean(volume[-20:])
                    vol_60d_avg = np.mean(volume[-60:])
                    if vol_60d_avg > 0:
                        metrics["volume_ratio_20d"] = float(vol_20d_avg / vol_60d_avg)
                    else:
                        metrics["volume_ratio_20d"] = np.nan
                else:
                    metrics["volume_ratio_20d"] = np.nan
            except:
                metrics["volume_ratio_5d"] = np.nan
                metrics["volume_ratio_20d"] = np.nan
        else:
            metrics["volume_ratio_5d"] = np.nan
            metrics["volume_ratio_20d"] = np.nan
        
        return metrics

    def calculate_all_factors(self, stock_data: pd.DataFrame, current_date: str) -> Dict[str, float]:
        """计算单只股票的所有动量因子"""
        factors: Dict[str, float] = {}

        close_prices = stock_data["close_adj"].values
        returns = stock_data["daily_return"].fillna(0).values

        # 价格动量系列
        factors["MOM_12M_1M"] = self.price_momentum(close_prices, 242, 20)
        factors["MOM_6M_1M"] = self.price_momentum(close_prices, 120, 20)
        factors["MOM_3M"] = self.price_momentum(close_prices, 60, 0)
        factors["MOM_1M"] = self.price_momentum(close_prices, 20, 0)

        # 夏普动量
        factors["SHARPE_20D"] = self.sharpe_momentum(returns, 20)
        factors["SHARPE_242D"] = self.sharpe_momentum(returns, 242)

        # 反转因子
        factors["REV_20D"] = self.reversal_factor(returns, 20)

        return factors


# ==================== 三、因子预处理模块 ====================


class FactorProcessor:
    @staticmethod
    def winsorize(factor_series: pd.Series, alpha: float = 0.025) -> pd.Series:
        """去极值处理：2.5%-97.5% 缩尾"""
        if factor_series.empty:
            return factor_series
        lower = factor_series.quantile(alpha)
        upper = factor_series.quantile(1 - alpha)
        return factor_series.clip(lower, upper)

    @staticmethod
    def neutralize_industry_marketcap(
        factor_series: pd.Series,
        industry_dummies: pd.DataFrame,
        marketcap_series: pd.Series,
    ) -> pd.Series:
        """行业 + 市值中性化（线性回归残差）"""
        common_index = factor_series.index.intersection(industry_dummies.index).intersection(marketcap_series.index)
        if len(common_index) == 0:
            return factor_series

        # 构建特征矩阵：市值（对数）+ 行业 one-hot
        marketcap_log = pd.DataFrame({'marketcap_log': np.log(marketcap_series.loc[common_index] + 1e-6)})
        X = pd.concat([marketcap_log, industry_dummies.loc[common_index]], axis=1)
        # 确保所有列名都是字符串（scikit-learn 要求）
        X.columns = X.columns.astype(str)
        y = factor_series.loc[common_index]

        try:
            from sklearn.linear_model import LinearRegression
        except ImportError:
            logger.warning("未安装 scikit-learn，跳过中性化步骤")
            return factor_series

        model = LinearRegression()
        model.fit(X, y)
        y_pred = pd.Series(model.predict(X), index=common_index)
        residuals = y - y_pred

        # 其余股票保留原值
        result = factor_series.copy()
        result.loc[common_index] = residuals
        return result

    @staticmethod
    def standardize(factor_series: pd.Series) -> pd.Series:
        """Z-score 标准化"""
        mean_val = factor_series.mean()
        std_val = factor_series.std()

        if std_val == 0 or pd.isna(std_val):
            return pd.Series(0, index=factor_series.index)

        return (factor_series - mean_val) / std_val

    def process_factors(
        self,
        raw_factors: pd.DataFrame,
        industry_series: pd.Series,
        marketcap_series: pd.Series,
        winsorize_alpha: float = 0.025,
    ) -> pd.DataFrame:
        """完整的因子预处理流程。winsorize_alpha：去极值分位数，如 0.025 表示 2.5%~97.5% 缩尾。"""
        if raw_factors.empty:
            return raw_factors

        processed_factors = pd.DataFrame(index=raw_factors.index)

        # 行业 one-hot
        industry_dummies = pd.get_dummies(industry_series)

        for factor_name in raw_factors.columns:
            factor_series = raw_factors[factor_name].astype(float)

            # 1. 去极值
            factor_series = self.winsorize(factor_series, alpha=winsorize_alpha)

            # 2. 行业 + 市值中性化
            factor_series = self.neutralize_industry_marketcap(factor_series, industry_dummies, marketcap_series)

            # 3. 标准化
            factor_series = self.standardize(factor_series)

            processed_factors[factor_name] = factor_series

        return processed_factors


# ==================== 四、因子评估与合成模块 ====================


class FactorEvaluator:
    def synthesize_factors(self, processed_factors: pd.DataFrame, weights: Optional[pd.Series] = None) -> pd.Series:
        """
        因子合成：计算最终动量分
        - 默认等权合成
        - REV 系列因子自动取负号
        """
        if processed_factors.empty:
            return pd.Series(dtype=float)

        if weights is None or weights.empty:
            n_factors = len(processed_factors.columns)
            weights = pd.Series(1.0 / n_factors, index=processed_factors.columns)

        momentum_scores = pd.Series(0.0, index=processed_factors.index)

        for factor_name, weight in weights.items():
            if factor_name not in processed_factors.columns:
                continue

            series = processed_factors[factor_name]
            if factor_name.upper().startswith("REV"):
                momentum_scores += -weight * series
            else:
                momentum_scores += weight * series

        return momentum_scores


# ==================== 五、股票状态分析模块 ====================


@dataclass
class StatusAnalyzerConfig:
    """
    股票状态分析参数配置（可配置，默认值仅供参考）
    
    优化版本：支持趋势空间/时间约束、子阶段识别、状态画像矩阵等
    """
    # ========== 原有参数（保持兼容）==========
    # 趋势判断参数（复用趋势回调算法）
    mass_short_period: int = 60          # MASS短期周期
    mass_long_period: int = 360          # MASS长期周期
    mass_threshold: float = 50.0         # MASS阈值（>=此值认为趋势健康）
    ma_slope_period: int = 5            # 均线斜率计算周期
    ma_slope_threshold: float = 0.002   # 均线斜率阈值（>=此值为上行，<-此值为下行，默认0.002提高趋势确认质量）
    
    # 震荡检测参数
    oscillation_lookback_days: int = 60  # 从最新交易日往前找多少天检测震荡
    oscillation_amplitude_threshold: float = 0.10  # 振幅阈值（10%以内可能为震荡，收紧以只识别真正窄幅盘整）
    oscillation_ma_period: int = 60  # 震荡判断使用的均线周期（与趋势判断的短期周期一致，默认60日）
    oscillation_ma_slope_max: float = 0.001  # 震荡时均线斜率绝对值应小于此值
    
    # 趋势指标参数
    volume_surge_multiplier: float = 1.5  # 明显放量倍数（成交额 > 均值 * 此倍数）
    volume_surge_days: int = 3            # 明显放量需持续天数
    five_day_return_window: int = 5      # 五日平均涨幅窗口
    five_day_return_acceleration_threshold: float = 0.02  # 五日涨幅明显升高阈值（2%）
    
    # 数据范围
    history_years: int = 3                # 历史数据年数（用于加载三年数据）
    
    # ========== 新增参数（阶段一：趋势空间/时间约束）==========
    # 趋势最小空间约束
    trend_min_return: float = 0.08        # 趋势最小累计涨跌幅（8%）
    trend_min_duration: int = 10          # 趋势最小持续天数（10个交易日）
    
    # 趋势判定窗口（用于局部验证）
    trend_validation_window: int = 20     # 局部验证窗口（20-30日）
    trend_continuous_days: int = 5        # 连续保持趋势的天数（5-10日）
    
    # ========== 新增参数（阶段一：震荡子类）==========
    # 窄幅震荡
    oscillation_narrow_amplitude: float = 0.10  # 窄幅震荡振幅阈值（≤10%）
    oscillation_narrow_slope: float = 0.0007    # 窄幅震荡MA20斜率阈值
    
    # 宽幅震荡
    oscillation_wide_amplitude: float = 0.20    # 宽幅震荡振幅阈值（10-20%）
    oscillation_wide_slope: float = 0.0015      # 宽幅震荡MA20斜率阈值
    
    # ========== 新增参数（阶段三：趋势子阶段）==========
    # MASS分段阈值
    mass_main_stage_threshold: float = 70.0     # 主升/主跌阶段MASS阈值
    mass_exhaustion_threshold: float = 85.0     # 衰竭阶段MASS阈值
    price_deviation_exhaustion: float = 0.15    # 价格偏离MA5的衰竭阈值（15%）
    slope_main_stage_threshold: float = 0.001   # 主升阶段斜率显著阈值
    
    # ========== 新增参数（阶段五：量能优化）==========
    volume_baseline_period: int = 120           # 长周期量能基准（120日）
    extreme_low_volume_threshold: float = 0.3   # 极度缩量阈值（<30%基准）
    volume_recovery_days: int = 5               # 量能恢复需要的连续天数
    
    # ========== 新增开关（用于渐进式启用和兼容性）==========
    enable_optimized_judgment: bool = True      # 是否启用优化判定（默认开启）
    enable_sub_stage: bool = True               # 是否启用子阶段（默认开启）
    enable_status_portrait: bool = True         # 是否启用状态画像（默认开启）


class StockStatusAnalyzer:
    """
    股票状态分析器：判断震荡/趋势上行/趋势下行，并计算相关指标
    
    复用趋势回调算法（uptrend_rebound_analysis）中的 MASS 和均线斜率计算
    """
    
    def __init__(self, config: Optional[StatusAnalyzerConfig] = None):
        self.config = config or StatusAnalyzerConfig()
        # 导入趋势回调算法的函数
        try:
            from uptrend_rebound_analysis import calculate_mass_score, calculate_ma_slope
            self.calculate_mass_score = calculate_mass_score
            self.calculate_ma_slope = calculate_ma_slope
        except ImportError:
            logger.warning("未找到 uptrend_rebound_analysis，状态分析功能可能受限")
            self.calculate_mass_score = None
            self.calculate_ma_slope = None
    
    def analyze_stock_status(
        self, 
        stock_data: pd.DataFrame, 
        current_date: str,
        previous_status: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        分析单只股票的状态
        
        参数：
        - stock_data: 包含 code, date, close_adj, high, low, amount 的 DataFrame（按 date 排序）
        - current_date: 当前分析日期
        
        返回：
        - Dict 包含：
          - status: '震荡' / '趋势上行' / '趋势下行'
          - status_start_date: 状态开始日期
          - 震荡指标（如果震荡）: amplitude, lowest_price, highest_price, amount_std
          - 趋势指标（如果趋势）: duration_days, trend_return, has_volume_surge, five_day_acceleration
        """
        if stock_data.empty or 'close_adj' not in stock_data.columns:
            return self._empty_status()
        
        # 确保按日期排序，并设置 date 为索引（如果还不是）
        if 'date' not in stock_data.index.names and 'date' in stock_data.columns:
            stock_data = stock_data.set_index('date').sort_index().copy()
        else:
            stock_data = stock_data.sort_index().copy()
        
        # 过滤到当前日期
        current_ts = pd.to_datetime(current_date)
        stock_data = stock_data[stock_data.index <= current_ts].copy()
        
        if stock_data.empty:
            return self._empty_status()
        
        # 提取价格和成交额 / 成交量序列（使用索引对齐）
        close = pd.to_numeric(stock_data['close_adj'], errors='coerce').dropna()
        if close.empty or len(close) < 60:
            return self._empty_status()
        
        # 使用相同的索引对齐所有序列
        common_index = close.index
        high = pd.to_numeric(stock_data['high'], errors='coerce').reindex(common_index).fillna(close) if 'high' in stock_data.columns else close
        low = pd.to_numeric(stock_data['low'], errors='coerce').reindex(common_index).fillna(close) if 'low' in stock_data.columns else close
        amount = pd.to_numeric(stock_data['amount'], errors='coerce').reindex(common_index).fillna(0) if 'amount' in stock_data.columns else pd.Series(0, index=common_index)
        # 成交量用于量能分析（如果缺失则回退为0）
        if 'volume' in stock_data.columns:
            volume = pd.to_numeric(stock_data['volume'], errors='coerce').reindex(common_index).fillna(0)
        else:
            volume = pd.Series(0, index=common_index)
        
        # 1. 判断当前状态（同时获取底层数据，包括量能信息）
        status, status_start_date, underlying_data = self._detect_status(close, high, low, volume, current_date)
        
        # 优化：如果状态为"需继承"，尝试使用前一日状态
        if status == '需继承':
            if previous_status and previous_status.get('status') in ['震荡', '趋势上行', '趋势下行']:
                # 沿用前一日状态
                status = previous_status['status']
                # 前一日状态开始日期保持不变（状态延续）
                prev_start_date = previous_status.get('status_start_date')
                if prev_start_date:
                    try:
                        status_start_date = pd.to_datetime(prev_start_date)
                    except:
                        status_start_date = None
                else:
                    status_start_date = None
            else:
                # 如果没有前一日状态（首日），默认归为震荡
                status = '震荡'
                # 使用改进的查找方法找震荡起点
                current_ts = pd.to_datetime(current_date)
                status_start_date = self._find_oscillation_start_optimized(close, high, low, current_ts)
        
        result = {
            'status': status,
            'status_start_date': status_start_date.strftime('%Y-%m-%d') if status_start_date else None
        }
        
        # 添加底层数据到结果中
        if underlying_data:
            result['underlying_data'] = underlying_data
        
        # 2. 根据状态计算指标
        if status == '震荡':
            result.update(self._calculate_oscillation_metrics(
                close, high, low, amount, status_start_date, current_date
            ))
        elif status in ['趋势上行', '趋势下行']:
            result.update(self._calculate_trend_metrics(
                close, high, low, amount, status, status_start_date, current_date
            ))
        
        return result
    
    def _detect_status(
        self,
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        volume: pd.Series,
        current_date: str
    ) -> Tuple[str, Optional[pd.Timestamp], Optional[Dict[str, Any]]]:
        """
        检测股票状态：震荡 / 趋势上行 / 趋势下行
        
        返回：(status, status_start_date, underlying_data)
        underlying_data包含：MASS值、均线斜率、当前价格等底层数据
        """
        if self.calculate_mass_score is None or self.calculate_ma_slope is None:
            return ('未知', None, None)
        
        current_ts = pd.to_datetime(current_date)
        cfg = self.config
        
        # 取到当前日期的数据（close 的索引已经是 date）
        close_up_to_current = close[close.index <= current_ts]
        if len(close_up_to_current) < cfg.mass_short_period:
            return ('未知', None, None)
        
        # 确保 close_up_to_current 是 Series 且索引是日期
        if not isinstance(close_up_to_current, pd.Series):
            close_up_to_current = pd.Series(close_up_to_current.values, index=close_up_to_current.index)
        
        # 计算 MASS 和均线斜率（复用趋势回调算法）
        mass_short = self.calculate_mass_score(close_up_to_current, cfg.mass_short_period)
        mass_long = self.calculate_mass_score(close_up_to_current, cfg.mass_long_period) if len(close_up_to_current) >= cfg.mass_long_period else 0
        
        # 计算价格均线
        ma5 = close_up_to_current.rolling(5).mean()
        ma10 = close_up_to_current.rolling(10).mean()
        ma20 = close_up_to_current.rolling(20).mean()
        ma5_slope = self.calculate_ma_slope(ma5, cfg.ma_slope_period)
        ma10_slope = self.calculate_ma_slope(ma10, cfg.ma_slope_period)
        
        # 当前价格和均线位置
        current_close = close_up_to_current.iloc[-1] if len(close_up_to_current) > 0 else 0
        current_ma5 = ma5.iloc[-1] if len(ma5) > 0 and not pd.isna(ma5.iloc[-1]) else current_close
        current_ma10 = ma10.iloc[-1] if len(ma10) > 0 and not pd.isna(ma10.iloc[-1]) else current_close
        current_ma20 = ma20.iloc[-1] if len(ma20) > 0 and not pd.isna(ma20.iloc[-1]) else current_close
        
        # 量能相关：对齐到当前日期的成交量序列
        volume_up_to_current = volume[volume.index <= current_ts]
        if not isinstance(volume_up_to_current, pd.Series):
            volume_up_to_current = pd.Series(volume_up_to_current.values, index=volume_up_to_current.index)
        vol_ma5 = volume_up_to_current.rolling(5).mean()
        vol_ma10 = volume_up_to_current.rolling(10).mean()
        current_volume = volume_up_to_current.iloc[-1] if len(volume_up_to_current) > 0 else 0
        current_vol_ma5 = vol_ma5.iloc[-1] if len(vol_ma5) > 0 and not pd.isna(vol_ma5.iloc[-1]) else None
        current_vol_ma10 = vol_ma10.iloc[-1] if len(vol_ma10) > 0 and not pd.isna(vol_ma10.iloc[-1]) else None
        
        # 量能比值：当前 vs 5日、10日；5日 vs 10日
        volume_vs_ma5 = None
        volume_vs_ma10 = None
        volume_ma5_vs_ma10 = None
        try:
            if current_vol_ma5 and current_vol_ma5 > 0:
                volume_vs_ma5 = float(current_volume / current_vol_ma5)
        except Exception:
            volume_vs_ma5 = None
        try:
            if current_vol_ma10 and current_vol_ma10 > 0:
                volume_vs_ma10 = float(current_volume / current_vol_ma10)
        except Exception:
            volume_vs_ma10 = None
        try:
            if current_vol_ma10 and current_vol_ma10 > 0 and current_vol_ma5 is not None:
                volume_ma5_vs_ma10 = float(current_vol_ma5 / current_vol_ma10)
        except Exception:
            volume_ma5_vs_ma10 = None
        
        # 构建底层数据（价格 + 量能）
        underlying_data = {
            'mass_short': round(float(mass_short), 2) if not pd.isna(mass_short) else None,
            'mass_long': round(float(mass_long), 2) if not pd.isna(mass_long) and mass_long > 0 else None,
            'ma5_slope': round(float(ma5_slope), 6) if not pd.isna(ma5_slope) else None,
            'ma10_slope': round(float(ma10_slope), 6) if not pd.isna(ma10_slope) else None,
            'current_price': round(float(current_close), 2) if not pd.isna(current_close) else None,
            'ma5': round(float(current_ma5), 2) if not pd.isna(current_ma5) else None,
            'ma10': round(float(current_ma10), 2) if not pd.isna(current_ma10) else None,
            'ma20': round(float(current_ma20), 2) if not pd.isna(current_ma20) else None,
            'price_vs_ma5': round(float((current_close - current_ma5) / current_ma5 * 100), 2) if current_ma5 > 0 and not pd.isna(current_ma5) else None,
            # 量能底层数据（用于前端量能分析页）
            'current_volume': float(current_volume) if current_volume is not None else None,
            'volume_ma5': float(current_vol_ma5) if current_vol_ma5 is not None else None,
            'volume_ma10': float(current_vol_ma10) if current_vol_ma10 is not None else None,
            'volume_vs_ma5': round(volume_vs_ma5, 3) if volume_vs_ma5 is not None else None,
            'volume_vs_ma10': round(volume_vs_ma10, 3) if volume_vs_ma10 is not None else None,
            'volume_ma5_vs_ma10': round(volume_ma5_vs_ma10, 3) if volume_ma5_vs_ma10 is not None else None,
        }
        
        # ========== 优化后的判定逻辑：先判趋势，再判震荡 ==========
        # 1. 先判断趋势候选（基础条件）
        trend_up_candidate = (
            mass_short >= cfg.mass_threshold and 
            (mass_long >= cfg.mass_threshold or mass_long == 0) and
            ma5_slope >= cfg.ma_slope_threshold and 
            ma10_slope >= cfg.ma_slope_threshold and
            current_close >= current_ma5
        )
        
        trend_down_candidate = (
            ma5_slope < -cfg.ma_slope_threshold and 
            ma10_slope < -cfg.ma_slope_threshold and
            current_close < current_ma5
        )
        
        # 2. 如果启用优化判定，对趋势候选进行空间/时间验证
        trend_up_ok = False
        trend_down_ok = False
        trend_start = None
        
        if cfg.enable_optimized_judgment:
            # 先找到候选起点（使用优化后的查找，不强制360日）
            if trend_up_candidate:
                candidate_start = self._find_trend_start_optimized(close_up_to_current, 'up', current_ts)
                # 验证空间和时间约束
                if candidate_start:
                    period_mask = (close_up_to_current.index >= candidate_start) & (close_up_to_current.index <= current_ts)
                    period_data = close_up_to_current[period_mask]
                    if len(period_data) >= 2:
                        start_price = period_data.iloc[0]
                        end_price = period_data.iloc[-1]
                        trend_return = (end_price - start_price) / start_price if start_price > 0 else 0
                        duration_days = len(period_data)
                        
                        # 检查是否满足最小空间和时间约束
                        if trend_return >= cfg.trend_min_return and duration_days >= cfg.trend_min_duration:
                            trend_up_ok = True
                            trend_start = candidate_start
                        else:
                            trend_up_ok = False
                            trend_start = None
                    else:
                        trend_up_ok = False
                        trend_start = None
                else:
                    trend_up_ok = False
                    trend_start = None
            
            if trend_down_candidate:
                candidate_start = self._find_trend_start_optimized(close_up_to_current, 'down', current_ts)
                # 验证空间和时间约束
                if candidate_start:
                    period_mask = (close_up_to_current.index >= candidate_start) & (close_up_to_current.index <= current_ts)
                    period_data = close_up_to_current[period_mask]
                    if len(period_data) >= 2:
                        start_price = period_data.iloc[0]
                        end_price = period_data.iloc[-1]
                        trend_return = (start_price - end_price) / start_price if start_price > 0 else 0  # 下行用跌幅
                        duration_days = len(period_data)
                        
                        # 检查是否满足最小空间和时间约束
                        if trend_return >= cfg.trend_min_return and duration_days >= cfg.trend_min_duration:
                            trend_down_ok = True
                            trend_start = candidate_start
                        else:
                            trend_down_ok = False
                            trend_start = None
                    else:
                        trend_down_ok = False
                        trend_start = None
                else:
                    trend_down_ok = False
                    trend_start = None
        else:
            # 不启用优化判定，使用原有逻辑（向后兼容）
            trend_up_ok = trend_up_candidate
            trend_down_ok = trend_down_candidate
            if trend_up_ok:
                trend_start = self._find_trend_start(close_up_to_current, 'up', current_ts)
            elif trend_down_ok:
                trend_start = self._find_trend_start(close_up_to_current, 'down', current_ts)
            else:
                trend_start = None
        
        # 3. 如果趋势候选不成立，再判断震荡
        if trend_up_ok:
            return ('趋势上行', trend_start, underlying_data)
        elif trend_down_ok:
            return ('趋势下行', trend_start, underlying_data)
        else:
            # 趋势候选不成立，判断震荡
            oscillation_ok, oscillation_start = self._detect_oscillation(
                close_up_to_current, high, low, current_ts
            )
            if oscillation_ok:
                return ('震荡', oscillation_start, underlying_data)
            else:
                # 优化：趋势和震荡都不成立时，返回特殊标记，由调用方决定是否沿用前一日状态
                # 而不是默认归为震荡（这样可以避免"每天一个新震荡段"的问题）
                return ('需继承', None, underlying_data)
    
    def _detect_oscillation(
        self,
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        current_date: pd.Timestamp
    ) -> Tuple[bool, Optional[pd.Timestamp]]:
        """
        从最新交易日往前找，检测是否处于震荡状态
        
        要求：判断的时间段不能短于oscillation_lookback_days，但可以长于
        
        返回：(is_oscillation, oscillation_start_date)
        """
        cfg = self.config
        lookback_days = cfg.oscillation_lookback_days
        
        # 至少需要oscillation_lookback_days的数据才能判断震荡
        dates_before_current = close[close.index <= current_date]
        if len(dates_before_current) < lookback_days:
            return (False, None)
        
        # 取最近 lookback_days 天的数据（可以更长）
        lookback_start = current_date - timedelta(days=lookback_days + 30)
        recent_close = close[close.index >= lookback_start]
        if len(recent_close) < lookback_days:
            return (False, None)
        
        # 确保 recent_close 是 Series
        if not isinstance(recent_close, pd.Series):
            recent_close = pd.Series(recent_close.values, index=recent_close.index)
        
        # 计算均线斜率（如果斜率很小，可能是震荡）
        # 使用与趋势判断一致的周期（默认60日），而不是20日
        ma_period = cfg.oscillation_ma_period
        ma_oscillation = recent_close.rolling(ma_period).mean()
        if len(ma_oscillation) < ma_period:
            return (False, None)
        
        ma_oscillation_slope = self.calculate_ma_slope(ma_oscillation, 5) if self.calculate_ma_slope else 0
        
        # 计算振幅（最高-最低）/最低
        # 确保 high/low 与 recent_close 索引对齐
        recent_high = high[high.index.isin(recent_close.index)] if isinstance(high, pd.Series) else recent_close
        recent_low = low[low.index.isin(recent_close.index)] if isinstance(low, pd.Series) else recent_close
        if not isinstance(recent_high, pd.Series) or len(recent_high) == 0:
            recent_high = recent_close
        if not isinstance(recent_low, pd.Series) or len(recent_low) == 0:
            recent_low = recent_close
        # 确保索引完全对齐
        recent_high = recent_high.reindex(recent_close.index).fillna(recent_close)
        recent_low = recent_low.reindex(recent_close.index).fillna(recent_close)
        amplitude = (recent_high.max() - recent_low.min()) / recent_low.min() if recent_low.min() > 0 else 0
        
        # 震荡判断：振幅在阈值内，且均线斜率很小
        is_oscillation = (
            amplitude <= cfg.oscillation_amplitude_threshold and
            abs(ma_oscillation_slope) < cfg.oscillation_ma_slope_max
        )
        
        if is_oscillation:
            # 往前找震荡开始日期（振幅开始变小的点）
            start_date = self._find_oscillation_start(close, high, low, current_date, lookback_days)
            return (True, start_date)
        
        return (False, None)
    
    def _find_oscillation_start(
        self,
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        current_date: pd.Timestamp,
        max_lookback: int
    ) -> pd.Timestamp:
        """
        从最新交易日往前找，找到震荡状态真正开始的日期
        通过检测振幅和均线斜率的变化来确定状态转换点
        
        要求：判断的时间段不能短于oscillation_lookback_days，但可以长于
        """
        cfg = self.config
        # 从当前日期往前找，最多找 max_lookback 个交易日
        current_idx = len(close) - 1
        if current_idx < 0:
            return current_date
        
        # 找到当前日期在close中的位置
        dates_before_current = close[close.index <= current_date]
        if len(dates_before_current) == 0:
            return current_date
        
        # 至少需要oscillation_lookback_days的数据才能判断震荡
        min_required_period = cfg.oscillation_lookback_days
        if len(dates_before_current) < min_required_period:
            return current_date
        
        # 从最新交易日往前遍历，找到状态变化的点
        lookback_start_idx = max(0, len(dates_before_current) - max_lookback - 1)
        dates_list = dates_before_current.index.tolist()
        
        # 从最新往前找，检测每个时间点的震荡状态
        oscillation_start_idx = len(dates_list) - 1  # 默认从当前开始
        
        for i in range(len(dates_list) - 1, lookback_start_idx, -1):
            check_date = dates_list[i]
            check_close = dates_before_current[dates_before_current.index <= check_date]
            
            # 判断时间段不能短于oscillation_lookback_days
            # 计算从check_date到current_date的时间段长度
            period_length = len(dates_before_current[dates_before_current.index >= check_date])
            if period_length < min_required_period:
                # 如果时间段太短，继续往前找（允许更长的判断时间）
                continue
            
            # 确保check_close至少有足够的数据用于计算
            ma_period = cfg.oscillation_ma_period
            if len(check_close) < ma_period:
                continue
            
            # 计算该时间点的振幅和均线斜率
            check_high = high[high.index.isin(check_close.index)] if isinstance(high, pd.Series) else check_close
            check_low = low[low.index.isin(check_close.index)] if isinstance(low, pd.Series) else check_close
            check_high = check_high.reindex(check_close.index).fillna(check_close)
            check_low = check_low.reindex(check_close.index).fillna(check_close)
            
            amplitude = (check_high.max() - check_low.min()) / check_low.min() if check_low.min() > 0 else 1.0
            ma_oscillation = check_close.rolling(ma_period).mean()
            if len(ma_oscillation) >= ma_period:
                ma_oscillation_slope = abs(self.calculate_ma_slope(ma_oscillation, 5)) if self.calculate_ma_slope else 0
            else:
                ma_oscillation_slope = 1.0
            
            # 判断是否满足震荡条件
            is_oscillation = (
                amplitude <= cfg.oscillation_amplitude_threshold and
                ma_oscillation_slope < cfg.oscillation_ma_slope_max
            )
            
            if is_oscillation:
                oscillation_start_idx = i
            else:
                # 如果当前不满足震荡条件，说明状态已经变化，返回上一个满足条件的日期
                break
        
        return pd.Timestamp(dates_list[oscillation_start_idx])
    
    def _find_trend_start(
        self,
        close: pd.Series,
        trend_direction: str,
        current_date: pd.Timestamp
    ) -> pd.Timestamp:
        """
        从最新交易日往前找，找到趋势状态真正开始的日期
        通过检测MASS值和均线斜率的变化来确定状态转换点
        
        要求：判断的时间段不能短于mass_short_period和mass_long_period，但可以长于
        """
        cfg = self.config
        # 从当前日期往前找，最多找 250 个交易日（约1年）
        max_lookback = 250
        
        dates_before_current = close[close.index <= current_date]
        # 至少需要mass_long_period的数据才能判断趋势（如果数据不足，使用mass_short_period作为最低要求）
        min_required_period = max(cfg.mass_short_period, cfg.mass_long_period)
        if len(dates_before_current) < min_required_period:
            return current_date
        
        dates_list = dates_before_current.index.tolist()
        lookback_start_idx = max(0, len(dates_list) - max_lookback - 1)
        
        trend_start_idx = len(dates_list) - 1  # 默认从当前开始
        
        for i in range(len(dates_list) - 1, lookback_start_idx, -1):
            check_date = dates_list[i]
            check_close = dates_before_current[dates_before_current.index <= check_date]
            
            # 判断时间段不能短于mass_short_period和mass_long_period
            # 计算从check_date到current_date的时间段长度
            period_length = len(dates_before_current[dates_before_current.index >= check_date])
            if period_length < min_required_period:
                # 如果时间段太短，继续往前找（允许更长的判断时间）
                continue
            
            # 确保check_close至少有mass_long_period的数据用于计算MASS
            if len(check_close) < cfg.mass_long_period:
                continue
            
            # 计算该时间点的MASS和均线斜率
            mass_short = self.calculate_mass_score(check_close, cfg.mass_short_period) if self.calculate_mass_score else 0
            mass_long = self.calculate_mass_score(check_close, cfg.mass_long_period) if self.calculate_mass_score else 0
            
            ma5 = check_close.rolling(5).mean()
            ma10 = check_close.rolling(10).mean()
            ma5_slope = self.calculate_ma_slope(ma5, cfg.ma_slope_period) if self.calculate_ma_slope and len(ma5) > 0 else 0
            ma10_slope = self.calculate_ma_slope(ma10, cfg.ma_slope_period) if self.calculate_ma_slope and len(ma10) > 0 else 0
            
            current_close = check_close.iloc[-1] if len(check_close) > 0 else 0
            current_ma5 = ma5.iloc[-1] if len(ma5) > 0 and not pd.isna(ma5.iloc[-1]) else current_close
            
            # 判断是否满足趋势条件
            if trend_direction == 'up':
                trend_ok = (
                    mass_short >= cfg.mass_threshold and 
                    (mass_long >= cfg.mass_threshold or mass_long == 0) and
                    ma5_slope >= cfg.ma_slope_threshold and 
                    ma10_slope >= cfg.ma_slope_threshold and
                    current_close >= current_ma5
                )
            else:  # trend_direction == 'down'
                trend_ok = (
                    ma5_slope < -cfg.ma_slope_threshold and 
                    ma10_slope < -cfg.ma_slope_threshold and
                    current_close < current_ma5
                )
            
            if trend_ok:
                trend_start_idx = i
            else:
                # 如果当前不满足趋势条件，说明状态已经变化，返回上一个满足条件的日期
                break
        
        return pd.Timestamp(dates_list[trend_start_idx])
    
    def _find_trend_start_optimized(
        self,
        close: pd.Series,
        trend_direction: str,
        current_date: pd.Timestamp
    ) -> Optional[pd.Timestamp]:
        """
        优化后的趋势起点查找：使用"状态切换点+首次连续满足"逻辑
        
        步骤：
        1. 找到最近一次状态切换点 T0
        2. 在 [T0, T] 区间内，寻找第一个满足趋势硬条件的日期 t*
        3. 使用局部窗口验证，要求连续保持趋势状态
        """
        cfg = self.config
        dates_before_current = close[close.index <= current_date]
        if len(dates_before_current) < cfg.mass_short_period:
            return current_date
        
        dates_list = dates_before_current.index.tolist()
        current_idx = len(dates_list) - 1
        
        # 步骤1：找到最近一次状态切换点 T0（简化版：从当前往前找，最多250日）
        max_lookback = 250
        lookback_start_idx = max(0, current_idx - max_lookback)
        
        # 步骤2：在 [lookback_start_idx, current_idx] 区间内，从前向后寻找第一个满足趋势条件的日期
        # 使用局部窗口验证，不强制要求整段≥360日
        for i in range(lookback_start_idx, current_idx + 1):
            check_date = dates_list[i]
            check_close = dates_before_current[dates_before_current.index <= check_date]
            
            # 确保有足够数据计算MASS
            if len(check_close) < cfg.mass_short_period:
                continue
            
            # 计算该时间点的MASS和均线斜率（使用局部窗口）
            mass_short = self.calculate_mass_score(check_close, cfg.mass_short_period) if self.calculate_mass_score else 0
            mass_long = self.calculate_mass_score(check_close, cfg.mass_long_period) if len(check_close) >= cfg.mass_long_period and self.calculate_mass_score else 0
            
            ma5 = check_close.rolling(5).mean()
            ma10 = check_close.rolling(10).mean()
            ma5_slope = self.calculate_ma_slope(ma5, cfg.ma_slope_period) if self.calculate_ma_slope and len(ma5) > 0 else 0
            ma10_slope = self.calculate_ma_slope(ma10, cfg.ma_slope_period) if self.calculate_ma_slope and len(ma10) > 0 else 0
            
            current_close = check_close.iloc[-1] if len(check_close) > 0 else 0
            current_ma5 = ma5.iloc[-1] if len(ma5) > 0 and not pd.isna(ma5.iloc[-1]) else current_close
            
            # 判断是否满足趋势条件
            if trend_direction == 'up':
                trend_ok = (
                    mass_short >= cfg.mass_threshold and 
                    (mass_long >= cfg.mass_threshold or mass_long == 0) and
                    ma5_slope >= cfg.ma_slope_threshold and 
                    ma10_slope >= cfg.ma_slope_threshold and
                    current_close >= current_ma5
                )
            else:  # trend_direction == 'down'
                trend_ok = (
                    ma5_slope < -cfg.ma_slope_threshold and 
                    ma10_slope < -cfg.ma_slope_threshold and
                    current_close < current_ma5
                )
            
            if trend_ok:
                # 步骤3：验证从该日期起连续保持趋势状态，并增加局部涨幅确认
                # 检查从check_date到current_date，是否连续满足趋势条件
                validation_end = min(i + cfg.trend_validation_window, current_idx + 1)
                continuous_days = 0
                
                # 优化：增加局部涨幅确认条件
                start_price = check_close.iloc[-1] if len(check_close) > 0 else 0
                max_drawdown = 0.0  # 最大回撤
                max_gain = 0.0  # 最大涨幅
                
                for j in range(i, validation_end):
                    val_date = dates_list[j]
                    val_close = dates_before_current[dates_before_current.index <= val_date]
                    if len(val_close) < cfg.mass_short_period:
                        break
                    
                    val_mass_short = self.calculate_mass_score(val_close, cfg.mass_short_period) if self.calculate_mass_score else 0
                    val_ma5 = val_close.rolling(5).mean()
                    val_ma10 = val_close.rolling(10).mean()
                    val_ma5_slope = self.calculate_ma_slope(val_ma5, cfg.ma_slope_period) if self.calculate_ma_slope and len(val_ma5) > 0 else 0
                    val_ma10_slope = self.calculate_ma_slope(val_ma10, cfg.ma_slope_period) if self.calculate_ma_slope and len(val_ma10) > 0 else 0
                    val_current_close = val_close.iloc[-1] if len(val_close) > 0 else 0
                    val_current_ma5 = val_ma5.iloc[-1] if len(val_ma5) > 0 and not pd.isna(val_ma5.iloc[-1]) else val_current_close
                    
                    if trend_direction == 'up':
                        val_trend_ok = (
                            val_mass_short >= cfg.mass_threshold and
                            val_ma5_slope >= cfg.ma_slope_threshold and
                            val_ma10_slope >= cfg.ma_slope_threshold and
                            val_current_close >= val_current_ma5
                        )
                        # 计算局部涨幅和回撤
                        if start_price > 0:
                            local_return = (val_current_close - start_price) / start_price
                            max_gain = max(max_gain, local_return)
                            # 如果当前价格低于起点，计算回撤
                            if val_current_close < start_price:
                                max_drawdown = min(max_drawdown, local_return)
                    else:
                        val_trend_ok = (
                            val_ma5_slope < -cfg.ma_slope_threshold and
                            val_ma10_slope < -cfg.ma_slope_threshold and
                            val_current_close < val_current_ma5
                        )
                        # 计算局部跌幅和反弹
                        if start_price > 0:
                            local_return = (start_price - val_current_close) / start_price
                            max_gain = max(max_gain, local_return)  # 下行时，gain是跌幅
                            # 如果当前价格高于起点，计算反弹
                            if val_current_close > start_price:
                                max_drawdown = min(max_drawdown, -local_return)
                    
                    if val_trend_ok:
                        continuous_days += 1
                    else:
                        break
                
                # 优化：增加局部涨幅确认条件
                # 如果起点之后回撤超过一定比例（例如5%），说明起点可能过早，需要前移
                local_drawdown_threshold = -0.05  # 局部回撤阈值5%
                if max_drawdown < local_drawdown_threshold:
                    # 回撤过大，起点可能过早，继续往前找
                    continue
                
                # 如果连续保持趋势状态的天数满足要求，且局部涨幅确认通过，返回该日期
                if continuous_days >= cfg.trend_continuous_days:
                    return pd.Timestamp(check_date)
        
        # 如果找不到，返回当前日期
        return current_date
    
    def _find_oscillation_start_optimized(
        self,
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        current_date: pd.Timestamp
    ) -> pd.Timestamp:
        """
        优化后的震荡起点查找：使用"状态切换点+首次连续满足"逻辑
        """
        cfg = self.config
        dates_before_current = close[close.index <= current_date]
        if len(dates_before_current) < cfg.oscillation_lookback_days:
            return current_date
        
        dates_list = dates_before_current.index.tolist()
        current_idx = len(dates_list) - 1
        
        # 从当前往前找，最多找 oscillation_lookback_days 天
        max_lookback = cfg.oscillation_lookback_days
        lookback_start_idx = max(0, current_idx - max_lookback)
        
        # 从前向后寻找第一个满足震荡条件的日期
        for i in range(lookback_start_idx, current_idx + 1):
            check_date = dates_list[i]
            check_close = dates_before_current[dates_before_current.index <= check_date]
            
            ma_period = cfg.oscillation_ma_period
            if len(check_close) < ma_period:
                continue
            
            # 计算该时间点的振幅和均线斜率（使用配置的周期）
            check_high = high[high.index.isin(check_close.index)] if isinstance(high, pd.Series) else check_close
            check_low = low[low.index.isin(check_close.index)] if isinstance(low, pd.Series) else check_close
            check_high = check_high.reindex(check_close.index).fillna(check_close)
            check_low = check_low.reindex(check_close.index).fillna(check_close)
            
            amplitude = (check_high.max() - check_low.min()) / check_low.min() if check_low.min() > 0 else 1.0
            ma_oscillation = check_close.rolling(ma_period).mean()
            if len(ma_oscillation) >= ma_period:
                ma_oscillation_slope = abs(self.calculate_ma_slope(ma_oscillation, 5)) if self.calculate_ma_slope else 0
            else:
                ma_oscillation_slope = 1.0
            
            # 判断是否满足震荡条件
            is_oscillation = (
                amplitude <= cfg.oscillation_amplitude_threshold and
                ma_oscillation_slope < cfg.oscillation_ma_slope_max
            )
            
            if is_oscillation:
                # 验证从该日期起连续保持震荡状态（至少10日）
                validation_end = min(i + 20, current_idx + 1)
                continuous_days = 0
                
                for j in range(i, validation_end):
                    val_date = dates_list[j]
                    val_close = dates_before_current[dates_before_current.index <= val_date]
                    if len(val_close) < ma_period:
                        break
                    
                    val_high = high[high.index.isin(val_close.index)] if isinstance(high, pd.Series) else val_close
                    val_low = low[low.index.isin(val_close.index)] if isinstance(low, pd.Series) else val_close
                    val_high = val_high.reindex(val_close.index).fillna(val_close)
                    val_low = val_low.reindex(val_close.index).fillna(val_close)
                    
                    val_amplitude = (val_high.max() - val_low.min()) / val_low.min() if val_low.min() > 0 else 1.0
                    val_ma_oscillation = val_close.rolling(ma_period).mean()
                    if len(val_ma_oscillation) >= ma_period:
                        val_ma_oscillation_slope = abs(self.calculate_ma_slope(val_ma_oscillation, 5)) if self.calculate_ma_slope else 0
                    else:
                        val_ma_oscillation_slope = 1.0
                    
                    val_is_oscillation = (
                        val_amplitude <= cfg.oscillation_amplitude_threshold and
                        val_ma_oscillation_slope < cfg.oscillation_ma_slope_max
                    )
                    
                    if val_is_oscillation:
                        continuous_days += 1
                    else:
                        break
                
                # 如果连续保持震荡状态至少10日，返回该日期
                if continuous_days >= 10:
                    return pd.Timestamp(check_date)
        
        return current_date
    
    def _calculate_oscillation_metrics(
        self,
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        amount: pd.Series,
        start_date: Optional[pd.Timestamp],
        current_date: str
    ) -> Dict[str, Any]:
        """
        计算震荡状态的指标：振幅、最低点、最高点、成交额标准差
        优化版本：增加子阶段识别、状态画像矩阵
        """
        current_ts = pd.to_datetime(current_date)
        cfg = self.config
        
        # 取震荡期间的数据
        if start_date:
            period_mask = (close.index >= start_date) & (close.index <= current_ts)
        else:
            period_mask = close.index <= current_ts
        
        period_data = close[period_mask]
        if period_data.empty:
            return {
                'amplitude': None,
                'lowest_price': None,
                'highest_price': None,
                'amount_std': None
            }
        
        period_high = high[period_mask] if isinstance(high, pd.Series) else high.reindex(period_data.index).fillna(period_data)
        period_low = low[period_mask] if isinstance(low, pd.Series) else low.reindex(period_data.index).fillna(period_data)
        period_amount = amount[period_mask] if isinstance(amount, pd.Series) and len(amount) > 0 else pd.Series(0, index=period_data.index)
        
        highest = period_high.max()
        lowest = period_low.min()
        amplitude = (highest - lowest) / lowest if lowest > 0 else 0
        amount_std = period_amount.std() if len(period_amount) > 1 and period_amount.sum() > 0 else 0
        
        # 计算箱体中枢和上下轨
        box_center = (highest + lowest) / 2
        box_upper = highest
        box_lower = lowest
        
        # 计算均线斜率（用于子阶段判断，使用配置的周期）
        ma_period = cfg.oscillation_ma_period
        ma_oscillation_current = close[close.index <= current_ts].rolling(ma_period).mean()
        ma_oscillation_slope_current = abs(self.calculate_ma_slope(ma_oscillation_current, 5)) if self.calculate_ma_slope and len(ma_oscillation_current) >= ma_period else 0
        
        # 子阶段识别（窄幅/宽幅震荡）
        sub_stage = None
        if cfg.enable_sub_stage:
            amplitude_pct = amplitude * 100
            if amplitude_pct <= cfg.oscillation_narrow_amplitude * 100 and ma_oscillation_slope_current < cfg.oscillation_narrow_slope:
                sub_stage = '窄幅震荡'
            elif amplitude_pct <= cfg.oscillation_wide_amplitude * 100 and ma_oscillation_slope_current < cfg.oscillation_wide_slope:
                sub_stage = '宽幅震荡'
            else:
                sub_stage = '震荡'
        
        # 计算假突破次数（简化版：价格突破箱体上下轨后回归）
        false_breakout_count = 0
        if len(period_data) > 10:
            for i in range(10, len(period_data)):
                price = period_data.iloc[i]
                prev_prices = period_data.iloc[max(0, i-5):i]
                if len(prev_prices) > 0:
                    prev_range = (prev_prices.max() - prev_prices.min()) / prev_prices.min() if prev_prices.min() > 0 else 0
                    # 如果价格突破箱体但随后回归，视为假突破
                    if price > box_upper * 1.02 or price < box_lower * 0.98:
                        # 检查后续是否回归
                        if i < len(period_data) - 3:
                            future_prices = period_data.iloc[i:i+3]
                            if (price > box_upper * 1.02 and future_prices.max() < box_upper) or \
                               (price < box_lower * 0.98 and future_prices.min() > box_lower):
                                false_breakout_count += 1
        
        # 判断是否缩量横盘
        amount_mean = period_amount.mean() if len(period_amount) > 0 and period_amount.sum() > 0 else 0
        is_low_volume_consolidation = False
        if amount_mean > 0:
            # 使用长周期基准
            all_amount = amount[amount.index <= current_ts]
            baseline_period = cfg.volume_baseline_period
            if len(all_amount) >= baseline_period:
                amount_baseline = all_amount.head(baseline_period).mean()
            else:
                amount_baseline = all_amount.mean() if len(all_amount) > 0 else amount_mean
            
            # 如果期间平均量能 < 基准的80%，视为缩量横盘
            is_low_volume_consolidation = amount_mean < amount_baseline * 0.8 if amount_baseline > 0 else False
        
        # ========== 构建返回结果（保持向后兼容）==========
        result = {
            # 原有字段（向后兼容）
            'amplitude': round(float(amplitude * 100), 2) if not pd.isna(amplitude) else None,  # 转为百分比
            'lowest_price': round(float(lowest), 2) if not pd.isna(lowest) else None,
            'highest_price': round(float(highest), 2) if not pd.isna(highest) else None,
            'amount_std': round(float(amount_std), 2) if not pd.isna(amount_std) else None,
        }
        
        # 新增字段（子阶段）
        if cfg.enable_sub_stage and sub_stage:
            result['sub_stage'] = sub_stage
        
        # 状态画像矩阵（如果启用）
        if cfg.enable_status_portrait:
            result['status_portrait'] = {
                'amplitude': round(float(amplitude * 100), 2) if not pd.isna(amplitude) else None,
                'box_center': round(float(box_center), 2) if not pd.isna(box_center) else None,
                'box_upper': round(float(box_upper), 2) if not pd.isna(box_upper) else None,
                'box_lower': round(float(box_lower), 2) if not pd.isna(box_lower) else None,
                'false_breakout_count': false_breakout_count,
                'volume_std': round(float(amount_std), 2) if not pd.isna(amount_std) else None,
                'is_low_volume_consolidation': is_low_volume_consolidation,
            }
            if cfg.enable_sub_stage and sub_stage:
                result['status_portrait']['sub_stage'] = sub_stage
        
        return result
    
    def _calculate_trend_metrics(
        self,
        close: pd.Series,
        high: pd.Series,
        low: pd.Series,
        amount: pd.Series,
        status: str,
        start_date: Optional[pd.Timestamp],
        current_date: str
    ) -> Dict[str, Any]:
        """
        计算趋势状态的指标：持续时间、涨幅、是否明显放量、五日涨幅是否明显升高
        优化版本：增加子阶段识别、状态画像矩阵、量能优化
        """
        current_ts = pd.to_datetime(current_date)
        cfg = self.config
        
        # 取趋势期间的数据
        if start_date:
            period_mask = (close.index >= start_date) & (close.index <= current_ts)
        else:
            period_mask = close.index <= current_ts
        
        period_data = close[period_mask]
        if period_data.empty or len(period_data) < 2:
            return self._empty_trend_metrics()
        
        period_amount = amount[period_mask] if isinstance(amount, pd.Series) and len(amount) > 0 else pd.Series(0, index=period_data.index)
        period_high = high[period_mask] if isinstance(high, pd.Series) else high.reindex(period_data.index).fillna(period_data)
        period_low = low[period_mask] if isinstance(low, pd.Series) else low.reindex(period_data.index).fillna(period_data)
        
        # 基础指标
        duration_days = len(period_data)
        start_price = period_data.iloc[0]
        end_price = period_data.iloc[-1]
        trend_return = (end_price - start_price) / start_price if start_price > 0 else 0
        
        # 计算当前MASS和斜率（用于子阶段判断）
        current_mass_short = self.calculate_mass_score(close[close.index <= current_ts], cfg.mass_short_period) if self.calculate_mass_score else 0
        ma5_current = close[close.index <= current_ts].rolling(5).mean()
        ma10_current = close[close.index <= current_ts].rolling(10).mean()
        ma5_slope_current = self.calculate_ma_slope(ma5_current, cfg.ma_slope_period) if self.calculate_ma_slope and len(ma5_current) > 0 else 0
        ma10_slope_current = self.calculate_ma_slope(ma10_current, cfg.ma_slope_period) if self.calculate_ma_slope and len(ma10_current) > 0 else 0
        current_close = period_data.iloc[-1]
        current_ma5 = ma5_current.iloc[-1] if len(ma5_current) > 0 and not pd.isna(ma5_current.iloc[-1]) else current_close
        price_deviation = (current_close - current_ma5) / current_ma5 if current_ma5 > 0 else 0
        
        # 子阶段识别（如果启用）
        sub_stage = None
        if cfg.enable_sub_stage:
            if status == '趋势上行':
                if current_mass_short >= cfg.mass_exhaustion_threshold and abs(price_deviation) > cfg.price_deviation_exhaustion:
                    sub_stage = '衰竭'
                elif current_mass_short >= cfg.mass_main_stage_threshold and ma5_slope_current >= cfg.slope_main_stage_threshold:
                    sub_stage = '主升'
                else:
                    sub_stage = '启动'
            elif status == '趋势下行':
                if current_mass_short >= cfg.mass_exhaustion_threshold and abs(price_deviation) > cfg.price_deviation_exhaustion:
                    sub_stage = '衰竭'
                elif current_mass_short >= cfg.mass_main_stage_threshold and ma5_slope_current <= -cfg.slope_main_stage_threshold:
                    sub_stage = '主跌'
                else:
                    sub_stage = '启动'
        
        # ========== 量能优化（阶段五）：使用长周期基准 + 极度缩量处理 ==========
        has_volume_surge = False
        volume_surge_ratio = 0.0
        moderate_volume_days_ratio = 0.0
        is_extreme_low_volume = False
        
        if len(period_amount) > cfg.volume_surge_days and period_amount.sum() > 0:
            # 使用120日长周期量能基准
            baseline_period = cfg.volume_baseline_period
            if start_date:
                pre_trend_mask = close.index < start_date
                if pre_trend_mask.sum() >= baseline_period:
                    pre_trend_amount = amount[pre_trend_mask]
                    if len(pre_trend_amount) > 0 and pre_trend_amount.sum() > 0:
                        amount_baseline = pre_trend_amount.tail(baseline_period).mean()
                    else:
                        # 如果趋势开始前数据不足，使用整个历史数据的前baseline_period日
                        all_amount = amount[amount.index <= current_ts]
                        if len(all_amount) >= baseline_period:
                            amount_baseline = all_amount.head(baseline_period).mean()
                        else:
                            amount_baseline = all_amount.mean() if len(all_amount) > 0 else period_amount.mean()
                else:
                    all_amount = amount[amount.index <= current_ts]
                    if len(all_amount) >= baseline_period:
                        amount_baseline = all_amount.head(baseline_period).mean()
                    else:
                        amount_baseline = all_amount.mean() if len(all_amount) > 0 else period_amount.mean()
            else:
                all_amount = amount[amount.index <= current_ts]
                if len(all_amount) >= baseline_period:
                    amount_baseline = all_amount.head(baseline_period).mean()
                else:
                    amount_baseline = all_amount.mean() if len(all_amount) > 0 else period_amount.mean()
            
            # 检测极度缩量场景
            if amount_baseline > 0:
                current_volume_ratio = period_amount.iloc[-1] / amount_baseline if len(period_amount) > 0 else 0
                is_extreme_low_volume = current_volume_ratio < cfg.extreme_low_volume_threshold
                
                # 在极度缩量期间，不触发放量判断
                if not is_extreme_low_volume:
                    # 检查是否有连续 cfg.volume_surge_days 天成交额 > 基准 * multiplier
                    for i in range(len(period_amount) - cfg.volume_surge_days + 1):
                        window = period_amount.iloc[i:i + cfg.volume_surge_days]
                        window_mean = window.mean()
                        window_ratio = window_mean / amount_baseline if amount_baseline > 0 else 0
                        
                        # 更严谨：窗口均值 > 基准 * multiplier，且窗口内至少有80%的天数 > 基准 * multiplier
                        if (window_mean > amount_baseline * cfg.volume_surge_multiplier and
                            (window > amount_baseline * cfg.volume_surge_multiplier).sum() >= int(cfg.volume_surge_days * 0.8)):
                            has_volume_surge = True
                            volume_surge_ratio = window_ratio
                            break
                
                # 计算温和放量天数占比（1.2-1.5倍基准）
                if amount_baseline > 0:
                    moderate_volume_count = ((period_amount > amount_baseline * 1.2) & 
                                            (period_amount <= amount_baseline * 1.5)).sum()
                    moderate_volume_days_ratio = moderate_volume_count / len(period_amount) if len(period_amount) > 0 else 0
        
        # 每五日平均涨幅是否明显升高（更严谨的计算）
        five_day_acceleration = False
        if len(period_data) >= cfg.five_day_return_window * 3:  # 至少需要3个窗口的数据
            # 计算滚动五日平均涨幅
            returns = period_data.pct_change().fillna(0)
            five_day_returns = returns.rolling(cfg.five_day_return_window).mean().dropna()
            
            if len(five_day_returns) >= 3:
                # 检查最近3个窗口是否呈现递增趋势
                recent_returns = five_day_returns.iloc[-3:]
                
                # 方法1：检查最近两个窗口的差值是否超过阈值
                acceleration_1 = recent_returns.iloc[-1] - recent_returns.iloc[-2]
                
                # 方法2：检查是否有明显的加速趋势（最近窗口 > 前一个窗口 > 再前一个窗口）
                is_increasing = (recent_returns.iloc[-1] > recent_returns.iloc[-2] and 
                                recent_returns.iloc[-2] > recent_returns.iloc[-3])
                
                # 方法3：检查最近窗口相对于前两个窗口的平均值是否有明显提升
                avg_previous = (recent_returns.iloc[-2] + recent_returns.iloc[-3]) / 2
                acceleration_2 = recent_returns.iloc[-1] - avg_previous
                
                # 综合判断：满足以下条件之一即可认为有明显加速
                # 1. 最近两个窗口差值超过阈值
                # 2. 三个窗口呈现递增趋势，且最近窗口相对于前两个窗口的平均值有明显提升
                five_day_acceleration = (
                    acceleration_1 >= cfg.five_day_return_acceleration_threshold or
                    (is_increasing and acceleration_2 >= cfg.five_day_return_acceleration_threshold * 0.8)
                )
        
        # ========== 计算波动指标 ==========
        returns = period_data.pct_change().fillna(0)
        volatility = returns.std() if len(returns) > 1 else 0
        
        # 计算最大回撤
        cumulative_returns = (1 + returns).cumprod()
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min() if len(drawdown) > 0 else 0
        
        # ========== 计算MASS一致性 ==========
        current_mass_long = self.calculate_mass_score(close[close.index <= current_ts], cfg.mass_long_period) if len(close[close.index <= current_ts]) >= cfg.mass_long_period and self.calculate_mass_score else 0
        mass_consistency = abs(current_mass_short - current_mass_long) if current_mass_long > 0 else None
        
        # ========== 计算加速强度 ==========
        acceleration_strength = 0.0
        if five_day_acceleration and len(five_day_returns) >= 3:
            recent_returns = five_day_returns.iloc[-3:]
            acceleration_strength = float(recent_returns.iloc[-1] - recent_returns.iloc[-3])
        
        # ========== 构建返回结果（保持向后兼容）==========
        result = {
            # 原有字段（向后兼容）
            'duration_days': duration_days,
            'trend_return': round(float(trend_return * 100), 2) if not pd.isna(trend_return) else None,  # 转为百分比
            'has_volume_surge': has_volume_surge,
            'five_day_acceleration': five_day_acceleration,
        }
        
        # 新增字段（子阶段）
        if cfg.enable_sub_stage and sub_stage:
            result['sub_stage'] = sub_stage
        
        # 新增字段（量能优化）
        if cfg.enable_status_portrait:
            result['volume_surge_ratio'] = round(float(volume_surge_ratio), 2) if volume_surge_ratio > 0 else None
            result['moderate_volume_days_ratio'] = round(float(moderate_volume_days_ratio), 2) if moderate_volume_days_ratio > 0 else None
            result['is_extreme_low_volume'] = is_extreme_low_volume
        
        # 状态画像矩阵（如果启用）
        if cfg.enable_status_portrait:
            result['status_portrait'] = {
                'time': {
                    'duration_days': duration_days,
                    'start_date': start_date.strftime('%Y-%m-%d') if start_date else None,
                },
                'space': {
                    'trend_return': round(float(trend_return * 100), 2) if not pd.isna(trend_return) else None,
                    'max_drawdown': round(float(max_drawdown * 100), 2) if not pd.isna(max_drawdown) else None,
                    'volatility': round(float(volatility * 100), 2) if not pd.isna(volatility) else None,
                },
                'structure': {
                    'mass_short': round(float(current_mass_short), 2) if not pd.isna(current_mass_short) else None,
                    'mass_long': round(float(current_mass_long), 2) if not pd.isna(current_mass_long) and current_mass_long > 0 else None,
                    'mass_consistency': round(float(mass_consistency), 2) if mass_consistency is not None and not pd.isna(mass_consistency) else None,
                },
                'momentum': {
                    'ma5_slope': round(float(ma5_slope_current), 6) if not pd.isna(ma5_slope_current) else None,
                    'ma10_slope': round(float(ma10_slope_current), 6) if not pd.isna(ma10_slope_current) else None,
                    'price_vs_ma5': round(float(price_deviation * 100), 2) if not pd.isna(price_deviation) else None,
                },
                'volume': {
                    'has_volume_surge': has_volume_surge,
                    'volume_surge_ratio': round(float(volume_surge_ratio), 2) if volume_surge_ratio > 0 else None,
                    'moderate_volume_days_ratio': round(float(moderate_volume_days_ratio), 2) if moderate_volume_days_ratio > 0 else None,
                    'is_extreme_low_volume': is_extreme_low_volume,
                },
                'acceleration': {
                    'five_day_acceleration': five_day_acceleration,
                    'acceleration_strength': round(float(acceleration_strength * 100), 2) if not pd.isna(acceleration_strength) else None,
                },
            }
            if cfg.enable_sub_stage and sub_stage:
                result['status_portrait']['sub_stage'] = sub_stage
        
        return result
    
    def _empty_trend_metrics(self) -> Dict[str, Any]:
        """返回空趋势指标（数据不足时）"""
        return {
            'duration_days': 0,
            'trend_return': None,
            'has_volume_surge': False,
            'five_day_acceleration': False
        }
    
    def _empty_status(self) -> Dict[str, Any]:
        """返回空状态（数据不足时）"""
        return {
            'status': '未知',
            'status_start_date': None,
            'amplitude': None,
            'lowest_price': None,
            'highest_price': None,
            'amount_std': None,
            'duration_days': 0,
            'trend_return': None,
            'has_volume_surge': False,
            'five_day_acceleration': False
        }


# ==================== 六、日度生产流水线 ====================


class DailyMomentumPipeline:
    """
    日度动量生产流水线

    用法示例（供 Flask / 脚本调用）：

    from momentum_pipeline import DailyMomentumPipeline
    pipeline = DailyMomentumPipeline()
    scores_df = pipeline.run_daily_pipeline('2026-01-20')
    """

    def __init__(
        self,
        data_loader: Optional[DataLoader] = None,
        factor_calculator: Optional[FactorCalculator] = None,
        factor_processor: Optional[FactorProcessor] = None,
        factor_evaluator: Optional[FactorEvaluator] = None,
        status_analyzer: Optional[StockStatusAnalyzer] = None,
        status_config: Optional[StatusAnalyzerConfig] = None,
    ):
        self.data_loader = data_loader or DataLoader()
        self.factor_calc = factor_calculator or FactorCalculator()
        self.processor = factor_processor or FactorProcessor()
        self.evaluator = factor_evaluator or FactorEvaluator()
        self.status_analyzer = status_analyzer or StockStatusAnalyzer(config=status_config)

    def run_daily_pipeline(self, current_date: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        日度流水线主函数
        - params: 可选，如 min_trading_days(默认300)、winsorize_alpha(默认0.025)。calc_window_days 在 DataLoaderConfig 中。
        返回：包含 code / date / momentum_score / 各单因子的 DataFrame
        """
        p = params or {}
        min_trading_days = int(p.get('min_trading_days', 300))
        winsorize_alpha = float(p.get('winsorize_alpha', 0.025))

        logger.info(f"[MomentumPipeline] 开始处理日期: {current_date}")

        # 1. 数据准备
        base_data = self.data_loader.prepare_base_data(current_date)
        daily_data = base_data["daily_data"]
        industry_data = base_data["industry_data"]

        if daily_data.empty:
            logger.warning("daily_data 为空，流水线终止。")
            return pd.DataFrame()

        # 2. 单因子计算（按 code 分组，多线程并行优化）
        logger.info("步骤2: 单因子计算（多线程并行）...")
        
        # 准备股票代码列表（过滤掉交易日数不足的）
        grouped = daily_data.groupby("code")
        stock_codes = []
        stock_data_dict = {}  # 预存每个股票的数据，避免重复分组
        for code, stock_df in grouped:
            if len(stock_df) >= min_trading_days:
                stock_codes.append(code)
                stock_data_dict[code] = stock_df.copy()  # 只复制一次
        
        if not stock_codes:
            logger.warning("未找到满足最小交易日数的股票，流水线终止。")
            return pd.DataFrame()
        
        logger.info(f"步骤2: 准备并行计算 {len(stock_codes)} 只股票的因子...")
        
        # 使用多线程并行计算因子
        try:
            from high_performance_threading import HighPerformanceThreadPool
            from config import MAX_WORKERS
            import multiprocessing
            
            cpu_count = multiprocessing.cpu_count()
            thread_pool = HighPerformanceThreadPool(
                progress_desc="单因子计算进度",
                enable_progress_bar=True,
                max_workers=MAX_WORKERS
            )
            logger.info(f"单因子计算并行配置: 线程数={MAX_WORKERS}, CPU核心数={cpu_count}, 股票数={len(stock_codes)}")
            
            def calculate_one_stock_factors(code: str) -> Tuple[str, Dict[str, float]]:
                """计算单只股票的因子"""
                try:
                    stock_df = stock_data_dict[code]
                    factors = self.factor_calc.calculate_all_factors(stock_df, current_date)
                    return (code, factors)
                except Exception as e:
                    logger.debug(f"股票 {code} 因子计算失败: {e}")
                    return (code, {})
            
            # 并行执行所有因子计算任务
            results = thread_pool.execute_parallel(stock_codes, calculate_one_stock_factors)
            
            # 转换为字典
            factors_dict = {code: factors for code, factors in results if factors}
            logger.info(f"步骤2完成: 成功计算 {len(factors_dict)} 只股票的因子")
            
        except ImportError:
            logger.warning("未找到 high_performance_threading，使用单线程计算因子")
            # 降级为单线程
            factors_dict = {}
            for code in stock_codes:
                try:
                    stock_df = stock_data_dict[code]
                    factors = self.factor_calc.calculate_all_factors(stock_df, current_date)
                    factors_dict[code] = factors
                except Exception as e:
                    logger.debug(f"股票 {code} 因子计算失败: {e}")
        
        if not factors_dict:
            logger.warning("未计算出任何单因子结果，流水线终止。")
            return pd.DataFrame()

        raw_factors = pd.DataFrame.from_dict(factors_dict, orient="index")
        raw_factors.index.name = "code"

        # 3. 因子预处理（行业 + 市值）- 优化：减少数据复制和索引操作
        logger.info("步骤3: 因子预处理...")
        # 当前日期行业与市值（优化：只查询一次，避免重复过滤）
        if not industry_data.empty:
            # 优化：直接使用 loc 而不是先 copy 再过滤
            current_industry_mask = industry_data["date"] == current_date
            if current_industry_mask.any():
                current_industry = industry_data.loc[current_industry_mask].copy()
            else:
                current_industry = pd.DataFrame()
        else:
            current_industry = pd.DataFrame()

        # 对齐代码（优化：减少不必要的操作）
        if not current_industry.empty:
            # 优化：一次性处理代码格式化和索引设置
            current_industry = current_industry.copy()
            current_industry["code"] = current_industry["code"].astype(str).str.zfill(6)
            current_industry = current_industry.set_index("code")
            
            # 行业字段名假定为 industry，如需可在此处调整
            if "industry" in current_industry.columns:
                industry_series = current_industry["industry"]
            else:
                industry_series = pd.Series("Unknown", index=raw_factors.index)

            if "total_mv" in current_industry.columns:
                marketcap_series = current_industry["total_mv"]
            else:
                marketcap_series = pd.Series(1e9, index=raw_factors.index)
        else:
            # 完全没有行业数据时，用占位
            industry_series = pd.Series("Unknown", index=raw_factors.index)
            marketcap_series = pd.Series(1e9, index=raw_factors.index)

        # 对齐索引（优化：只在需要时对齐）
        if not industry_series.index.equals(raw_factors.index):
            industry_series = industry_series.reindex(raw_factors.index).fillna("Unknown")
        if not marketcap_series.index.equals(raw_factors.index):
            marketcap_series = marketcap_series.reindex(raw_factors.index).fillna(1e9)

        processed_factors = self.processor.process_factors(
            raw_factors, industry_series, marketcap_series, winsorize_alpha=winsorize_alpha
        )

        # 4. 因子合成（默认等权）
        logger.info("步骤4: 因子合成...")
        momentum_scores = self.evaluator.synthesize_factors(processed_factors)

        # 5. 计算动量指标（多周期涨幅、振幅、极值、趋势、成交量）
        logger.info("步骤5: 计算动量指标...")
        momentum_metrics_dict = {}
        grouped = daily_data.groupby("code")
        for code, stock_df in grouped:
            try:
                metrics = self.factor_calc.calculate_momentum_metrics(stock_df)
                momentum_metrics_dict[code] = metrics
            except Exception as e:
                logger.debug(f"股票 {code} 动量指标计算失败: {e}")
                momentum_metrics_dict[code] = {}
        
        # 6. 结果整理
        logger.info("步骤6: 结果整理...")
        result_df = processed_factors.copy()
        result_df["momentum_score"] = momentum_scores
        result_df.reset_index(inplace=True)
        result_df["date"] = current_date
        
        # 将动量指标添加到结果DataFrame
        momentum_metrics_cols = [
            'return_1d', 'return_5d', 'return_10d', 'return_20d', 'return_60d', 'return_120d', 'return_250d',
            'amplitude_5d', 'amplitude_20d', 'amplitude_60d', 'daily_volatility', 'annualized_volatility',
            'max_gain_20d', 'max_gain_60d', 'max_drawdown_20d', 'max_drawdown_60d',
            'recent_high', 'recent_low', 'price_vs_high',
            'ma5_slope', 'ma10_slope', 'ma20_slope', 'price_vs_ma5', 'price_vs_ma20', 'ma5_vs_ma20', 'trend_strength',
            'volume_ratio_5d', 'volume_ratio_20d'
        ]
        for col in momentum_metrics_cols:
            result_df[col] = result_df['code'].map(lambda c: momentum_metrics_dict.get(c, {}).get(col, np.nan))

        # 7.（已停用）股票状态分析
        # 根据你的最新要求，这一步的「状态分析」逻辑（震荡 / 趋势上行 / 趋势下行）
        # 已经停用，不再在流水线中执行，只保留动量相关指标的计算与展示。
        #
        # 保留 StockStatusAnalyzer 等代码以备将来需要时重新启用，但当前 run_daily_pipeline
        # 不再调用状态分析，避免日志中出现“股票状态分析”步骤，也避免额外的耗时。

        logger.info(f"[MomentumPipeline] 日期 {current_date} 处理完成（仅动量相关指标），共 {len(result_df)} 只股票。")
        return result_df
    
    def _analyze_stock_statuses_batch(
        self,
        daily_data: pd.DataFrame,
        stock_codes: List[str],
        current_date: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        批量分析股票状态（多线程优化）
        
        返回：{code: {status: ..., status_start_date: ..., ...}}
        """
        if not stock_codes or daily_data.empty:
            return {}
        
        # 检查是否启用状态分析（可通过 params 控制）
        p = params or {}
        enable_status_analysis = p.get('enable_status_analysis', True)
        if not enable_status_analysis:
            return {}
        
        logger.info(f"开始批量分析 {len(stock_codes)} 只股票的状态...")
        
        # 优化：获取前一日状态（用于状态继承）- 批量查询优化
        previous_statuses = {}
        try:
            import sqlite3
            from datetime import timedelta
            current_ts = pd.to_datetime(current_date)
            prev_date = (current_ts - timedelta(days=7)).strftime('%Y-%m-%d')  # 往前找7天内的数据
            
            if stock_codes:
                conn = sqlite3.connect('database.db')
                conn.row_factory = sqlite3.Row
                c = conn.cursor()
                
                # 优化：使用更高效的查询方式，一次性获取所有需要的前一日状态
                # 使用 GROUP BY 和 MAX 来获取每个code的最新状态，避免在Python中过滤
                placeholders = ','.join(['?'] * len(stock_codes))
                # 使用子查询获取每个code的最新状态记录
                c.execute(
                    f'''SELECT code, status, status_start_date 
                       FROM momentum_scores 
                       WHERE date < ? AND code IN ({placeholders})
                       AND (code, date) IN (
                           SELECT code, MAX(date) 
                           FROM momentum_scores 
                           WHERE date < ? AND code IN ({placeholders})
                           GROUP BY code
                       )''',
                    [current_date] + stock_codes + [current_date] + stock_codes
                )
                rows = c.fetchall()
                conn.close()
                
                # 构建前一日状态字典
                for row in rows:
                    code = row['code']
                    if row['status']:  # 只保存有效状态
                        previous_statuses[code] = {
                            'status': row['status'],
                            'status_start_date': row['status_start_date']
                        }
                
                if previous_statuses:
                    logger.debug(f"获取到 {len(previous_statuses)} 只股票的前一日状态")
        except Exception as e:
            logger.debug(f"获取前一日状态失败: {e}，将不使用状态继承")
        
        # 使用多线程批量处理
        try:
            from high_performance_threading import HighPerformanceThreadPool
            from config import MAX_WORKERS
            import multiprocessing
            
            # 状态分析是计算密集型任务，直接使用 execute_parallel 最大化并行度
            # 这样可以避免批次管理的开销，所有任务直接并行执行
            cpu_count = multiprocessing.cpu_count()
            thread_pool = HighPerformanceThreadPool(
                progress_desc="状态分析进度",
                enable_progress_bar=True,
                max_workers=MAX_WORKERS  # 使用配置的最大线程数
            )
            logger.info(f"状态分析并行配置: 线程数={MAX_WORKERS}, CPU核心数={cpu_count}, 股票数={len(stock_codes)}")
            
            # 优化：预先准备股票数据，避免重复过滤和复制
            # 将 daily_data 按 code 分组并预处理，减少每个线程的数据处理开销
            preprocessed_stock_data = {}
            for code in stock_codes:
                stock_df = daily_data[daily_data['code'] == code].copy()
                if not stock_df.empty:
                    # 预处理：设置索引
                    if 'date' in stock_df.columns:
                        stock_df = stock_df.set_index('date').sort_index()
                    elif stock_df.index.name != 'date' and isinstance(stock_df.index, pd.DatetimeIndex):
                        stock_df.index.name = 'date'
                    preprocessed_stock_data[code] = stock_df
            
            def analyze_one_stock(code: str) -> Tuple[str, Dict[str, Any]]:
                """分析单只股票的状态（优化版：使用预处理的数据）"""
                try:
                    stock_df = preprocessed_stock_data.get(code)
                    if stock_df is None or stock_df.empty:
                        return (code, self.status_analyzer._empty_status())
                    
                    # 获取前一日状态（如果有）
                    prev_status = previous_statuses.get(code)
                    
                    status_result = self.status_analyzer.analyze_stock_status(
                        stock_df, current_date, previous_status=prev_status
                    )
                    return (code, status_result)
                except Exception as e:
                    logger.debug(f"股票 {code} 状态分析失败: {e}")
                    return (code, self.status_analyzer._empty_status())
            
            # 对于状态分析这种计算密集型任务，使用 execute_parallel 直接并行执行所有任务
            # 这样可以最大化并行度，避免批次管理的开销
            results = thread_pool.execute_parallel(stock_codes, analyze_one_stock)
            
            # 转换为字典
            status_dict = {code: result for code, result in results if result}
            logger.info(f"状态分析完成，成功分析 {len(status_dict)} 只股票")
            return status_dict
            
        except ImportError:
            logger.warning("未找到 high_performance_threading，使用单线程处理状态分析")
            # 降级为单线程
            status_dict = {}
            for code in stock_codes:
                try:
                    stock_df = daily_data[daily_data['code'] == code].copy()
                    if stock_df.empty:
                        continue
                    if 'date' in stock_df.columns:
                        stock_df = stock_df.set_index('date').sort_index()
                    elif stock_df.index.name != 'date' and isinstance(stock_df.index, pd.DatetimeIndex):
                        stock_df.index.name = 'date'
                    status_result = self.status_analyzer.analyze_stock_status(stock_df, current_date)
                    status_dict[code] = status_result
                except Exception as e:
                    logger.debug(f"股票 {code} 状态分析失败: {e}")
            return status_dict


def save_momentum_scores_to_csv(df: pd.DataFrame, current_date: str, output_dir: str = "results") -> str:
    """
    辅助函数：将结果保存为 CSV 文件，便于调度/调试或备份。
    Flask 后端集成时会将结果写入 SQLite，但也可以同时保留 CSV。
    """
    if df.empty:
        raise ValueError("结果 DataFrame 为空，无法保存")

    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"momentum_scores_{current_date}.csv")
    df.to_csv(file_path, index=False, encoding="utf-8-sig")
    logger.info(f"[MomentumPipeline] 动量分结果已保存到 {file_path}")
    return file_path

