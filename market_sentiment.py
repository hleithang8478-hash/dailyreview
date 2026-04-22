# -*- coding: utf-8 -*-
"""
每日市场情绪打分 v5 — 八大模块全面平滑打分改造
  改造一：成交额加权情绪（Amount-Weighted Breadth）
  改造二：风格因子敞口与崩溃监控（Size + Vol Factor Spreads）
  改造三：聪明钱与日内承接力（Smart Money & Intraday Premium）
  改造四：股票池锚定（Universe Isolation — 剔除流动性后10%）
  改造五：全模块平滑打分（连续映射 + Z-Score 动态自适应，废弃硬编码阈值）

八维度：
  M1 资金多空比（成交额加权）
  M2 流动性缩量
  M3 加权横截面离散度 (WCSV)
  M4 宽基指数趋势（价格偏离度平滑）
  M5 风格因子监控（Size + Vol Factor Spreads，Z-Score自适应）
  M6 短线赚钱效应
  M7 聪明钱与日内承接力（Smart Money）
  M8 强势股补跌（Z-Score自适应）
"""
import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

SENTIMENT_VERSION = 'v5'

# ─── 常量 ────────────────────────────────────────────
LIMIT_THRESHOLD = 0.098
UNIVERSE_LIQUIDITY_CUTOFF = 0.10  # 每日剔除成交额最低10%

# Module 1: 资金多空比（平滑打分参数）
BREADTH_LOOKBACK_DAYS = 252
# 跌停恐慌：历史分位数映射，80%→不扣分，98%→扣满40分
M1_LIMITDOWN_SAFE_PCT   = 80.0
M1_LIMITDOWN_DANGER_PCT = 98.0
M1_LIMITDOWN_MAX_PENALTY = 40
# 资金多空比 MFR：比率映射，≥0.5不扣分，≤0.2扣满15分
M1_MFR_SAFE        = 0.5
M1_MFR_DANGER      = 0.2
M1_MFR_MAX_PENALTY = 15

# Module 2: 流动性缩量（平滑打分参数）
LIQUIDITY_LOOKBACK_DAYS = 252
# 短期断崖缩量：5/20日均量比，≥0.85不扣分，≤0.60扣满15分
M2_SHRINK_SAFE        = 0.85
M2_SHRINK_DANGER      = 0.60
M2_SHRINK_MAX_PENALTY = 15
# 绝对地量：历史分位数，≥20%不扣分，≤5%扣满15分
M2_VOLUME_SAFE_PCT    = 20.0
M2_VOLUME_DANGER_PCT  = 5.0
M2_VOLUME_MAX_PENALTY = 15

# Module 3: 加权横截面离散度（平滑打分参数）
CSV_LOOKBACK_DAYS = 61
# WCSV分位数：≥25%不扣分，≤5%扣满20分
M3_CSV_SAFE_PCT    = 25.0
M3_CSV_DANGER_PCT  = 5.0
M3_CSV_MAX_PENALTY = 20

# Module 4: 宽基指数趋势（价格偏离度平滑）
BROAD_INDEX_MA_DAYS = 20
BROAD_INDEX_MA60_DAYS = 60
INNERCODE_CSI1000 = 3145
INNERCODE_SH50 = 11089
CORE_INDEX_MA60 = (46, 30, 3145)
# 中证1000偏离MA20：≥+1%不扣分，≤-4%扣满15分
M4_CSI1000_MA20_SAFE        = 0.01
M4_CSI1000_MA20_DANGER      = -0.04
M4_CSI1000_MA20_MAX_PENALTY = 15
# 三指数偏离MA60均值：≥+1%不扣分，≤-5%扣满30分
M4_MA60_SAFE        = 0.01
M4_MA60_DANGER      = -0.05
M4_MA60_MAX_PENALTY = 30

# Module 5: 风格因子（Z-Score 动态自适应）
FACTOR_QUINTILE      = 0.2   # 分组用 20% 分位
FACTOR_VOL_WINDOW    = 20    # 20日滚动波动率
FACTOR_ZSCORE_WINDOW = 60    # Z-Score 参考窗口（天）
# Z-Score步进：从-1.0σ起扣，每下偏0.5σ扣5分
M5_ZSCORE_START       = -1.0
M5_ZSCORE_STEP        = 0.5
M5_ZSCORE_STEP_PENALTY = 5
M5_SIZE_MAX_PENALTY   = 20
M5_VOL_MAX_PENALTY    = 15

# Module 6: 短线赚钱效应（平滑打分参数）
HOT_LOOKBACK_DAYS = 3
HOT_MIN_COUNT = 10
# 核按钮比例：≤5%不扣分，≥25%扣满20分
M6_NUKE_SAFE        = 0.05
M6_NUKE_DANGER      = 0.25
M6_NUKE_MAX_PENALTY = 20
# 昨涨停股今均收益：≥0%不扣分，≤-5%扣满20分
M6_HOT_AVG_SAFE        = 0.0
M6_HOT_AVG_DANGER      = -0.05
M6_HOT_AVG_MAX_PENALTY = 20

# 连续能量模型 (Continuous Energy Index)
ENERGY_LOOKBACK_DAYS = 252         # Z-Score 参考窗口
ENERGY_ROLL_MEAN_DAYS = 60         # 动态中性线窗口
ENERGY_BASE_DECAY = 0.05           # 基础衰减
ENERGY_CAP = 150.0                 # Cap 系数
ENERGY_NEG_WEIGHT = 1.5            # 环境变差时的负向放大
ENERGY_Z_THRESH = 2.0              # 异常 Z-Score 阈值
ENERGY_MIN_HISTORY = 40            # 至少多少历史点再开始计算
ENERGY_CORR_WINDOW = 20            # 系统收敛度滚动相关窗口

# Module 7: 聪明钱（四形态解构 + 对冲折算 + 平滑打分）
TURNOVER_TOP_PCT = 0.10          # 换手率前10%
M7_BODY_RATIO_THRESH = 0.70     # 单边形态实体占全天波幅的最低比例
M7_HEDGE_ALPHA         = 0.80   # 诱空吸筹对诱多派发的对冲折算系数（抛压重力 > 托盘动能）
# 净派发占比：≤30%不扣分，≥60%扣满15分
M7_DISTRIB_SAFE        = 0.30
M7_DISTRIB_DANGER      = 0.60
M7_DISTRIB_MAX_PENALTY = 15
# 活跃池均收益：≥0%不扣分，≤-4%扣满15分
M7_TURNOVER_SAFE        = 0.0
M7_TURNOVER_DANGER      = -0.04
M7_TURNOVER_MAX_PENALTY = 15

# Module 8: 强势股补跌（Z-Score 自适应）
STRONG_TOP_PERCENT = 10
# Z-Score步进：从-1.0σ起扣，每下偏0.5σ扣5分，满20分
M8_ZSCORE_START        = -1.0
M8_ZSCORE_STEP         = 0.5
M8_ZSCORE_STEP_PENALTY = 5
M8_MAX_PENALTY         = 20


# ─── 平滑打分辅助函数 ────────────────────────────────

def _smooth_penalty(value: float, safe_bound: float, danger_bound: float,
                    max_penalty: float) -> float:
    """连续线性扣分映射。
    value 在 safe_bound 以内不扣分，到达 danger_bound 扣满 max_penalty，中间线性插值。
    支持递减（safe > danger，值越低越危险）和递增（safe < danger，值越高越危险）两个方向。
    """
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return 0.0
    if safe_bound > danger_bound:  # 值越低越危险（如比率、分位数）
        if value >= safe_bound:
            return 0.0
        if value <= danger_bound:
            return float(max_penalty)
        ratio = (safe_bound - value) / (safe_bound - danger_bound)
    else:  # 值越高越危险（如核按钮比例、诱多派发占比）
        if value <= safe_bound:
            return 0.0
        if value >= danger_bound:
            return float(max_penalty)
        ratio = (value - safe_bound) / (danger_bound - safe_bound)
    return round(float(max_penalty) * ratio, 2)


def _zscore_step_penalty(z_score: float, start: float = -1.0, step: float = 0.5,
                         step_penalty: float = 5.0, max_penalty: float = 20.0) -> float:
    """Z-Score 步进扣分：z_score 高于 start 时不扣分，
    从 start 开始每向下偏离 step 个 sigma 扣 step_penalty 分，上限 max_penalty。
    例：start=-1.0, step=0.5, step_penalty=5 → z=-1.0→0, z=-1.5→5, z=-2.0→10, z=-3.0→20。
    """
    if z_score is None or (isinstance(z_score, float) and not np.isfinite(z_score)):
        return 0.0
    if z_score >= start:
        return 0.0
    steps = int((start - z_score) / step)
    return min(float(steps * step_penalty), float(max_penalty))


# ─── 股票池锚定（改造四）────────────────────────────

def _filter_universe(df: pd.DataFrame) -> pd.DataFrame:
    """剔除每日成交额最低10%的股票（流动性过差，实盘无法交易）。
    使用 merge 代替 groupby.transform(lambda) 以大幅提速。"""
    if df is None or df.empty or 'Amount' not in df.columns:
        return df
    q_df = df.groupby('Date')['Amount'].quantile(UNIVERSE_LIQUIDITY_CUTOFF).rename('_amt_cutoff')
    df2 = df.merge(q_df, left_on='Date', right_index=True, how='left')
    result = df2[df2['Amount'] >= df2['_amt_cutoff']].drop(columns=['_amt_cutoff'])
    return result


# ─── 动态涨跌停阈值（区分主板/双创/北交/ST）──────────

def _get_dynamic_limit_thresholds(df: pd.DataFrame) -> pd.Series:
    """根据代码前缀和名称返回每只股票的涨跌停阈值 (正数)。
    主板 0.095, 创业板/科创板 0.195, 北交所 0.295, ST 0.048。"""
    thresholds = pd.Series(0.095, index=df.index)
    if 'SecuCode' in df.columns:
        code = df['SecuCode'].astype(str)
        thresholds[code.str.match(r'^(688|300)')] = 0.195
        thresholds[code.str.match(r'^[84]')] = 0.295
    if 'SecuAbbr' in df.columns:
        thresholds[df['SecuAbbr'].astype(str).str.contains('ST', na=False)] = 0.048
    return thresholds


# ─── 向量化辅助（改造一：成交额加权）──────────────────

def _vectorized_daily_breadth(df: pd.DataFrame) -> pd.DataFrame:
    """一次性计算所有日期的涨跌停统计 + 成交额/市值加权多空比。
    输入 df 需包含列：Date, R, Open, High, Low, Close, Amount, NegotiableMV"""
    if df is None or df.empty or 'R' not in df.columns:
        return pd.DataFrame()
    d = df.dropna(subset=['R']).copy()
    if d.empty:
        return pd.DataFrame()

    for col in ['Open', 'High', 'Low', 'Close', 'Amount', 'NegotiableMV']:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors='coerce')

    has_amount = 'Amount' in d.columns
    has_mv = 'NegotiableMV' in d.columns
    if has_amount:
        d['Amount'] = d['Amount'].fillna(0)
    if has_mv:
        d['NegotiableMV'] = d['NegotiableMV'].fillna(0)

    d['_up'] = d['R'] > 0
    d['_down'] = d['R'] < 0
    _dyn_thresh = _get_dynamic_limit_thresholds(d)
    d['_limit_up'] = d['R'] > _dyn_thresh
    d['_limit_down'] = d['R'] < -_dyn_thresh

    if has_amount:
        d['_amt_up'] = d['Amount'] * d['_up'].astype(float)
        d['_amt_down'] = d['Amount'] * d['_down'].astype(float)
    if has_mv:
        d['_mv_up'] = d['NegotiableMV'] * d['_up'].astype(float)
        d['_mv_down'] = d['NegotiableMV'] * d['_down'].astype(float)

    agg_dict = {
        '_up': 'sum', '_down': 'sum',
        '_limit_up': 'sum', '_limit_down': 'sum',
        'R': 'size',
    }
    if has_amount:
        agg_dict['_amt_up'] = 'sum'
        agg_dict['_amt_down'] = 'sum'
    if has_mv:
        agg_dict['_mv_up'] = 'sum'
        agg_dict['_mv_down'] = 'sum'

    stats = d.groupby('Date').agg(agg_dict)
    stats = stats.rename(columns={
        'R': 'total_count', '_up': 'up_count', '_down': 'down_count',
        '_limit_up': 'limit_up', '_limit_down': 'limit_down',
    })

    if has_amount:
        stats.rename(columns={'_amt_up': 'amount_up', '_amt_down': 'amount_down'}, inplace=True)
        denom = stats['amount_down'].replace(0, np.nan)
        stats['money_flow_ratio'] = stats['amount_up'] / denom
    if has_mv:
        stats.rename(columns={'_mv_up': 'mv_up', '_mv_down': 'mv_down'}, inplace=True)
        denom = stats['mv_down'].replace(0, np.nan)
        stats['mv_flow_ratio'] = stats['mv_up'] / denom

    has_ohlc = all(c in d.columns for c in ['Open', 'High', 'Low', 'Close'])
    if has_ohlc:
        d2 = d.dropna(subset=['Open', 'High', 'Low', 'Close'])
        d2 = d2.assign(
            _one_word=(d2['Open'] == d2['High']) & (d2['High'] == d2['Low']) & (d2['Low'] == d2['Close']),
        )
        _dyn2 = _dyn_thresh.reindex(d2.index)
        d2 = d2.assign(
            _owld=d2['_one_word'] & (d2['R'] < -_dyn2),
            _owlu=d2['_one_word'] & (d2['R'] > _dyn2),
        )
        ow = d2.groupby('Date').agg(
            one_word_limit_down=('_owld', 'sum'),
            one_word_limit_up=('_owlu', 'sum'),
        )
        stats = stats.join(ow, how='left')
    else:
        stats['one_word_limit_down'] = 0
        stats['one_word_limit_up'] = 0

    int_cols = ['total_count', 'up_count', 'down_count',
                'limit_up', 'limit_down', 'one_word_limit_down', 'one_word_limit_up']
    stats = stats.fillna(0)
    for c in int_cols:
        if c in stats.columns:
            stats[c] = stats[c].astype(int)
    stats['up_ratio'] = stats['up_count'] / stats['total_count'].replace(0, np.nan)
    stats = stats.reset_index()
    return stats


# ─── 模块 1（改造一：成交额加权情绪）────────────────

def module1_market_breadth(daily_df: pd.DataFrame, trading_day: str) -> Dict[str, Any]:
    """资金多空比（成交额加权情绪）。daily_df 来自 _vectorized_daily_breadth。"""
    empty = {
        'money_flow_ratio': None, 'mv_flow_ratio': None,
        'divergence': None,
        'up_count': 0, 'down_count': 0,
        'limit_up': 0, 'limit_down': 0,
        'one_word_limit_down': 0, 'total_count': 0, 'up_ratio': None,
        'p90_limit_down': None, 'p98_limit_down': None,
        'penalty': 0, 'penalty_detail': [],
    }
    if daily_df is None or daily_df.empty:
        empty['error'] = '无有效数据'
        return empty

    daily_df = daily_df.copy()
    daily_df['Date'] = daily_df['Date'].astype(str)
    today_row = daily_df[daily_df['Date'] == trading_day]
    if today_row.empty:
        today_row = daily_df.iloc[[-1]]
        trading_day = str(today_row['Date'].iloc[0])
    today = today_row.iloc[0]

    hist = daily_df[daily_df['Date'] != trading_day]
    if hist.empty:
        hist = daily_df

    ld_series = hist['limit_down'].values
    ow_series = hist['one_word_limit_down'].values
    # 保留P90/P98参考值（仅用于展示，不再用于硬阈值扣分）
    p90_limit = float(np.nanpercentile(ld_series, 90))
    p98_limit = float(np.nanpercentile(ld_series, 98))

    limit_down_today = int(today.get('limit_down', 0))
    one_word_today = int(today.get('one_word_limit_down', 0))
    up_ratio_today = float(today.get('up_ratio', 0) or 0)
    up_count = int(today.get('up_count', 0))
    down_count = int(today.get('down_count', 0))
    total_count = int(today.get('total_count', 0))
    limit_up_today = int(today.get('limit_up', 0))

    mfr = today.get('money_flow_ratio', None)
    mfr = round(float(mfr), 4) if mfr is not None and np.isfinite(mfr) else None
    mvfr = today.get('mv_flow_ratio', None)
    mvfr = round(float(mvfr), 4) if mvfr is not None and np.isfinite(mvfr) else None
    if mfr is not None and mvfr is not None and mfr > 0 and mvfr > 0:
        divergence = round(float(np.log(mfr) - np.log(mvfr)), 4)
    else:
        divergence = None

    # ── 跌停恐慌：连续分位数映射 ──────────────────────────────────────
    # 取今日跌停数（含一字跌停取较大值）在历史252日的百分位
    ld_max_today = max(limit_down_today, one_word_today)
    ld_percentile = float(np.sum(ld_series <= ld_max_today) / len(ld_series) * 100) if len(ld_series) > 0 else 0.0
    penalty_ld = _smooth_penalty(ld_percentile, M1_LIMITDOWN_SAFE_PCT, M1_LIMITDOWN_DANGER_PCT, M1_LIMITDOWN_MAX_PENALTY)

    # ── 资金多空比 MFR：连续比率映射 ────────────────────────────────────
    penalty_mfr = 0.0
    if mfr is not None and total_count > 0:
        penalty_mfr = _smooth_penalty(mfr, M1_MFR_SAFE, M1_MFR_DANGER, M1_MFR_MAX_PENALTY)

    penalty = round(penalty_ld + penalty_mfr)
    penalty_detail = []
    if penalty_ld > 0:
        penalty_detail.append(
            f'跌停恐慌蔓延：跌停数处于252日{ld_percentile:.1f}%分位'
            f'（安全边界{M1_LIMITDOWN_SAFE_PCT:.0f}%，危险边界{M1_LIMITDOWN_DANGER_PCT:.0f}%），'
            f'扣{penalty_ld:.1f}分（满分{M1_LIMITDOWN_MAX_PENALTY}分）')
    if penalty_mfr > 0:
        penalty_detail.append(
            f'资金多空比MFR={mfr:.3f}（安全边界{M1_MFR_SAFE}，危险边界{M1_MFR_DANGER}），'
            f'扣{penalty_mfr:.1f}分（满分{M1_MFR_MAX_PENALTY}分）')

    return {
        'money_flow_ratio': mfr, 'mv_flow_ratio': mvfr,
        'divergence': divergence,
        'up_count': up_count, 'down_count': down_count,
        'limit_up': limit_up_today, 'limit_down': limit_down_today,
        'one_word_limit_down': one_word_today,
        'total_count': total_count, 'up_ratio': round(up_ratio_today, 4),
        'ld_percentile': round(ld_percentile, 2),
        'p90_limit_down': round(p90_limit, 0), 'p98_limit_down': round(p98_limit, 0),
        'penalty': penalty, 'penalty_detail': penalty_detail,
    }


# ─── 模块 2 ─────────────────────────────────────────

def module2_liquidity(
    turnover_today: float,
    turnover_series_5: List[float],
    turnover_series_20: List[float],
    turnover_series_252: Optional[List[float]] = None,
) -> Dict[str, Any]:
    vol_5 = np.mean(turnover_series_5) if turnover_series_5 else 0
    vol_20 = np.mean(turnover_series_20) if turnover_series_20 else 0
    penalty_detail = []

    # ── 短期断崖缩量：5/20日均量比，连续映射 ──────────────────────────
    shrink_ratio = (vol_5 / vol_20) if vol_20 > 0 else 1.0
    penalty_shrink = _smooth_penalty(shrink_ratio, M2_SHRINK_SAFE, M2_SHRINK_DANGER, M2_SHRINK_MAX_PENALTY)
    if penalty_shrink > 0:
        penalty_detail.append(
            f'短期断崖缩量：5/20日均量比={shrink_ratio:.2%}'
            f'（安全边界{M2_SHRINK_SAFE:.0%}，危险边界{M2_SHRINK_DANGER:.0%}），'
            f'扣{penalty_shrink:.1f}分（满分{M2_SHRINK_MAX_PENALTY}分）')

    # ── 绝对地量：成交额历史分位数，连续映射 ─────────────────────────
    volume_percentile = None
    p10_turnover = None
    penalty_volume = 0.0
    if turnover_series_252 and len(turnover_series_252) > 0:
        arr252 = np.array([v for v in turnover_series_252 if v is not None and np.isfinite(v)])
        if len(arr252) > 0:
            p10_turnover = float(np.nanpercentile(arr252, 10))
            volume_percentile = float(np.sum(arr252 <= turnover_today) / len(arr252) * 100)
            penalty_volume = _smooth_penalty(volume_percentile, M2_VOLUME_SAFE_PCT, M2_VOLUME_DANGER_PCT, M2_VOLUME_MAX_PENALTY)
            if penalty_volume > 0:
                penalty_detail.append(
                    f'绝对地量：成交额{turnover_today:.0f}亿处于252日{volume_percentile:.1f}%分位'
                    f'（安全边界{M2_VOLUME_SAFE_PCT:.0f}%，危险边界{M2_VOLUME_DANGER_PCT:.0f}%），'
                    f'扣{penalty_volume:.1f}分（满分{M2_VOLUME_MAX_PENALTY}分）')

    penalty = round(penalty_shrink + penalty_volume)
    return {
        'turnover_today': round(turnover_today, 2),
        'vol_5': round(vol_5, 2),
        'vol_20': round(vol_20, 2),
        'shrink_ratio': round(shrink_ratio, 4),
        'volume_percentile': round(volume_percentile, 2) if volume_percentile is not None else None,
        'p10_turnover_252': round(p10_turnover, 2) if p10_turnover is not None else None,
        'penalty': penalty,
        'penalty_detail': penalty_detail,
    }


# ─── 模块 3（改造一：加权横截面离散度 WCSV）─────────

def _weighted_cross_sectional_std(df_day: pd.DataFrame) -> float:
    """sqrt(成交额) 加权的横截面收益率标准差。
    用 sqrt 降低超大盘股的寡头效应，让中小盘波动信号不被淹没。"""
    r = df_day['R'].values
    raw_w = df_day['Amount'].values if 'Amount' in df_day.columns else np.ones(len(r))
    mask = np.isfinite(r) & np.isfinite(raw_w) & (raw_w > 0)
    r, raw_w = r[mask], raw_w[mask]
    if len(r) < 10:
        return np.nan
    w = np.sqrt(raw_w)
    w_norm = w / w.sum()
    mean_r = np.average(r, weights=w_norm)
    return float(np.sqrt(np.average((r - mean_r) ** 2, weights=w_norm)))


def module3_cross_sectional_volatility(
    sigma_weighted_today: float,
    sigma_weighted_history: List[float],
    sigma_equal_today: Optional[float] = None,
) -> Dict[str, Any]:
    """加权横截面离散度 (WCSV)：扣分基于成交额加权 sigma。"""
    if sigma_weighted_today is None or not sigma_weighted_history:
        return {'sigma_weighted': None, 'sigma_equal': None,
                'percentile': None, 'penalty': 0, 'penalty_detail': []}
    arr = np.array(sigma_weighted_history, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return {'sigma_weighted': round(sigma_weighted_today, 6),
                'sigma_equal': round(sigma_equal_today, 6) if sigma_equal_today else None,
                'percentile': None, 'penalty': 0, 'penalty_detail': []}
    percentile = float(np.sum(arr <= sigma_weighted_today) / len(arr) * 100)
    # ── WCSV 分位数：连续映射，25%→不扣，5%→扣满20分 ────────────────
    penalty_raw = _smooth_penalty(percentile, M3_CSV_SAFE_PCT, M3_CSV_DANGER_PCT, M3_CSV_MAX_PENALTY)
    penalty = round(penalty_raw)
    penalty_detail = []
    if penalty > 0:
        penalty_detail.append(
            f'加权WCSV处于过去60日{percentile:.1f}%分位'
            f'（安全边界{M3_CSV_SAFE_PCT:.0f}%，危险边界{M3_CSV_DANGER_PCT:.0f}%），'
            f'同涨同跌/因子失效风险上升，扣{penalty_raw:.1f}分（满分{M3_CSV_MAX_PENALTY}分）')
    return {
        'sigma_weighted': round(sigma_weighted_today, 6),
        'sigma_equal': round(sigma_equal_today, 6) if sigma_equal_today is not None else None,
        'percentile': round(percentile, 2),
        'penalty': penalty,
        'penalty_detail': penalty_detail,
    }


# ─── 模块 4 ─────────────────────────────────────────

def module4_broad_index_trend(
    df_today: pd.DataFrame,
    df_series_20: pd.DataFrame,
    df_series_60: pd.DataFrame,
    trading_day: str,
) -> Dict[str, Any]:
    if df_today is None or df_today.empty:
        return {
            'indices': [], 'summary': '无指数数据',
            'penalty': 0, 'penalty_detail': [],
            'below_ma_count': 0, 'core_below_ma': [],
            'csi1000_below_ma20': False, 'all_three_below_ma60': False,
        }
    df_today = df_today.copy()
    df_today['ChangePCT'] = pd.to_numeric(df_today['ChangePCT'], errors='coerce')
    df_today['ClosePrice'] = pd.to_numeric(df_today['ClosePrice'], errors='coerce')

    ma20_by_index = {}
    if df_series_20 is not None and not df_series_20.empty and 'ClosePrice' in df_series_20.columns:
        d = df_series_20.copy()
        d['ClosePrice'] = pd.to_numeric(d['ClosePrice'], errors='coerce')
        for ic, g in d.groupby('InnerCode'):
            g = g.dropna(subset=['ClosePrice']).sort_values('TradingDay').tail(BROAD_INDEX_MA_DAYS)
            if len(g) >= int(BROAD_INDEX_MA_DAYS * 0.8):
                ma20_by_index[ic] = float(g['ClosePrice'].mean())

    ma60_by_index = {}
    if df_series_60 is not None and not df_series_60.empty and 'ClosePrice' in df_series_60.columns:
        d = df_series_60.copy()
        d['ClosePrice'] = pd.to_numeric(d['ClosePrice'], errors='coerce')
        for ic, g in d.groupby('InnerCode'):
            g = g.dropna(subset=['ClosePrice']).sort_values('TradingDay').tail(BROAD_INDEX_MA60_DAYS)
            if len(g) >= int(BROAD_INDEX_MA60_DAYS * 0.8):
                ma60_by_index[ic] = float(g['ClosePrice'].mean())

    rows = []
    csi1000_dev_ma20 = None    # 中证1000偏离MA20比率（close/ma20 - 1）
    csi1000_below_ma20 = False  # 向后兼容字段
    for _, r in df_today.iterrows():
        ic = r.get('InnerCode')
        name = r.get('ChiName') or r.get('SecuAbbr', '')
        close = r.get('ClosePrice')
        change_pct = r.get('ChangePCT')
        ma20 = ma20_by_index.get(ic)
        below_ma20 = None
        dev_ma20 = None
        if ma20 is not None and close is not None and np.isfinite(close) and np.isfinite(ma20) and ma20 > 0:
            dev_ma20 = round(float(close / ma20 - 1), 6)
            below_ma20 = dev_ma20 < 0
            if ic == INNERCODE_CSI1000:
                csi1000_dev_ma20 = dev_ma20
                csi1000_below_ma20 = below_ma20
        rows.append({
            'name': name, 'abbr': r.get('SecuAbbr', ''),
            'change_pct': round(float(change_pct), 2) if change_pct is not None and np.isfinite(change_pct) else None,
            'category': r.get('Category', ''),
            'close': round(float(close), 2) if close is not None and np.isfinite(close) else None,
            'ma20': round(ma20, 2) if ma20 is not None else None,
            'below_ma': below_ma20,
            'dev_ma20': dev_ma20,
        })

    # ── 三大宽基偏离MA60：计算均值偏离度 ────────────────────────────────
    core_devs_ma60 = []
    all_three_below_ma60 = True
    for ic in CORE_INDEX_MA60:
        sub = df_today[df_today['InnerCode'] == ic]
        close = float(sub['ClosePrice'].iloc[0]) if len(sub) and pd.notna(sub['ClosePrice'].iloc[0]) else None
        ma60 = ma60_by_index.get(ic)
        if close is not None and ma60 is not None and np.isfinite(close) and ma60 > 0:
            dev = float(close / ma60 - 1)
            core_devs_ma60.append(dev)
            if dev >= 0:
                all_three_below_ma60 = False
        else:
            all_three_below_ma60 = False
    core_avg_dev_ma60 = float(np.mean(core_devs_ma60)) if core_devs_ma60 else None

    penalty_detail = []

    # ── 中证1000偏离MA20：连续映射，+1%→不扣，-4%→扣满15分 ────────────
    penalty_csi1000 = 0.0
    if csi1000_dev_ma20 is not None:
        penalty_csi1000 = _smooth_penalty(
            csi1000_dev_ma20, M4_CSI1000_MA20_SAFE, M4_CSI1000_MA20_DANGER, M4_CSI1000_MA20_MAX_PENALTY)
        if penalty_csi1000 > 0:
            penalty_detail.append(
                f'中证1000偏离MA20={csi1000_dev_ma20:+.2%}'
                f'（安全边界{M4_CSI1000_MA20_SAFE:+.0%}，危险边界{M4_CSI1000_MA20_DANGER:+.0%}），'
                f'扣{penalty_csi1000:.1f}分（满分{M4_CSI1000_MA20_MAX_PENALTY}分）')

    # ── 三大宽基偏离MA60均值：连续映射，+1%→不扣，-5%→扣满30分 ─────────
    penalty_ma60 = 0.0
    if core_avg_dev_ma60 is not None:
        penalty_ma60 = _smooth_penalty(
            core_avg_dev_ma60, M4_MA60_SAFE, M4_MA60_DANGER, M4_MA60_MAX_PENALTY)
        if penalty_ma60 > 0:
            penalty_detail.append(
                f'三大宽基偏离MA60均值={core_avg_dev_ma60:+.2%}'
                f'（安全边界{M4_MA60_SAFE:+.0%}，危险边界{M4_MA60_DANGER:+.0%}），'
                f'扣{penalty_ma60:.1f}分（满分{M4_MA60_MAX_PENALTY}分）')

    penalty = round(penalty_csi1000 + penalty_ma60)
    return {
        'indices': rows,
        'summary': (f'共 {len(rows)} 只指数；中证1000偏MA20={csi1000_dev_ma20:+.2%}，'
                    f'三指数偏MA60均值={core_avg_dev_ma60:+.2%}') if csi1000_dev_ma20 is not None and core_avg_dev_ma60 is not None
                   else f'共 {len(rows)} 只指数',
        'penalty': penalty, 'penalty_detail': penalty_detail,
        'csi1000_dev_ma20': round(csi1000_dev_ma20, 6) if csi1000_dev_ma20 is not None else None,
        'core_avg_dev_ma60': round(core_avg_dev_ma60, 6) if core_avg_dev_ma60 is not None else None,
        'csi1000_below_ma20': csi1000_below_ma20,
        'all_three_below_ma60': all_three_below_ma60,
    }


# ─── 模块 5 ─────────────────────────────────────────

def module5_style_factors(
    df_today_slice: pd.DataFrame,
    size_spread_history: Optional[List[float]] = None,
    vol_spread_history: Optional[List[float]] = None,
    df_idx_pair: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """风格因子监控：Size Factor + Volatility Factor（Z-Score 动态自适应）。
    df_today_slice: 当日全市场（需含 R, NegotiableMV, _vol20）。
    size/vol_spread_history: 最近 FACTOR_ZSCORE_WINDOW=60 日的 spread 序列（计算 Z-Score）。
    df_idx_pair: 当日指数数据（含 ChangePCT, InnerCode）—— 仅作信息展示，不再单独扣分。
    """
    empty = {
        'spread_size': None, 'spread_vol': None,
        'r_csi1000_vs_sh50': None,
        'size_z_score': None, 'vol_z_score': None,
        'penalty': 0, 'penalty_detail': [],
    }
    if df_today_slice is None or df_today_slice.empty or 'R' not in df_today_slice.columns:
        empty['error'] = '无当日股票数据'
        return empty

    d = df_today_slice.dropna(subset=['R']).copy()
    penalty = 0
    detail = []

    # --- Size Factor ---
    spread_size = None
    if 'NegotiableMV' in d.columns and d['NegotiableMV'].notna().sum() > 50:
        mv_q_lo = d['NegotiableMV'].quantile(FACTOR_QUINTILE)
        mv_q_hi = d['NegotiableMV'].quantile(1 - FACTOR_QUINTILE)
        r_small = d.loc[d['NegotiableMV'] <= mv_q_lo, 'R'].mean()
        r_large = d.loc[d['NegotiableMV'] >= mv_q_hi, 'R'].mean()
        if np.isfinite(r_small) and np.isfinite(r_large):
            spread_size = round(float(r_small - r_large), 6)

    # ── Size Z-Score 步进扣分 ─────────────────────────────────────────
    size_z_score = None
    if spread_size is not None and size_spread_history:
        valid_s = np.array([v for v in size_spread_history if v is not None and np.isfinite(v)])
        if len(valid_s) >= 10:
            s_mean = float(valid_s.mean())
            s_std = float(valid_s.std())
            if s_std > 0:
                size_z_score = round(float((spread_size - s_mean) / s_std), 4)
                p_size = _zscore_step_penalty(size_z_score, M5_ZSCORE_START, M5_ZSCORE_STEP,
                                              M5_ZSCORE_STEP_PENALTY, M5_SIZE_MAX_PENALTY)
                if p_size > 0:
                    penalty += p_size
                    detail.append(
                        f'Size Factor Z-Score={size_z_score:.2f}σ'
                        f'（60日均值{s_mean:.2%}/σ={s_std:.2%}），'
                        f'从{M5_ZSCORE_START}σ起每{M5_ZSCORE_STEP}σ扣{M5_ZSCORE_STEP_PENALTY}分，'
                        f'扣{p_size:.0f}分（满分{M5_SIZE_MAX_PENALTY}分）')

    # --- Volatility Factor ---
    spread_vol = None
    if '_vol20' in d.columns and d['_vol20'].notna().sum() > 50:
        vol_q_lo = d['_vol20'].quantile(FACTOR_QUINTILE)
        vol_q_hi = d['_vol20'].quantile(1 - FACTOR_QUINTILE)
        r_low_vol = d.loc[d['_vol20'] <= vol_q_lo, 'R'].mean()
        r_high_vol = d.loc[d['_vol20'] >= vol_q_hi, 'R'].mean()
        if np.isfinite(r_low_vol) and np.isfinite(r_high_vol):
            spread_vol = round(float(r_high_vol - r_low_vol), 6)

    # ── Vol Z-Score 步进扣分 ──────────────────────────────────────────
    vol_z_score = None
    if spread_vol is not None and vol_spread_history:
        valid_v = np.array([v for v in vol_spread_history if v is not None and np.isfinite(v)])
        if len(valid_v) >= 10:
            v_mean = float(valid_v.mean())
            v_std = float(valid_v.std())
            if v_std > 0:
                vol_z_score = round(float((spread_vol - v_mean) / v_std), 4)
                p_vol = _zscore_step_penalty(vol_z_score, M5_ZSCORE_START, M5_ZSCORE_STEP,
                                             M5_ZSCORE_STEP_PENALTY, M5_VOL_MAX_PENALTY)
                if p_vol > 0:
                    penalty += p_vol
                    detail.append(
                        f'Vol Factor Z-Score={vol_z_score:.2f}σ'
                        f'（60日均值{v_mean:.2%}/σ={v_std:.2%}），'
                        f'从{M5_ZSCORE_START}σ起每{M5_ZSCORE_STEP}σ扣{M5_ZSCORE_STEP_PENALTY}分，'
                        f'扣{p_vol:.0f}分（满分{M5_VOL_MAX_PENALTY}分）')

    # --- CSI1000 vs SH50（仅作信息展示，不再单独扣分）---
    r_csi1000_vs_sh50 = None
    if df_idx_pair is not None and not df_idx_pair.empty and 'ChangePCT' in df_idx_pair.columns:
        ip = df_idx_pair.copy()
        ip['ChangePCT'] = pd.to_numeric(ip['ChangePCT'], errors='coerce')
        r50s = ip.loc[ip['InnerCode'] == INNERCODE_SH50, 'ChangePCT']
        r1000s = ip.loc[ip['InnerCode'] == INNERCODE_CSI1000, 'ChangePCT']
        if not r50s.empty and not r1000s.empty:
            r50 = float(r50s.iloc[0]) / 100.0
            r1000 = float(r1000s.iloc[0]) / 100.0
            r_csi1000_vs_sh50 = round(r1000 - r50, 4)

    return {
        'spread_size': spread_size,
        'spread_vol': spread_vol,
        'r_csi1000_vs_sh50': r_csi1000_vs_sh50,
        'size_z_score': size_z_score,
        'vol_z_score': vol_z_score,
        'penalty': penalty,
        'penalty_detail': detail,
    }


# ─── 模块 6 ─────────────────────────────────────────

def module6_hot_money(df_2: pd.DataFrame, yesterday: str, today: str) -> Dict[str, Any]:
    if df_2 is None or df_2.empty or 'R' not in df_2.columns:
        return {
            'hot_avg_return': None, 'hot_count': 0,
            'nuke_count': 0, 'nuke_ratio': None,
            'penalty': 0, 'penalty_detail': [], 'error': '无最近两日收益率数据'
        }
    df = df_2
    dates = df['Date'].astype(str)
    df_y = df[dates == yesterday]
    df_t = df[dates == today]
    if df_y.empty or df_t.empty:
        return {
            'hot_avg_return': None, 'hot_count': 0,
            'nuke_count': 0, 'nuke_ratio': None,
            'penalty': 0, 'penalty_detail': [], 'error': '缺少昨日或今日数据'
        }
    # 动态涨停阈值：根据代码前缀区分板块 + Close==High 兜底
    dy = df_y.copy()
    for _c in ['Close', 'High', 'Open']:
        if _c in dy.columns:
            dy[_c] = pd.to_numeric(dy[_c], errors='coerce')
    _thresholds = pd.Series(0.095, index=dy.index)
    if 'SecuCode' in dy.columns:
        _is_kc_cy = dy['SecuCode'].astype(str).str.match(r'^(688|300)')
        _is_bj = dy['SecuCode'].astype(str).str.match(r'^[84]')
        _thresholds[_is_kc_cy] = 0.195
        _thresholds[_is_bj] = 0.295
    if 'SecuAbbr' in dy.columns:
        _is_st = dy['SecuAbbr'].astype(str).str.contains('ST', na=False)
        _thresholds[_is_st] = 0.048
    _has_hc = 'Close' in dy.columns and 'High' in dy.columns
    _is_limit_up = (dy['R'] > _thresholds)
    if _has_hc:
        _is_limit_up = _is_limit_up & (dy['Close'] == dy['High'])
    hot_codes = set(dy.loc[_is_limit_up, 'SecuCode'])
    if not hot_codes:
        return {
            'hot_avg_return': None, 'hot_count': 0,
            'nuke_count': 0, 'nuke_ratio': None,
            'penalty': 0, 'penalty_detail': [], 'error': '昨日无涨停股'
        }
    df_t_hot = df_t[df_t['SecuCode'].isin(hot_codes)]
    hot_count = len(df_t_hot)
    if hot_count == 0:
        return {
            'hot_avg_return': None, 'hot_count': 0,
            'nuke_count': 0, 'nuke_ratio': None,
            'penalty': 0, 'penalty_detail': [], 'error': '昨日涨停股今日无行情'
        }
    hot_avg = float(df_t_hot['R'].mean())
    _nuke_thresh = _get_dynamic_limit_thresholds(df_t_hot)
    nuke_count = int((df_t_hot['R'] < -_nuke_thresh).sum())
    nuke_ratio = nuke_count / hot_count
    detail: List[str] = []

    # ── 昨涨停股今均收益：连续映射，≥0%不扣，≤-5%扣满20分 ─────────────
    penalty_hot_avg = _smooth_penalty(hot_avg, M6_HOT_AVG_SAFE, M6_HOT_AVG_DANGER, M6_HOT_AVG_MAX_PENALTY)
    if penalty_hot_avg > 0:
        detail.append(
            f'昨涨停股今均收益{hot_avg:.2%}'
            f'（安全边界{M6_HOT_AVG_SAFE:.0%}，危险边界{M6_HOT_AVG_DANGER:.0%}），'
            f'接力意愿减退，扣{penalty_hot_avg:.1f}分（满分{M6_HOT_AVG_MAX_PENALTY}分）')

    # ── 核按钮比例：连续映射，≤5%不扣，≥25%扣满20分 ─────────────────
    penalty_nuke = 0.0
    if hot_count >= HOT_MIN_COUNT:
        penalty_nuke = _smooth_penalty(nuke_ratio, M6_NUKE_SAFE, M6_NUKE_DANGER, M6_NUKE_MAX_PENALTY)
        if penalty_nuke > 0:
            detail.append(
                f'核按钮比例{nuke_ratio:.1%}（{nuke_count}/{hot_count}）'
                f'（安全边界{M6_NUKE_SAFE:.0%}，危险边界{M6_NUKE_DANGER:.0%}），'
                f'热钱遭遇打击，扣{penalty_nuke:.1f}分（满分{M6_NUKE_MAX_PENALTY}分）')

    penalty = round(penalty_hot_avg + penalty_nuke)
    return {
        'hot_avg_return': round(hot_avg, 4), 'hot_count': hot_count,
        'nuke_count': nuke_count,
        'nuke_ratio': round(nuke_ratio, 4) if nuke_ratio is not None else None,
        'penalty': penalty, 'penalty_detail': detail,
    }


# ─── 模块 7（改造三：聪明钱与日内承接力）────────────

def module7_smart_money(
    df_today_slice: pd.DataFrame,
    market_return_today: Optional[float],
) -> Dict[str, Any]:
    """Smart Money 四形态解构：诱多派发 / 诱空吸筹 / 单边上涨 / 单边下跌。
    成交额加权聚合 + 对冲折算 + 换手率溢价双维度打分。
    df_today_slice: 当日全市场，需含 Open, Close, High, Low, PreClose, R, Amount, TurnoverRate。"""
    empty = {
        'smart_distrib_ratio': None, 'smart_accum_ratio': None,
        'unilateral_up_ratio': None, 'unilateral_down_ratio': None,
        'net_distrib_ratio': None,
        'count_distrib': 0, 'count_accum': 0,
        'count_uni_up': 0, 'count_uni_down': 0, 'count_unclassified': 0,
        'top_turnover_avg_return': None, 'top_turnover_count': 0,
        'market_return_today': market_return_today,
        'penalty': 0, 'penalty_detail': [],
    }
    if df_today_slice is None or df_today_slice.empty:
        empty['error'] = '当日无股票数据'
        return empty

    d = df_today_slice.copy()
    for col in ['Open', 'Close', 'High', 'Low', 'PreClose', 'Amount', 'TurnoverRate', 'R']:
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors='coerce')

    penalty = 0.0
    detail: List[str] = []

    # ═══ 第一层：四种互斥微观形态解构（K 线解剖学）═══
    need_cols = ['Open', 'Close', 'High', 'Low']
    has_ohlc = all(c in d.columns for c in need_cols)
    has_pc = 'PreClose' in d.columns
    has_amt = 'Amount' in d.columns

    smart_distrib_ratio = None
    smart_accum_ratio = None
    uni_up_ratio = None
    uni_down_ratio = None
    net_distrib_ratio = None
    cnt_dist = cnt_accum = cnt_up = cnt_down = cnt_other = 0

    if has_ohlc and has_pc:
        mask = (d['Open'].notna() & d['Close'].notna() & d['High'].notna()
                & d['Low'].notna() & d['PreClose'].notna())
        dm = d[mask].copy()
        total = len(dm)

        if total > 0:
            dm_body = (dm['Close'] - dm['Open']).abs()
            dm_range = dm['High'] - dm['Low']
            dm_upper = dm['High'] - dm[['Close', 'Open']].max(axis=1)
            dm_lower = dm[['Close', 'Open']].min(axis=1) - dm['Low']

            # Pattern 1 — 诱多派发: (O > PC) ∧ (C < O) ∧ (Upper > Lower)
            is_dist = ((dm['Open'] > dm['PreClose'])
                       & (dm['Close'] < dm['Open'])
                       & (dm_upper > dm_lower))

            # Pattern 2 — 诱空吸筹: (O ≤ PC) ∧ (C > O) ∧ (Lower > Upper)
            is_accum = ((dm['Open'] <= dm['PreClose'])
                        & (dm['Close'] > dm['Open'])
                        & (dm_lower > dm_upper))

            body_dominant = dm_body > dm_range * M7_BODY_RATIO_THRESH

            # Pattern 3 — 单边上涨: (C > O) ∧ (Body > Range×0.7) ∧ ¬诱空吸筹
            is_uni_up = (dm['Close'] > dm['Open']) & body_dominant & ~is_accum

            # Pattern 4 — 单边下跌: (C < O) ∧ (Body > Range×0.7) ∧ ¬诱多派发
            is_uni_down = (dm['Close'] < dm['Open']) & body_dominant & ~is_dist

            cnt_dist = int(is_dist.sum())
            cnt_accum = int(is_accum.sum())
            cnt_up = int(is_uni_up.sum())
            cnt_down = int(is_uni_down.sum())
            cnt_other = total - cnt_dist - cnt_accum - cnt_up - cnt_down

            # ═══ 第二层：成交额加权聚合（资金即真理）═══
            if has_amt:
                v_total = dm['Amount'].sum()
                if v_total > 0:
                    smart_distrib_ratio = round(float(dm.loc[is_dist, 'Amount'].sum() / v_total), 4)
                    smart_accum_ratio = round(float(dm.loc[is_accum, 'Amount'].sum() / v_total), 4)
                    uni_up_ratio = round(float(dm.loc[is_uni_up, 'Amount'].sum() / v_total), 4)
                    uni_down_ratio = round(float(dm.loc[is_uni_down, 'Amount'].sum() / v_total), 4)

                    # ═══ 第三层：对冲折算（净派发 = 派发 − 吸筹×α）═══
                    net_distrib_ratio = round(
                        max(0.0, smart_distrib_ratio - smart_accum_ratio * M7_HEDGE_ALPHA), 4)

            # ═══ 第四层：平滑惩罚映射 ═══
            if net_distrib_ratio is not None:
                p_smart = _smooth_penalty(
                    net_distrib_ratio, M7_DISTRIB_SAFE, M7_DISTRIB_DANGER, M7_DISTRIB_MAX_PENALTY)
                if p_smart > 0:
                    penalty += p_smart
                    detail.append(
                        f'净派发能量占比{net_distrib_ratio:.1%}'
                        f'（派发{smart_distrib_ratio:.1%}−吸筹{smart_accum_ratio:.1%}×{M7_HEDGE_ALPHA}），'
                        f'安全≤{M7_DISTRIB_SAFE:.0%}/危险≥{M7_DISTRIB_DANGER:.0%}，'
                        f'扣{p_smart:.1f}分（满分{M7_DISTRIB_MAX_PENALTY}分）')

    # ═══ 换手率溢价检测（独立维度，不变）═══
    top_ret = None
    top_count = 0
    if 'TurnoverRate' in d.columns and 'R' in d.columns:
        dr = d.dropna(subset=['TurnoverRate', 'R'])
        if len(dr) > 20:
            q_hi = dr['TurnoverRate'].quantile(1 - TURNOVER_TOP_PCT)
            top_pool = dr[dr['TurnoverRate'] >= q_hi]
            top_count = len(top_pool)
            if top_count > 0:
                top_ret = round(float(top_pool['R'].mean()), 4)
                p_tov = _smooth_penalty(
                    top_ret, M7_TURNOVER_SAFE, M7_TURNOVER_DANGER, M7_TURNOVER_MAX_PENALTY)
                if p_tov > 0:
                    penalty += p_tov
                    detail.append(
                        f'换手率前{TURNOVER_TOP_PCT:.0%}活跃池（{top_count}只）均收益{top_ret:.2%}'
                        f'（安全≥{M7_TURNOVER_SAFE:.0%}/危险≤{M7_TURNOVER_DANGER:.0%}），'
                        f'承接力衰退，扣{p_tov:.1f}分（满分{M7_TURNOVER_MAX_PENALTY}分）')

    return {
        'smart_distrib_ratio': smart_distrib_ratio,
        'smart_accum_ratio': smart_accum_ratio,
        'unilateral_up_ratio': uni_up_ratio,
        'unilateral_down_ratio': uni_down_ratio,
        'net_distrib_ratio': net_distrib_ratio,
        'count_distrib': cnt_dist, 'count_accum': cnt_accum,
        'count_uni_up': cnt_up, 'count_uni_down': cnt_down,
        'count_unclassified': cnt_other,
        'top_turnover_avg_return': top_ret,
        'top_turnover_count': top_count,
        'market_return_today': market_return_today,
        'penalty': round(penalty),
        'penalty_detail': detail,
    }


# ─── 模块 8（向量化） ────────────────────────────────

def module8_strong_stock_crash(
    df_window20: pd.DataFrame,
    df_today_slice: pd.DataFrame,
    market_return_today: Optional[float],
) -> Dict[str, Any]:
    """向量化版：df_window20 = 过去20日（不含今日）全市场，df_today_slice = 今日。"""
    empty = {
        'strong_count': 0, 'strong_avg_return': None,
        'threshold_cum20': None, 'limitdown_count': 0,
        'market_return_today': market_return_today,
        'penalty': 0, 'penalty_detail': [],
    }
    if df_window20 is None or df_window20.empty or df_today_slice is None or df_today_slice.empty:
        empty['error'] = '窗口期或今日数据不足'
        return empty
    if 'R' not in df_window20.columns or 'R' not in df_today_slice.columns:
        empty['error'] = '缺少R列'
        return empty

    cum20 = df_window20.groupby('SecuCode').agg(
        cnt=('R', 'size'),
        cum=('R', lambda x: float((1 + x).prod() - 1)),
    )
    cum20 = cum20[cum20['cnt'] >= 10]
    if cum20.empty:
        empty['error'] = '无可用于计算的强势股历史数据'
        return empty

    threshold = float(np.nanpercentile(cum20['cum'].values, 100 - STRONG_TOP_PERCENT))
    strong_codes = cum20.index[cum20['cum'] >= threshold]

    today_map = df_today_slice.set_index('SecuCode')['R']
    strong_today = today_map.reindex(strong_codes).dropna()
    strong_count = len(strong_today)
    if strong_count == 0:
        empty['threshold_cum20'] = round(threshold, 4)
        empty['error'] = '强势股池为空'
        return empty

    strong_avg = float(strong_today.mean())
    _strong_df = df_today_slice[df_today_slice['SecuCode'].isin(strong_codes)]
    _strong_thresh = _get_dynamic_limit_thresholds(_strong_df)
    _strong_thresh.index = _strong_df['SecuCode'].values
    limitdown_count = int((strong_today < -_strong_thresh.reindex(strong_today.index).fillna(0.095)).sum())

    # ── 强势股补跌：基于窗口内日收益历史波动率计算 Z-Score ──────────────
    # 用 df_window20 中强势股池的每日平均收益，建立历史分布
    daily_strong_returns = (
        df_window20[df_window20['SecuCode'].isin(strong_codes)]
        .groupby('Date')['R'].mean()
        .dropna()
    )
    detail: List[str] = []
    strong_z_score = None
    penalty = 0

    if (market_return_today is not None and market_return_today < 0
            and len(daily_strong_returns) >= 5):
        hist_mean = float(daily_strong_returns.mean())
        hist_std = float(daily_strong_returns.std())
        if hist_std > 0:
            strong_z_score = round(float((strong_avg - hist_mean) / hist_std), 4)
            p_m8 = _zscore_step_penalty(strong_z_score, M8_ZSCORE_START, M8_ZSCORE_STEP,
                                        M8_ZSCORE_STEP_PENALTY, M8_MAX_PENALTY)
            if p_m8 > 0:
                penalty = int(p_m8)
                detail.append(
                    f'大盘下跌，强势股池今日均收益{strong_avg:.2%}'
                    f'，Z-Score={strong_z_score:.2f}σ'
                    f'（历史均值{hist_mean:.2%}/σ={hist_std:.2%}），'
                    f'动量崩塌，扣{p_m8:.0f}分（满分{M8_MAX_PENALTY}分）')
    elif (market_return_today is not None and market_return_today < 0
          and len(daily_strong_returns) < 5):
        # 历史数据不足，降级为固定阈值兜底（避免漏报）
        if strong_avg <= -0.03:
            penalty = M8_MAX_PENALTY
            detail.append(
                f'大盘下跌且强势股池均跌{strong_avg:.2%}≤-3%（历史数据不足，固定阈值兜底），'
                f'扣{M8_MAX_PENALTY}分')

    return {
        'strong_count': strong_count,
        'strong_avg_return': round(strong_avg, 4),
        'threshold_cum20': round(threshold, 4),
        'limitdown_count': limitdown_count,
        'strong_z_score': strong_z_score,
        'market_return_today': market_return_today,
        'penalty': penalty, 'penalty_detail': detail,
    }


# ══════════════════════════════════════════════════════
#  单日入口（优化：仅 1 次全市场查询 + 1 次指数查询）
# ══════════════════════════════════════════════════════

def compute_daily_sentiment(trading_day: str, fetcher) -> Dict[str, Any]:
    trading_day = pd.to_datetime(trading_day).strftime('%Y-%m-%d')

    dates_252 = fetcher.get_trading_days_inclusive(trading_day, count=BREADTH_LOOKBACK_DAYS)
    if not dates_252:
        return {
            'trading_day': trading_day, 'version': SENTIMENT_VERSION,
            'module1': {}, 'module2': {}, 'module3': {}, 'module4': {},
            'module5': {}, 'module6': {}, 'module7': {}, 'module8': {},
            'total_score': 100, 'total_penalty': 0, 'penalty_details': [],
            'error': '无交易日',
        }
    trading_day = dates_252[-1]

    result = {
        'trading_day': trading_day, 'version': SENTIMENT_VERSION,
        'module1': {}, 'module2': {}, 'module3': {}, 'module4': {},
        'module5': {}, 'module6': {}, 'module7': {}, 'module8': {},
        'total_score': 100, 'total_penalty': 0, 'penalty_details': [],
    }

    # ── 数据拉取 ──
    df_252 = fetcher.get_all_stocks_returns_for_dates(dates_252)
    df_252_ok = df_252 is not None and not df_252.empty and 'R' in df_252.columns
    if df_252_ok:
        df_252['Date'] = df_252['Date'].astype(str)
        df_252 = _filter_universe(df_252)

    index_inner_codes = [1, 1059]
    all_index_codes = list(set(
        index_inner_codes +
        [1, 1055, 11089, 3145, 4978, 46, 30, 4074, 39144, 36324,
         6973, 4078, 3036, 7544, 39376, 31398, 48542, 19475, 217313,
         3469, 3471, 4089, 225892, 303968] +
        [INNERCODE_SH50, INNERCODE_CSI1000] + list(CORE_INDEX_MA60)
    ))
    start_date_idx = dates_252[0]
    df_idx_all = fetcher.get_index_quote_for_dates(all_index_codes, start_date_idx, trading_day)
    market_return_today: Optional[float] = None

    # ── MODULE 1: 资金多空比 ──
    if not df_252_ok:
        result['module1'] = {'penalty': 0, 'penalty_detail': [], 'error': '无252日收益率数据'}
    else:
        breadth_df = _vectorized_daily_breadth(df_252)
        result['module1'] = module1_market_breadth(breadth_df, trading_day)

    # ── MODULE 2: 流动性 ──
    dates_20 = dates_252[-20:] if len(dates_252) >= 20 else dates_252
    if df_idx_all is None or df_idx_all.empty or 'TurnoverValue' not in df_idx_all.columns:
        result['module2'] = {'penalty': 0, 'penalty_detail': [], 'error': '无指数成交额'}
    else:
        df_idx_all['TurnoverValue'] = pd.to_numeric(df_idx_all['TurnoverValue'], errors='coerce').fillna(0)
        if 'ChangePCT' in df_idx_all.columns:
            df_idx_all['ChangePCT'] = pd.to_numeric(df_idx_all['ChangePCT'], errors='coerce')
        df_liq = df_idx_all[df_idx_all['InnerCode'].isin(index_inner_codes)]
        # 综合判断：上证+深成指，任一下跌即视为大盘偏空
        _idx_today = df_liq[df_liq['TradingDay'] == trading_day]
        if not _idx_today.empty and 'ChangePCT' in _idx_today.columns:
            _idx_pcts = _idx_today.set_index('InnerCode')['ChangePCT'].dropna()
            if not _idx_pcts.empty:
                _sh = float(_idx_pcts.get(1, 0)) / 100.0
                _sz = float(_idx_pcts.get(1059, 0)) / 100.0
                market_return_today = min(_sh, _sz)
        daily_total = df_liq.groupby('TradingDay')['TurnoverValue'].sum()
        daily_billion = (daily_total / 1e8).to_dict()
        turnover_today = float(daily_billion.get(trading_day, 0))
        series_5 = [daily_billion.get(d, 0) for d in dates_20[-5:]]
        series_20 = [daily_billion.get(d, 0) for d in dates_20]
        series_252 = [daily_billion.get(d, 0) for d in dates_252]
        result['module2'] = module2_liquidity(turnover_today, series_5, series_20, turnover_series_252=series_252)

    # ── MODULE 3: 加权横截面离散度 ──
    if not df_252_ok:
        result['module3'] = {'sigma_weighted': None, 'sigma_equal': None,
                             'percentile': None, 'penalty': 0, 'penalty_detail': []}
    else:
        dates_61 = dates_252[-CSV_LOOKBACK_DAYS:] if len(dates_252) >= CSV_LOOKBACK_DAYS else dates_252
        df_61 = df_252[df_252['Date'].isin(set(dates_61))]
        sigma_w_series = df_61.groupby('Date').apply(_weighted_cross_sectional_std)
        sigma_e_series = df_61.groupby('Date')['R'].std()
        sw_today = float(sigma_w_series.get(trading_day, np.nan)) if trading_day in sigma_w_series.index else np.nan
        se_today = float(sigma_e_series.get(trading_day, np.nan)) if trading_day in sigma_e_series.index else np.nan
        result['module3'] = module3_cross_sectional_volatility(
            sw_today, sigma_w_series.tolist(), sigma_equal_today=se_today)

    # ── MODULE 4: 宽基指数趋势 ──
    CATEGORY_MAP = {
        1: '市场综合基准', 1055: '市场综合基准', 11089: '市场综合基准', 3145: '市场综合基准', 4978: '市场综合基准',
        46: '规模与风格', 30: '规模与风格', 4074: '规模与风格', 39144: '规模与风格', 36324: '规模与风格',
        6973: '主题与策略', 4078: '主题与策略', 3036: '主题与策略', 7544: '主题与策略',
        39376: '主题与策略', 31398: '主题与策略', 48542: '主题与策略', 19475: '主题与策略', 217313: '主题与策略',
        3469: '其他常用', 3471: '其他常用', 4089: '其他常用', 225892: '其他常用', 303968: '其他常用',
    }
    broad_codes = list(CATEGORY_MAP.keys())
    if df_idx_all is not None and not df_idx_all.empty:
        df_broad = df_idx_all[(df_idx_all['InnerCode'].isin(broad_codes)) & (df_idx_all['TradingDay'] == trading_day)].copy()
        df_broad['Category'] = df_broad['InnerCode'].map(CATEGORY_MAP).fillna('')
        dates_20_idx = dates_252[-BROAD_INDEX_MA_DAYS:] if len(dates_252) >= BROAD_INDEX_MA_DAYS else dates_252
        df_series_20 = df_idx_all[
            (df_idx_all['InnerCode'].isin(broad_codes)) & (df_idx_all['TradingDay'].isin(set(dates_20_idx)))]
        dates_60_idx = dates_252[-BROAD_INDEX_MA60_DAYS:] if len(dates_252) >= BROAD_INDEX_MA60_DAYS else dates_252
        df_series_60 = df_idx_all[
            (df_idx_all['InnerCode'].isin(list(CORE_INDEX_MA60))) & (df_idx_all['TradingDay'].isin(set(dates_60_idx)))]
        result['module4'] = module4_broad_index_trend(df_broad, df_series_20, df_series_60, trading_day)
    else:
        result['module4'] = {'indices': [], 'summary': '无指数数据', 'penalty': 0, 'penalty_detail': []}

    # ── MODULE 5: 风格因子监控 ──
    try:
        if df_252_ok:
            df_today_stocks = df_252[df_252['Date'] == trading_day].copy()
            # 20日滚动波动率（shift(1) 用昨日波动率对今日分组，消除内生性）
            vol20_raw = df_252.groupby('SecuCode')['R'].rolling(FACTOR_VOL_WINDOW, min_periods=10).std()
            vol20_shifted = vol20_raw.groupby(level=0).shift(1)
            df_252_tmp = df_252.copy()
            df_252_tmp['_vol20'] = vol20_shifted.reset_index(level=0, drop=True)
            df_today_with_vol = df_252_tmp[df_252_tmp['Date'] == trading_day]

            # 最近60天的 spread history（供 Z-Score 计算）
            recent_dates = dates_252[-(FACTOR_ZSCORE_WINDOW + 1):-1] if len(dates_252) > FACTOR_ZSCORE_WINDOW else dates_252[:-1]
            size_hist, vol_hist = [], []
            for dd in recent_dates:
                dd_slice = df_252_tmp[df_252_tmp['Date'] == dd]
                if dd_slice.empty:
                    size_hist.append(None); vol_hist.append(None); continue
                dd_d = dd_slice.dropna(subset=['R'])
                if 'NegotiableMV' in dd_d.columns and dd_d['NegotiableMV'].notna().sum() > 50:
                    mql = dd_d['NegotiableMV'].quantile(FACTOR_QUINTILE)
                    mqh = dd_d['NegotiableMV'].quantile(1 - FACTOR_QUINTILE)
                    rs = dd_d.loc[dd_d['NegotiableMV'] <= mql, 'R'].mean()
                    rl = dd_d.loc[dd_d['NegotiableMV'] >= mqh, 'R'].mean()
                    size_hist.append(float(rs - rl) if np.isfinite(rs) and np.isfinite(rl) else None)
                else:
                    size_hist.append(None)
                if '_vol20' in dd_d.columns and dd_d['_vol20'].notna().sum() > 50:
                    vql = dd_d['_vol20'].quantile(FACTOR_QUINTILE)
                    vqh = dd_d['_vol20'].quantile(1 - FACTOR_QUINTILE)
                    rlv = dd_d.loc[dd_d['_vol20'] <= vql, 'R'].mean()
                    rhv = dd_d.loc[dd_d['_vol20'] >= vqh, 'R'].mean()
                    vol_hist.append(float(rhv - rlv) if np.isfinite(rlv) and np.isfinite(rhv) else None)
                else:
                    vol_hist.append(None)

            df_idx_pair = None
            if df_idx_all is not None and not df_idx_all.empty:
                df_idx_pair = df_idx_all[
                    (df_idx_all['InnerCode'].isin([INNERCODE_SH50, INNERCODE_CSI1000])) &
                    (df_idx_all['TradingDay'] == trading_day)]
            result['module5'] = module5_style_factors(
                df_today_with_vol, size_hist, vol_hist, df_idx_pair)
        else:
            result['module5'] = {'penalty': 0, 'penalty_detail': [], 'error': '无行情数据'}
    except Exception as e:
        logger.error(f'计算风格因子失败: {e}', exc_info=True)
        result['module5'] = {'penalty': 0, 'penalty_detail': [], 'error': str(e)}


    # ── MODULE 6: 短线赚钱效应 ──
    if not df_252_ok or len(dates_252) < 2:
        result['module6'] = {'penalty': 0, 'penalty_detail': [], 'error': '交易日不足两天'}
    else:
        hot_dates = dates_252[-HOT_LOOKBACK_DAYS:]
        df_hot = df_252[df_252['Date'].isin(set(hot_dates))]
        yesterday, today2 = hot_dates[-2], hot_dates[-1]
        try:
            result['module6'] = module6_hot_money(df_hot, yesterday, today2)
        except Exception as e:
            logger.error(f'计算短线赚钱效应失败: {e}')
            result['module6'] = {'penalty': 0, 'penalty_detail': [], 'error': str(e)}

    # ── MODULE 7: 聪明钱与日内承接力 ──
    if df_252_ok:
        df_today_stocks = df_252[df_252['Date'] == trading_day]
        result['module7'] = module7_smart_money(df_today_stocks, market_return_today)
    else:
        result['module7'] = {'penalty': 0, 'penalty_detail': [], 'error': '无行情数据'}

    # ── MODULE 8: 强势股补跌 ──
    if df_252_ok and len(dates_252) >= 21:
        dates_21 = dates_252[-21:]
        window20 = dates_21[:-1]
        df_window20 = df_252[df_252['Date'].isin(set(window20))]
        df_today_stocks = df_252[df_252['Date'] == trading_day]
        result['module8'] = module8_strong_stock_crash(df_window20, df_today_stocks, market_return_today)
    else:
        result['module8'] = {'penalty': 0, 'penalty_detail': [], 'error': '历史交易日不足21天'}

    # ── 汇总 ──
    total_penalty = 0
    penalty_details = []
    for key in ['module1', 'module2', 'module3', 'module4', 'module5', 'module6', 'module7', 'module8']:
        m = result.get(key, {})
        p = m.get('penalty', 0) or 0
        if p > 0:
            total_penalty += p
            penalty_details.extend(m.get('penalty_detail', []))
    result['total_penalty'] = total_penalty
    result['penalty_details'] = penalty_details
    result['total_score'] = 100 - total_penalty
    return result


# ══════════════════════════════════════════════════════
#  批量入口：一次拉数据 + 滚动窗口
# ══════════════════════════════════════════════════════

def compute_batch_sentiment(
    start_date: str, end_date: str, fetcher
) -> List[Dict[str, Any]]:
    """同步版批量计算（供导出 CSV 等非流式场景使用）。"""
    results = []
    for msg in compute_batch_sentiment_streaming(start_date, end_date, fetcher):
        if msg.get('type') == 'result':
            results.append(msg['data'])
    return results


def compute_batch_sentiment_streaming(
    start_date: str, end_date: str, fetcher,
    skip_existing: bool = True,
):
    """
    批量计算生成器，yield 进度消息字典。
    skip_existing=True 时，先从 SQLite 读取已有结果，跳过不重算。
    """
    import time as _time

    start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    end_date = pd.to_datetime(end_date).strftime('%Y-%m-%d')

    yield {'type': 'stage', 'stage': 'trading_days', 'msg': '正在获取交易日列表…'}

    big_count = BREADTH_LOOKBACK_DAYS + 1500
    all_dates_raw = fetcher.get_trading_days_inclusive(end_date, count=big_count)
    if not all_dates_raw:
        yield {'type': 'error', 'error': '无法获取交易日列表'}
        return

    all_dates = [str(d) for d in all_dates_raw]

    try:
        idx_start = next(i for i, d in enumerate(all_dates) if d >= start_date)
    except StopIteration:
        yield {'type': 'error', 'error': f'{start_date} 超出可用交易日范围'}
        return
    try:
        idx_end = next(i for i in range(len(all_dates) - 1, -1, -1) if all_dates[i] <= end_date)
    except StopIteration:
        yield {'type': 'error', 'error': f'{end_date} 超出可用交易日范围'}
        return

    earliest_idx = max(0, idx_start - BREADTH_LOOKBACK_DAYS)
    needed_dates = all_dates[earliest_idx:idx_end + 1]
    target_dates = all_dates[idx_start:idx_end + 1]

    if not needed_dates or not target_dates:
        yield {'type': 'error', 'error': '计算区间为空'}
        return

    total = len(target_dates)

    # 查询已有缓存，版本一致才复用
    cached_results = {}
    if skip_existing:
        try:
            import sqlite3, json as _jl
            conn = sqlite3.connect('database.db')
            c = conn.cursor()
            c.execute(
                "SELECT trading_day, result_json FROM market_sentiment_results "
                "WHERE trading_day >= ? AND trading_day <= ?",
                (target_dates[0], target_dates[-1])
            )
            for row in c.fetchall():
                try:
                    _d = _jl.loads(row[1])
                    if _d.get('version') == SENTIMENT_VERSION:
                        cached_results[row[0]] = _d
                except Exception:
                    pass
            conn.close()
        except Exception:
            pass

    to_compute = [d for d in target_dates if d not in cached_results]
    cached_count = total - len(to_compute)
    if cached_count > 0:
        yield {'type': 'stage', 'stage': 'cache_check',
               'msg': f'共 {total} 个交易日，其中 {cached_count} 天已有缓存可直接使用，需新计算 {len(to_compute)} 天',
               'total': total, 'cached_count': cached_count}

    # 若只有少量日期需要计算，将 needed_dates 裁剪到仅包含
    # "to_compute 中最早日期往前 BREADTH_LOOKBACK_DAYS 天" 至 end，
    # 避免区间很大但绝大多数已缓存时仍加载全量历史数据。
    if to_compute:
        earliest_new = min(to_compute)
        try:
            earliest_new_idx = next(i for i, d in enumerate(all_dates) if d >= earliest_new)
        except StopIteration:
            earliest_new_idx = idx_start
        trimmed_earliest = max(0, earliest_new_idx - BREADTH_LOOKBACK_DAYS)
        needed_dates = all_dates[trimmed_earliest:idx_end + 1]

    if not to_compute:
        # 全部命中缓存，直接逐条返回
        for i, td in enumerate(target_dates):
            done = i + 1
            yield {
                'type': 'result', 'data': cached_results[td],
                'current': done, 'total': total,
                'pct': round(done / total * 100, 1),
                'date': td, 'msg': f'{done}/{total}（缓存）{td}',
                'from_cache': True,
            }
        logger.info(f'批量计算：{total} 天全部命中缓存')
        return

    import threading as _threading
    import queue as _queue

    # ── 后台线程分批加载，通过队列实时推送每批进度 ──
    progress_q = _queue.Queue()
    _result_box = {}

    def _on_batch_progress(batch_idx, total_batches, rows_so_far, label):
        progress_q.put((batch_idx, total_batches, rows_so_far, label))

    def _load_data():
        try:
            _result_box['df_all'] = fetcher.get_all_stocks_returns_for_dates(
                needed_dates, on_progress=_on_batch_progress)
        except Exception as e:
            _result_box['error'] = str(e)
        finally:
            progress_q.put(None)  # sentinel

    yield {'type': 'stage', 'stage': 'load_stocks',
           'msg': f'正在分批加载全市场行情（{len(needed_dates)} 天）…',
           'total': total}

    t_load = _time.time()
    loader_thread = _threading.Thread(target=_load_data, daemon=True)
    loader_thread.start()

    while True:
        try:
            item = progress_q.get(timeout=2.0)
        except _queue.Empty:
            elapsed = round(_time.time() - t_load, 1)
            yield {'type': 'heartbeat', 'stage': 'load_stocks',
                   'msg': f'正在查询数据库… 已耗时 {elapsed} 秒',
                   'elapsed': elapsed}
            continue
        if item is None:
            break
        batch_idx, total_batches, rows_so_far, label = item
        elapsed = round(_time.time() - t_load, 1)
        yield {'type': 'heartbeat', 'stage': 'load_stocks',
               'msg': f'数据加载 第{batch_idx}/{total_batches}批 [{label}]，累计 {rows_so_far:,} 行，耗时 {elapsed}s',
               'elapsed': elapsed,
               'batch': batch_idx, 'total_batches': total_batches, 'rows': rows_so_far}

    loader_thread.join()

    if 'error' in _result_box:
        yield {'type': 'error', 'error': f'数据加载失败: {_result_box["error"]}'}
        return

    df_all = _result_box.get('df_all')
    if df_all is None or df_all.empty:
        yield {'type': 'error', 'error': '全市场行情数据为空'}
        return
    df_all['Date'] = df_all['Date'].astype(str)
    load_stock_sec = round(_time.time() - t_load, 1)
    logger.info(f'全市场行情加载完成: {load_stock_sec}秒, {len(df_all)}条')

    yield {'type': 'stage', 'stage': 'load_index',
           'msg': f'行情加载完成（{load_stock_sec}秒，{len(df_all):,}条），正在加载指数…',
           'total': total}

    all_index_codes = list(set(
        [1, 1059, 1055, 11089, 3145, 4978, 46, 30, 4074, 39144, 36324,
         6973, 4078, 3036, 7544, 39376, 31398, 48542, 19475, 217313,
         3469, 3471, 4089, 225892, 303968] +
        [INNERCODE_SH50, INNERCODE_CSI1000] + list(CORE_INDEX_MA60)
    ))
    df_idx_all = fetcher.get_index_quote_for_dates(all_index_codes, needed_dates[0], needed_dates[-1])
    if df_idx_all is not None and not df_idx_all.empty:
        df_idx_all['TurnoverValue'] = pd.to_numeric(df_idx_all['TurnoverValue'], errors='coerce').fillna(0)
        if 'ChangePCT' in df_idx_all.columns:
            df_idx_all['ChangePCT'] = pd.to_numeric(df_idx_all['ChangePCT'], errors='coerce')
        df_idx_all['TradingDay'] = df_idx_all['TradingDay'].astype(str)

    t_pre = _time.time()
    yield {'type': 'stage', 'stage': 'precompute',
           'msg': '正在预计算：股票池过滤、涨跌停、加权离散度、风格因子…',
           'total': total}

    # 改造四：股票池锚定 — 剔除流动性后10%
    df_all = _filter_universe(df_all)

    breadth_df = _vectorized_daily_breadth(df_all)
    if breadth_df is not None and not breadth_df.empty:
        breadth_df['Date'] = breadth_df['Date'].astype(str)

    # 加权横截面离散度 (WCSV)
    sigma_w_by_date = df_all.groupby('Date').apply(_weighted_cross_sectional_std)
    sigma_e_by_date = df_all.groupby('Date')['R'].std()

    # 风格因子 spread 预计算（向量化）
    # shift(1) 剔除今日收益对今日分组的内生性干扰
    vol20_raw = df_all.groupby('SecuCode')['R'].rolling(FACTOR_VOL_WINDOW, min_periods=10).std()
    vol20_shifted = vol20_raw.groupby(level=0).shift(1)
    df_all['_vol20'] = vol20_shifted.reset_index(level=0, drop=True)

    size_spread_by_date = {}
    vol_spread_by_date = {}
    _df_r = df_all.dropna(subset=['R'])
    if 'NegotiableMV' in _df_r.columns:
        mv_lo = _df_r.groupby('Date')['NegotiableMV'].quantile(FACTOR_QUINTILE).rename('_mv_lo')
        mv_hi = _df_r.groupby('Date')['NegotiableMV'].quantile(1 - FACTOR_QUINTILE).rename('_mv_hi')
        _tmp = _df_r[['Date', 'R', 'NegotiableMV']].merge(mv_lo, left_on='Date', right_index=True)
        _tmp = _tmp.merge(mv_hi, left_on='Date', right_index=True)
        r_small = _tmp[_tmp['NegotiableMV'] <= _tmp['_mv_lo']].groupby('Date')['R'].mean()
        r_large = _tmp[_tmp['NegotiableMV'] >= _tmp['_mv_hi']].groupby('Date')['R'].mean()
        spread_size = (r_small - r_large).dropna()
        size_spread_by_date = spread_size.to_dict()

    if '_vol20' in _df_r.columns:
        _vr = _df_r.dropna(subset=['_vol20'])
        vol_lo = _vr.groupby('Date')['_vol20'].quantile(FACTOR_QUINTILE).rename('_vl')
        vol_hi = _vr.groupby('Date')['_vol20'].quantile(1 - FACTOR_QUINTILE).rename('_vh')
        _vtmp = _vr[['Date', 'R', '_vol20']].merge(vol_lo, left_on='Date', right_index=True)
        _vtmp = _vtmp.merge(vol_hi, left_on='Date', right_index=True)
        r_low_v = _vtmp[_vtmp['_vol20'] <= _vtmp['_vl']].groupby('Date')['R'].mean()
        r_high_v = _vtmp[_vtmp['_vol20'] >= _vtmp['_vh']].groupby('Date')['R'].mean()
        spread_vol = (r_high_v - r_low_v).dropna()
        vol_spread_by_date = spread_vol.to_dict()

    index_liq_codes = [1, 1059]
    daily_turnover_billion = {}
    if df_idx_all is not None and not df_idx_all.empty:
        df_liq = df_idx_all[df_idx_all['InnerCode'].isin(index_liq_codes)]
        daily_total = df_liq.groupby('TradingDay')['TurnoverValue'].sum()
        daily_turnover_billion = (daily_total / 1e8).to_dict()

    CATEGORY_MAP = {
        1: '市场综合基准', 1055: '市场综合基准', 11089: '市场综合基准', 3145: '市场综合基准', 4978: '市场综合基准',
        46: '规模与风格', 30: '规模与风格', 4074: '规模与风格', 39144: '规模与风格', 36324: '规模与风格',
        6973: '主题与策略', 4078: '主题与策略', 3036: '主题与策略', 7544: '主题与策略',
        39376: '主题与策略', 31398: '主题与策略', 48542: '主题与策略', 19475: '主题与策略', 217313: '主题与策略',
        3469: '其他常用', 3471: '其他常用', 4089: '其他常用', 225892: '其他常用', 303968: '其他常用',
    }
    broad_codes = list(CATEGORY_MAP.keys())

    pre_sec = round(_time.time() - t_pre, 1)
    logger.info(f'预计算完成: {pre_sec}秒')

    compute_count = len(to_compute)
    yield {'type': 'stage', 'stage': 'computing',
           'msg': f'预计算完成（{pre_sec}秒），开始逐日评分（共 {total} 天，新算 {compute_count}，缓存 {cached_count}）…',
           'total': total, 'current': 0, 'pct': 0}

    t_calc = _time.time()
    import json as _json
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        import psutil as _psutil
    except ImportError:
        _psutil = None
    import os as _os

    # ── 自适应线程数：根据 CPU / 内存实时决定 ──
    def _auto_workers():
        cpu_count = _os.cpu_count() or 4
        if _psutil is None:
            return min(cpu_count, 16)
        try:
            mem = _psutil.virtual_memory()
            mem_avail_gb = mem.available / (1024 ** 3)
            cpu_pct = _psutil.cpu_percent(interval=0.1)
        except Exception:
            return min(cpu_count, 16)
        if mem_avail_gb < 1.0 or cpu_pct > 90:
            return max(4, cpu_count // 2)
        if mem_avail_gb < 2.0 or cpu_pct > 75:
            return cpu_count
        return min(cpu_count * 2, 32)

    def _calc_one(td):
        try:
            return _compute_single_day_from_cache(
                td, all_dates, df_all, df_idx_all,
                breadth_df, sigma_w_by_date, sigma_e_by_date,
                daily_turnover_billion, CATEGORY_MAP, broad_codes,
                size_spread_by_date=size_spread_by_date,
                vol_spread_by_date=vol_spread_by_date,
            )
        except Exception as e:
            logger.error(f'批量计算 {td} 失败: {e}', exc_info=True)
            return {
                'trading_day': td, 'version': SENTIMENT_VERSION,
                'module1': {}, 'module2': {}, 'module3': {}, 'module4': {},
                'module5': {}, 'module6': {}, 'module7': {}, 'module8': {},
                'total_score': 100, 'total_penalty': 0, 'penalty_details': [],
                'error': str(e),
            }

    computed_results = {}
    WORKERS = min(_auto_workers(), max(1, len(to_compute)))

    if to_compute:
        _done_atomic = [0]
        _lock = _threading.Lock()

        try:
            mem = _psutil.virtual_memory()
            cpu_pct = _psutil.cpu_percent(interval=0.1)
            sys_info = f'CPU {cpu_pct:.0f}% / 内存 {mem.percent:.0f}%（可用 {mem.available/1024**3:.1f}GB）'
        except Exception:
            sys_info = ''

        yield {'type': 'heartbeat', 'stage': 'computing',
               'msg': f'启动 {WORKERS} 线程并行评分（{compute_count} 天） {sys_info}'}
        logger.info(f'并行评分: {WORKERS} workers, {compute_count} 天, {sys_info}')

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            future_map = {executor.submit(_calc_one, td): td for td in to_compute}
            for future in as_completed(future_map):
                td = future_map[future]
                r = future.result()
                computed_results[td] = r
                with _lock:
                    _done_atomic[0] += 1
                    done_new = _done_atomic[0]
                elapsed_calc = _time.time() - t_calc
                speed = done_new / elapsed_calc if elapsed_calc > 0 else 0
                remaining = compute_count - done_new
                eta = round(remaining / speed, 1) if speed > 0 else 0
                if done_new % 10 == 0 or done_new == compute_count:
                    try:
                        mem = _psutil.virtual_memory()
                        cpu_pct = _psutil.cpu_percent(interval=0)
                        res_tag = f'CPU {cpu_pct:.0f}% 内存 {mem.percent:.0f}%'
                    except Exception:
                        res_tag = ''
                    yield {'type': 'heartbeat', 'stage': 'computing',
                           'msg': f'并行评分 {done_new}/{compute_count}，{speed:.1f} 天/s，ETA {eta}s | {res_tag}',
                           'batch': done_new, 'total_batches': compute_count}

    calc_sec = round(_time.time() - t_calc, 1)
    logger.info(f'并行评分完成: {calc_sec}秒, {len(computed_results)}天')

    # ── 批量写入 SQLite（串行，避免锁冲突）──
    if computed_results:
        try:
            import sqlite3
            conn = sqlite3.connect('database.db')
            c = conn.cursor()
            rows_to_write = [(td, _json.dumps(r, ensure_ascii=False))
                             for td, r in computed_results.items()]
            c.executemany(
                "INSERT OR REPLACE INTO market_sentiment_results (trading_day, result_json) VALUES (?, ?)",
                rows_to_write
            )
            conn.commit()
            conn.close()
        except Exception as ex:
            logger.error(f'批量写入缓存失败: {ex}')

    # ── 计算连续能量指数并回写 DB，确保后续缓存读取也能看到 ──
    energy_map = {}
    try:
        energy_map = _compute_energy_map_from_db()
    except Exception as _ex_e:
        logger.error(f'批量计算连续能量指数失败: {_ex_e}', exc_info=True)

    # 将能量值回写进 market_sentiment_results 的 result_json，
    # 否则后续通过缓存 API 读取时 energy_index 会缺失
    if energy_map:
        try:
            import sqlite3 as _sq3, json as _jj
            _conn_e = _sq3.connect('database.db')
            _ce = _conn_e.cursor()
            _upd_rows = []
            for _td in target_dates:
                if _td not in energy_map:
                    continue
                _ce.execute(
                    "SELECT result_json FROM market_sentiment_results WHERE trading_day=?", (_td,))
                _row = _ce.fetchone()
                if _row and _row[0]:
                    _d = _jj.loads(_row[0])
                    _d['energy_index'] = energy_map[_td][0]
                    _d['energy_reset'] = energy_map[_td][1]
                    _upd_rows.append((_jj.dumps(_d, ensure_ascii=False), _td))
            if _upd_rows:
                _ce.executemany(
                    "UPDATE market_sentiment_results SET result_json=? WHERE trading_day=?",
                    _upd_rows)
                _conn_e.commit()
            _conn_e.close()
            logger.info(f'已将能量指数回写进 {len(_upd_rows)} 条 DB 记录')
        except Exception as _ex_upd:
            logger.error(f'回写能量指数到主表失败: {_ex_upd}', exc_info=True)

    # ── 按日期顺序 yield 结果（缓存 + 新算合并 + 能量指数）──
    for i, td in enumerate(target_dates):
        from_cache = td in cached_results
        r = cached_results[td] if from_cache else computed_results.get(td, {})
        if td in energy_map:
            r['energy_index'] = energy_map[td][0]
            r['energy_reset'] = energy_map[td][1]
        done = i + 1
        pct = round(done / total * 100, 1)
        tag = '缓存' if from_cache else '新算'

        yield {
            'type': 'result',
            'data': r,
            'current': done,
            'total': total,
            'pct': pct,
            'date': td,
            'msg': f'{done}/{total}（{pct}%）{td}（{tag}）',
            'from_cache': from_cache,
        }

    total_sec = round(_time.time() - t_calc, 1)
    logger.info(f'批量计算完成：{total} 天（新算 {len(computed_results)}，缓存 {cached_count}），评分+推送 {total_sec}s')


def _compute_single_day_from_cache(
    trading_day: str,
    all_dates: List[str],
    df_all: pd.DataFrame,
    df_idx_all: pd.DataFrame,
    breadth_df: pd.DataFrame,
    sigma_w_by_date: pd.Series,
    sigma_e_by_date: pd.Series,
    daily_turnover_billion: dict,
    category_map: dict,
    broad_codes: list,
    size_spread_by_date: Optional[dict] = None,
    vol_spread_by_date: Optional[dict] = None,
) -> Dict[str, Any]:
    """从预加载的全量数据中，切片计算单日情绪（v2 升级版）。"""
    result = {
        'trading_day': trading_day, 'version': SENTIMENT_VERSION,
        'module1': {}, 'module2': {}, 'module3': {}, 'module4': {},
        'module5': {}, 'module6': {}, 'module7': {}, 'module8': {},
        'total_score': 100, 'total_penalty': 0, 'penalty_details': [],
    }

    idx_td = None
    for i, d in enumerate(all_dates):
        if d == trading_day:
            idx_td = i
            break
    if idx_td is None:
        result['error'] = f'{trading_day} 不在交易日列表中'
        return result

    w252_start = max(0, idx_td - BREADTH_LOOKBACK_DAYS + 1)
    dates_252 = all_dates[w252_start:idx_td + 1]
    set_252 = set(dates_252)

    market_return_today: Optional[float] = None

    # MODULE 1
    if breadth_df is not None and not breadth_df.empty:
        breadth_252 = breadth_df[breadth_df['Date'].isin(set_252)]
        result['module1'] = module1_market_breadth(breadth_252, trading_day)
    else:
        result['module1'] = {'penalty': 0, 'penalty_detail': [], 'error': '无breadth数据'}

    # MODULE 2
    dates_20 = dates_252[-20:] if len(dates_252) >= 20 else dates_252
    if daily_turnover_billion:
        turnover_today = daily_turnover_billion.get(trading_day, 0)
        series_5 = [daily_turnover_billion.get(d, 0) for d in dates_20[-5:]]
        series_20 = [daily_turnover_billion.get(d, 0) for d in dates_20]
        series_252 = [daily_turnover_billion.get(d, 0) for d in dates_252]
        if df_idx_all is not None and not df_idx_all.empty:
            _idx_td = df_idx_all[df_idx_all['TradingDay'] == trading_day]
            if not _idx_td.empty and 'ChangePCT' in _idx_td.columns:
                _ip = _idx_td.set_index('InnerCode')['ChangePCT'].dropna()
                if not _ip.empty:
                    _sh_r = float(_ip.get(1, 0)) / 100.0
                    _sz_r = float(_ip.get(1059, 0)) / 100.0
                    market_return_today = min(_sh_r, _sz_r)
        result['module2'] = module2_liquidity(turnover_today, series_5, series_20, turnover_series_252=series_252)
    else:
        result['module2'] = {'penalty': 0, 'penalty_detail': [], 'error': '无成交额数据'}

    # MODULE 3 (WCSV)
    dates_61 = dates_252[-CSV_LOOKBACK_DAYS:] if len(dates_252) >= CSV_LOOKBACK_DAYS else dates_252
    sw_slice = sigma_w_by_date.reindex(dates_61).dropna() if sigma_w_by_date is not None else pd.Series(dtype=float)
    sw_today = float(sw_slice.get(trading_day, np.nan)) if trading_day in sw_slice.index else np.nan
    se_today = float(sigma_e_by_date.get(trading_day, np.nan)) if sigma_e_by_date is not None and trading_day in sigma_e_by_date.index else np.nan
    result['module3'] = module3_cross_sectional_volatility(sw_today, sw_slice.tolist(), sigma_equal_today=se_today)

    # MODULE 4
    if df_idx_all is not None and not df_idx_all.empty:
        df_broad_today = df_idx_all[
            (df_idx_all['InnerCode'].isin(broad_codes)) & (df_idx_all['TradingDay'] == trading_day)].copy()
        df_broad_today['Category'] = df_broad_today['InnerCode'].map(category_map).fillna('')
        dates_20_idx = dates_252[-BROAD_INDEX_MA_DAYS:] if len(dates_252) >= BROAD_INDEX_MA_DAYS else dates_252
        df_series_20 = df_idx_all[
            (df_idx_all['InnerCode'].isin(broad_codes)) & (df_idx_all['TradingDay'].isin(set(dates_20_idx)))]
        dates_60_idx = dates_252[-BROAD_INDEX_MA60_DAYS:] if len(dates_252) >= BROAD_INDEX_MA60_DAYS else dates_252
        df_series_60 = df_idx_all[
            (df_idx_all['InnerCode'].isin(list(CORE_INDEX_MA60))) & (df_idx_all['TradingDay'].isin(set(dates_60_idx)))]
        result['module4'] = module4_broad_index_trend(df_broad_today, df_series_20, df_series_60, trading_day)
    else:
        result['module4'] = {'indices': [], 'summary': '无指数数据', 'penalty': 0, 'penalty_detail': []}

    # MODULE 5 (Style Factors)
    try:
        df_today_stocks = df_all[df_all['Date'] == trading_day]
        recent_idx = max(0, idx_td - FACTOR_ZSCORE_WINDOW)
        recent_dates = all_dates[recent_idx:idx_td]
        size_hist = [size_spread_by_date.get(d) for d in recent_dates] if size_spread_by_date else []
        vol_hist = [vol_spread_by_date.get(d) for d in recent_dates] if vol_spread_by_date else []
        df_idx_pair = None
        if df_idx_all is not None and not df_idx_all.empty:
            df_idx_pair = df_idx_all[
                (df_idx_all['InnerCode'].isin([INNERCODE_SH50, INNERCODE_CSI1000])) &
                (df_idx_all['TradingDay'] == trading_day)]
        result['module5'] = module5_style_factors(df_today_stocks, size_hist, vol_hist, df_idx_pair)
    except Exception as e:
        result['module5'] = {'penalty': 0, 'penalty_detail': [], 'error': str(e)}

    # MODULE 6
    if len(dates_252) >= HOT_LOOKBACK_DAYS:
        hot_dates = dates_252[-HOT_LOOKBACK_DAYS:]
        df_hot = df_all[df_all['Date'].isin(set(hot_dates))]
        yesterday, today2 = hot_dates[-2], hot_dates[-1]
        try:
            result['module6'] = module6_hot_money(df_hot, yesterday, today2)
        except Exception as e:
            result['module6'] = {'penalty': 0, 'penalty_detail': [], 'error': str(e)}
    else:
        result['module6'] = {'penalty': 0, 'penalty_detail': [], 'error': '交易日不足'}

    # MODULE 7 (Smart Money)
    df_today_stocks = df_all[df_all['Date'] == trading_day]
    result['module7'] = module7_smart_money(df_today_stocks, market_return_today)

    # MODULE 8
    if len(dates_252) >= 21:
        dates_21 = dates_252[-21:]
        window20_set = set(dates_21[:-1])
        df_window20 = df_all[df_all['Date'].isin(window20_set)]
        result['module8'] = module8_strong_stock_crash(df_window20, df_today_stocks, market_return_today)
    else:
        result['module8'] = {'penalty': 0, 'penalty_detail': [], 'error': '历史交易日不足21天'}

    # 汇总
    total_penalty = 0
    penalty_details = []
    for key in ['module1', 'module2', 'module3', 'module4', 'module5', 'module6', 'module7', 'module8']:
        m = result.get(key, {})
        p = m.get('penalty', 0) or 0
        if p > 0:
            total_penalty += p
            penalty_details.extend(m.get('penalty_detail', []))
    result['total_penalty'] = total_penalty
    result['penalty_details'] = penalty_details
    result['total_score'] = 100 - total_penalty
    return result


# ═══════════════════════════════════════════════════════════════════
#  报告生成器 —— 纯文字（Markdown），与计算逻辑完全解耦
# ═══════════════════════════════════════════════════════════════════

def generate_sentiment_report(data: dict) -> str:
    """
    根据 compute_daily_sentiment / 缓存返回的 result dict，
    生成一份带算法理论与量化解读的完整市场情绪诊断报告（Markdown 格式）。
    """
    day = data.get('trading_day', '未知')
    score = data.get('total_score', 100)
    penalty = data.get('total_penalty', 0)

    if score >= 85:
        regime, pos, risk = '主升浪 / 活跃资金主导', '建议满仓进攻，趋势共振强烈', '🟢 绿灯'
    elif score >= 70:
        regime, pos, risk = '温和多头 / 结构性行情', '建议7成仓持有核心标的', '🟢 绿灯'
    elif score >= 50:
        regime, pos, risk = '震荡分化 / 选股为王', '建议5成仓精选个股，回避弱势板块', '🟡 黄灯'
    elif score >= 30:
        regime, pos, risk = '弱势防御 / 风控优先', '建议3成以下仓位，严格止损', '🟡 黄灯'
    else:
        regime, pos, risk = '极端恐慌 / 系统性风险', '建议空仓回避，等待右侧信号', '🔴 红灯'

    def _pct(v, digits=2):
        return f'{v * 100:+.{digits}f}%' if v is not None else '-'
    def _f(v, digits=2):
        return f'{v:.{digits}f}' if v is not None and not (isinstance(v, float) and np.isnan(v)) else '-'
    def _yn(v):
        return '是' if v else '否'

    L = []

    # ════════════════════════ 封面摘要 ════════════════════════
    L.append(f'# 市场情绪诊断报告 · {day}\n')
    L.append(f'**综合得分：{score} / 100**（累计扣分 {penalty}分） · 风控信号：{risk}')
    L.append(f'**市场环境研判**：{regime}')
    L.append(f'**仓位操作指令**：{pos}\n')

    # 能量指数摘要
    ei = data.get('energy_index')
    er = data.get('energy_reset', False)
    if ei is not None:
        ei_tag = ('⚡ 冰点重置 — 能量归零' if er
                  else '🔥 过热区域' if ei > 60
                  else '📈 上升积累期' if ei > 20
                  else '〰 中性震荡区' if ei > -10
                  else '❄ 能量消耗中')
        L.append(f'**连续能量指数（水库水位）**：{ei:.1f} · {ei_tag}')
        if er:
            L.append('> ⚡ **Regime Shift**：今日多维度异常共振，系统判定为市场绝望点。能量从零开始重新积累，历史上这是最优的中期介入时机之一，请等待右侧确认后跟进。')
    L.append('')

    # ════════════════════════ M1 ════════════════════════
    m1 = data.get('module1') or {}
    L.append('---')
    L.append('## M1 · 资金多空比（成交额加权情绪）')
    L.append('> **算法原理**：MFR = Σ(上涨股成交额) / Σ(下跌股成交额)。用真金白银衡量多空对比，避免涨跌家数被量能悬殊股票误导。同时计算市值加权多空比（MVFR），两者背离度 = ln(MFR) − ln(MVFR) 揭示"资金面与市值面的分歧"：正值意味着小市值资金流向比大市值更为积极，是题材市的典型特征。')
    L.append(f'> **平滑打分逻辑**：①跌停恐慌蔓延——取今日跌停数在252日历史中的百分位，{M1_LIMITDOWN_SAFE_PCT:.0f}%以下不扣分，达到{M1_LIMITDOWN_DANGER_PCT:.0f}%扣满{M1_LIMITDOWN_MAX_PENALTY}分，中间线性插值；②MFR比率映射——MFR≥{M1_MFR_SAFE}不扣分，跌至{M1_MFR_DANGER}扣满{M1_MFR_MAX_PENALTY}分。两项合计最高{M1_LIMITDOWN_MAX_PENALTY + M1_MFR_MAX_PENALTY}分，消除了旧版P90/P98硬档的突变跳跃。')
    if m1.get('error'):
        L.append(f'> ⚠ 数据异常：{m1["error"]}')
    else:
        mfr = m1.get('money_flow_ratio')
        mvfr = m1.get('mv_flow_ratio')
        dv = m1.get('divergence')
        if mfr is not None:
            if mfr >= 2.0:
                mfr_interp = f'多头极度强势（每1元空方资金对应{mfr:.1f}元多方）'
            elif mfr >= 1.0:
                mfr_interp = f'多头占优（多空资金比 {mfr:.2f}:1）'
            elif mfr >= 0.5:
                mfr_interp = f'多空均衡偏空（多空资金比 {mfr:.2f}:1）'
            else:
                mfr_interp = f'空头主导，恐慌情绪显著（多空比仅 {mfr:.2f}）'
            L.append(f'- **成交额多空比（MFR）= {_f(mfr)}**：{mfr_interp}')
        if mvfr is not None:
            L.append(f'- 市值加权多空比（MVFR）= {_f(mvfr)}')
        if dv is not None:
            dv_interp = ('资金流向偏向中小市值股（题材活跃）' if dv > 0.1
                         else '资金均衡分布' if dv > -0.1
                         else '资金向大市值蓝筹集中（防御情绪）')
            L.append(f'- 背离度 = {_f(dv, 3)}：{dv_interp}')
        L.append(f'- 今日市场：上涨 {m1.get("up_count","?")} 只 / 下跌 {m1.get("down_count","?")} 只，涨停 {m1.get("limit_up","?")} / 跌停 {m1.get("limit_down","?")}，一字跌停 {m1.get("one_word_limit_down","?")} 只')
        ld_pct = m1.get('ld_percentile')
        if ld_pct is not None:
            L.append(f'- 跌停数历史分位：**{ld_pct:.1f}%**（安全线{M1_LIMITDOWN_SAFE_PCT:.0f}% / 危险线{M1_LIMITDOWN_DANGER_PCT:.0f}%）')
        p1 = m1.get('penalty', 0)
        if p1:
            L.append(f'- **⚠ 扣分 {p1} 分**：{"；".join(m1.get("penalty_detail", []))}')
        else:
            L.append('- ✅ 本模块无扣分：多空力量均衡，未触发跌停恐慌或资金极端撤退')
    L.append('')

    # ════════════════════════ M2 ════════════════════════
    m2 = data.get('module2') or {}
    L.append('## M2 · 流动性缩量监测')
    L.append('> **算法原理**：两层缩量检验。① 短期断崖缩量：5日均量/20日均量。② 绝对地量：当日总成交额在252日历史中的百分位。两者均通过连续映射打分，可叠加。')
    L.append(f'> **平滑打分逻辑**：①断崖缩量——5/20日均量比≥{M2_SHRINK_SAFE:.0%}不扣分，跌至{M2_SHRINK_DANGER:.0%}扣满{M2_SHRINK_MAX_PENALTY}分；②绝对地量——成交额分位≥{M2_VOLUME_SAFE_PCT:.0f}%不扣分，跌至{M2_VOLUME_DANGER_PCT:.0f}%扣满{M2_VOLUME_MAX_PENALTY}分。废弃旧版"<70%硬扣15分"/"P10硬扣15分"的突变逻辑。')
    if m2.get('error'):
        L.append(f'> ⚠ 数据异常：{m2["error"]}')
    else:
        t = m2.get('turnover_today')
        v5 = m2.get('vol_5')
        v20 = m2.get('vol_20')
        p10 = m2.get('p10_turnover_252')
        sr = m2.get('shrink_ratio')
        vp = m2.get('volume_percentile')
        if t is not None and v20 is not None and v20 > 0:
            ratio = sr if sr is not None else (t / v20)
            ratio_interp = ('远高于均量，市场活跃' if ratio > 1.3
                            else '均量水平，流动性正常' if ratio > M2_SHRINK_SAFE
                            else '轻微缩量，观望情绪上升' if ratio > M2_SHRINK_DANGER
                            else '显著缩量，市场参与度下降')
            L.append(f'- **当日成交额 {_f(t)} 亿**（20日均量 {_f(v20)} 亿，5/20日比值 {ratio:.0%}）：{ratio_interp}')
        if vp is not None:
            vol_pct_interp = (f'处于历史{vp:.1f}%分位，绝对地量区间，流动性极度枯竭' if vp < M2_VOLUME_DANGER_PCT
                              else f'处于历史{vp:.1f}%分位，低流动性区间' if vp < M2_VOLUME_SAFE_PCT
                              else f'处于历史{vp:.1f}%分位，流动性尚可')
            L.append(f'- 成交额252日分位：{vol_pct_interp}（安全线{M2_VOLUME_SAFE_PCT:.0f}% / 危险线{M2_VOLUME_DANGER_PCT:.0f}%）')
        elif p10 is not None and t is not None:
            pct_rank = '历史绝对地量区间' if t < p10 else f'高于252日P10（{_f(p10)}亿），流动性尚可'
            L.append(f'- 252日10%分位参照 = {_f(p10)} 亿：{pct_rank}')
        p2 = m2.get('penalty', 0)
        if p2:
            L.append(f'- **⚠ 扣分 {p2} 分**：{"；".join(m2.get("penalty_detail", []))}')
        else:
            L.append('- ✅ 本模块无扣分：市场成交量充裕，流动性未发出危险信号')
    L.append('')

    # ════════════════════════ M3 ════════════════════════
    m3 = data.get('module3') or {}
    L.append('## M3 · 加权横截面离散度（WCSV）')
    L.append('> **算法原理**：WCSV（Weighted Cross-Sectional Volatility）= 以 √成交额 为权重计算全市场个股收益率的横截面标准差。使用 √ 而非直接用成交额，是为了降低超大盘股的"寡头效应"，让中小盘股的波动信号不被淹没。公式：σ_w = √[Σ wᵢ(rᵢ − r̄)²]，其中 wᵢ = √Amountᵢ / Σ√Amountⱼ。')
    L.append(f'> **平滑打分逻辑**：计算WCSV在近60日的百分位，≥{M3_CSV_SAFE_PCT:.0f}%不扣分（选股空间尚可），跌至{M3_CSV_DANGER_PCT:.0f}%扣满{M3_CSV_MAX_PENALTY}分（极致系统性Beta主导），中间连续插值。废弃旧版"后10%硬扣20分"的突变阈值。')
    if m3.get('error'):
        L.append(f'> ⚠ 数据异常：{m3["error"]}')
    else:
        sw = m3.get('sigma_weighted')
        se = m3.get('sigma_equal')
        pct = m3.get('percentile')
        if sw is not None:
            sw_interp = ('极高离散度，个股机会丰富' if sw > 0.04
                         else '正常离散度，选股有效' if sw > 0.02
                         else '低离散度，分化不明显' if sw > 0.015
                         else '极低离散度，市场同向共振，选股因子暂时失效')
            L.append(f'- **加权WCSV = {_f(sw, 4)}**（等权σ = {_f(se, 4)}）：{sw_interp}')
        if pct is not None:
            L.append(f'- 处于过去60日 **{_f(pct)}% 分位**（安全线{M3_CSV_SAFE_PCT:.0f}% / 危险线{M3_CSV_DANGER_PCT:.0f}%，满分{M3_CSV_MAX_PENALTY}分）')
        p3 = m3.get('penalty', 0)
        if p3:
            L.append(f'- **⚠ 扣分 {p3} 分**：{"；".join(m3.get("penalty_detail", []))}')
        else:
            L.append('- ✅ 本模块无扣分：截面离散度正常，个股选择空间充裕')
    L.append('')

    # ════════════════════════ M4 ════════════════════════
    m4 = data.get('module4') or {}
    L.append('## M4 · 宽基指数均线趋势（价格偏离度平滑）')
    L.append('> **算法原理**：将"跌破均线"这一非黑即白的布尔判断，改为连续的"价格偏离度"映射（偏离度 = close/MA − 1）。双层结构：① 中证1000偏离MA20；② 沪深300+中证500+中证1000偏离MA60的均值。')
    L.append(f'> **平滑打分逻辑**：①中证1000偏MA20——偏离度≥{M4_CSI1000_MA20_SAFE:+.0%}不扣分，跌至{M4_CSI1000_MA20_DANGER:+.0%}扣满{M4_CSI1000_MA20_MAX_PENALTY}分；②三指数偏MA60均值——≥{M4_MA60_SAFE:+.0%}不扣分，跌至{M4_MA60_DANGER:+.0%}扣满{M4_MA60_MAX_PENALTY}分。均价上方1%处于绝对安全区，消除"差1分钱就突然扣15分"的悬崖效应。')
    if m4.get('error'):
        L.append(f'> ⚠ 数据异常：{m4["error"]}')
    else:
        csi1000_dev = m4.get('csi1000_dev_ma20')
        core_dev = m4.get('core_avg_dev_ma60')
        if csi1000_dev is not None:
            dev_interp = ('✅ 均线上方安全区' if csi1000_dev >= M4_CSI1000_MA20_SAFE
                          else f'⚠ 偏离均线 {csi1000_dev:+.2%}，趋势破位风险' if csi1000_dev < 0
                          else f'接近安全边界（{csi1000_dev:+.2%}）')
            L.append(f'- **中证1000偏离MA20 = {csi1000_dev:+.2%}**：{dev_interp}')
        if core_dev is not None:
            cd_interp = ('✅ 三指数均位于60日均线上方' if core_dev >= M4_MA60_SAFE
                         else f'⚠ 三指数平均偏离MA60 = {core_dev:+.2%}，系统性承压' if core_dev < 0
                         else f'三指数接近60日均线（{core_dev:+.2%}）')
            L.append(f'- **三大宽基偏离MA60均值 = {core_dev:+.2%}**：{cd_interp}')
        for nm in ['上证50', '沪深300', '中证500', '中证1000']:
            for r in (m4.get('indices') or []):
                if nm in (r.get('name', '') + r.get('abbr', '')):
                    chg = f"（{r.get('change_pct','?')}%）" if r.get('change_pct') is not None else ''
                    dev_ma20 = r.get('dev_ma20')
                    dev_str = f'，偏MA20={dev_ma20:+.2%}' if dev_ma20 is not None else ''
                    L.append(f'- {nm}：收盘 {r.get("close","?")}{chg}{dev_str}，MA20 = {r.get("ma20","?")}')
                    break
        p4 = m4.get('penalty', 0)
        if p4:
            L.append(f'- **⚠ 扣分 {p4} 分**：{"；".join(m4.get("penalty_detail", []))}')
        else:
            L.append('- ✅ 本模块无扣分：核心宽基指数偏离度处于安全区间，中短期趋势健康')
    L.append('')

    # ════════════════════════ M5 ════════════════════════
    m5 = data.get('module5') or {}
    L.append('## M5 · 风格因子监控（Size + Volatility Factor，Z-Score自适应）')
    L.append('> **算法原理**：构建两个纯因子回报（Pure Factor Return）。① **Size Factor**：Spread = 最小20%市值组收益 − 最大20%市值组收益。② **Vol Factor**：Spread = 最高20%波动率组收益 − 最低20%波动率组收益。波动率使用 shift(1) 剔除内生性偏差。')
    L.append(f'> **Z-Score动态自适应**：计算今日Spread在过去{FACTOR_ZSCORE_WINDOW}天的Z-Score（偏离了过去均值几个标准差）。从{M5_ZSCORE_START}σ开始起扣，每向下偏离{M5_ZSCORE_STEP}σ扣{M5_ZSCORE_STEP_PENALTY}分（Size最多{M5_SIZE_MAX_PENALTY}分，Vol最多{M5_VOL_MAX_PENALTY}分）。优势：股灾高波市场σ大，自动放宽容忍度；死水低波市场σ小，自动收紧警报线，彻底废弃"3日累计<-3%"的硬编码。')
    if m5.get('error'):
        L.append(f'> ⚠ 数据异常：{m5["error"]}')
    else:
        ss = m5.get('spread_size')
        sv = m5.get('spread_vol')
        sz = m5.get('size_z_score')
        vz = m5.get('vol_z_score')
        csi_diff = m5.get('r_csi1000_vs_sh50')
        if ss is not None:
            ss_interp = ('小盘强势，题材活跃' if ss > 0.01
                         else '多空均衡' if ss > -0.005
                         else '小盘弱势，资金撤离中小盘')
            z_str = f'，Z-Score={sz:.2f}σ' if sz is not None else ''
            L.append(f'- **Size Spread = {_pct(ss, 2)}{z_str}**：{ss_interp}')
        if sv is not None:
            sv_interp = ('高弹性股强势，风险偏好高' if sv > 0.01
                         else '风格均衡' if sv > -0.005
                         else '高弹性股弱势，市场在规避波动率')
            vz_str = f'，Z-Score={vz:.2f}σ' if vz is not None else ''
            L.append(f'- **Vol Spread = {_pct(sv, 2)}{vz_str}**：{sv_interp}')
        if csi_diff is not None:
            cd_interp = ('中小盘强于大盘，偏进攻型' if csi_diff > 0.01
                         else '多空均衡' if csi_diff > -0.01
                         else '大盘蓝筹相对强势，资金抱团防御')
            L.append(f'- 中证1000 vs 上证50 = {_pct(csi_diff, 2)}：{cd_interp}（仅展示，已并入Z-Score逻辑）')
        p5 = m5.get('penalty', 0)
        if p5:
            L.append(f'- **⚠ 扣分 {p5} 分**：{"；".join(m5.get("penalty_detail", []))}')
        else:
            L.append('- ✅ 本模块无扣分：风格因子Z-Score处于正常区间，市场结构均衡')
    L.append('')

    # ════════════════════════ M6 ════════════════════════
    m6 = data.get('module6') or {}
    L.append('## M6 · 短线赚钱效应（涨停接力监测）')
    L.append('> **算法原理**：追踪昨日所有涨停股在今日的表现，计算两个指标：① **热钱平均收益**（Hot Avg Return）；② **核按钮比例**（Nuke Ratio）= 今日近似跌停家数 / 昨日涨停总家数。')
    L.append(f'> **平滑打分逻辑**：①昨涨停股今均收益——≥{M6_HOT_AVG_SAFE:.0%}不扣分，跌至{M6_HOT_AVG_DANGER:.0%}扣满{M6_HOT_AVG_MAX_PENALTY}分；②核按钮比例——≤{M6_NUKE_SAFE:.0%}不扣分（正常摩擦），达到{M6_NUKE_DANGER:.0%}扣满{M6_NUKE_MAX_PENALTY}分。废弃旧版"<-2%一刀切"/"≥20%一刀切"的硬阈值，打板资金亏损程度与市场情绪成比例反映。')
    if m6.get('error'):
        L.append(f'> ⚠ {m6["error"]}')
    else:
        hc = m6.get('hot_count', 0)
        har = m6.get('hot_avg_return')
        nk = m6.get('nuke_count', 0)
        nr = m6.get('nuke_ratio')
        if har is not None:
            har_interp = ('打板策略盈利，资金接力意愿强烈' if har > 0.02
                          else '微幅正收益，情绪一般' if har > 0
                          else '轻微亏损，接力情绪降温' if har > -0.02
                          else '接力策略亏损严重，热钱溃逃风险上升')
            L.append(f'- **昨日涨停 {hc} 只，今日均收益 {_pct(har)}**：{har_interp}')
        if nr is not None and hc > 0:
            nr_interp = (f'核按钮比例极高（{nk}/{hc}），打板资金遭遇灭顶之灾，情绪将骤然降温' if nr >= 0.2
                         else f'有 {nk} 只触发核按钮，比例尚可接受' if nr > 0.05
                         else '核按钮极少，打板风险可控')
            L.append(f'- **核按钮 {nk} 只（比例 {_pct(nr)}）**：{nr_interp}')
        p6 = m6.get('penalty', 0)
        if p6:
            L.append(f'- **⚠ 扣分 {p6} 分**：{"；".join(m6.get("penalty_detail", []))}')
        else:
            L.append('- ✅ 本模块无扣分：打板资金盈利，接力意愿与赚钱效应维持')
    L.append('')

    # ════════════════════════ M7 ════════════════════════
    m7 = data.get('module7') or {}
    L.append('## M7 · 聪明钱与日内承接力（Smart Money 四形态解构）')
    L.append('> **算法原理**：将全市场日内 K 线解构为四种互斥微观形态（成交额加权）：'
             '① **诱多派发**（高开+收阴+上影>下影）② **诱空吸筹**（平低开+收阳+下影>上影）'
             '③ **单边上涨**（实体占波幅>{0:.0%}的阳线）④ **单边下跌**（实体占波幅>{0:.0%}的阴线）。'
             '计算净派发能量 = 派发占比 − 吸筹占比×{1}（对冲系数），反映"抛压重力>托盘动能"的不对称性。'.format(M7_BODY_RATIO_THRESH, M7_HEDGE_ALPHA))
    L.append(f'> **平滑打分逻辑**：①净派发占比——≤{M7_DISTRIB_SAFE:.0%}不扣分，达到{M7_DISTRIB_DANGER:.0%}扣满{M7_DISTRIB_MAX_PENALTY}分；'
             f'②换手率前{TURNOVER_TOP_PCT:.0%}活跃池均收益——≥{M7_TURNOVER_SAFE:.0%}不扣分，跌至{M7_TURNOVER_DANGER:.0%}扣满{M7_TURNOVER_MAX_PENALTY}分。')
    if m7.get('error'):
        L.append(f'> ⚠ 数据异常：{m7["error"]}')
    else:
        sdr = m7.get('smart_distrib_ratio')
        sar = m7.get('smart_accum_ratio')
        uur = m7.get('unilateral_up_ratio')
        udr = m7.get('unilateral_down_ratio')
        ndr = m7.get('net_distrib_ratio')
        c_d = m7.get('count_distrib', 0)
        c_a = m7.get('count_accum', 0)
        c_u = m7.get('count_uni_up', 0)
        c_dn = m7.get('count_uni_down', 0)
        c_o = m7.get('count_unclassified', 0)

        if sdr is not None:
            L.append(f'- **诱多派发** {_pct(sdr)}（{c_d}只） · '
                     f'**诱空吸筹** {_pct(sar)}（{c_a}只） · '
                     f'**单边上涨** {_pct(uur)}（{c_u}只） · '
                     f'**单边下跌** {_pct(udr)}（{c_dn}只） · '
                     f'其他 {c_o}只')
        if ndr is not None:
            ndr_interp = ('净抛压极重，主力系统性出货' if ndr >= 0.50
                          else '净抛压较大，机构派发明显' if ndr >= 0.30
                          else '多空分歧中偏空，需留意' if ndr >= 0.15
                          else '派发与吸筹基本对冲，市场博弈均衡' if ndr >= 0.05
                          else '吸筹力量充分对冲派发，微观结构健康')
            L.append(f'- **净派发能量 = {_pct(ndr)}**'
                     f'（{_pct(sdr)} − {_pct(sar)}×{M7_HEDGE_ALPHA}）：{ndr_interp}')

        ttr = m7.get('top_turnover_avg_return')
        ttc = m7.get('top_turnover_count', 0)
        if ttr is not None:
            ttr_interp = ('活跃资金大幅盈利，热钱主导行情' if ttr > 0.02
                          else '活跃资金小幅盈利，承接力稳定' if ttr > 0
                          else '活跃资金轻微亏损，承接趋于谨慎' if ttr > -0.02
                          else f'活跃池（{ttc}只）整体亏损 {_pct(ttr)}，热钱被系统性闷杀')
            L.append(f'- **换手率TOP{TURNOVER_TOP_PCT:.0%} 活跃池（{ttc}只）均收益 = {_pct(ttr)}**：{ttr_interp}')

        p7 = m7.get('penalty', 0)
        if p7:
            L.append(f'- **⚠ 扣分 {p7} 分**：{"；".join(m7.get("penalty_detail", []))}')
        else:
            L.append('- ✅ 本模块无扣分：净派发与承接力均在安全区间，微观结构无系统性出货特征')
    L.append('')

    # ════════════════════════ M8 ════════════════════════
    m8 = data.get('module8') or {}
    L.append('## M8 · 强势股补跌预警（动量崩溃检测，Z-Score自适应）')
    L.append('> **算法原理**：构建"强势股池"——过去20个交易日累计收益处于全市场前10%的股票。提取该池在窗口期内的每日平均收益率时间序列，计算历史均值和波动率（σ）。')
    L.append(f'> **Z-Score自适应打分**：大盘下跌日激活。计算今日强势股均收益的Z-Score，从{M8_ZSCORE_START}σ开始起扣，每向下偏离{M8_ZSCORE_STEP}σ扣{M8_ZSCORE_STEP_PENALTY}分，上限{M8_MAX_PENALTY}分。优势：只有当强势股今日跌幅"相对于它自己过去的波动习惯显得极不正常"时才判定为动量崩塌，彻底废弃固定-3%阈值的死板逻辑。')
    if m8.get('error'):
        L.append(f'> ⚠ {m8["error"]}')
    else:
        sc = m8.get('strong_count', 0)
        sar = m8.get('strong_avg_return')
        thr = m8.get('threshold_cum20')
        mkt = m8.get('market_return_today')
        ldc = m8.get('limitdown_count', 0)
        if mkt is not None:
            mkt_interp = f"大盘{'下跌' if mkt < 0 else '上涨'} {_pct(mkt)}"
            L.append(f'- **市场日收益 {_pct(mkt)}**：{mkt_interp}')
        sz8 = m8.get('strong_z_score')
        if sar is not None and sc > 0:
            sar_interp = ('强势股今日强势上涨，动量完好' if sar > 0.01
                          else '强势股表现平稳，动量延续' if sar > -0.01
                          else '强势股出现较大回撤，动量减弱' if sar > -0.02
                          else f'强势股集体重挫，触发动量崩溃预警！其中跌停 {ldc} 只')
            z8_str = f'，Z-Score={sz8:.2f}σ' if sz8 is not None else ''
            L.append(f'- **强势股池 {sc} 只，今日均收益 {_pct(sar)}{z8_str}**（20日前10%阈值：{_pct(thr)}）：{sar_interp}')
        p8 = m8.get('penalty', 0)
        if p8:
            L.append(f'- **⚠ 扣分 {p8} 分**：{"；".join(m8.get("penalty_detail", []))}')
        else:
            L.append('- ✅ 本模块无扣分：强势股动量因子稳定，未出现集体补跌或动量崩溃')
    L.append('')

    # ════════════════════════ 连续能量指数 ════════════════════════
    L.append('---')
    L.append('## ⚡ 连续能量指数（Continuous Energy Index）')
    L.append('> **模型架构**：水库水位模型，记录从每次"市场绝望点"归零后累计积聚的信心能量。公式：**S_t = S_{t-1} × (1 − Decay_t) + ΔS_t**，其中 ΔS_t = (今日分数 − 60日动态中性线)，若 ΔS < 0 则放大1.5倍（模拟"信心易碎"心理）；Decay_t = 0.05 × (1 + |S_{t-1}|/150)（能量越大衰减越强，防止无限累积）。')
    L.append('> **冰点重置（Regime Shift）触发机制**：4个并行条件，任意满足3个即触发归零。① M1多空比Z-Score < -2σ（资金面极端离群）；② M2成交额Z-Score < -2σ（流动性极度枯竭）；③ M6核按钮比例Z-Score > +2σ（热钱遭灭顶）；④ 8维指标滚动相关性矩阵出现Z-Score突变 > +2σ（系统性坍塌共振）。所有阈值均基于252日滚动分布动态计算，无任何硬编码。')
    if ei is not None:
        if er:
            L.append(f'- **今日触发冰点重置** ⚡：能量从零开始重新积累。这是最具参考价值的中期介入信号，但需要配合右侧量价确认后再进场。')
        else:
            ei_stage = ('⚠ 能量过热区，上涨动能开始自适应衰减，需警惕阶段性顶部' if ei > 60
                        else '📈 能量稳定上升阶段，多头合力有效，持仓向好' if ei > 20
                        else '〰 中性震荡区，缺乏方向性动能，以精选为主' if ei > -10
                        else '❄ 能量持续消耗，多头力量逐步流失，注意仓位风险')
            L.append(f'- **今日能量值 = {ei:.1f}**：{ei_stage}')
    else:
        L.append('- 历史数据不足（需至少40天截面分数），能量指数暂未启动')
    L.append('')

    # ════════════════════════ 综合诊断 ════════════════════════
    L.append('---')
    L.append('## 综合诊断与操作建议')
    modules = [data.get(f'module{i}') or {} for i in range(1, 9)]
    triggered = [f'M{i+1}' for i, mx in enumerate(modules) if (mx.get('penalty') or 0) > 0]
    clean = [f'M{i+1}' for i, mx in enumerate(modules) if (mx.get('penalty') or 0) == 0]
    if not triggered:
        L.append('**八大维度均未触发扣分**，市场全面健康。资金、流动性、趋势、风格、赚钱效应、聪明钱、动量因子全线绿灯，可积极参与主线方向。')
    else:
        L.append(f'**风险触发维度（{len(triggered)}个）**：{"、".join(triggered)}')
        if clean:
            L.append(f'**健康维度（{len(clean)}个）**：{"、".join(clean)}')
        L.append('')
        if score >= 70:
            L.append('整体评估：风险点分散，多头框架完整。局部扣分属于正常市场摩擦，不影响大局。建议维持7成以上仓位，重点关注触发模块对应的操作细节（如M5触发则回避小盘题材）。')
        elif score >= 50:
            L.append('整体评估：结构性分化信号出现，市场进入精选阶段。建议将仓位降至5成，重仓符合强势方向的标的，回避弱势板块。相比趋势跟踪策略，当前更适合均值回归类操作。')
        elif score >= 30:
            L.append('整体评估：多项风控指标同时亮灯，系统性压力显著。建议将仓位降至3成以下，以保护本金为首要目标。严格止损，不轻易抄底，等待分数回升至50以上再逐步增仓。')
        else:
            L.append('整体评估：**极端恐慌状态**，系统性风险正在集中释放。建议清仓回避，保留现金等待右侧信号。历史上此类低分区间往往是阶段性底部附近，但左侧进场风险极大，请耐心等待市场企稳确认（建议配合连续能量指数从零开始回升再介入）。')

    return '\n'.join(L)


# ═══════════════════════════════════════════════════════════════════
#  连续能量指数计算（Dynamic Anomaly Resonance + Energy Index v3）
# ═══════════════════════════════════════════════════════════════════

def _extract_energy_inputs(result_dict: dict) -> dict:
    """从单日情绪评分结果中提取连续能量指数所需的全部指标。"""
    m1 = result_dict.get('module1') or {}
    m2 = result_dict.get('module2') or {}
    m3 = result_dict.get('module3') or {}
    m5 = result_dict.get('module5') or {}
    m6 = result_dict.get('module6') or {}
    m7 = result_dict.get('module7') or {}
    m8 = result_dict.get('module8') or {}
    return {
        'trading_day': result_dict.get('trading_day'),
        'total_score': result_dict.get('total_score'),
        'm1_mfr': m1.get('money_flow_ratio'),
        'm2_turnover': m2.get('turnover_today'),
        'm3_sigma': m3.get('sigma_weighted'),
        'm5_spread': m5.get('spread_size'),
        'm6_hot': m6.get('hot_avg_return'),
        'm6_nuke': m6.get('nuke_ratio'),
        'm7_smart': m7.get('smart_distrib_ratio'),
        'm8_strong': m8.get('strong_avg_return'),
    }


def compute_energy_index_series(df_hist: pd.DataFrame) -> pd.DataFrame:
    """
    基于历史截面结果，计算连续能量指数（Continuous Energy Index）。

    模型架构（异常共振 + 能量耗散）:
      Phase 1 — Z-Score 异常判定: M1/M2/M6 的 252 日滚动 Z-Score 离群
      Phase 2 — 系统收敛度: 8 维指标间 C(n,2) 对滚动相关性的 Z-Score 突变检测
      Phase 3 — 冰点重置: 4 个条件任意满足 3 个 → 能量归零
      Phase 4 — 能量累加: 动态中性线 + 自适应对称衰减 + 非对称恐惧动量

    输入列:
      - trading_day, total_score (必须)
      - m1_mfr, m2_turnover, m6_nuke (Z-Score 异常判定)
      - m3_sigma, m5_spread, m6_hot, m7_smart, m8_strong (系统收敛度)

    输出列（追加）:
      - energy_index: 连续能量指数 S_t
      - energy_reset: 是否为冰点重置日
    """
    if df_hist is None or df_hist.empty:
        return df_hist

    df = df_hist.copy()
    if 'trading_day' in df.columns:
        df = df.sort_values('trading_day')
        df['trading_day'] = pd.to_datetime(df['trading_day'])
    else:
        df = df.sort_index()

    all_numeric = ['total_score', 'm1_mfr', 'm2_turnover', 'm3_sigma',
                   'm5_spread', 'm6_nuke', 'm6_hot', 'm7_smart', 'm8_strong']
    for col in all_numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'total_score' not in df.columns or df['total_score'].notna().sum() < ENERGY_MIN_HISTORY:
        df['energy_index'] = 0.0
        df['energy_reset'] = False
        return df

    if 'trading_day' in df.columns:
        df = df.set_index('trading_day')

    # ── Phase 1: Z-Score 异常判定 (M1, M2, M6) ──────────────────
    def _zscore(col: str) -> pd.Series:
        if col not in df.columns:
            return pd.Series(np.nan, index=df.index)
        x = df[col]
        roll_mean = x.rolling(ENERGY_LOOKBACK_DAYS, min_periods=ENERGY_MIN_HISTORY).mean().shift(1)
        roll_std = x.rolling(ENERGY_LOOKBACK_DAYS, min_periods=ENERGY_MIN_HISTORY).std().shift(1)
        return (x - roll_mean) / roll_std.replace(0, np.nan)

    z_m1 = _zscore('m1_mfr')
    z_m2 = _zscore('m2_turnover')
    z_m6 = _zscore('m6_nuke')

    cond_m1 = z_m1 < -ENERGY_Z_THRESH
    cond_m2 = z_m2 < -ENERGY_Z_THRESH
    cond_m6 = z_m6 > ENERGY_Z_THRESH

    anomaly_count = (cond_m1.fillna(False).astype(int) +
                     cond_m2.fillna(False).astype(int) +
                     cond_m6.fillna(False).astype(int))

    # ── Phase 2: 系统收敛度 (8 维滚动相关性 Z-Score 突变) ────────
    # 统一方向: lower = 市场更差
    conv_sign = {
        'm1_mfr': 1,       # 低 → 空头占优
        'm2_turnover': 1,  # 低 → 流动性枯竭
        'm3_sigma': 1,     # 低 → 同涨同跌 / 因子失效
        'm5_spread': 1,    # 负 → 小盘被抽血
        'm6_hot': 1,       # 负 → 赚钱效应差
        'm6_nuke': -1,     # 高 → 核按钮多, 取负
        'm7_smart': -1,    # 高 → 主力派发, 取负
        'm8_strong': 1,    # 负 → 强势股补跌
    }
    conv_data = {}
    for col, sign in conv_sign.items():
        if col in df.columns and df[col].notna().sum() > ENERGY_MIN_HISTORY:
            conv_data[col] = _zscore(col) * sign

    sys_reson = pd.Series(False, index=df.index)
    if len(conv_data) >= 3:
        df_z = pd.DataFrame(conv_data)
        cols = df_z.columns.tolist()
        pair_corrs = []
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                pc = df_z[cols[i]].rolling(
                    ENERGY_CORR_WINDOW, min_periods=10
                ).corr(df_z[cols[j]])
                pair_corrs.append(pc)
        if pair_corrs:
            mean_corr = pd.concat(pair_corrs, axis=1).mean(axis=1)
            # 用相关性自身的 Z-Score 判定"突然升高" → 非硬编码
            corr_rm = mean_corr.rolling(
                ENERGY_LOOKBACK_DAYS, min_periods=ENERGY_MIN_HISTORY
            ).mean().shift(1)
            corr_rs = mean_corr.rolling(
                ENERGY_LOOKBACK_DAYS, min_periods=ENERGY_MIN_HISTORY
            ).std().shift(1)
            corr_z = (mean_corr - corr_rm) / corr_rs.replace(0, np.nan)
            sys_reson = (corr_z > ENERGY_Z_THRESH).fillna(False)

    # ── Phase 3: 冰点判定 — 4 个条件满足任意 3 个 ────────────────
    is_anomaly = ((anomaly_count >= 3) |
                  (sys_reson & (anomaly_count >= 2)))

    # ── Phase 4: 连续能量累加 ────────────────────────────────────
    scores = df['total_score'].ffill()
    roll_neutral = scores.rolling(
        ENERGY_ROLL_MEAN_DAYS,
        min_periods=max(10, ENERGY_ROLL_MEAN_DAYS // 2)
    ).mean().shift(1)
    # 历史不足时用扩展均值兜底（不使用全局前视均值）
    roll_neutral = roll_neutral.fillna(
        scores.expanding(min_periods=1).mean().shift(1)
    )

    energy = []
    reset_flags = []
    s_prev = 0.0
    in_reset = False

    for day in scores.index:
        score_today = scores.loc[day]
        neutral = roll_neutral.loc[day]
        if pd.isna(neutral):
            neutral = score_today
        delta = score_today - neutral

        if delta < 0:
            delta *= ENERGY_NEG_WEIGHT

        reset_today = False
        anomaly_val = is_anomaly.loc[day] if day in is_anomaly.index else False
        if pd.notna(anomaly_val) and anomaly_val:
            s_t = 0.0
            in_reset = True
            reset_today = True
        else:
            if in_reset:
                s_prev = 0.0
                in_reset = False
            # 对称衰减: |S| 越大 → 衰减越强, 正负都被拉向零点
            decay = ENERGY_BASE_DECAY * (1.0 + abs(s_prev) / ENERGY_CAP)
            decay = min(decay, 0.95)
            s_t = s_prev * (1.0 - decay) + delta

        energy.append(s_t)
        reset_flags.append(reset_today)
        s_prev = s_t

    df['energy_index'] = energy
    df['energy_reset'] = reset_flags
    df = df.reset_index()
    return df


# ═══════════════════════════════════════════════════════════════════
#  连续能量指数：数据调度与闭环整合 (全量刷新/增量更新)
# ═══════════════════════════════════════════════════════════════════

def _save_energy_to_db(df_energy: pd.DataFrame, db_path: str = 'database.db'):
    """将连续能量指数持久化到 SQLite。"""
    if df_energy is None or df_energy.empty:
        return
    import sqlite3 as _sq
    try:
        conn = _sq.connect(db_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS energy_index_results (
                trading_day TEXT PRIMARY KEY,
                total_score REAL,
                energy_index REAL,
                energy_reset BOOLEAN
            )
        ''')
        df_s = df_energy[['trading_day', 'total_score', 'energy_index', 'energy_reset']].copy()
        if pd.api.types.is_datetime64_any_dtype(df_s['trading_day']):
            df_s['trading_day'] = df_s['trading_day'].dt.strftime('%Y-%m-%d')
        df_s['energy_reset'] = df_s['energy_reset'].astype(int)
        rows = [(r['trading_day'], r['total_score'], r['energy_index'], r['energy_reset'])
                for r in df_s.to_dict('records')]
        conn.executemany(
            "INSERT OR REPLACE INTO energy_index_results "
            "(trading_day, total_score, energy_index, energy_reset) VALUES (?, ?, ?, ?)",
            rows
        )
        conn.commit()
        conn.close()
        logger.info(f"成功保存 {len(rows)} 条连续能量指数数据到数据库。")
    except Exception as e:
        logger.error(f"保存连续能量指数失败: {e}", exc_info=True)


def update_and_sync_energy_index(db_path: str = 'database.db') -> pd.DataFrame:
    """
    从数据库读取全部历史截面结果，提取 8 维指标，
    计算连续能量指数并持久化。
    """
    import sqlite3 as _sq
    import json as _jl
    logger.info("开始同步计算连续能量指数 (Continuous Energy Index)...")
    try:
        conn = _sq.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT trading_day, result_json FROM market_sentiment_results ORDER BY trading_day ASC")
        rows = c.fetchall()
        conn.close()
        if not rows:
            logger.warning("数据库中没有市场情绪历史数据，无法计算连续能量指数。")
            return pd.DataFrame()

        records = []
        for td, json_str in rows:
            try:
                res = _jl.loads(json_str)
                records.append(_extract_energy_inputs(res))
            except Exception as parse_e:
                logger.error(f"解析 {td} 数据异常: {parse_e}")

        df_hist = pd.DataFrame(records)
        fill_cols = [c for c in ['m1_mfr', 'm2_turnover', 'm3_sigma', 'm5_spread',
                                  'm6_hot', 'm6_nuke', 'm7_smart', 'm8_strong']
                     if c in df_hist.columns]
        if fill_cols:
            df_hist[fill_cols] = df_hist[fill_cols].ffill().fillna(0)

        df_energy = compute_energy_index_series(df_hist)
        _save_energy_to_db(df_energy, db_path)
        logger.info(f"连续能量指数计算完成，成功更新 {len(df_energy)} 天的数据。")
        return df_energy
    except Exception as e:
        logger.error(f"连续能量指数闭环计算失败: {e}", exc_info=True)
        return pd.DataFrame()


def _compute_energy_map_from_db(db_path: str = 'database.db') -> dict:
    """读取全量历史并返回 {trading_day: (energy_index, energy_reset)} 映射。"""
    import sqlite3 as _sq
    import json as _jl
    try:
        conn = _sq.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT trading_day, result_json FROM market_sentiment_results ORDER BY trading_day ASC")
        rows = c.fetchall()
        conn.close()
        if not rows:
            return {}
        # 逐条容错解析，单条 JSON 损坏不会中断整体计算
        records = []
        for _td_raw, rj in rows:
            if not rj:
                continue
            try:
                records.append(_extract_energy_inputs(_jl.loads(rj)))
            except Exception as _pe:
                logger.warning(f"_compute_energy_map_from_db: 跳过损坏记录 {_td_raw}: {_pe}")
        if len(records) < ENERGY_MIN_HISTORY:
            logger.info(f"能量指数计算：记录数 {len(records)} < 最小值 {ENERGY_MIN_HISTORY}，跳过")
            return {}
        df_e = compute_energy_index_series(pd.DataFrame(records))
        if df_e is None or df_e.empty:
            logger.warning("compute_energy_index_series 返回空 DataFrame")
            return {}
        _save_energy_to_db(df_e, db_path)
        result = {}
        for _, row in df_e.iterrows():
            td = str(row['trading_day'])[:10]
            result[td] = (
                float(row.get('energy_index', 0.0) or 0.0),
                bool(row.get('energy_reset', False)),
            )
        logger.info(f"能量映射计算完成，共 {len(result)} 天")
        return result
    except Exception as e:
        logger.error(f"计算能量映射失败: {e}", exc_info=True)
        return {}