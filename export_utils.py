#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
导出工具模块
提供统一的文件路径管理和股票代码格式化功能

功能：
1. 统一导出路径：outputs/YYYYMMDD/
2. 股票代码格式化：深圳的加.SZ，上海的加.SH
3. 可复用的导出辅助函数

作者：AI Assistant
创建时间：2024年12月
"""

import os
import logging
from datetime import datetime, date
from typing import Optional, Union, List
import pandas as pd

logger = logging.getLogger(__name__)


def get_output_dir(base_dir: str = "outputs", target_date: Optional[Union[date, str, datetime]] = None) -> str:
    """
    获取输出目录路径（带日期文件夹）
    
    参数：
    - base_dir: 基础目录，默认为"outputs"
    - target_date: 目标日期，默认为None（使用当前日期）
                  可以是date对象、datetime对象或字符串（YYYYMMDD格式）
    
    返回：
    - 完整的输出目录路径，格式：base_dir/YYYYMMDD/
    
    示例：
    >>> get_output_dir()  # outputs/20241215/
    >>> get_output_dir("outputs", date(2024, 12, 15))  # outputs/20241215/
    >>> get_output_dir("outputs", "20241215")  # outputs/20241215/
    """
    # 确定日期
    if target_date is None:
        dir_date = datetime.now().date()
    elif isinstance(target_date, datetime):
        dir_date = target_date.date()
    elif isinstance(target_date, date):
        dir_date = target_date
    elif isinstance(target_date, str):
        # 支持YYYYMMDD格式
        if len(target_date) == 8 and target_date.isdigit():
            dir_date = datetime.strptime(target_date, "%Y%m%d").date()
        else:
            # 尝试其他常见格式
            try:
                dir_date = datetime.strptime(target_date, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"无法解析日期格式: {target_date}，使用当前日期")
                dir_date = datetime.now().date()
    else:
        logger.warning(f"不支持的日期类型: {type(target_date)}，使用当前日期")
        dir_date = datetime.now().date()
    
    # 构建目录路径
    date_str = dir_date.strftime("%Y%m%d")
    output_dir = os.path.join(base_dir, date_str)
    
    # 创建目录（如果不存在）
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            logger.debug(f"创建输出目录: {output_dir}")
        except Exception as e:
            logger.error(f"创建输出目录失败: {e}")
            # 如果创建失败，返回基础目录
            return base_dir
    
    return output_dir


def format_stock_code(code: Union[str, int]) -> str:
    """
    格式化股票代码，添加交易所后缀
    
    规则：
    - 深圳股票（00、30开头）：添加.SZ
    - 上海股票（60、68开头）：添加.SH
    - 北交所股票（8开头）：添加.BJ
    - 如果已有后缀，直接返回
    
    参数：
    - code: 股票代码（6位数字字符串或整数）
    
    返回：
    - 格式化后的股票代码（带后缀）
    
    示例：
    >>> format_stock_code("000001")  # "000001.SZ"
    >>> format_stock_code("600000")  # "600000.SH"
    >>> format_stock_code("688001")  # "688001.SH"
    >>> format_stock_code("830001")  # "830001.BJ"
    >>> format_stock_code("000001.SZ")  # "000001.SZ" (已有后缀)
    """
    if code is None:
        return ""
    
    # 转换为字符串并去除空格
    code_str = str(code).strip()
    
    # 如果为空，直接返回
    if not code_str:
        return code_str
    
    # 如果已经有后缀（包含点号），直接返回
    if '.' in code_str:
        return code_str
    
    # 确保代码是6位数字（前面补0）
    if code_str.isdigit():
        code_str = code_str.zfill(6)
    else:
        # 如果不是纯数字，返回原值
        logger.debug(f"股票代码不是纯数字: {code_str}")
        return code_str
    
    # 根据股票代码前缀添加后缀
    if code_str.startswith('00') or code_str.startswith('30'):
        # 深圳：00xxxx（主板、中小板）、30xxxx（创业板）
        return f"{code_str}.SZ"
    elif code_str.startswith('60') or code_str.startswith('68'):
        # 上海：60xxxx（主板）、68xxxx（科创板）
        return f"{code_str}.SH"
    elif code_str.startswith('8'):
        # 北交所：8xxxxx
        return f"{code_str}.BJ"
    else:
        # 未知交易所，保持原样
        logger.debug(f"未知交易所的股票代码: {code_str}")
        return code_str


def format_stock_code_in_df(df: pd.DataFrame, code_column: str = '股票代码', inplace: bool = False) -> pd.DataFrame:
    """
    在DataFrame中批量格式化股票代码列
    
    参数：
    - df: 包含股票代码列的DataFrame
    - code_column: 股票代码列名，默认为'股票代码'
    - inplace: 是否就地修改，默认为False（返回新的DataFrame）
    
    返回：
    - 格式化后的DataFrame（如果inplace=False）或None（如果inplace=True）
    
    示例：
    >>> df = pd.DataFrame({'股票代码': ['000001', '600000', '688001']})
    >>> df_formatted = format_stock_code_in_df(df)
    >>> print(df_formatted['股票代码'])
    # 0    000001.SZ
    # 1    600000.SH
    # 2    688001.SH
    """
    if df is None or df.empty:
        return df
    
    if code_column not in df.columns:
        logger.warning(f"DataFrame中不存在列: {code_column}")
        return df
    
    # 复制DataFrame（如果不需要就地修改）
    if not inplace:
        df = df.copy()
    
    # 格式化股票代码列
    df[code_column] = df[code_column].apply(format_stock_code)
    
    return df


def get_output_file_path(base_dir: str = "outputs", 
                        filename: str = "result.xlsx",
                        target_date: Optional[Union[date, str, datetime]] = None,
                        create_dir: bool = True) -> str:
    """
    获取输出文件的完整路径（带日期文件夹）
    
    参数：
    - base_dir: 基础目录，默认为"outputs"
    - filename: 文件名，默认为"result.xlsx"
    - target_date: 目标日期，默认为None（使用当前日期）
    - create_dir: 是否创建目录，默认为True
    
    返回：
    - 完整的输出文件路径
    
    示例：
    >>> get_output_file_path("outputs", "分析结果.xlsx")
    # outputs/20241215/分析结果.xlsx
    """
    if create_dir:
        output_dir = get_output_dir(base_dir, target_date)
    else:
        # 不创建目录，只计算路径
        if target_date is None:
            dir_date = datetime.now().date()
        elif isinstance(target_date, datetime):
            dir_date = target_date.date()
        elif isinstance(target_date, date):
            dir_date = target_date
        elif isinstance(target_date, str) and len(target_date) == 8 and target_date.isdigit():
            dir_date = datetime.strptime(target_date, "%Y%m%d").date()
        else:
            dir_date = datetime.now().date()
        
        date_str = dir_date.strftime("%Y%m%d")
        output_dir = os.path.join(base_dir, date_str)
    
    return os.path.join(output_dir, filename)


def add_timestamp_to_filename(filename: str, 
                              timestamp_format: str = "%Y%m%d_%H%M%S",
                              separator: str = "_") -> str:
    """
    在文件名中添加时间戳
    
    参数：
    - filename: 原始文件名（可以包含路径）
    - timestamp_format: 时间戳格式，默认为"%Y%m%d_%H%M%S"
    - separator: 分隔符，默认为"_"
    
    返回：
    - 添加时间戳后的文件名
    
    示例：
    >>> add_timestamp_to_filename("分析结果.xlsx")
    # 分析结果_20241215_143025.xlsx
    >>> add_timestamp_to_filename("outputs/分析结果.xlsx")
    # outputs/分析结果_20241215_143025.xlsx
    """
    # 分离目录和文件名
    dir_path = os.path.dirname(filename)
    base_name = os.path.basename(filename)
    
    # 分离文件名和扩展名
    name, ext = os.path.splitext(base_name)
    
    # 生成时间戳
    timestamp = datetime.now().strftime(timestamp_format)
    
    # 组合新的文件名
    new_base_name = f"{name}{separator}{timestamp}{ext}"
    
    # 如果原文件名包含路径，保持路径
    if dir_path:
        return os.path.join(dir_path, new_base_name)
    else:
        return new_base_name


def format_stock_codes_in_columns(df: pd.DataFrame, 
                                  code_columns: Optional[List[str]] = None,
                                  inplace: bool = False) -> pd.DataFrame:
    """
    在DataFrame中格式化多个股票代码列
    
    参数：
    - df: DataFrame
    - code_columns: 股票代码列名列表，默认为None（自动查找包含"代码"的列）
    - inplace: 是否就地修改
    
    返回：
    - 格式化后的DataFrame
    """
    if df is None or df.empty:
        return df
    
    # 如果没有指定列，自动查找包含"代码"的列
    if code_columns is None:
        code_columns = [col for col in df.columns if '代码' in str(col)]
    
    if not code_columns:
        logger.debug("未找到股票代码列")
        return df
    
    # 复制DataFrame（如果不需要就地修改）
    if not inplace:
        df = df.copy()
    
    # 格式化每个代码列
    for col in code_columns:
        if col in df.columns:
            df[col] = df[col].apply(format_stock_code)
        else:
            logger.warning(f"列 {col} 不存在于DataFrame中")
    
    return df


# 便捷函数：一键获取带时间戳的输出文件路径
def get_timestamped_output_path(base_dir: str = "outputs",
                                base_filename: str = "result.xlsx",
                                target_date: Optional[Union[date, str, datetime]] = None,
                                add_timestamp: bool = True) -> str:
    """
    获取带时间戳的输出文件路径（带日期文件夹）
    
    参数：
    - base_dir: 基础目录
    - base_filename: 基础文件名
    - target_date: 目标日期（用于创建日期文件夹）
    - add_timestamp: 是否在文件名中添加时间戳
    
    返回：
    - 完整的输出文件路径
    
    示例：
    >>> get_timestamped_output_path("outputs", "分析结果.xlsx")
    # outputs/20241215/分析结果_20241215_143025.xlsx
    """
    if add_timestamp:
        filename = add_timestamp_to_filename(base_filename)
    else:
        filename = base_filename
    
    return get_output_file_path(base_dir, filename, target_date, create_dir=True)


if __name__ == '__main__':
    # 测试代码
    print("=" * 60)
    print("导出工具模块测试")
    print("=" * 60)
    
    # 测试1: 获取输出目录
    print("\n1. 测试获取输出目录")
    output_dir = get_output_dir()
    print(f"输出目录: {output_dir}")
    
    output_dir2 = get_output_dir("outputs", "20241215")
    print(f"指定日期输出目录: {output_dir2}")
    
    # 测试2: 格式化股票代码
    print("\n2. 测试格式化股票代码")
    test_codes = ["000001", "600000", "688001", "830001", "000001.SZ", 600036, None, ""]
    for code in test_codes:
        formatted = format_stock_code(code)
        print(f"  {code} -> {formatted}")
    
    # 测试3: DataFrame中格式化股票代码
    print("\n3. 测试DataFrame中格式化股票代码")
    df = pd.DataFrame({
        '股票代码': ['000001', '600000', '688001', '830001'],
        '其他字段': [1, 2, 3, 4]
    })
    print("原始DataFrame:")
    print(df)
    
    df_formatted = format_stock_code_in_df(df)
    print("\n格式化后DataFrame:")
    print(df_formatted)
    
    # 测试4: 获取输出文件路径
    print("\n4. 测试获取输出文件路径")
    file_path = get_output_file_path("outputs", "测试结果.xlsx")
    print(f"输出文件路径: {file_path}")
    
    # 测试5: 添加时间戳到文件名
    print("\n5. 测试添加时间戳到文件名")
    filename = add_timestamp_to_filename("分析结果.xlsx")
    print(f"添加时间戳后: {filename}")
    
    # 测试6: 获取带时间戳的输出路径
    print("\n6. 测试获取带时间戳的输出路径")
    timestamped_path = get_timestamped_output_path("outputs", "分析结果.xlsx")
    print(f"带时间戳的输出路径: {timestamped_path}")

