#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
时间配置模块 - 支持指定分析基期
"""

import datetime
from datetime import timedelta
import pandas as pd

class TimeConfig:
    """时间配置类"""
    
    def __init__(self, base_date=None, analysis_period='latest'):
        """
        初始化时间配置
        Args:
            base_date: 分析基期，格式为 'YYYY-MM-DD' 或 datetime对象
            analysis_period: 分析期间，'latest'表示最新，'historical'表示历史
        """
        self.analysis_period = analysis_period
        
        if base_date is None:
            # 默认使用最新数据
            self.base_date = datetime.datetime.now().date()
            self.is_latest = True
        else:
            if isinstance(base_date, str):
                self.base_date = datetime.datetime.strptime(base_date, '%Y-%m-%d').date()
            else:
                self.base_date = base_date
            self.is_latest = False
        
        # 计算相关时间点
        self._calculate_time_points()
    
    def _calculate_time_points(self):
        """计算相关时间点"""
        # 分析基期
        self.analysis_date = self.base_date
        
        # 大跌筛选相关时间点
        self.crash_start_date = self.analysis_date - timedelta(days=700)  # 3年前
        self.crash_end_date = self.analysis_date
        
        # 基本面分析相关时间点
        self.fundamental_start_date = self.analysis_date - timedelta(days=365*5)  # 5年前
        self.fundamental_end_date = self.analysis_date
        
        # 相关性分析相关时间点
        self.correlation_start_date = self.analysis_date - timedelta(days=365*2)  # 2年前
        self.correlation_end_date = self.analysis_date + timedelta(days=365)  # 1年后
        
        # 数据截止时间（用于筛选活跃股票）
        self.data_cutoff_date = self.analysis_date - timedelta(days=30)  # 30天前
    
    def get_crash_filter_config(self):
        """获取大跌筛选配置"""
        return {
            'start_date': self.crash_start_date,
            'end_date': self.crash_end_date,
            'analysis_date': self.analysis_date,
            'max_days_ago': 30  # 最大允许的行情日期滞后天数
        }
    
    def get_fundamental_config(self):
        """获取基本面分析配置"""
        return {
            'start_date': self.fundamental_start_date,
            'end_date': self.fundamental_end_date,
            'analysis_date': self.analysis_date,
            'years': 5  # 分析年数
        }
    
    def get_correlation_config(self):
        """获取相关性分析配置"""
        return {
            'start_date': self.correlation_start_date,
            'end_date': self.correlation_end_date,
            'analysis_date': self.analysis_date,
            'periods': 4  # 分析周期数（季度）
        }
    
    def get_expectation_config(self):
        """获取业绩预期分析配置"""
        # 业绩预期分析需要前瞻性数据，时间范围应该包含：
        # 1. 历史数据：用于对比分析（过去3个月）
        # 2. 当前数据：分析基期
        # 3. 未来数据：预期数据（最多1个月，避免查询过多未来数据）
        # 优化：缩短时间范围以提高查询效率
        from datetime import datetime
        current_date = datetime.now().date()
        
        expectation_start_date = self.analysis_date - timedelta(days=90)   # 3个月前
        # 确保不查询未来数据，最多查询到当前日期
        # 如果分析基期是历史日期，则end_date就是分析基期本身
        if self.analysis_date < current_date:
            expectation_end_date = self.analysis_date  # 历史分析：不查询未来数据
        else:
            expectation_end_date = min(self.analysis_date + timedelta(days=30), current_date)  # 最多1个月后，但不超当前日期
        
        return {
            'start_date': expectation_start_date,
            'end_date': expectation_end_date,
            'analysis_date': self.analysis_date,
            'historical_period': 90,   # 历史数据天数（从365天缩短到90天）
            'forward_period': 30,      # 前瞻数据天数（从90天缩短到30天）
            'quarterly_periods': 4     # 季度数据周期数
        }
    
    def get_momentum_config(self):
        """获取动量指标分析配置"""
        # 动量指标主要基于技术分析，需要较短期的历史数据
        momentum_start_date = self.analysis_date - timedelta(days=180)  # 6个月前
        momentum_end_date = self.analysis_date + timedelta(days=30)     # 1个月后
        
        return {
            'start_date': momentum_start_date,
            'end_date': momentum_end_date,
            'analysis_date': self.analysis_date,
            'short_term': 30,    # 短期（30天）
            'medium_term': 90,   # 中期（90天）
            'long_term': 180     # 长期（180天）
        }
    
    def get_output_suffix(self):
        """获取输出文件后缀"""
        if self.is_latest:
            return "最新"
        else:
            return f"{self.base_date.strftime('%Y%m%d')}"
    
    def print_config(self):
        """打印时间配置"""
        print("=" * 60)
        print("时间配置信息")
        print("=" * 60)
        print(f"分析基期: {self.analysis_date}")
        print(f"分析类型: {'最新数据' if self.is_latest else '历史回测'}")
        print()
        print("大跌筛选时间范围:")
        print(f"  开始日期: {self.crash_start_date}")
        print(f"  结束日期: {self.crash_end_date}")
        print()
        print("基本面分析时间范围:")
        print(f"  开始日期: {self.fundamental_start_date}")
        print(f"  结束日期: {self.fundamental_end_date}")
        print()
        print("相关性分析时间范围:")
        print(f"  开始日期: {self.correlation_start_date}")
        print(f"  结束日期: {self.correlation_end_date}")
        print()
        print("数据截止时间:")
        print(f"  活跃股票截止: {self.data_cutoff_date}")
        print("=" * 60)


def create_time_config(base_date=None, analysis_period='latest'):
    """
    创建时间配置
    Args:
        base_date: 分析基期，可以是字符串 'YYYY-MM-DD' 或 datetime对象
        analysis_period: 分析期间
    Returns:
        TimeConfig对象
    """
    return TimeConfig(base_date, analysis_period)


def get_common_analysis_dates():
    """
    获取常用的分析日期
    Returns:
        dict: 常用分析日期字典
    """
    today = datetime.datetime.now().date()
    
    return {
        '最新': None,  # 使用最新数据
        '2024年底': '2024-12-31',
        '2024年中': '2024-06-30',
        '2023年底': '2023-12-31',
        '2023年中': '2023-06-30',
        '2022年底': '2022-12-31',
        '2022年中': '2022-06-30',
        '2021年底': '2021-12-31',
        '2021年中': '2021-06-30',
        '2020年底': '2020-12-31',
        '2020年中': '2020-06-30',
        '2019年底': '2019-12-31',
        '2019年中': '2019-06-30',
    }


def select_analysis_date():
    """
    交互式选择分析日期
    Returns:
        TimeConfig对象
    """
    print("请选择分析基期:")
    print("=" * 40)
    
    common_dates = get_common_analysis_dates()
    
    for i, (name, date) in enumerate(common_dates.items(), 1):
        if date is None:
            print(f"{i:2d}. {name} (使用最新数据)")
        else:
            print(f"{i:2d}. {name} ({date})")
    
    print("0. 自定义日期")
    print("=" * 40)
    
    while True:
        try:
            choice = input("请输入选择 (0-12): ").strip()
            
            if choice == '0':
                # 自定义日期
                custom_date = input("请输入自定义日期 (格式: YYYY-MM-DD): ").strip()
                try:
                    datetime.datetime.strptime(custom_date, '%Y-%m-%d')
                    return create_time_config(custom_date)
                except ValueError:
                    print("日期格式错误，请使用 YYYY-MM-DD 格式")
                    continue
            
            choice_num = int(choice)
            if 1 <= choice_num <= len(common_dates):
                date_name = list(common_dates.keys())[choice_num - 1]
                date_value = list(common_dates.values())[choice_num - 1]
                
                config = create_time_config(date_value)
                print(f"\n已选择: {date_name}")
                config.print_config()
                return config
            else:
                print("选择无效，请重新输入")
                
        except ValueError:
            print("输入无效，请输入数字")
        except KeyboardInterrupt:
            print("\n用户取消选择")
            return None


if __name__ == "__main__":
    # 测试时间配置
    print("时间配置模块测试")
    print("=" * 60)
    
    # 测试最新数据
    print("1. 测试最新数据配置:")
    config1 = create_time_config()
    config1.print_config()
    
    print("\n" + "=" * 60)
    
    # 测试历史数据
    print("2. 测试历史数据配置:")
    config2 = create_time_config('2023-12-31')
    config2.print_config()
    
    print("\n" + "=" * 60)
    
    # 测试交互式选择
    print("3. 测试交互式选择:")
    config3 = select_analysis_date()
    if config3:
        print("选择完成")
    else:
        print("未选择") 