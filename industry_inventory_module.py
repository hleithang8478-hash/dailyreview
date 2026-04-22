#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
行业库存分析模块 - 公共模块
用于网站中的行业库存分析功能

功能：
1. 获取股票或行业的数据
2. 处理库存、营收、利润数据
3. 生成分析结果（DataFrame格式，用于前端展示）
"""

import sys
import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# 优先从本地目录（ReviewsDaily）导入模块
# 所有依赖文件应该已经复制到 ReviewsDaily 目录中
current_file = Path(__file__).resolve()
reviews_daily_dir = current_file.parent  # ReviewsDaily 目录

logger.info(f"ReviewsDaily目录: {reviews_daily_dir}")

# 优先添加当前目录（ReviewsDaily）到路径，因为所有模块都在这里
if str(reviews_daily_dir) not in sys.path:
    sys.path.insert(0, str(reviews_daily_dir))
    logger.info(f"已添加当前目录到路径: {reviews_daily_dir}")

# 如果本地目录没有，再尝试从其他位置查找（向后兼容）
project_root = None
if not (reviews_daily_dir / 'data_fetcher.py').exists():
    logger.warning(f"本地目录未找到 data_fetcher.py，尝试从其他位置查找...")
    
    # 可能的项目根目录位置（按优先级排序）
    possible_roots = [
        Path(r'e:\开发\郑玲'),  # 原始项目根目录
        reviews_daily_dir.parent,  # Desktop
        Path(r'C:\Users\Administrator\Desktop\开发\郑玲'),  # 可能的新位置
        Path(r'C:\Users\tanjiarong\Desktop\开发\郑玲'),  # 另一个可能的位置
    ]
    
    # 尝试找到包含 data_fetcher.py 的目录
    for root in possible_roots:
        root_path = Path(root)
        if root_path.exists() and (root_path / 'data_fetcher.py').exists():
            project_root = root_path
            logger.info(f"在其他位置找到项目根目录: {project_root}")
            break
    
    # 如果还没找到，尝试从配置文件读取
    if project_root is None:
        config_file = reviews_daily_dir / 'project_root.txt'
        if config_file.exists():
            try:
                project_root = Path(config_file.read_text(encoding='utf-8').strip())
                if project_root.exists() and (project_root / 'data_fetcher.py').exists():
                    logger.info(f"从配置文件读取项目根目录: {project_root}")
                else:
                    logger.warning(f"配置文件中指定的项目根目录不存在或缺少模块: {project_root}")
                    project_root = None
            except Exception as e:
                logger.warning(f"读取配置文件失败: {e}")
                project_root = None
    
    # 如果还是没找到，尝试遍历父目录查找
    if project_root is None:
        current = reviews_daily_dir
        max_levels = 5
        for level in range(max_levels):
            if (current / 'data_fetcher.py').exists() and (current / 'industry_inventory_analysis.py').exists():
                project_root = current
                logger.info(f"在父目录第{level}层找到项目根目录: {project_root}")
                break
            parent = current.parent
            if parent == current:  # 到达根目录
                break
            current = parent
    
    # 将项目根目录添加到路径（如果有的话）
    if project_root:
        project_root = project_root.resolve()
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
            logger.info(f"已将项目根目录添加到路径: {project_root}")
else:
    logger.info(f"✓ 在本地目录找到模块文件，将优先使用本地模块")

# 尝试导入模块
try:
    from data_fetcher import JuyuanDataFetcher
    from industry_inventory_analysis import IndustryInventoryAnalyzer
    logger.info("成功导入数据获取模块")
    print("✓ 成功导入数据获取模块")
except ImportError as e:
    error_msg = f"无法导入数据获取模块: {e}\n"
    error_msg += f"ReviewsDaily目录: {reviews_daily_dir}\n"
    if project_root:
        error_msg += f"项目根目录: {project_root}\n"
    else:
        error_msg += "未找到项目根目录\n"
        error_msg += "\n解决方案：\n"
        error_msg += "1. 在 ReviewsDaily 目录下创建 project_root.txt 文件\n"
        error_msg += "2. 文件内容填写项目根目录的绝对路径（包含 data_fetcher.py 的目录）\n"
        error_msg += "   例如：e:\\开发\\郑玲\n"
        error_msg += "   或者：C:\\path\\to\\your\\project\n"
    logger.error(error_msg)
    print(f"✗ {error_msg}")
    JuyuanDataFetcher = None
    IndustryInventoryAnalyzer = None

import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from datetime import date, datetime

logger = logging.getLogger(__name__)


class IndustryInventoryWebModule:
    """行业库存分析Web模块"""
    
    def __init__(self):
        """初始化模块"""
        if JuyuanDataFetcher is None or IndustryInventoryAnalyzer is None:
            raise ImportError("无法导入数据获取模块，请检查依赖")
        
        try:
            # 初始化数据获取器
            self.fetcher = JuyuanDataFetcher(use_connection_pool=True, lazy_init_pool=True)
            # 初始化分析器
            self.analyzer = IndustryInventoryAnalyzer(self.fetcher)
            logger.info("行业库存分析模块初始化成功")
        except Exception as e:
            logger.error(f"初始化失败: {e}", exc_info=True)
            raise
    
    def get_stock_list_by_code_or_name(self, query: str) -> List[Dict]:
        """
        根据股票代码或名称搜索股票
        
        参数:
        - query: 搜索关键词（股票代码或名称）
        
        返回:
        - List[Dict]: 股票列表，每个元素包含 {code, name, company_code, industry}
        """
        try:
            if not query or len(query.strip()) == 0:
                return []
            
            query = query.strip()
            
            # 构建SQL查询
            # 支持模糊匹配股票代码、股票名称
            # 注意：SQL Server 使用 TOP 而不是 LIMIT
            sql = f"""
            SELECT DISTINCT TOP 50
                s.SecuCode,
                s.SecuAbbr,
                s.CompanyCode,
                i.ThirdIndustryName,
                i.SecondIndustryName,
                i.FirstIndustryName
            FROM SecuMain s
            LEFT JOIN LC_ExgIndustry i ON s.CompanyCode = i.CompanyCode
                AND i.Standard = '38'
                AND i.IfPerformed = 1
            WHERE s.SecuCategory = 1
            AND (
                s.SecuCode LIKE '%{query}%'
                OR s.SecuAbbr LIKE '%{query}%'
            )
            ORDER BY s.SecuCode
            """
            
            df = self.fetcher.query(sql)
            
            result = []
            for _, row in df.iterrows():
                result.append({
                    'code': row.get('SecuCode', ''),
                    'name': row.get('SecuAbbr', ''),
                    'company_code': int(row.get('CompanyCode', 0)),
                    'third_industry': row.get('ThirdIndustryName', ''),
                    'second_industry': row.get('SecondIndustryName', ''),
                    'first_industry': row.get('FirstIndustryName', '')
                })
            
            # Web场景下减少日志输出
            if len(result) > 0:
                logger.debug(f"搜索 '{query}' 找到 {len(result)} 只股票")
            return result
            
        except Exception as e:
            logger.error(f"搜索股票失败: {e}", exc_info=True)
            return []
    
    def get_industry_list(self) -> List[str]:
        """
        获取所有三级行业列表
        
        返回:
        - List[str]: 行业名称列表
        """
        try:
            sql = """
            SELECT DISTINCT ThirdIndustryName
            FROM LC_ExgIndustry
            WHERE Standard = '38'
            AND IfPerformed = 1
            AND ThirdIndustryName IS NOT NULL
            AND ThirdIndustryName != ''
            ORDER BY ThirdIndustryName
            """
            
            df = self.fetcher.query(sql)
            industries = df['ThirdIndustryName'].unique().tolist()
            
            # Web场景下减少日志输出
            logger.debug(f"获取到 {len(industries)} 个三级行业")
            return industries
            
        except Exception as e:
            logger.error(f"获取行业列表失败: {e}", exc_info=True)
            return []
    
    def analyze_by_stock_code(self, stock_code: str, years: int = 10) -> Dict:
        """
        根据股票代码分析（Web优化版本：只分析该股票所属的行业）
        
        参数:
        - stock_code: 股票代码
        - years: 分析年限
        
        返回:
        - Dict: 包含分析结果的字典
        """
        try:
            # 获取股票信息
            stocks = self.get_stock_list_by_code_or_name(stock_code)
            if not stocks:
                return {'success': False, 'error': f'未找到股票代码: {stock_code}'}
            
            stock = stocks[0]  # 取第一个匹配的股票
            company_code = stock['company_code']
            
            # 获取该股票所属的所有行业
            sql = """
            SELECT DISTINCT ThirdIndustryName
            FROM LC_ExgIndustry
            WHERE CompanyCode = ?
            AND Standard = '38'
            AND IfPerformed = 1
            AND ThirdIndustryName IS NOT NULL
            """
            
            industries_df = self.fetcher.query(sql, (company_code,))
            industries = industries_df['ThirdIndustryName'].unique().tolist()
            
            if not industries:
                return {'success': False, 'error': f'股票 {stock_code} 未找到所属行业'}
            
            # ⚠️ 优化：只获取该股票所属行业的股票列表，而不是全部行业
            # 这样可以减少数据量，提高分析速度
            industry_stocks = self.analyzer.get_industry_stock_list()
            if industry_stocks.empty:
                return {'success': False, 'error': '未获取到行业股票列表'}
            
            # 筛选出该股票所属行业的股票
            industry_stocks_filtered = industry_stocks[
                industry_stocks['ThirdIndustryName'].isin(industries)
            ]
            
            if industry_stocks_filtered.empty:
                return {'success': False, 'error': f'未找到行业 {", ".join(industries)} 的股票数据'}
            
            # 获取这些行业的公司代码
            company_codes = industry_stocks_filtered['CompanyCode'].unique().tolist()
            
            # Web场景下减少日志输出
            logger.debug(f"股票 {stock_code} 所属行业: {', '.join(industries)}，共 {len(company_codes)} 家公司")
            
            # 只获取这些公司的库存和营收数据（静默模式）
            inventory_data = self.analyzer.get_inventory_data(company_codes, years=years, silent=True)
            if inventory_data.empty:
                # 添加更详细的错误信息
                logger.warning(f"未获取到库存数据 - 公司代码数量: {len(company_codes)}，公司代码示例: {company_codes[:5] if company_codes else '无'}")
                # 尝试检查是否是查询条件太严格导致没有数据
                return {
                    'success': False, 
                    'error': f'未获取到库存数据。可能原因：1) 所选行业公司代码 ({len(company_codes)} 家) 在指定时间范围内没有库存数据；2) 数据查询条件过于严格。请尝试减少分析年限或检查数据源。'
                }
            
            revenue_data = self.analyzer.get_revenue_data(company_codes, years=years, silent=True)
            
            # 合并数据并按行业汇总（使用静默模式，减少日志输出）
            result = self.analyzer.analyze_industry_inventory(
                years=years, 
                industry_stocks=industry_stocks_filtered,
                inventory_data=inventory_data,
                revenue_data=revenue_data,
                silent=True  # Web场景下静默模式
            )
            
            # 筛选出该股票所属行业的数据（确保只返回相关行业）
            if not result.empty and '三级行业' in result.columns:
                result = result[result['三级行业'].isin(industries)]
            
            # 转换为字典格式
            result_dict = self._format_result(result, stock_code=stock_code, stock_name=stock['name'])
            
            return result_dict
            
        except Exception as e:
            logger.error(f"分析股票 {stock_code} 失败: {e}", exc_info=True)
            return {'success': False, 'error': f'分析失败: {str(e)}'}
    
    def analyze_by_industry(self, industry_name: str, years: int = 10) -> Dict:
        """
        根据行业名称分析（Web优化版本：只分析指定行业）
        
        参数:
        - industry_name: 行业名称
        - years: 分析年限
        
        返回:
        - Dict: 包含分析结果的字典
        """
        try:
            # ⚠️ 优化：只获取指定行业的股票列表，而不是全部行业
            industry_stocks = self.analyzer.get_industry_stock_list()
            if industry_stocks.empty:
                return {'success': False, 'error': '未获取到行业股票列表'}
            
            # 筛选出指定行业的股票
            industry_stocks_filtered = industry_stocks[
                industry_stocks['ThirdIndustryName'] == industry_name
            ]
            
            if industry_stocks_filtered.empty:
                return {'success': False, 'error': f'未找到行业 {industry_name} 的股票数据'}
            
            # 获取该行业的公司代码
            company_codes = industry_stocks_filtered['CompanyCode'].unique().tolist()
            
            # Web场景下减少日志输出
            logger.debug(f"行业 {industry_name} 共 {len(company_codes)} 家公司")
            
            # 只获取这些公司的库存和营收数据
            inventory_data = self.analyzer.get_inventory_data(company_codes, years=years)
            if inventory_data.empty:
                # 添加更详细的错误信息
                logger.warning(f"未获取到库存数据 - 行业: {industry_name}，公司代码数量: {len(company_codes)}，公司代码示例: {company_codes[:5] if company_codes else '无'}")
                # 尝试检查是否是查询条件太严格导致没有数据
                return {
                    'success': False, 
                    'error': f'未获取到库存数据。可能原因：1) 行业"{industry_name}"的公司代码 ({len(company_codes)} 家) 在指定时间范围内没有库存数据；2) 数据查询条件过于严格。请尝试减少分析年限或检查数据源。'
                }
            
            revenue_data = self.analyzer.get_revenue_data(company_codes, years=years, silent=True)
            
            # 合并数据并按行业汇总（使用静默模式，减少日志输出）
            result = self.analyzer.analyze_industry_inventory(
                years=years,
                industry_stocks=industry_stocks_filtered,
                inventory_data=inventory_data,
                revenue_data=revenue_data,
                silent=True  # Web场景下静默模式
            )
            
            # 确保只返回指定行业的数据
            if not result.empty and '三级行业' in result.columns:
                result = result[result['三级行业'] == industry_name]
            
            # 转换为字典格式
            result_dict = self._format_result(result, industry_name=industry_name)
            
            return result_dict
            
        except Exception as e:
            logger.error(f"分析行业 {industry_name} 失败: {e}", exc_info=True)
            return {'success': False, 'error': f'分析失败: {str(e)}'}
    
    def _format_result(self, df: pd.DataFrame, stock_code: str = None, 
                      stock_name: str = None, industry_name: str = None) -> Dict:
        """
        将DataFrame格式化为字典格式，便于前端展示
        
        参数:
        - df: 分析结果DataFrame
        - stock_code: 股票代码（可选）
        - stock_name: 股票名称（可选）
        - industry_name: 行业名称（可选）
        
        返回:
        - Dict: 格式化后的结果
        """
        if df.empty:
            return {
                'success': False,
                'error': '未找到数据',
                'data': [],
                'summary': {}
            }
        
        # 转换为字典列表
        data = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                value = row[col]
                # 处理NaN值
                if pd.isna(value):
                    value = None
                # 处理日期
                elif isinstance(value, date):
                    value = value.strftime('%Y-%m-%d')
                # 处理数值
                elif isinstance(value, (int, float)):
                    if pd.isna(value):
                        value = None
                    else:
                        value = float(value)
                else:
                    value = str(value)
                
                record[col] = value
            
            data.append(record)
        
        # 计算汇总信息
        summary = {}
        if len(data) > 0:
            summary['total_records'] = len(data)
            summary['industries'] = list(df['三级行业'].unique()) if '三级行业' in df.columns else []
            summary['date_range'] = {
                'start': str(df['报告期'].min()) if '报告期' in df.columns else None,
                'end': str(df['报告期'].max()) if '报告期' in df.columns else None
            }
        
        result = {
            'success': True,
            'message': '分析成功',
            'data': data,
            'summary': summary,
            'query_info': {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'industry_name': industry_name
            }
        }
        
        return result
    
    def close(self):
        """关闭数据获取器"""
        try:
            if hasattr(self, 'fetcher') and self.fetcher:
                self.fetcher.close()
                logger.info("数据获取器已关闭")
        except Exception as e:
            logger.error(f"关闭数据获取器失败: {e}")


# 全局模块实例（单例模式）
_module_instance = None


def get_module() -> IndustryInventoryWebModule:
    """获取模块实例（单例模式）"""
    global _module_instance
    if _module_instance is None:
        _module_instance = IndustryInventoryWebModule()
    return _module_instance
