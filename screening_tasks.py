#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选任务执行模块

功能：封装功能12-15的执行逻辑，支持手工和自动执行
结果保存到outputs目录，执行历史记录到数据库
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, date
from typing import Dict, Optional, Any
import traceback

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from uptrend_rebound_analysis import run_uptrend_rebound_analysis
from momentum_screening_analysis import run_momentum_screening
from near_lowest_price_analysis import run_near_lowest_price_screening
from oscillation_stock_analysis import run_oscillation_stock_analysis

logger = logging.getLogger(__name__)

# 任务类型定义
TASK_TYPES = {
    'function_12': {
        'name': '上升趋势反弹识别',
        'module': 'uptrend_rebound_analysis',
        'function': run_uptrend_rebound_analysis
    },
    'function_13': {
        'name': '动量筛选策略',
        'module': 'momentum_screening_analysis',
        'function': run_momentum_screening
    },
    'function_14': {
        'name': '接近历史最低价筛选',
        'module': 'near_lowest_price_analysis',
        'function': run_near_lowest_price_screening
    },
    'function_15': {
        'name': '震荡股票识别',
        'module': 'oscillation_stock_analysis',
        'function': run_oscillation_stock_analysis
    }
}


def get_output_dir() -> str:
    """获取输出目录（按日期分类）"""
    today = datetime.now().strftime("%Y%m%d")
    output_dir = os.path.join("outputs", today)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def save_execution_record(
    task_type: str,
    task_name: str,
    status: str,
    output_file_path: Optional[str] = None,
    execution_params: Optional[Dict] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    error_message: Optional[str] = None,
    result_summary: Optional[str] = None,
    created_by: str = "system",
    record_id: Optional[int] = None,
) -> Optional[int]:
    """保存执行记录到数据库"""
    try:
        import sqlite3
        # 与 app 主库一致，使用 database.db（init_db 在该库中创建本表）
        db_path = "database.db"
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        # 计算执行时长
        duration_seconds = None
        if start_time and end_time:
            duration_seconds = int((end_time - start_time).total_seconds())
        
        # 将参数转为JSON字符串
        params_json = json.dumps(execution_params, ensure_ascii=False, default=str) if execution_params else None
        
        if record_id is not None:
            # 更新已有记录（任务结束：成功/失败）
            c.execute('''UPDATE screening_task_executions SET
                         task_type=?, task_name=?, status=?, output_file_path=?, execution_params=?,
                         start_time=?, end_time=?, duration_seconds=?, error_message=?, result_summary=?
                         WHERE id=?''',
                      (task_type, task_name, status, output_file_path, params_json,
                       start_time, end_time, duration_seconds, error_message, result_summary, record_id))
        else:
            # 新增记录（任务开始，仅 pending/running）
            c.execute('''INSERT INTO screening_task_executions 
                         (task_type, task_name, status, output_file_path, execution_params,
                          start_time, end_time, duration_seconds, error_message, result_summary, created_by)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (task_type, task_name, status, output_file_path, params_json,
                       start_time, end_time, duration_seconds, error_message, result_summary, created_by))
            record_id = c.lastrowid
        
        conn.commit()
        conn.close()
        
        logger.info(f"执行记录已保存: ID={record_id}, 任务={task_name}, 状态={status}")
        return record_id
        
    except Exception as e:
        logger.error(f"保存执行记录失败: {e}", exc_info=True)
        return 0


def execute_task(
    task_type: str,
    execution_params: Optional[Dict] = None,
    created_by: str = "system"
) -> Dict[str, Any]:
    """
    执行筛选任务
    
    参数:
    - task_type: 任务类型 ('function_12', 'function_13', 'function_14', 'function_15')
    - execution_params: 执行参数（字典）
    - created_by: 执行者
    
    返回:
    - 执行结果字典
    """
    if task_type not in TASK_TYPES:
        return {
            'success': False,
            'error': f'未知的任务类型: {task_type}'
        }
    
    task_info = TASK_TYPES[task_type]
    task_name = task_info['name']
    task_function = task_info['function']
    
    start_time = datetime.now()
    record_id = None
    output_file_path = None
    
    try:
        # 准备输出目录
        output_dir = get_output_dir()
        
        # 准备参数（设置默认值）
        if execution_params is None:
            execution_params = {}
        
        # 确保有output_dir参数
        execution_params['output_dir'] = output_dir
        
        # 创建执行记录（pending状态）
        record_id = save_execution_record(
            task_type=task_type,
            task_name=task_name,
            status='running',
            execution_params=execution_params,
            start_time=start_time,
            created_by=created_by
        )
        
        logger.info(f"开始执行任务: {task_name} (类型: {task_type})")
        
        # 执行任务
        output_file_path = task_function(**execution_params)
        
        end_time = datetime.now()
        
        # 判断执行结果
        if output_file_path and os.path.exists(output_file_path):
            status = 'success'
            error_message = None
            
            # 生成结果摘要（尝试读取Excel文件获取行数）
            result_summary = None
            try:
                import pandas as pd
                if output_file_path.endswith('.xlsx'):
                    # 读取Excel文件，统计各个sheet的行数
                    xls = pd.ExcelFile(output_file_path)
                    sheet_summaries = []
                    for sheet_name in xls.sheet_names:
                        df = pd.read_excel(output_file_path, sheet_name=sheet_name)
                        row_count = len(df)
                        sheet_summaries.append(f"{sheet_name}: {row_count}条")
                    result_summary = "; ".join(sheet_summaries)
            except Exception as e:
                logger.debug(f"读取结果摘要失败: {e}")
            
            # 更新执行记录（同一条，避免重复 INSERT）
            save_execution_record(
                task_type=task_type,
                task_name=task_name,
                status=status,
                output_file_path=output_file_path,
                execution_params=execution_params,
                start_time=start_time,
                end_time=end_time,
                result_summary=result_summary,
                created_by=created_by,
                record_id=record_id,
            )
            
            logger.info(f"任务执行成功: {task_name}, 输出文件: {output_file_path}")
            
            return {
                'success': True,
                'task_type': task_type,
                'task_name': task_name,
                'output_file_path': output_file_path,
                'result_summary': result_summary,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': int((end_time - start_time).total_seconds()),
                'record_id': record_id
            }
        else:
            status = 'failed'
            error_message = '任务执行完成，但未生成输出文件'
            
            # 更新执行记录（同一条）
            save_execution_record(
                task_type=task_type,
                task_name=task_name,
                status=status,
                output_file_path=None,
                execution_params=execution_params,
                start_time=start_time,
                end_time=end_time,
                error_message=error_message,
                created_by=created_by,
                record_id=record_id,
            )
            
            logger.warning(f"任务执行失败: {task_name}, 原因: {error_message}")
            
            return {
                'success': False,
                'task_type': task_type,
                'task_name': task_name,
                'error': error_message,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': int((end_time - start_time).total_seconds()),
                'record_id': record_id
            }
            
    except Exception as e:
        end_time = datetime.now()
        error_message = str(e)
        error_traceback = traceback.format_exc()
        
        logger.error(f"任务执行异常: {task_name}, 错误: {error_message}", exc_info=True)
        
        # 更新执行记录（同一条）
        save_execution_record(
            task_type=task_type,
            task_name=task_name,
            status='failed',
            output_file_path=output_file_path,
            execution_params=execution_params,
            start_time=start_time,
            end_time=end_time,
            error_message=f"{error_message}\n\n{error_traceback}",
            created_by=created_by,
            record_id=record_id,
        )
        
        return {
            'success': False,
            'task_type': task_type,
            'task_name': task_name,
            'error': error_message,
            'error_traceback': error_traceback,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': int((end_time - start_time).total_seconds()),
            'record_id': record_id
        }


def get_execution_history(task_type: Optional[str] = None, limit: int = 50) -> list:
    """获取执行历史记录"""
    try:
        import sqlite3
        db_path = "database.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # 使用Row工厂，方便转换为字典
        c = conn.cursor()
        
        if task_type:
            c.execute('''SELECT * FROM screening_task_executions 
                         WHERE task_type = ?
                         ORDER BY created_at DESC 
                         LIMIT ?''', (task_type, limit))
        else:
            c.execute('''SELECT * FROM screening_task_executions 
                         ORDER BY created_at DESC 
                         LIMIT ?''', (limit,))
        
        rows = c.fetchall()
        conn.close()
        
        # 转换为字典列表
        history = []
        for row in rows:
            try:
                params = json.loads(row['execution_params']) if row['execution_params'] else None
            except (json.JSONDecodeError, TypeError):
                params = None
            history.append({
                'id': row['id'],
                'task_type': row['task_type'],
                'task_name': row['task_name'],
                'status': row['status'],
                'output_file_path': row['output_file_path'],
                'execution_params': params,
                'start_time': row['start_time'],
                'end_time': row['end_time'],
                'duration_seconds': row['duration_seconds'],
                'error_message': row['error_message'],
                'result_summary': row['result_summary'],
                'created_by': row['created_by'],
                'created_at': row['created_at']
            })
        
        return history
        
    except Exception as e:
        logger.error(f"获取执行历史失败: {e}", exc_info=True)
        return []


if __name__ == '__main__':
    # 命令行执行示例
    import argparse
    
    parser = argparse.ArgumentParser(description='执行筛选任务')
    parser.add_argument('--task', type=str, required=True,
                        choices=['function_12', 'function_13', 'function_14', 'function_15'],
                        help='任务类型')
    parser.add_argument('--params', type=str, help='执行参数（JSON字符串）')
    parser.add_argument('--user', type=str, default='system', help='执行者')
    
    args = parser.parse_args()
    
    # 解析参数
    params = json.loads(args.params) if args.params else {}
    
    # 执行任务
    result = execute_task(
        task_type=args.task,
        execution_params=params,
        created_by=args.user
    )
    
    # 打印结果
    print(json.dumps(result, ensure_ascii=False, indent=2))
