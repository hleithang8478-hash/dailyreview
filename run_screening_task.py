#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
独立的任务执行脚本

用途：
1. 手工执行：python run_screening_task.py --task function_12
2. 定时执行：配置到Windows任务计划程序或Linux cron

示例：
    # 执行功能12（上升趋势反弹识别）
    python run_screening_task.py --task function_12

    # 执行功能13，限制股票数量为500
    python run_screening_task.py --task function_13 --limit 500

    # 执行功能14，自定义回看年数为5年
    python run_screening_task.py --task function_14 --params '{"lookback_years": 5}'
"""

import sys
import os
import argparse
import json
import logging
from datetime import datetime

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='执行筛选任务',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 执行功能12（上升趋势反弹识别）
  python run_screening_task.py --task function_12

  # 执行功能13，限制股票数量为500
  python run_screening_task.py --task function_13 --limit 500

  # 执行功能14，自定义参数
  python run_screening_task.py --task function_14 --params '{"lookback_years": 5, "price_tolerance": 0.15}'
        """
    )
    
    parser.add_argument(
        '--task', 
        type=str, 
        required=True,
        choices=['function_12', 'function_13', 'function_14', 'function_15'],
        help='任务类型: function_12(上升趋势反弹识别), function_13(动量筛选), function_14(接近历史最低价), function_15(震荡股票识别)'
    )
    
    parser.add_argument(
        '--limit', 
        type=int, 
        help='股票数量限制（可选）'
    )
    
    parser.add_argument(
        '--params', 
        type=str, 
        help='执行参数（JSON字符串），例如: \'{"lookback_years": 5, "price_tolerance": 0.15}\''
    )
    
    parser.add_argument(
        '--user', 
        type=str, 
        default='system',
        help='执行者名称（默认: system）'
    )
    
    args = parser.parse_args()
    
    # 准备参数
    params = {}
    
    if args.limit:
        params['limit'] = args.limit
    
    if args.params:
        try:
            custom_params = json.loads(args.params)
            params.update(custom_params)
        except json.JSONDecodeError as e:
            logger.error(f"参数JSON解析失败: {e}")
            print(f"错误: 参数JSON格式不正确: {e}")
            sys.exit(1)
    
    # 导入任务执行模块
    try:
        from screening_tasks import execute_task, TASK_TYPES
        
        task_info = TASK_TYPES[args.task]
        task_name = task_info['name']
        
        print("=" * 60)
        print(f"开始执行任务: {task_name}")
        print(f"任务类型: {args.task}")
        print(f"执行者: {args.user}")
        if params:
            print(f"执行参数: {json.dumps(params, ensure_ascii=False, indent=2)}")
        print("=" * 60)
        print()
        
        # 执行任务
        start_time = datetime.now()
        result = execute_task(
            task_type=args.task,
            execution_params=params if params else None,
            created_by=args.user
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print()
        print("=" * 60)
        if result['success']:
            print(f"✅ 任务执行成功!")
            print(f"输出文件: {result.get('output_file_path', 'N/A')}")
            if result.get('result_summary'):
                print(f"结果摘要: {result['result_summary']}")
        else:
            print(f"❌ 任务执行失败!")
            print(f"错误信息: {result.get('error', '未知错误')}")
        
        print(f"执行时长: {format_duration(duration)}")
        print(f"记录ID: {result.get('record_id', 'N/A')}")
        print("=" * 60)
        
        # 根据执行结果设置退出码
        sys.exit(0 if result['success'] else 1)
        
    except ImportError as e:
        logger.error(f"导入任务执行模块失败: {e}")
        print(f"错误: 无法导入任务执行模块: {e}")
        print("请确保 screening_tasks.py 文件存在且可以正常导入")
        sys.exit(1)
    except Exception as e:
        logger.error(f"执行任务失败: {e}", exc_info=True)
        print(f"错误: {e}")
        sys.exit(1)


def format_duration(seconds):
    """格式化时长"""
    if seconds < 60:
        return f"{seconds:.1f}秒"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}分{secs}秒"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}小时{minutes}分"


if __name__ == '__main__':
    main()
