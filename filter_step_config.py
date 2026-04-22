#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选步骤配置模块

用于功能12（上升趋势中的反弹识别）的可配置筛选步骤
"""

from typing import Dict, List, Optional


def prompt_filter_steps() -> Dict[str, bool]:
    """
    提示用户选择要执行的筛选步骤
    
    返回：
    - 字典，键为步骤名称，值为是否执行该步骤
    """
    print("=" * 60)
    print("筛选步骤配置")
    print("=" * 60)
    print("请选择要执行的筛选步骤（输入步骤编号，多个编号用逗号分隔，如：1,2,3）")
    print()
    print("可选步骤：")
    print("  1. 上升趋势过滤（MASS短期、MASS长期、均线斜率等）")
    print("  2. 回调识别（偏离度检查，价格相对EMA20的偏离度在-10% ~ -2%之间）")
    print("  3. 末端确认（反弹确认，包括价格上穿MA5/中轨、成交量确认等）")
    print()
    print("说明：")
    print("  - 步骤1：确保股票处于上升趋势（MASS短期≥50，长期≥50）")
    print("  - 步骤2：确保价格回调到位（偏离度在-10% ~ -2%之间）")
    print("  - 步骤3：确保反弹开始确认（价格上穿均线、成交量配合）")
    print()
    
    while True:
        try:
            choice_input = input("请输入要执行的步骤编号（默认全部执行，直接回车）：").strip()
            
            if not choice_input:
                # 默认全部执行
                return {
                    'trend_filter': True,      # 步骤1：上升趋势过滤
                    'pullback_filter': True,   # 步骤2：回调识别
                    'end_confirm_filter': True  # 步骤3：末端确认
                }
            
            # 解析输入的编号
            selected_steps = []
            for step_str in choice_input.split(','):
                step_str = step_str.strip()
                if step_str:
                    step_num = int(step_str)
                    if step_num < 1 or step_num > 3:
                        print(f"❌ 无效的步骤编号: {step_num}，请输入1-3之间的数字")
                        raise ValueError(f"无效的步骤编号: {step_num}")
                    selected_steps.append(step_num)
            
            if not selected_steps:
                print("❌ 未选择任何步骤，请重新输入")
                continue
            
            # 构建配置字典
            config = {
                'trend_filter': 1 in selected_steps,      # 步骤1：上升趋势过滤
                'pullback_filter': 2 in selected_steps,   # 步骤2：回调识别
                'end_confirm_filter': 3 in selected_steps  # 步骤3：末端确认
            }
            
            # 显示选择的配置
            print()
            print("已选择的筛选步骤：")
            if config['trend_filter']:
                print("  ✓ 步骤1：上升趋势过滤")
            if config['pullback_filter']:
                print("  ✓ 步骤2：回调识别")
            if config['end_confirm_filter']:
                print("  ✓ 步骤3：末端确认")
            print()
            
            confirm = input("确认执行上述步骤？(Y/n): ").strip().lower()
            if confirm and confirm != 'y' and confirm != 'yes':
                print("已取消，请重新选择")
                continue
            
            return config
            
        except ValueError as e:
            print(f"❌ 输入错误: {e}，请重新输入")
            continue
        except Exception as e:
            print(f"❌ 输入格式错误: {e}，请重新输入")
            continue

