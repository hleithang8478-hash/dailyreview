#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志配置模块
用于管理应用程序的日志系统
"""

import logging
import os
import datetime
from logging.handlers import RotatingFileHandler
import sys

class LoggerConfig:
    """日志配置类"""
    
    def __init__(self, log_dir="logs"):
        """
        初始化日志配置
        Args:
            log_dir: 日志文件目录
        """
        self.log_dir = log_dir
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_id = f"session_{self.timestamp}"
        
        # 创建日志目录
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # 创建会话日志目录
        self.session_log_dir = os.path.join(log_dir, self.session_id)
        if not os.path.exists(self.session_log_dir):
            os.makedirs(self.session_log_dir)
        
        # 初始化日志器
        self._setup_loggers()
        
        # 重定向标准输出和错误输出到日志文件
        self._redirect_stdout_stderr()
    
    def _setup_loggers(self):
        """设置各种日志器"""
        
        # 1. 主日志器 - 记录所有重要操作
        self.main_logger = self._create_logger(
            name="main",
            log_file=os.path.join(self.session_log_dir, "main.log"),
            level=logging.INFO,
            format_str="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
        # 2. 数据获取日志器 - 记录数据获取过程
        self.data_logger = self._create_logger(
            name="data",
            log_file=os.path.join(self.session_log_dir, "data_fetch.log"),
            level=logging.DEBUG,
            format_str="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        
        # 3. 分析日志器 - 记录分析过程
        self.analysis_logger = self._create_logger(
            name="analysis",
            log_file=os.path.join(self.session_log_dir, "analysis.log"),
            level=logging.INFO,
            format_str="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        
        # 4. 错误日志器 - 记录错误信息
        self.error_logger = self._create_logger(
            name="error",
            log_file=os.path.join(self.session_log_dir, "error.log"),
            level=logging.ERROR,
            format_str="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        
        # 5. 性能日志器 - 记录性能指标
        self.performance_logger = self._create_logger(
            name="performance",
            log_file=os.path.join(self.session_log_dir, "performance.log"),
            level=logging.INFO,
            format_str="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        
        # 6. 调试日志器 - 记录调试信息
        self.debug_logger = self._create_logger(
            name="debug",
            log_file=os.path.join(self.session_log_dir, "debug.log"),
            level=logging.DEBUG,
            format_str="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        
        # 7. 控制台输出日志器 - 重定向所有print输出
        self.console_logger = self._create_logger(
            name="console",
            log_file=os.path.join(self.session_log_dir, "console.log"),
            level=logging.INFO,
            format_str="%(asctime)s - %(message)s"
        )
        
        # 8. 进度条日志器 - 专门用于进度条显示
        self.progress_logger = self._create_progress_logger()
    
    def _create_progress_logger(self):
        """创建进度条日志器，只在控制台显示进度信息"""
        logger = logging.getLogger("progress")
        logger.setLevel(logging.INFO)
        
        # 避免重复添加处理器
        if logger.handlers:
            return logger
        
        # 创建格式化器
        formatter = logging.Formatter("%(message)s")
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        # 添加处理器
        logger.addHandler(console_handler)
        
        # 设置不传播到父日志器
        logger.propagate = False
        
        return logger
    
    def _redirect_stdout_stderr(self):
        """重定向标准输出和错误输出到日志文件"""
        class LoggerWriter:
            def __init__(self, logger, level):
                self.logger = logger
                self.level = level
                self.buffer = ""
            
            def write(self, message):
                if message and not message.isspace():
                    self.buffer += message
                    if message.endswith('\n'):
                        self.logger.log(self.level, self.buffer.rstrip())
                        self.buffer = ""
            
            def flush(self):
                if self.buffer:
                    self.logger.log(self.level, self.buffer.rstrip())
                    self.buffer = ""
        
        # 注释掉重定向标准输出，让用户交互界面和进度条显示在控制台
        # sys.stdout = LoggerWriter(self.console_logger, logging.INFO)
        # sys.stderr = LoggerWriter(self.console_logger, logging.ERROR)
        
        # 只记录到日志文件，不重定向控制台输出
        self.console_logger.info("标准输出重定向已禁用，用户交互界面将显示在控制台")
    
    def _create_logger(self, name, log_file, level, format_str):
        """
        创建日志器
        Args:
            name: 日志器名称
            log_file: 日志文件路径
            level: 日志级别
            format_str: 日志格式
        Returns:
            Logger: 配置好的日志器
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)
        
        # 避免重复添加处理器
        if logger.handlers:
            return logger
        
        # 创建格式化器
        formatter = logging.Formatter(format_str)
        
        # 创建文件处理器（带轮转）
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        
        # 添加处理器
        logger.addHandler(file_handler)
        
        # 设置不传播到父日志器
        logger.propagate = False
        
        return logger
    
    def log_session_start(self, config_info=None):
        """记录会话开始"""
        self.main_logger.info("=" * 80)
        self.main_logger.info("应用程序会话开始")
        self.main_logger.info(f"会话ID: {self.session_id}")
        self.main_logger.info(f"开始时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if config_info:
            self.main_logger.info("配置信息:")
            for key, value in config_info.items():
                self.main_logger.info(f"  {key}: {value}")
        
        self.main_logger.info("=" * 80)
    
    def log_session_end(self, summary_info=None):
        """记录会话结束"""
        self.main_logger.info("=" * 80)
        self.main_logger.info("应用程序会话结束")
        self.main_logger.info(f"结束时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if summary_info:
            self.main_logger.info("执行摘要:")
            for key, value in summary_info.items():
                self.main_logger.info(f"  {key}: {value}")
        
        self.main_logger.info("=" * 80)
    
    def log_step_start(self, step_name, step_info=None):
        """记录步骤开始"""
        self.main_logger.info(f"开始执行: {step_name}")
        if step_info:
            for key, value in step_info.items():
                self.main_logger.info(f"  {key}: {value}")
    
    def log_step_end(self, step_name, duration, result_info=None):
        """记录步骤结束"""
        self.main_logger.info(f"完成执行: {step_name} (耗时: {duration:.1f}秒)")
        if result_info:
            for key, value in result_info.items():
                self.main_logger.info(f"  {key}: {value}")
    
    def log_data_operation(self, operation, details):
        """记录数据操作"""
        self.data_logger.info(f"数据操作: {operation}")
        self.data_logger.info(f"详细信息: {details}")
    
    def log_analysis_result(self, analysis_type, result_summary):
        """记录分析结果"""
        self.analysis_logger.info(f"分析类型: {analysis_type}")
        self.analysis_logger.info(f"结果摘要: {result_summary}")
    
    def log_error(self, error_msg, error_details=None, exc_info=None):
        """记录错误信息"""
        self.error_logger.error(f"错误: {error_msg}")
        if error_details:
            self.error_logger.error(f"错误详情: {error_details}")
        if exc_info:
            self.error_logger.error("异常信息:", exc_info=exc_info)
    
    def log_performance(self, operation, duration, details=None):
        """记录性能信息"""
        self.performance_logger.info(f"性能指标 - {operation}: {duration:.2f}秒")
        if details:
            self.performance_logger.info(f"详细信息: {details}")
    
    def log_debug(self, message, details=None):
        """记录调试信息"""
        self.debug_logger.debug(f"调试信息: {message}")
        if details:
            self.debug_logger.debug(f"详细信息: {details}")
    
    def log_progress(self, message):
        """记录进度信息（只在控制台显示）"""
        self.progress_logger.info(message)
    
    def get_session_info(self):
        """获取会话信息"""
        return {
            'session_id': self.session_id,
            'session_log_dir': self.session_log_dir,
            'timestamp': self.timestamp
        }

# 全局日志配置实例
_logger_config = None

def get_logger_config():
    """获取全局日志配置实例"""
    global _logger_config
    if _logger_config is None:
        _logger_config = LoggerConfig()
    return _logger_config

def init_logger(log_dir="logs"):
    """初始化日志系统"""
    global _logger_config
    _logger_config = LoggerConfig(log_dir)
    return _logger_config
