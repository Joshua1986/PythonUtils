# coding=utf-8
import logging
import time
import os

log_path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir) + "/logs/") + \
           '/{0}.log'.format(time.strftime('%Y-%m-%d-%H', time.localtime(time.time())))
# 创建一个logger
logger = logging.getLogger('settlement')
logger.setLevel(logging.DEBUG)

# 创建一个handler，用于写入日志文件
fh = logging.FileHandler(log_path)
fh.setLevel(logging.DEBUG)

# 创建一个handler，用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.WARNING)

# 定义handler的输出格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# 给logger添加handler
logger.addHandler(fh)
logger.addHandler(ch)

