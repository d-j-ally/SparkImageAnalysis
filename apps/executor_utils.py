import logging
import sys
from pyspark import TaskContext

def setup_executor_logger(logging_level=logging.INFO):
    """
    Setup logging for executor-side operations
    Must be called inside UDFs/functions that run on executors
    """
    
    executor_logger = logging.getLogger('executor')
    
    try:
        ctx = TaskContext.get()
        partition_id = ctx.partitionId() if ctx else "unknown"
    except:
        partition_id = "unknown"
    
    # Only configure once per executor
    if not executor_logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(
            f'[%(asctime)s] [Partition-{partition_id}] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        executor_logger.addHandler(handler)
        executor_logger.setLevel(logging_level)
        executor_logger.propagate = False  # Don't send to root logger
    
    return executor_logger