from redis_server import RedisServer
from processor_manager import ProcessorManager
from log_file import setup_logging
import multiprocessing
import sys
logger = setup_logging("MAIN")


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    try:
        logger.info("Loading cameras into Redis")
        redis_obj = RedisServer()
        cameras = redis_obj.load_json_to_redis()
        logger.info(f"Loaded {len(cameras)} cameras")
        processor = ProcessorManager(cameras)
        processor.start()

    except Exception as e:
        error_type, error_msg, error_line = sys.exc_info()
        logger.info(f'Error in Line no : {error_line.tb_lineno}: due to {error_msg}')