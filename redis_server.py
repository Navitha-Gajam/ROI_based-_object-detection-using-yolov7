import redis
import json
from config import redis_server
from log_file import setup_logging
import sys
logger = setup_logging("REDIS_SERVER")

class RedisServer:

    def __init__(self):
        self.r = redis.Redis(host=redis_server["host"],port=redis_server["port"],db=redis_server["db"],decode_responses=True)
        self.json_file = redis_server["json_file"]

    def load_json_to_redis(self):
        try:
            with open(self.json_file, "r") as f:
                data = json.load(f)

            for _, cam_data in data.items():
                self.r.set(cam_data["camera_id"], json.dumps(cam_data))
                logger.info(f"Loaded camera {cam_data['camera_name']} into Redis")

            return data
        except Exception as e:
            error_type, error_msg, error_line = sys.exc_info()
            logger.info(f'Error in Line no : {error_line.tb_lineno}: due to {error_msg}')

    def get_camera(self, camera_id):
        try:
            data = self.r.get(camera_id)
            return json.loads(data) if data else None
        except Exception as e:
            error_type, error_msg, error_line = sys.exc_info()
            logger.info(f'Error in Line no : {error_line.tb_lineno}: due to {error_msg}')
            return None