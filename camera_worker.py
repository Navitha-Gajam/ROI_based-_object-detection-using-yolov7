from threading import Thread
import sys
from camera_manager import CameraManager
from log_file import setup_logging
logger = setup_logging("CAMERA_WORKER")

class CameraWorker:
    def __init__(self, cams):
        self.cams = cams
    def start(self):
        try:
            manager = CameraManager()
            threads = []
            for cam in self.cams:
                logger.info(f"Starting thread for {cam['camera_name']}")
                t = Thread(target=manager.process_camera,args=(cam,))
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
            logger.info("Worker completed")

        except Exception as e:
            error_type, error_msg, error_line = sys.exc_info()
            logger.error(f"Error in line {error_line.tb_lineno} : {error_msg}")