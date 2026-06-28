from multiprocessing import Process
from camera_worker import CameraWorker
from log_file import setup_logging
import sys
logger = setup_logging("PROCESSOR_MANAGER")

class ProcessorManager:

    def __init__(self, cameras):
            self.cameras = list(cameras.values())

    def run_process(self, cam_subset):
            worker = CameraWorker(cam_subset)
            worker.start()

    def start(self):
        try:
            processes = []
            chunk_size = 2
            for i in range(0, len(self.cameras), chunk_size):
                subset = self.cameras[i:i + chunk_size]
                logger.info(f"Launching process for {len(subset)} cameras")
                p = Process(target=self.run_process, args=(subset,))
                p.start()
                processes.append(p)
            for p in processes:
                p.join()
            logger.info("All processes finished")

        except Exception as e:
            error_type, error_msg, error_line = sys.exc_info()
            logger.info(f'Error in Line no : {error_line.tb_lineno}: due to {error_msg}')