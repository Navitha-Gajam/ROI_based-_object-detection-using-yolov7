import cv2
import base64
import pika
import json
import time
import sys
from threading import Thread

from config import rabbitmq, routing_keys
from log_file import setup_logging
logger = setup_logging("camera_manager")


class CameraManager:

    def process_camera(self, cam):
        connection = None
        cap = None
        FPS = 8
        try:
            logger.info(f"Starting camera {cam['camera_name']}")
            logger.info(f"Opening Video Path : {cam['camera_feed']}")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq["host"],port=rabbitmq["port"], heartbeat=600))
            channel = connection.channel()
            channel.exchange_declare(exchange=rabbitmq["exchange"],exchange_type=rabbitmq["exchange_type"],durable=rabbitmq["durable"])
            cap = cv2.VideoCapture(cam["camera_feed"])

            if not cap.isOpened():
                logger.error(f"Cannot open video : {cam['camera_feed']}")
                return
            labels = cam["camera_outcome"]
            routing_key = (routing_keys["person"]if "person" in labels else routing_keys["non_person"])
            frame_count = 0
            while True:
                start_time = time.time()
                ret, frame = cap.read()
                if not ret:
                    logger.warning(f"{cam['camera_name']} stream ended")
                    break
                frame_count += 1
                frame = cv2.resize(frame, (640, 640))
                encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 70]
                _, buffer = cv2.imencode(".jpg", frame, encode_param)
                frame_b64 = base64.b64encode(buffer).decode()
                data = {
                    "version": "v2",
                    "camera_id": cam["camera_id"],
                    "camera_name": cam["camera_name"],
                    "camera_ip": cam["camera_ip"],
                    "camera_outcome": cam["camera_outcome"],
                    "frame": frame_b64
                }
                channel.basic_publish(exchange=rabbitmq["exchange"],routing_key=routing_key,body=json.dumps(data))
                logger.info(f"Sending frame from {cam['camera_name']}")
                if frame_count % 20 == 0:
                    logger.info(f"{cam['camera_name']} sent {frame_count} frames")
                elapsed = time.time() - start_time
                sleep_time = max(0, (1 / FPS) - elapsed)
                time.sleep(sleep_time)
        except Exception as e:
            logger.exception(f"Camera {cam['camera_name']} failed")


        finally:
            if cap:
                cap.release()
            if connection and connection.is_open:
                connection.close()
            logger.info(f"{cam['camera_name']} finished")



'''if __name__ == "__main__":

    logger.info("Application Started")

    with open("camera.json", "r") as f:
        cameras = json.load(f)

    manager = CameraManager()

    threads = []

    for cam in cameras.values():
        t = Thread(
            target=manager.process_camera,
            args=(cam,)
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()'''