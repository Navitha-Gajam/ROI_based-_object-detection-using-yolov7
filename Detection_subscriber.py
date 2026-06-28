import pika
import json
import base64
import cv2
import numpy as np
import os
import sys
import torch

from config import rabbitmq, queues
from log_file import setup_logging

logger = setup_logging("DETECTION_SUBSCRIBER")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
YOLO_PATH = os.path.join(BASE_DIR,"yolov7")
sys.path.insert(0, YOLO_PATH)
from models.experimental import attempt_load
from utils.general import non_max_suppression

COLORS = {
    "person": (124, 255, 156),
    "car": (255, 230, 78),
    "bike": (56, 255, 255),
    "fire": (255, 0, 255),
    "smoke": (150, 200, 128),
    "helmet": (255, 155, 100),
    "jacket": (155, 190, 255)
}

class DetectionSubscriber:

    def __init__(self):

        try:

            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq["host"],port=rabbitmq["port"],heartbeat=0))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=rabbitmq["exchange"],exchange_type=rabbitmq["exchange_type"],durable=False)
            self.channel.exchange_declare(exchange=rabbitmq["detection_exchange"],exchange_type="direct",durable=False)
            self.channel.queue_declare(queue=queues["non_person"],durable=True)
            self.channel.queue_bind(exchange=rabbitmq["exchange"],queue=queues["non_person"],routing_key="non_person")

            self.channel.queue_declare(queue="vehicle_queue",durable=True)
            self.channel.queue_bind(exchange=rabbitmq["detection_exchange"],queue="vehicle_queue",routing_key="vehicle")

            self.channel.queue_declare(queue="fire_smoke_queue",durable=True)
            self.channel.queue_bind(exchange=rabbitmq["detection_exchange"],queue="fire_smoke_queue",routing_key="fire_smoke")

            self.channel.queue_declare(queue="safety_queue",durable=True)
            self.channel.queue_bind(exchange=rabbitmq["detection_exchange"],queue="safety_queue",routing_key="safety")

            logger.info("Detection Subscriber Started")

            self.device = "cpu"
            weights_path = os.path.join(BASE_DIR,"best (1).pt")
            logger.info(f"Loading Model : {weights_path}")

            self.model = attempt_load(weights_path,map_location=self.device)
            self.model.eval()

            logger.info("YOLOv7 Loaded Successfully")

        except Exception as e:
            logger.error(f"Initialization Error : {e}")

    def process_frame(self,frame):

        try:
            original = frame.copy()
            detections = []
            img = cv2.cvtColor(frame,cv2.COLOR_BGR2RGB)
            img = cv2.resize(img,(640, 640))
            img_tensor = torch.from_numpy(img)
            img_tensor = (img_tensor.permute(2, 0, 1).float() / 255.0)
            img_tensor = img_tensor.unsqueeze(0)

            with torch.no_grad():
                pred = self.model(img_tensor)[0]

            pred = non_max_suppression(pred,0.25,0.45)[0]

            if pred is not None:
                for *xyxy, conf, cls in pred:
                    x1, y1, x2, y2 = map(int,xyxy)

                    label_name = self.model.names[int(cls)]
                    color = COLORS.get(label_name,(0, 255, 0))
                    label = (f"{label_name} "f"{conf:.2f}")

                    cv2.rectangle(original,(x1, y1),(x2, y2),color,2)
                    cv2.putText(original,label,(x1, y1 - 10),cv2.FONT_HERSHEY_SIMPLEX,0.6,color,2)

                    detections.append({
                        "class": label_name,
                        "confidence": float(conf),
                        "bbox": [
                            x1,
                            y1,
                            x2,
                            y2]})
            return original, detections

        except Exception as e:
            logger.error(f"Process Frame Error : {e}")
            return frame, []

    def publish_detection_frame(self, frame, detections, camera_name):

        try:
            _, buffer = cv2.imencode(".jpg", frame)
            frame_b64 = base64.b64encode(buffer).decode("utf-8")

            frame_id = int(cv2.getTickCount())

            vehicle_detections = [
                det for det in detections
                if det["class"] in ["car", "bike"]
            ]

            if vehicle_detections:
                vehicle_payload = {
                    "meta": {
                        "cam_id": camera_name,
                        "frame_id": frame_id,
                        "detections": vehicle_detections
                    },
                    "frame_b64": frame_b64
                }

                self.channel.basic_publish(
                    exchange=rabbitmq["detection_exchange"],
                    routing_key="vehicle",
                    body=json.dumps(vehicle_payload)
                )

                logger.info("Published To Vehicle Queue")

            fire_detections = [
                det for det in detections
                if det["class"] in ["fire", "smoke"]
            ]

            if fire_detections:
                fire_payload = {
                    "meta": {
                        "cam_id": camera_name,
                        "frame_id": frame_id,
                        "detections": fire_detections
                    },
                    "frame_b64": frame_b64
                }

                self.channel.basic_publish(
                    exchange=rabbitmq["detection_exchange"],
                    routing_key="fire_smoke",
                    body=json.dumps(fire_payload)
                )

                logger.info("Published To Fire Smoke Queue")

            safety_detections = [
                det for det in detections
                if det["class"] in ["helmet", "jacket"]
            ]

            if safety_detections:
                safety_payload = {
                    "meta": {
                        "cam_id": camera_name,
                        "frame_id": frame_id,
                        "detections": safety_detections
                    },
                    "frame_b64": frame_b64
                }

                self.channel.basic_publish(
                    exchange=rabbitmq["detection_exchange"],
                    routing_key="safety",
                    body=json.dumps(safety_payload)
                )

                logger.info("Published To Safety Queue")

        except Exception as e:
            logger.error(f"Publish Error : {e}")

    def callback(self,ch,method,properties,body):

        try:
            data = json.loads(body)
            camera_name = data["camera_name"]
            logger.info(f"Processing : {camera_name}")
            frame_data = base64.b64decode(data["frame"])

            np_arr = np.frombuffer(frame_data,np.uint8)
            frame = cv2.imdecode(np_arr,cv2.IMREAD_COLOR)

            if frame is None:
                logger.error("Frame Decode Failed")
                return

            detected_frame, detections = \
                self.process_frame(frame)

            self.publish_detection_frame(detected_frame,detections,camera_name)

            display_frame = cv2.resize(detected_frame,(500, 400))
            cv2.imshow(f"Detection - {camera_name}",display_frame)
            key = cv2.waitKey(1)
            if key == ord("q"):
                cv2.destroyAllWindows()
                sys.exit(0)

        except Exception as e:
            logger.error(f"Callback Error : {e}")

    def start(self):

        try:
            self.channel.basic_consume(queue=queues["non_person"],on_message_callback=self.callback,auto_ack=True)
            logger.info("Waiting For Frames...")
            self.channel.start_consuming()

        except KeyboardInterrupt:
            logger.info("Stopped")
            cv2.destroyAllWindows()
            self.connection.close()

        except Exception as e:
            logger.error(f"Start Error : {e}")

if __name__ == "__main__":

    obj = DetectionSubscriber()
    obj.start()