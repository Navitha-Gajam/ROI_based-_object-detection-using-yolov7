redis_server = {
    "host": "localhost",
    "port": 6379,
    "db": 1,
    "key": "camera_data",
    "json_file": "camera.json"
}

rabbitmq = {
    "host": "localhost",
    "port": 5672,
    "exchange": "camera",
    "detection_exchange": "yolo_detection_exchange",
    "exchange_type": "direct",
    "durable": False
}

queues = {
    "person": "person_queue",
    "non_person": "non_person_queue",

    "vehicle": "vehicle_queue",
    "fire_smoke": "fire_smoke_queue",
    "safety": "safety_queue"
}

detection_queues = {
    "vehicle": "vehicle_queue",
    "fire_smoke": "fire_smoke_queue",
    "safety": "safety_queue"
}

routing_keys = {
    "person": "person",
    "non_person": "non_person",

    "vehicle": "vehicle",
    "fire_smoke": "fire_smoke",
    "safety": "safety"

}