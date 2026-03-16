from confluent_kafka import Producer
import json

class Sender:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def send(self, msg, reason):
        value = {"bad message": msg,
                 "reason": reason}
        value = json.dumps(value).encode('utf-8')
        self.producer.produce('intel_signals_dlq', value=value)
        self.producer.flush()