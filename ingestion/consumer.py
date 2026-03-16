import json

from confluent_kafka import Consumer


class Reciever:
    def __init__(self, log_callback):
        self.config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "ingestion",
            "auto.offset.reset": "earliest"
        }
        self.log_callback = log_callback
        self.consumer = Consumer(self.config)
    
    def listen(self):
        self.consumer.subscribe(['intel', 'attack', 'damage'])
        self.log_callback("info", "consumer is subscribed to topics: intel, attack, and damage")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    self.log_callback('error', msg.error())
                    continue
                value = msg.value().decode('utf-8')
                ingest = json.loads(value)
                self.log_callback('info', f"recieved information {ingest}")

        except KeyboardInterrupt:
            self.log_callback('info', 'consumer stopped')
