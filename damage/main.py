from pydantic import BaseModel, ValidationError
import json
from confluent_kafka import Consumer
from pymongo import MongoClient
from logger import log_event

from models import DamageAlert

class MongoIn:
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:27017')['test']['targets']
    def add(self, target):
        self.client.insert_one(target)
    
    def get(self, target):
        return self.client.find_one({'entity_id':target['entity_id']})

class KafkaOut:
    def __init__(self, db:MongoIn):
        
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": "order-tracker",
            "auto.offset.reset": "earliest"
        })

        self.consumer.subscribe(["damage"])
        
        self.db = db
    def listen(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log_event("error",f"❌ Error: {msg.error()}")
                    continue

                value = msg.value().decode("utf-8")
                try:
                    order = json.loads(value)
                except json.JSONDecodeError:
                    log_event("error","invalid json")
                
                    
                try:
                    model = DamageAlert.model_validate(order).model_dump()
                
                except ValidationError:
                    log_event("error","data is invalid")

                log_event("info",model)
                self.db.add(model)

        except KeyboardInterrupt:
            log_event("info","\n🔴 Stopping consumer")

        finally:
            self.consumer.close()

db = MongoIn()

kafkaout = KafkaOut(db)

kafkaout.listen()