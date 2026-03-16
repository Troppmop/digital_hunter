from pydantic import BaseModel, ValidationError
import json
from confluent_kafka import Consumer
from pymongo import MongoClient

class AttackAlert(BaseModel):
    timestamp: str
    attack_id: str
    entity_id: str
    weapon_type: str

class MongoIn:
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:270127')['test']['targets']
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

        self.consumer.subscribe(["attack"])
        
        self.db = db
    def listen(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("❌ Error:", msg.error())
                    continue

                value = msg.value().decode("utf-8")
                try:
                    order = json.loads(value)
                except json.JSONDecodeError:
                    print("invalid json")
                
                    
                try:
                    model = AttackAlert.model_validate(order).model_dump()
                
                except ValidationError:
                    print("data is invalid")

                self.db.add(model)
                print(model)
        except KeyboardInterrupt:
            print("\n🔴 Stopping consumer")

        finally:
            self.consumer.close()

