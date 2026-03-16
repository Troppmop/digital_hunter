import json
from pydantic import BaseModel, ValidationError
from confluent_kafka import Consumer, Producer
from pymongo import MongoClient
import haversine
from logger import log_event

from models import Intel


class MongoIn:
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:270127')['test']['targets']
    def add(self, target):
        self.client.insert_one(target)
    
    def get(self, target):
        return self.client.find_one({'entity_id':target['entity_id']})
            

class KafkaIn:
    def __init__(self):
        self.producer = Producer({"bootstrap.servers": "localhost:9092"})
    
    def send(self, value, reason):
        package = {"value": value,
                   "reason": reason}
        package_json = json.dumps(package).encode('utf-8')
        self.producer.produce('intel_signals_dlq', value=package_json)
        self.producer.flush()

class KafkaOut:
    def __init__(self, producer: KafkaIn, db:MongoIn):
        
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:9092",
            "group.id": "order-tracker",
            "auto.offset.reset": "earliest"
        })

        self.consumer.subscribe(["intel"])
        self.producer = producer
        self.db = db
    def listen(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    log_event("error",f"❌ Error:, {msg.error()}")
                    continue

                value = msg.value().decode("utf-8")
                try:
                    order = json.loads(value)
                except json.JSONDecodeError:
                    log_event("error","invalid json")
                
                    self.producer.send(value, "invalid json")
                try:
                    model = Intel.model_validate(order).model_dump()

                    
                except ValidationError:
                    log_event("error","invalid model")
                    self.producer.send(order, "invalid model")
                

            
                old_log = self.db.get(model['entity_id']) 


                if old_log is None:
                    model['priority_level'] = 99
                    old_lat = 0
                    old_lon = 0
                else:
                    old_lat = old_log['lat']
                    old_lon = old_log['lon']
                calculation = haversine.haversine_km(old_lat, old_lon, model['lat'], model['lon'])
                log_event("info",calculation)
                try:
                    self.db.add(model)
                except:
                    log_event("error","failed to add to db")

        except KeyboardInterrupt:
            log_event("info","\n🔴 Stopping consumer")

        finally:
            self.consumer.close()

db = MongoIn()
kafkain = KafkaIn()
kafkaout = KafkaOut(kafkain,db)

kafkaout.listen()