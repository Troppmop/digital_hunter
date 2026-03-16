from consumer import Reciever
from logger import log_event
from mongo import MongoConnect
from producer import Sender

db = MongoConnect()
producer = Sender()
consumer = Reciever(log_callback=log_event, producer=producer)

consumer.listen()
