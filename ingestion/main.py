from consumer import Reciever
from logger import log_event
from mongo import MongoConnect

db = MongoConnect()

consumer = Reciever(log_callback=log_event)

consumer.listen()
