from consumer import Reciever
from logger import log_event

consumer = Reciever(log_callback=log_event)

consumer.listen()
