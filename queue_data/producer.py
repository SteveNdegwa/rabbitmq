import pika
from django.conf import settings
from queue_data.worker import Worker
from queue_data.connection import RabbitMQConnection


class Producers(object):
    def __init__(self):
        self.conn = RabbitMQConnection()


    def process_sms(self, **data):
        k = {"type": "SMS"}
        self.conn.connect()
        channel = self.conn.get_channel(**k)
        message = data.get('message')
        channel.basic_publish(
            exchange=settings.SMS_EXCHANGE,
            routing_key=settings.SMS_QUEUE,
            body=message,
            properties=pika.BasicProperties(
            ))
        self.conn.close()
        print(f" [x] Sent {message}")
        return {"code": "200.000", "message": f" [x] Sent {message}"}

    def process_documents(self, **data):
        k = {"type": "DOCUMENT"}
        self.conn.connect()
        channel = self.conn.get_channel(**k)
        message = data.get('message')
        channel.basic_publish(
            exchange=settings.DOCUMENT_EXCHANGE,
            routing_key=settings.DOCUMENT_QUEUE,
            body=message,
            properties=pika.BasicProperties(
            ))
        self.conn.close()
        print(f" [x] Sent {message}")
        return {"code": "200.000", "message": f" [x] Sent {message}"}



