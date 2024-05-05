import json
import pika
from django.conf import settings
from processors.connection import RabbitMQConnection


class Producers(object):
    def __init__(self):
        self.conn = RabbitMQConnection()

    def process_sms(self, **data):
        self.conn.connect()
        k = {'type': 'SMS'}
        message = data.get('message')
        channel = self.conn.get_channel(**k)
        channel.basic_publish(
            exchange=settings.SMS_EXCHANGE,
            routing_key=settings.SMS_QUEUE,
            body=str(data),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        self.conn.close()
        print(f" [x] Sent SMS to 'sms_queue': {message}")
        return {"code": "200.000", "message": f" [x] Sent SMS to 'sms_queue': {message}"}

    def process_documents(self, data):
        self.conn.connect()
        k = {'type': 'DOCUMENT'}
        channel = self.conn.get_channel(**k)
        # data = json.dumps(data)
        channel.basic_publish(
            exchange=settings.DOCUMENT_EXCHANGE,
            routing_key=settings.DOCUMENT_QUEUE,
            body=str(data),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        self.conn.close()
        print(f" [x] Sent document to 'documents_queue': {data}")
        return {"code": "200.000", "message": f" [x] Sent document to 'documents_queue': {data}"}
