import requests
from django.conf import settings

from queue.connection import RabbitMQConnection


class Worker(object):
    def __init__(self):
        self.conn = RabbitMQConnection()
        self.sms_channel = self.conn.get_channel(**{'type': 'SMS'})

    def run_sms_worker(self):
        self.sms_channel.basic_qos(prefetch_count=1)
        self.sms_channel.basic_consume(queue=settings.SMS_QUEUE, on_message_callback=self.trend_sms_worker)
        self.sms_channel.start_consuming()

    def trend_sms_worker (self, ch, method, properties, body):
        resp = requests.post(url=settings.SMS_TREND_URL, data=body, verify=False).json()
        print(resp)
        return ch.basic_ack(delivery_tag=method.delivery_tag)
