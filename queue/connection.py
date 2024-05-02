import time

from django.conf import settings
from pika import PlainCredentials, ConnectionParameters, BlockingConnection, exceptions

# import config


class RabbitMQConnection:
    _instance = None

    def __new__(cls, host=settings.HOST, port=5672, username=settings.USERNAME, password=settings.PASSWORD):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, port=5672):
        self.host = settings.HOST
        self.port = port
        self.username = settings.USERNAME
        self.password = settings.PASSWORD
        self.connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        retries = 0
        while retries < 10:
            try:
                credentials = PlainCredentials(self.username, self.password)
                parameters = ConnectionParameters(host=self.host, port=self.port, credentials=credentials)
                self.connection = BlockingConnection(parameters)
                print("Connected to RabbitMQ")
                return
            except exceptions.AMQPConnectionError as e:
                print("Failed to connect to RabbitMQ:", e)
                retries += 1
                wait_time = 2 ** retries
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

        print("Exceeded maximum number of connection retries. Stopping the code.")

    def is_connected(self):
        return self.connection is not None and self.connection.is_open

    def close(self):
        if self.is_connected():
            self.connection.close()
            self.connection = None
            print("Closed RabbitMQ connection")

    def get_channel(self, **kwargs):
        type = kwargs['type']
        if self.is_connected():
            if type == 'SMS':
                queue_obj = settings.SMS_QUEUE
                exchange_obj = settings.SMS_EXCHANGE
                self.connection.channel().exchange_declare(exchange_obj, durable=True)
                self.connection.channel().queue_declare(queue=queue_obj, durable=True)
                self.connection.channel().queue_bind(exchange=exchange_obj, queue=queue_obj, routing_key=queue_obj)
            else:
                queue_obj = settings.SMS_QUEUE
                exchange_obj = settings.SMS_EXCHANGE
                self.connection.channel().exchange_declare(queue_obj, durable=True)
                self.connection.channel().queue_declare(queue=queue_obj, durable=True)
                self.connection.channel().queue_bind(exchange=exchange_obj, queue=queue_obj, routing_key=queue_obj)
            return self.connection.channel()
        return None

