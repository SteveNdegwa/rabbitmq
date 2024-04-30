import pika

from connection import RabbitMQConnection

conn = RabbitMQConnection()
conn.connect()
channel = conn.get_channel()

channel.exchange_declare('test', durable=True)

channel.queue_declare(queue='A', durable=True)
channel.queue_bind(exchange='test', queue='A', routing_key='A')

channel.queue_declare(queue='B', durable=True)
channel.queue_bind(exchange='test', queue='B', routing_key='B')

channel.queue_declare(queue='C', durable=True)
channel.queue_bind(exchange='test', queue='C')

message = "Message for A"
channel.basic_publish(
    exchange='test',
    routing_key='A',
    body=message,
    properties=pika.BasicProperties(
    ))
print(f" [x] Sent {message}")

# message = "Message for B"
# channel.basic_publish(
#     exchange='test',
#     routing_key='B',
#     body=message,
#     properties=pika.BasicProperties(
#     ))
# print(f" [x] Sent {message}")
#
# message = "Message for C"
# channel.basic_publish(
#     exchange='test',
#     routing_key='C',
#     body=message,
#     properties=pika.BasicProperties(
#     ))
# print(f" [x] Sent {message}")

conn.close()
