import time

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
channel.queue_bind(exchange='test', queue='C', routing_key='C')

print(' [*] Waiting for messages. To exit press CTRL+C')


def callback_a(ch, method, properties, body):
    print(f" [x] Received {body.decode()} from queue A")
    time.sleep(10)
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def callback_b(ch, method, properties, body):
    print(f" [x] Received {body.decode()} from queue B")
    time.sleep(10)
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def callback_c(ch, method, properties, body):
    print(f" [x] Received {body.decode()} from queue C")
    time.sleep(10)
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)

channel.basic_consume(queue='A', on_message_callback=callback_a)
channel.basic_consume(queue='B', on_message_callback=callback_b)
channel.basic_consume(queue='C', on_message_callback=callback_c)

channel.start_consuming()