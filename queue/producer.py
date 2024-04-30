import pika

credentials = pika.PlainCredentials('root', 'tenant')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
channel = connection.channel()

channel.exchange_declare('test', durable=True)

channel.queue_declare(queue='A')
channel.queue_bind(exchange='test', queue='A', routing_key='A')

channel.queue_declare(queue='B')
channel.queue_bind(exchange='test', queue='B', routing_key='B')

channel.queue_declare(queue='C')
channel.queue_bind(exchange='test', queue='C', routing_key='C')

message = 'I AM HERE MY FRIEND !!!'
channel.basic_publish(exchange='test', routing_key='C', body=message)

channel.close()