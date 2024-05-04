import sys
import time
import pika.exceptions


sys.path.append('/home/steve/Desktop/RABBITQUEUE')
from processors.connection import RabbitMQConnection

count = 0

def consume_channel(queue):
    while True:
        try:
            with RabbitMQConnection() as connection:
                channel = connection.get_channel(**{"type": "DOCUMENT"})
                if not channel:
                    print(f"Failed to get channel for queue '{queue}'. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue

                def callback(ch, method, properties, body):
                    global count
                    count += 1
                    print(f"Received message {count} from queue '{method.routing_key}': {body.decode()}")
                    time.sleep(10)
                    print(f"Processed message {count}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
                print(f"Started consuming from queue '{queue}'")
                channel.start_consuming()
        except pika.exceptions.StreamLostError as e:
            print(f"Failed to consume from queue '{queue}': {e}. Reconnecting and retrying in 5 seconds...")
            time.sleep(5)
            consume_channel(queue)
        except pika.exceptions.ChannelClosedByBroker as e:
            print(f"Channel for queue '{queue}' closed: {e}. Reconnecting and retrying in 5 seconds...")
            time.sleep(5)
            consume_channel(queue)
        except Exception as e:
            print(f"Unexpected error occurred: {e}. Reconnecting and retrying in 5 seconds...")
            time.sleep(5)
            consume_channel(queue)


if __name__ == "__main__":
    consume_channel('DOCUMENTQUEUE')
