import time
import pika.exceptions
import sys
import multiprocessing

sys.path.append('/home/steve/Desktop/RABBITQUEUE')
from processors.connection import RabbitMQConnection

count = 0

def consume_channel(queue):
    while True:
        try:
            with RabbitMQConnection() as connection:
                if queue == "SMSQUEUE":
                    channel = connection.get_channel(**{"type": "SMS"})
                else:
                    channel = connection.get_channel(**{"type": "DOCUMENT"})
                if not channel:
                    print(f"Failed to get channel for queue '{queue}'. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue

                def callback(ch, method, properties, body):
                    global count
                    count += 1
                    print(f"Received message {count} from queue '{method.routing_key}': {body}")
                    time.sleep(10)
                    print(f"Processed data from {method.routing_key} {count} I am at the workers")
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
                print(f"Started consuming from queue '{queue}'")
                try:
                    channel.start_consuming()
                except KeyboardInterrupt:
                    print("Stopping consumer...")
                    channel.stop_consuming()
        except pika.exceptions.StreamLostError as e:
            print(f"Failed to consume from queue '{queue}': {e}. Reconnecting and retrying in 5 seconds...")
            time.sleep(5)
        except pika.exceptions.ChannelClosedByBroker as e:
            print(f"Channel for queue '{queue}' closed: {e}. Reconnecting and retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error occurred: {e}. Reconnecting and retrying in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=8)
    pool.map(consume_channel, ['SMSQUEUE', 'DOCUMENTQUEUE'] * 4)
    pool.close()
    pool.join()
