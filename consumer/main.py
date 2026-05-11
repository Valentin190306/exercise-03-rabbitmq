import pika
import os
import json
import time
import sys

def main():
    rabbitmq_url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    queue_name = "node_events"

    # Retry connection to RabbitMQ
    connection = None
    while not connection:
        try:
            params = pika.URLParameters(rabbitmq_url)
            connection = pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ connection failed, retrying in 2 seconds...")
            time.sleep(2)

    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    def callback(ch, method, properties, body):
        try:
            event_data = json.loads(body)
            event = event_data.get("event")
            node_name = event_data.get("node_name")
            timestamp = event_data.get("timestamp")
            
            print(f"EVENT: {event} | node: {node_name} | time: {timestamp}", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Error processing message: {e}", file=sys.stderr)

    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print(f" [*] Waiting for messages in {queue_name}. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()
