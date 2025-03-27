import pika
import json
import logging
import time

# RabbitMQ configuration
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_POISONED_QUEUE = "poisoned_queue"

logging.basicConfig(filename="errors.log", level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

def wait_for_rabbitmq():
    for i in range(10):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            connection.close()
            return  # Successful connection
        except pika.exceptions.AMQPConnectionError:
            print(f"Intent {i+1}: RabbitMQ is nto ready. Trying again in 5 seconds...")
            time.sleep(5)
    raise Exception("Could not connection to RabbitMQ after several intents.")

def listen_poisoned_events(channel):
    # Define callback function that executes when there's a new event in the queue
    def callback(ch, method, properties, body):
        event = json.loads(body)
        error_message = f"Error event: {event}"

        logging.error(error_message)
        print(error_message)

    # Set which queue the listener is consuming events from (poisoned_queue)
    channel.basic_consume(queue=RABBITMQ_POISONED_QUEUE, on_message_callback=callback, auto_ack=True)

    # Start consuming
    print("Waiting events on poisoned_queue...")
    channel.start_consuming()

def main():
    # Wait until RabbitMQ is ready
    wait_for_rabbitmq()

    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Declare queues
    channel.queue_declare(queue=RABBITMQ_POISONED_QUEUE)

    # Start listening
    listen_poisoned_events(channel)

if __name__ == "__main__":
    main()
