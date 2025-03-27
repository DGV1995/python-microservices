import json
import time
from kombu import Connection, Queue, Consumer

# RabbitMQ configuration
AMQP_URI = "pyamqp://guest:guest@rabbitmq"
RABBITMQ_POISONED_QUEUE = "poisoned_queue"

# Waits until RabbitMQ service is ready
def wait_for_rabbitmq(max_retries=10, delay=5):
    for i in range(max_retries):
        try:
            with Connection(AMQP_URI) as conn:
                conn.ensure_connection(max_retries=1)  # Test the connection
            print("RabbitMQ is ready.")
            return
        except Exception:
            print(f"⏳ RabbitMQ is not ready. Try number {i+1}/{max_retries}...")
            time.sleep(delay)
    raise Exception("Could not connect to RabbitMQ after several intents.")

# Start consuming the queue
def listen_poisoned_events():
    # Define callback function that executes when there's a new event in the queue
    def callback(body, message):
        event = json.loads(message.body)
        error_message = f"Error event: {event}"

        # logging.error(error_message)
        print(error_message)

    # Start consuming
    with Connection(AMQP_URI) as conn:
        channel = conn.channel()
        queue = Queue(RABBITMQ_POISONED_QUEUE, exchange="")
        queue.maybe_bind(conn)
        queue.declare()

        with Consumer(channel, queues=[queue], callbacks=[callback], accept=["json"]):
            print("Waiting for messages...")
            while True:
                conn.drain_events()
    
    print("Waiting events on poisoned_queue...")

def main():
    print("Starting...")

    # Wait until RabbitMQ is ready
    wait_for_rabbitmq()

    # Start listening
    listen_poisoned_events()

    print("Waiting for messages...")

if __name__ == "__main__":
    main()
