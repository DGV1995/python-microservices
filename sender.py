import time
import argparse
import logging
import pika
import json
import uuid
from kombu import Connection, Exchange, Queue, Producer, Consumer

# Configure login
logging.basicConfig(filename="rpc_logs.log", level=logging.INFO, format="%(asctime)s - %(message)s")

# RabbitMQ configuration
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_RPC_QUEUE = "rpc_queue"
RABBITMQ_RESULTS_QUEUE = "results"

AMQP_URI = "pyamqp://guest:guest@rabbitmq"

# class RpcClient:
#     def __init__(self):
#         self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
#         self.channel = self.connection.channel()
#         self.channel.queue_declare(queue=RABBITMQ_RPC_QUEUE)

#         # Create a queue for receiving answers
#         result = self.channel.queue_declare(queue="", exclusive=True)
#         self.callback_queue = result.method.queue

#         self.channel.basic_consume(
#             queue=self.callback_queue,
#             on_message_callback=self.on_response,
#             auto_ack=True
#         )

#         self.response = None
#         self.corr_id = None

#     def on_response(self, ch, method, props, body):
#         if self.corr_id == props.correlation_id:
#             self.response = body

#     def call(self, action, numbers=None):
#         self.response = None
#         self.corr_id = str(uuid.uuid4())

#         # Create message
#         message = {"action": action}
#         if numbers:
#             message["numbers"] = numbers

#         print(f"Sending: {message}")

#         # start_time = time.time()

#         # Send message to rpc_queue
#         self.channel.basic_publish(
#             exchange="",
#             routing_key=RABBITMQ_RPC_QUEUE,
#             properties=pika.BasicProperties(
#                 reply_to=self.callback_queue,
#                 correlation_id=self.corr_id,
#             ),
#             body=json.dumps(message)
#         )

#         print(f"Message sent to {RABBITMQ_RPC_QUEUE} with correlation_id {self.corr_id}. Waiting for response...")

#         # Wait for the answer
#         while self.response is None:
#             self.connection.process_data_events()

#         return str(self.response)

def send_rpc_request(channel, action, numbers=None):
    # Create event message
    message = {"action": action}

    if numbers:
        message["numbers"] = numbers

    # Send to RabbitMQ
    # start_time = time.time()
    channel.basic_publish(exchange="", routing_key=RABBITMQ_RPC_QUEUE, body=json.dumps(message))

    print(f"Message sent to {RABBITMQ_RPC_QUEUE}")
    print("Waiting for response...")

def send_message(action, numbers=None):
    reply_queue = f"reply_queue_{uuid.uuid4()}"  # Unique answer queue
    payload = {"action": action, "reply_to": reply_queue}

    if numbers:
        payload["numbers"] = numbers

    with Connection(AMQP_URI) as conn:
        channel = conn.channel()
        exchange = Exchange("", type="direct")
        rpc_queue = Queue(RABBITMQ_RPC_QUEUE, exchange)
        rpc_queue.maybe_bind(conn)
        rpc_queue.declare()

        # Publish to rpc_queue
        producer = Producer(channel, exchange)
        producer.publish(payload, routing_key=RABBITMQ_RPC_QUEUE, serializer="json")

        print(f"Sent Message: {payload}")

        # Create queue for response
        reply_queue_obj = Queue(reply_queue, exchange="")
        reply_queue_obj.maybe_bind(conn)
        reply_queue_obj.declare()

        # Wait for response
        response = None
        def handle_response(body, message):
            nonlocal response
            response = body

        with Consumer(channel, queues=[reply_queue_obj], callbacks=[handle_response], accept=["json"]):
            while response is None:
                conn.drain_events()

        return response

def parse_and_send():
    # Get parser instance
    parser = argparse.ArgumentParser()

    parser.add_argument("request_type", choices=["time", "sum"], help="Action type")
    parser.add_argument("--numbers", nargs="*", type=int, help="Numbers list for 'sum' action")

    # Get args
    args = parser.parse_args()

    print(f"Body: {args}")

    if args.request_type == "sum" and args.numbers is None:
        print("You need to pass an integers array for 'sum' action")
    else:
        # client = RpcClient()
        # response = client.call(action=args.request_type, numbers=args.numbers)
        # Send message
        # response = send_rpc_request(channel=channel, action=args.request_type, numbers=args.numbers)
        response = send_message(args.request_type, args.numbers)

    print(f"Result: {response}")

if __name__ == "__main__":
    # Parse and send request
    parse_and_send()