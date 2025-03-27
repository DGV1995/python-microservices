import service_a_pb2
import service_a_pb2_grpc
import json
from datetime import datetime
from concurrent import futures
import time
from nameko.rpc import rpc
from kombu import Connection, Exchange, Queue, Producer, Consumer
import grpc

# RabbitMQ configuration
AMQP_URI = "pyamqp://guest:guest@rabbitmq"
RABBITMQ_RPC_QUEUE = "rpc_queue"
RABBITMQ_POISONED_QUEUE = "poisoned_queue"

# Service port
SERVICE_PORT = "50051"

# gRPC service implementation
class ServiceAImpl(service_a_pb2_grpc.ServiceAServicer):
    def WhatTimeIsIt(self, request, context):
        return service_a_pb2.TimeResponse(current_time=datetime.now().isoformat())
    
    def Sum(self, request, context):
        return service_a_pb2.SumResponse(total=sum(request.numbers))
        
def process_message(body, message):
    try:
        data = json.loads(message.body)
        action = data.get("action")
        reply_to = data.get("reply_to")

        print (f"Received message: {data}")
        print(f"Action: {action}")
        print(f"Reply to: {reply_to}")

        result = {}

        # Execute gRPC procedure
        with grpc.insecure_channel(f"localhost:{SERVICE_PORT}") as grpc_channel:
            # Get ServiceA instance
            stub = service_a_pb2_grpc.ServiceAStub(grpc_channel)

            if action == "time":
                response = stub.WhatTimeIsIt(service_a_pb2.Empty())
                result["Result"] = response.current_time
            elif action == "sum":
                numbers = data.get("numbers", [])
                response = stub.Sum(service_a_pb2.SumRequest(numbers=numbers))
                result["Result"] = response.total
            else:
                raise ValueError("Unknown action")

            # Send result back
            with Connection(AMQP_URI) as conn:
                channel = conn.channel()
                exchange = Exchange("", type="direct")
                queue = Queue(reply_to, exchange)
                queue.maybe_bind(conn)
                queue.declare()

                producer = Producer(channel, exchange)
                producer.publish(result, routing_key=reply_to, serializer="json")
    except Exception as e:
        # Log the error
        print("INTERNAL SERVER ERROR")

        # Send an event to poisoned queue
        with Connection(AMQP_URI) as conn:
            channel = conn.channel()
            exchange = Exchange("", type="direct")
            queue = Queue(reply_to, exchange)
            queue.maybe_bind(conn)
            queue.declare()

            producer = Producer(channel, exchange)
            producer.publish({"Error": str(e)}, routing_key=RABBITMQ_POISONED_QUEUE, serializer="json")

# Start RabbitMQ consumer for rpc_queue
def start_consumer():
    with Connection(AMQP_URI) as conn:
        channel = conn.channel()
        queue = Queue(RABBITMQ_RPC_QUEUE, exchange="")
        queue.maybe_bind(conn)
        queue.declare()

        with Consumer(channel, queues=[queue], callbacks=[process_message], accept=["json"]):
            print("Waiting for messages...")
            while True:
                conn.drain_events()

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

# Start gRPC server and RabbitMQ listener
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Set ServiceA implementation
    service_a_pb2_grpc.add_ServiceAServicer_to_server(ServiceAImpl(), server)

    server.add_insecure_port(f"[::]:{SERVICE_PORT}")
    server.start()
    
    print(f"Service A running on {SERVICE_PORT} port...")

    # Wait until RabbitMQ is ready
    wait_for_rabbitmq()

    # Start consuming
    start_consumer()

    # Wait for server ending
    server.wait_for_termination()

if __name__ == "__main__":
    serve()