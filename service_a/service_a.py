import grpc
import pika
import service_a_pb2
import service_a_pb2_grpc
import json
from datetime import datetime
from concurrent import futures
import logging
import time
from nameko.rpc import rpc
# from nameko.messaging import consume
from kombu import Connection, Exchange, Queue, Producer, Consumer

# RabbitMQ configuration
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_RPC_QUEUE = "rpc_queue"
RABBITMQ_POISONED_QUEUE = "poisoned_queue"

# Service port
SERVICE_PORT = "50051"

AMQP_URI = "pyamqp://guest:guest@rabbitmq"

def what_time_is_it():
    return datetime.now().isoformat()

def get_sum(numbers):
    return sum(numbers)

def process_message(body, message):
    try:
        data = json.loads(message.body)
        action = data.get("action")
        reply_to = data.get("reply_to")

        print (f"Received message: {data}")
        print(f"Action: {action}")
        print(f"Reply to: {reply_to}")

        response = {}

        if action == "time":
            response["result"] = what_time_is_it()
        elif action == "sum":
            numbers = data.get("numbers", [])
            response["result"] = get_sum(numbers)
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
            producer.publish(response, routing_key=reply_to, serializer="json")
    except Exception as e:
        # TODO: Send message to poison queue
        print(str(e))

def start_consumer():
    with Connection(AMQP_URI) as conn:
        channel = conn.channel()
        queue = Queue(RABBITMQ_RPC_QUEUE, exchange="")
        queue.maybe_bind(conn)
        queue.declare()

        with Consumer(channel, queues=[queue], callbacks=[process_message], accept=["json"]):
            print("Esperando mensajes...")
            while True:
                conn.drain_events()  # Mantiene el consumo activo

def wait_for_rabbitmq(max_retries=10, delay=5):
    """Espera a que RabbitMQ esté listo antes de continuar."""
    for i in range(max_retries):
        try:
            with Connection(AMQP_URI) as conn:
                conn.ensure_connection(max_retries=1)  # Prueba la conexión
            print("✅ RabbitMQ está listo")
            return
        except Exception:
            print(f"⏳ RabbitMQ no está listo. Intento {i+1}/{max_retries}...")
            time.sleep(delay)
    raise Exception("❌ No se pudo conectar a RabbitMQ después de varios intentos.")

if __name__ == "__main__":
    wait_for_rabbitmq()
    start_consumer()

# class ServiceA:
#     name = "service_a"

#     @rpc
#     def what_time_is_it(self):
#         return datetime.now().isoformat()

#     @rpc
#     def sum(self, numbers):
#         return sum(numbers)

#     @consume(RABBITMQ_RPC_QUEUE)
#     def process_message(self, message):
#         try:
#             data = json.loads(message)
#             action = data.get("action")
#             reply_to = data.get("reply_to")

#             response = {}

#             if action == "time":
#                 response["result"] = self.what_time_is_it()
#             elif action == "sum":
#                 numbers = data.get("numbers", [])
#                 response["result"] = self.sum(numbers)
#             else:
#                 raise ValueError("Unknown action")

#             with Connection(AMQP_URI) as conn:
#                 channel = conn.channel()
#                 exchange = Exchange("", type="direct")
#                 queue = Queue(reply_to, exchange)
#                 queue.maybe_bind(conn)
#                 queue.declare()

#                 producer = Producer(channel, exchange)
#                 producer.publish(json.dumps(response), routing_key=reply_to)
#         except Exception as e:
#             # TODO: Send message to poison queue
#             print(str(e))



# Configure login
# logging.basicConfig(filename="rpc_logs.log", level=logging.INFO, format="%(asctime)s - %(message)s")

# gRPC service implementation
# class ServiceAImpl(service_a_pb2_grpc.ServiceAServicer):
#     def WhatTimeIsIt(self, request, context):
#         return service_a_pb2.TimeResponse(current_time=datetime.now().isoformat())
    
#     def Sum(self, request, context):
#         return service_a_pb2.SumResponse(total=sum(request.numbers))
    
# # RabbitMQ listener
# def listen_to_rpc_queue(channel):
#     # Define callback function that executes when there's a new event in the queue
#     def callback(ch, method, properties, body):
#         try:
#             data = json.loads(body)
#             action = data.get("action")

#             with grpc.insecure_channel(f"localhost:{SERVICE_PORT}") as grpc_channel:
#                 # Get ServiceA instance
#                 stub = service_a_pb2_grpc.ServiceAStub(grpc_channel)

#                 if action == "time":
#                     response = stub.WhatTimeIsIt(service_a_pb2.Empty())
#                     result = response.current_time
#                 elif action == "sum":
#                     numbers = data.get("numbers", [])
#                     response = stub.Sum(service_a_pb2.SumRequest(numbers=numbers))
#                     result = response.total
#                 else:
#                     raise ValueError("Unknown action")

#                 # Send result back
#                 ch.basic_publish(
#                     exchange="",
#                     routing_key=properties.reply_to,  # Properties is sent from sender.py message
#                     body=json.dumps(response),
#                 )

#                 # ch.basic_publish(exchange="", routing_key=RABBITMQ_POISONED_QUEUE, body=json.dumps({"error": result_message}))
#         except Exception as e:
#             # On error, send an event to poisoned queue
#             ch.basic_publish(exchange="", routing_key=RABBITMQ_POISONED_QUEUE, body=json.dumps({"error": str(e)}))

#         ch.basic_ack(delivery_tag=method.delivery_tag)

#     # Set which queue the listener is consuming events from (rpc_queue)
#     channel.basic_consume(queue=RABBITMQ_RPC_QUEUE, on_message_callback=callback, auto_ack=False)

#     print("Waiting for events on rpc_queue...")
    
#     channel.start_consuming()

# def wait_for_rabbitmq():
#     for i in range(10):
#         try:
#             connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
#             connection.close()
#             return  # Successful connection
#         except pika.exceptions.AMQPConnectionError:
#             print(f"Intent {i+1}: RabbitMQ is nto ready. Trying again in 5 seconds...")
#             time.sleep(5)
#     raise Exception("Could not connection to RabbitMQ after several intents.")

# # Start gRPC server and RabbitMQ listener
# def main():
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

#     # Set ServiceA implementation
#     service_a_pb2_grpc.add_ServiceAServicer_to_server(ServiceAImpl(), server)

#     server.add_insecure_port(f"[::]:{SERVICE_PORT}")
#     server.start()
    
#     print(f"Service A running on {SERVICE_PORT} port...")

#     # Wait until RabbitMQ is ready
#     wait_for_rabbitmq()

#     # RabbitMQ connection and channel
#     connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
#     channel = connection.channel()
    
#     # Declare queues
#     channel.queue_declare(queue=RABBITMQ_RPC_QUEUE)
#     channel.queue_declare(queue=RABBITMQ_POISONED_QUEUE)

#     # Start listening the channel
#     listen_to_rpc_queue(channel)

#     # Wait for ending
#     server.wait_for_termination()

# if __name__ == "__main__":
#     main()