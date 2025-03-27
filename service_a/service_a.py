import service_a_pb2
import service_a_pb2_grpc
import json
from datetime import datetime
import grpc
import aio_pika
import asyncio

# RabbitMQ configuration
AMQP_URI = "amqp://guest:guest@rabbitmq"
RABBITMQ_RPC_QUEUE = "rpc_queue"
RABBITMQ_POISONED_QUEUE = "poisoned_queue"

# Service port
SERVICE_PORT = "50051"

# gRPC service implementation
class ServiceAImpl(service_a_pb2_grpc.ServiceAServicer):
    async def WhatTimeIsIt(self, request, context):
        return service_a_pb2.TimeResponse(current_time=datetime.now().isoformat())
    
    async def Sum(self, request, context):
        return service_a_pb2.SumResponse(total=sum(request.numbers))

# Process queue message       
async def process_message(message: aio_pika.IncomingMessage):
    async with message.process(): # Auto-ACK
        try:
            data = json.loads(message.body.decode())
            print(f"Received message: {data}")

            action = data.get("action")
            reply_to = data.get("reply_to")

            result = {}

            # Execute gRPC procedure
            async with grpc.aio.insecure_channel(f"localhost:{SERVICE_PORT}") as grpc_channel:
                # Get ServiceA instance
                stub = service_a_pb2_grpc.ServiceAStub(grpc_channel)

                if action == "time":
                    response = await stub.WhatTimeIsIt(service_a_pb2.Empty())
                    result["Result"] = response.current_time
                elif action == "sum":
                    numbers = data.get("numbers", [])
                    response = await stub.Sum(service_a_pb2.SumRequest(numbers=numbers))
                    result["Result"] = response.total
                else:
                    raise ValueError("Unknown action")
                
                # Send result back
                connection = await aio_pika.connect_robust(AMQP_URI)
                channel = await connection.channel()

                await channel.default_exchange.publish(
                    aio_pika.Message(body=json.dumps(result).encode(), content_type="application/json"),
                    routing_key=reply_to)

                await channel.close()
                await connection.close()
        except Exception as e:
            # Log the error
            print("INTERNAL SERVER ERROR")

            # Send an event to poisoned queue
            connection = await aio_pika.connect_robust(AMQP_URI)
            channel = await connection.channel()

            await channel.default_exchange.publish(
                    aio_pika.Message(body=json.dumps(str(e)).encode(), content_type="application/json"),
                    routing_key=RABBITMQ_POISONED_QUEUE)
            
            await channel.close()
            await connection.close()

# Start RabbitMQ consumer for rpc_queue
async def start_consumer():
    # Create connection
    connection = await aio_pika.connect_robust(AMQP_URI)
    channel = await connection.channel()

    # Declare queue
    queue = await channel.declare_queue(RABBITMQ_RPC_QUEUE, durable=True)
    
    # Start consuming
    await queue.consume(process_message)

    # Keeps the process running
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        print("Consumer has been stopped")

# Waits until RabbitMQ service is ready
async def wait_for_rabbitmq(max_retries=10, delay=5):
    for i in range(max_retries):
        try:
            connection = await aio_pika.connect_robust(AMQP_URI)
            async with connection:
                print("RabbitMQ is ready.")
            return
        except Exception:
            print(f"RabbitMQ is not ready. Try number {i+1}/{max_retries}...")
            await asyncio.sleep(delay)
    raise Exception("Could not connect to RabbitMQ after several intents.")

# Start gRPC server and RabbitMQ listener
async def serve():
    server = grpc.aio.server()

    # Set ServiceA implementation
    service_a_pb2_grpc.add_ServiceAServicer_to_server(ServiceAImpl(), server)

    server.add_insecure_port(f"[::]:{SERVICE_PORT}")
    await server.start()
    
    print(f"Service A running on {SERVICE_PORT} port...")

    # Wait until RabbitMQ is ready
    await wait_for_rabbitmq()

    # Start consuming
    # start_consumer()
    await start_consumer()

    # Wait for server ending
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())