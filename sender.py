import time
import uuid
import asyncio
import aio_pika
import json

# RabbitMQ configuration
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_RPC_QUEUE = "rpc_queue"
RABBITMQ_RESULTS_QUEUE = "results"

# AMQP_URI = "pyamqp://guest:guest@rabbitmq"
AMQP_URI = "amqp://guest:guest@rabbitmq"

async def send_message(operation_id, action, numbers=None):
    reply_queue = f"reply_queue_{operation_id}"  # Unique answer queue

    payload = {
        "operation_id": str(operation_id),
        "action": action, 
        "reply_to": reply_queue
    }

    if numbers:
        payload["numbers"] = numbers

    connection = await aio_pika.connect_robust(AMQP_URI)
    
    async with connection:
        channel = await connection.channel()

        try:
            start_time = time.time()

            await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(payload).encode(), content_type="application/json"),
                routing_key=RABBITMQ_RPC_QUEUE)
            
            print(f"Sent Message: {payload}")

            # Create queue for response
            reply_queue_obj = await channel.declare_queue(reply_queue, durable=True)

            # Wait for the answer
            try:
                response_message = await reply_queue_obj.get(timeout=10) # 10 seconds timeout

                if response_message:
                    response = json.loads(response_message.body.decode())
                else:
                    response = {"Result": "No response"}
            except asyncio.TimeoutError:
                response = {"Result": "Timeout waiting for response"}

            end_time = time.time()

            latency = end_time - start_time

            result = {
                "operation_id": str(operation_id),
                "result": response.get("Result"),
                "answer time": f"{latency:.4f} seconds"
            }

            # return result
            print(f"Response: {result}")
        finally:
            await channel.close()

async def main():
    # Concurrent calls parameters
    request_ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
    actions = ["time", "sum", "time", "time"]
    numbersArray = [None, [2, 2], None, None]

    # Execute the requests
    tasks = [send_message(id, action, numbers) for id, action, numbers in zip(request_ids, actions, numbersArray)]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())