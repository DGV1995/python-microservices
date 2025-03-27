import time
import uuid
import asyncio
import aio_pika
import json

# RabbitMQ configuration
AMQP_URI = "amqp://guest:guest@rabbitmq"
RABBITMQ_RPC_QUEUE = "rpc_queue"


# Consumes from que queue until it receives a new message
async def wait_for_response(queue, timeout=60):
    future = asyncio.get_event_loop().create_future()

    async def on_message(message: aio_pika.IncomingMessage):
        # Callback for processing the message
        async with message.process():
            if not future.done():
                future.set_result(json.loads(message.body.decode()))

    # Start consuming
    await queue.consume(on_message)

    try:
        return await asyncio.wait_for(future, timeout=timeout)
    except asyncio.TimeoutError:
        print("Timeout: Did not get a response in the required time.")
        return {"Result": "Timeout waiting for response"}

# Sends message to rpc_queue and waits for the response
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
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=100)

    start_time = time.time()

    await channel.default_exchange.publish(
        aio_pika.Message(body=json.dumps(payload).encode(), content_type="application/json"),
        routing_key=RABBITMQ_RPC_QUEUE)
    
    print(f"Sent Message: {payload}")

    # Create queue for response
    reply_queue_obj = await channel.declare_queue(reply_queue, durable=True, auto_delete=True)

    # Wait for the answer
    try:
        response = await wait_for_response(reply_queue_obj)
    except asyncio.TimeoutError:
        response = {"Result": "Timeout waiting for response"}
    except Exception as e:
        response = {"Result": f"ERROR: {str(e)}"}

    end_time = time.time()

    latency = end_time - start_time

    result = {
        "operation_id": str(operation_id),
        "result": response.get("Result"),
        "answer time": f"{latency:.4f} seconds"
    }

    # return result
    print(f"Response: {result}")

async def main():
    # Concurrent calls parameters
    request_ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
    actions = ["time", "sum", "time", "sum", "time", "sum", "time"]
    numbersArray = [None, [2, 2], None, [10, 20], None, [15, 50], None]

    # Execute the requests
    tasks = [send_message(id, action, numbers) for id, action, numbers in zip(request_ids, actions, numbersArray)]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())