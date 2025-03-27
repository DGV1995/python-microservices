import time
import uuid
import asyncio
import aio_pika
import json
import random

# RabbitMQ configuration
AMQP_URI = "amqp://guest:guest@rabbitmq"
RABBITMQ_RPC_QUEUE = "rpc_queue"

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

# Gets a list of random numbers
def get_random_numbers(numbers_length):
    return [random.randint(1, 100) for i in range(numbers_length)]

async def main():
    # Wait for RabbitMQ
    await wait_for_rabbitmq()

    running = True

    time = "what_time_is_it"
    sum = "sum"

    while running:
        tasks = []

        # Ask the user for number of 'what_time_is_it' procedures to execute
        n_what_time_input = input("\nHow many times do you want to run 'what_time_is_it' procedure?\n")

        # Check if the value is a valid integer
        try:
            n_what_time = int(n_what_time_input)
        except ValueError:
            print("Invalid number")
            n_what_time = 0

        if (n_what_time > 0):
            # Add a new 'what_time_is_it' async task to the list
            tasks += [send_message(uuid.uuid4(), time) for i in range(n_what_time)]

        # Ask the user for number of 'sum' procedures to execute
        n_sum_input = input("\nHow many times do you want to run 'sum' procedure?\n")

        # Check if the value is a valid integer
        try:
            n_sum = int(n_sum_input)
        except ValueError:
            print("Invalid number\n")
            n_sum = 0

        if (n_sum > 0):
            numbers_length_input = input("\nWith how many random numbers?\n")

            try:
                numbers_length = int(numbers_length_input)
            except ValueError:
                print("Invalid number\n")
                numbers_length = 0

            if numbers_length > 0:
                # Add a new 'sum' async task to the list
                tasks += [send_message(uuid.uuid4(), sum, get_random_numbers(numbers_length)) for i in range(n_sum)]

        print("\n")
        await asyncio.gather(*tasks)

        # Ask the user if run the process again
        repeat_input = input("\nDo you want to repeat? [y/n]\n")
        running = repeat_input == "y"

if __name__ == "__main__":
    asyncio.run(main())