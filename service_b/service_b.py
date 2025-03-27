import json
import aio_pika
import asyncio

# RabbitMQ configuration
AMQP_URI = "amqp://guest:guest@rabbitmq"
RABBITMQ_POISONED_QUEUE = "poisoned_queue"

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

async def process_message(message: aio_pika.IncomingMessage):
    async with message.process(): # Auto-ACK
        event = json.loads(message.body.decode())
        print(f"Error event: {event}")

# Start consuming the queue
async def start_consumer():
    # Create connection
    connection = await aio_pika.connect_robust(AMQP_URI)
    channel = await connection.channel()

    # Declare queue
    queue = await channel.declare_queue(RABBITMQ_POISONED_QUEUE, durable=True)
    
    # Start consuming
    await queue.consume(process_message)

    # Keeps the process running
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        print("Consumer has been stopped")

async def main():
    print("Starting...")

    # Wait until RabbitMQ is ready
    await wait_for_rabbitmq()

    # Start listening
    await start_consumer()

    print("Waiting for messages...")

if __name__ == "__main__":
    asyncio.run(main())
