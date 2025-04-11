import asyncio
import aio_pika
import json
from pymongo import MongoClient

RABBITMQ_URL = "amqp://guest:guest@localhost/"
MONGO_URI = "mongodb://localhost:27017"

async def main():
    client = MongoClient(MONGO_URI)
    db = client.cloudops
    events_collection = db.events

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue("cloudops_events", durable=True)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                event = json.loads(message.body)
                print("Event received:", event)
                events_collection.insert_one(event)

if __name__ == "__main__":
    asyncio.run(main())
