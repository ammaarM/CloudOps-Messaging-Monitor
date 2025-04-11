from fastapi import FastAPI
from pydantic import BaseModel
import aio_pika
import json
import os

app = FastAPI()

class Event(BaseModel):
    event_type: str
    payload: dict

RABBITMQ_URL = "amqp://guest:guest@rabbitmq/"

@app.post("/event")
async def publish_event(event: Event):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue("cloudops_events", durable=True)

    message = aio_pika.Message(
        json.dumps(event.dict()).encode()
    )
    await channel.default_exchange.publish(message, routing_key=queue.name)
    await connection.close()
    return {"status": "event queued"}
