import asyncio
import logging

import aio_pika
from aio_pika.abc import AbstractIncomingMessage


async def on_message(message: AbstractIncomingMessage) -> None:
    async with message.process():
        print("get message")
        await asyncio.sleep(5)
        print(f"Message body is: {message.body!r}")


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(
        "amqp://admin:admin@192.168.45.131/",
    )

    queue_name = "image_validation"

    async with connection:
        channel = await connection.channel()

        await channel.set_qos(prefetch_count=2)
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.consume(on_message, no_ack=False)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
