import asyncio
import os

from app.config.aio_boto import AioBoto
from app.config.aio_consumer import AioConsumer

minio_host = os.getenv("MINIO_HOST", "localhost")
minio_port = int(os.getenv("MINIO_PORT", 9000))

rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
rabbitmq_port = int(os.getenv("RABBITMQ_PORT", 5672))
rabbitmq_user = os.getenv("RABBITMQ_USER", "admin")
rabbitmq_password = os.getenv("RABBITMQ_PASSWORD", "admin")
rabbitmq_consume_queue = os.getenv("RABBITMQ_CONSUME_QUEUE", "image_validate")


async def main():
    consumer = AioConsumer(
        amqp_url=f"amqp://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_host}:{rabbitmq_port}/",
        queue_name=rabbitmq_consume_queue,
    )
    minio = AioBoto(f"http://{minio_host}:{minio_port}")

    await consumer.connect()
    await consumer.consume()

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
