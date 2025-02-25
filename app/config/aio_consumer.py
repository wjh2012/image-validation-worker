import io
import json

import aio_pika
import logging
from aio_pika.abc import AbstractIncomingMessage

from app.config.aio_boto import AioBoto

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class AioConsumer:
    def __init__(
        self,
        minio_manager: AioBoto,
        amqp_url: str,
        queue_name: str,
        prefetch_count: int = 2,
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.minio_manager = minio_manager
        self._connection = None
        self._channel = None
        self._queue = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self.amqp_url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self.prefetch_count)

        self._queue = await self._channel.declare_queue(self.queue_name, durable=True)
        logging.info(f"✅ RabbitMQ 연결 성공: {self.amqp_url}, 큐: {self.queue_name}")

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process():
            logging.info("📩 메시지 수신!")
            data = json.loads(message.body)
            file_name = data["file_name"]
            bucket_name = data["bucket"]

            file_obj = io.BytesIO()
            await self.minio_manager.download_image_with_client(
                bucket_name=bucket_name, key=file_name, file_obj=file_obj
            )
            file_length = file_obj.getbuffer().nbytes
            file_obj.close()
            logging.info(f"✅ MinIO 파일 다운로드 성공: Size: {file_length} bytes")

    async def consume(self):
        if not self._queue:
            await self.connect()

        logging.info(f"📡 큐({self.queue_name})에서 메시지 소비 시작...")
        await self._queue.consume(self.on_message, no_ack=False)

    async def close(self):
        if self._connection:
            await self._connection.close()
            logging.info("🔴 RabbitMQ 연결 종료")
