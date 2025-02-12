import os

from dotenv import load_dotenv
from app.config.minio import MinioConnection
from app.config.rabbitmq import RabbitMQConnection
import json

from app.config.custom_logger import logger

# 환경 변수 로드
load_dotenv()


def process_message(minio_client, msg):
    logger.info(f"📩 Received message: {msg}")
    try:
        data = json.loads(msg)
        image_keys = data.get("image_keys", [])
        if not image_keys:
            logger.warning("메시지에 'image_keys' 필드가 없습니다.")
            return

        for image_key in image_keys:
            try:
                response = minio_client.get_object(
                    os.getenv("MINIO_BUCKET", "your-bucket"), image_key
                )

                image_data = response["Body"].read()
                response["Body"].close()
                logger.info(
                    f"✅ Image '{image_key}' fetched successfully. Size: {len(image_data)} bytes"
                )

            except Exception as e:
                logger.error(f"❌ Error fetching image '{image_key}': {e}")

    except json.JSONDecodeError as e:
        logger.error(f"JSON 파싱 오류: {e}")
    except Exception as e:
        logger.error(f"메시지 처리 중 오류 발생: {e}")


def main():
    minio_client = MinioConnection()
    rabbitmq_consumer = RabbitMQConnection(auto_connect=True)

    try:
        rabbitmq_consumer.consume_messages(
            callback=lambda msg: process_message(minio_client, msg)
        )
    except Exception as ex:
        logger.error(f"❌ 오류 발생: {ex}")


if __name__ == "__main__":
    main()
