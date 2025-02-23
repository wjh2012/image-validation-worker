from dotenv import load_dotenv
from blocking_app.config.minio import MinioConnection
from blocking_app.config.blocking_rabbitmq import BlockingConsumer
import json

from blocking_app.config.custom_logger import logger

load_dotenv()


def process_message(minio_client, msg):
    logger.info(f"📩 Received message: {msg}")
    try:
        data = json.loads(msg)
        file_name = data["file_name"]
        bucket_name = data["bucket"]

        logger.info(f"🔍 Processing file: {file_name} from bucket: {bucket_name}")
        try:
            response = minio_client.get_object(bucket=bucket_name, key=file_name)
            image_data = response["Body"].read()
            response["Body"].close()

            logger.info(
                f"✅ Successfully fetched '{file_name}'. Size: {len(image_data)} bytes"
            )

        except Exception as e:
            logger.error(f"❌ Error fetching image '{file_name}': {e}")

    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON 파싱 오류: {e}")
    except Exception as e:
        logger.error(f"❌ 메시지 처리 중 오류 발생: {e}")


def main():
    minio_client = MinioConnection()
    minio_client.list_buckets()
    rabbitmq_consumer = BlockingConsumer(auto_connect=True)
    try:
        rabbitmq_consumer.consume_messages(
            callback=lambda msg: process_message(minio_client, msg)
        )
    except Exception as ex:
        logger.error(f"❌ 오류 발생: {ex}")


if __name__ == "__main__":
    main()
