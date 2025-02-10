import threading
from dotenv import load_dotenv

# 로그 및 연결 관련 모듈 가져오기
from app.config.custom_logger import logger
from app.config.minio_connection import MinioConnection
from app.config.rabbitmq_connection import RabbitMQConnection

# 환경 변수 로드
load_dotenv()


def process_message(msg):
    logger.info(f"📩 Received message: {msg}")


def start_rabbitmq():
    try:
        with RabbitMQConnection(auto_connect=True) as rabbitmq:
            rabbitmq.consume_messages(callback=process_message)
    except Exception as e:
        logger.error(f"❌ RabbitMQ 소비 중 오류 발생: {e}")


def main():
    try:
        rabbitmq_thread = threading.Thread(target=start_rabbitmq, daemon=True)
        rabbitmq_thread.start()

        minio_client = MinioConnection()

        rabbitmq_thread.join()

    except Exception as ex:
        logger.error(f"❌ 오류 발생: {ex}")


if __name__ == "__main__":
    main()
