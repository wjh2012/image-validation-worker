import os
import pika
import threading
from pika.exceptions import AMQPConnectionError

from app.config.custom_logger import logger


class RabbitMQConnection:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, auto_connect: bool = True, override: bool = False):
        if self._initialized and not override:
            return

        # 환경 변수에서 RabbitMQ 연결 정보 로드
        self.rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
        self.rabbitmq_port = int(os.getenv("RABBITMQ_PORT", 5672))
        self.rabbitmq_user = os.getenv("RABBITMQ_USER", "admin")
        self.rabbitmq_password = os.getenv("RABBITMQ_PASSWORD", "admin")
        self.rabbitmq_consume_queue = os.getenv(
            "RABBITMQ_CONSUME_QUEUE", "image_validate"
        )
        self.rabbitmq_publish_queue = os.getenv(
            "RABBITMQ_RESULT_QUEUE", "image_validate_result"
        )

        # 연결 객체와 채널 초기화
        self.connection = None
        self.channel = None

        if auto_connect:
            self.connect()

        self._initialized = True

    def connect(self):
        try:
            credentials = pika.PlainCredentials(
                self.rabbitmq_user, self.rabbitmq_password
            )
            parameters = pika.ConnectionParameters(
                host=self.rabbitmq_host,
                port=self.rabbitmq_port,
                credentials=credentials,
                heartbeat=600,  # heartbeat 설정
                blocked_connection_timeout=300,  # 연결 차단 타임아웃 설정
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            logger.info(
                f"✅ Connected to RabbitMQ at {self.rabbitmq_host}:{self.rabbitmq_port}"
            )
            # 소비와 발행에 사용할 두 큐 모두 선언
            self.create_queue(self.rabbitmq_consume_queue)
            self.create_queue(self.rabbitmq_publish_queue)
            logger.info("🚀 RabbitMQ 연결 성공!")

        except AMQPConnectionError as e:
            logger.error(f"❌ RabbitMQ 연결 실패: {e}")
            raise

    def create_queue(self, queue_name: str):
        if not self.channel or self.channel.is_closed:
            logger.error("채널이 닫혀 있습니다. 연결 상태를 확인하세요.")
            return

        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
            logger.info(f"🚦Queue '{queue_name}' is ready")
        except Exception as e:
            logger.error(f"❌ 큐 선언 실패: {e}")
            raise

    def publish_message(self, message, queue_name: str = None):
        if queue_name is None:
            queue_name = self.rabbitmq_publish_queue

        if not self.channel or self.channel.is_closed:
            logger.error("채널이 닫혀 있습니다. 메시지 전송 불가.")
            return

        try:
            if isinstance(message, str):
                message = message.encode()

            self.channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2  # 메시지 영속성 (durable)
                ),
            )
            logger.info(f"❌ Sent message to queue '{queue_name}'")
        except Exception as e:
            logger.error(f"❌ 메시지 전송 실패: {e}")
            raise

    def consume_messages(
        self, callback, queue_name: str = None, auto_ack: bool = False
    ):
        if queue_name is None:
            queue_name = self.rabbitmq_consume_queue

        if not self.channel or self.channel.is_closed:
            logger.error("채널이 닫혀 있습니다. 메시지 소비 불가.")
            return

        def wrapper(ch, method, properties, body):
            try:
                callback(body.decode())
                if not auto_ack:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"메시지 처리 중 오류 발생: {e}")
                if not auto_ack:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        try:
            self.channel.basic_consume(
                queue=queue_name, on_message_callback=wrapper, auto_ack=auto_ack
            )
            logger.info(
                f"🔄 Waiting for messages on '{queue_name}'. To exit press CTRL+C"
            )
            self.channel.start_consuming()
        except Exception as e:
            logger.error(f"메시지 소비 중 오류 발생: {e}")
            raise

    def close(self):
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logger.info("❌ Connection closed")
        except Exception as e:
            logger.error(f"연결 종료 중 오류 발생: {e}")
            raise

    def __enter__(self):
        if not self.connection or self.connection.is_closed:
            self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
