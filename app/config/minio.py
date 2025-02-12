import os
import threading
import boto3
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, EndpointConnectionError
from app.config.custom_logger import logger


class MinioConnection:
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

        # 환경 변수에서 MinIO 정보 가져오기
        self.minio_host = os.getenv("MINIO_HOST", "localhost")
        self.minio_port = int(os.getenv("MINIO_PORT", 9000))
        self.minio_username = os.getenv("MINIO_USERNAME", "admin")
        self.minio_password = os.getenv("MINIO_PASSWORD", "adminadmin")

        # MinIO 엔드포인트 URL 생성
        self.endpoint_url = f"http://{self.minio_host}:{self.minio_port}"

        # boto3 클라이언트
        self.minio_client = None

        if auto_connect:
            self.connect()

        self._initialized = True

    def connect(self):
        """MinIO 연결 시도 및 확인"""
        try:
            self.minio_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.minio_username,
                aws_secret_access_key=self.minio_password,
                config=Config(signature_version="s3v4"),
            )

            # 🚀 연결 확인을 위해 버킷 목록을 조회
            response = self.minio_client.list_buckets()

            if not isinstance(response, dict):
                raise ValueError("MinIO에서 예상하지 못한 응답을 받음.")

            logger.info(f"🚀 MinIO 연결 성공! ({self.endpoint_url})")

        except EndpointConnectionError:
            self.minio_client = None
            logger.error(
                f"❌ MinIO 서버에 연결할 수 없습니다. (Endpoint: {self.endpoint_url})"
            )
        except NoCredentialsError:
            self.minio_client = None
            logger.error("❌ MinIO 인증 정보가 없습니다.")
        except Exception as e:
            self.minio_client = None
            logger.error(f"❌ MinIO 연결 실패: {e}")

    def list_buckets(self):
        """MinIO 버킷 리스트 조회 (연결 실패와 버킷 없음 구분)"""
        if not self.minio_client:
            logger.error("❌ MinIO에 연결되지 않았습니다. 버킷 조회 불가!")
            return None  # 버킷 없음이 아니라 '연결 실패' 상태임을 명확히 함.

        try:
            response = self.minio_client.list_buckets()
            bucket_list = [bucket["Name"] for bucket in response.get("Buckets", [])]

            if not bucket_list:
                logger.warning("⚠️ MinIO에 존재하는 버킷이 없습니다.")
            else:
                logger.info(f"📂 MinIO 버킷 리스트: {bucket_list}")

            return bucket_list
        except Exception as e:
            logger.error(f"❌ 버킷 조회 실패: {e}")
            return None

    def get_object(self, bucket_name, object_name):
        return self.minio_client.get_object(bucket_name, object_name)
