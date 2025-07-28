from functools import lru_cache

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict
import os

if os.getenv("RUN_MODE", "develop") == "develop":
    load_dotenv(dotenv_path=".env")

class Settings(BaseSettings):
    run_mode: str

    minio_host: str
    minio_port: int
    minio_user: str
    minio_password: str

    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_user: str
    rabbitmq_password: str

    rabbitmq_image_validation_consume_exchange: str
    rabbitmq_image_validation_consume_queue: str
    rabbitmq_image_validation_consume_routing_key: str

    rabbitmq_image_validation_publish_exchange: str
    rabbitmq_image_validation_publish_routing_key: str

    rabbitmq_image_validation_dlx: str
    rabbitmq_image_validation_dlx_routing_key: str

    database_url: str
    alembic_database_url: str

    model_config = SettingsConfigDict(
        env_file=".env" if os.getenv("RUN_MODE", "develop") == "develop" else None
    )


@lru_cache
def get_settings():
    return Settings()
