import os

from dotenv import load_dotenv

load_dotenv(verbose=True)

minio_host = os.getenv("MINIO_HOST")
minio_port = os.getenv("MINIO_PORT")
minio_username = os.getenv("MINIO_USERNAME")
minio_password = os.getenv("MINIO_PASSWORD")
