FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 먼저 의존성 파일만 복사
COPY pyproject.toml uv.lock /app/

WORKDIR /app

# 의존성 먼저 설치 (락 파일 기반)
RUN uv sync --no-cache

# 그 후에 나머지 소스 복사
COPY . /app

# 커맨드
CMD ["python", "app/main.py"]
