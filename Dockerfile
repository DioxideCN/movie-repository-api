# Author: Dioxide_CN
FROM python:3.10.8-slim AS builder
WORKDIR /app

RUN pip install "poetry==1.8.3"

COPY pyproject.toml poetry.lock ./

RUN poetry export -f requirements.txt --output requirements.txt --without-hashes
RUN pip install --user -r requirements.txt

# FastAPI application runs on port 8080
FROM python:3.10.8-slim
WORKDIR /app
COPY --from=builder /root/.local /root/.local

ENV PATH=/root/.local/bin:$PATH
COPY . .
CMD ["uvicorn", "movie_repository.main:app", "--host", "0.0.0.0", "--port", "8080"]
