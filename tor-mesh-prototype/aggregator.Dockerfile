FROM python:3.12-slim

ARG http_proxy
ARG https_proxy
ARG HTTP_PROXY
ARG HTTPS_PROXY

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev libxml2-dev libxslt-dev curl \
    && rm -rf /var/lib/apt/lists/*

RUN echo "curl_cffi>=0.6.2\nparsel>=1.9\naiosqlite>=0.20\nstructlog>=24.1\nAPScheduler>=3.10\nyarl>=1.9\ntenacity>=8.3\npydantic>=2.6.0\nrich>=13.7.0" > requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py forums.json ./
CMD python seed_db.py && python main.py
