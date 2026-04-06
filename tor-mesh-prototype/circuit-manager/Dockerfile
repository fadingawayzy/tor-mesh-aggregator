FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY circuit-manager/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY circuit-manager/manager.py .
COPY circuit-manager/entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh

RUN useradd -r -s /bin/false cmgr
USER cmgr

EXPOSE 9999

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
