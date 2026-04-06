FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    privoxy \
    curl \
    netcat-openbsd \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/log/privoxy /var/run/privoxy \
    && chown -R 100:65534 /var/log/privoxy /var/run/privoxy

COPY privoxy/config /etc/privoxy/config
COPY privoxy/entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh \
    && chmod 644 /etc/privoxy/config

EXPOSE 8118

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
