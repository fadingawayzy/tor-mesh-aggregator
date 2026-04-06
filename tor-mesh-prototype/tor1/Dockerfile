FROM golang:1.22 AS builder

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG http_proxy
ARG https_proxy

ENV GOPROXY=https://goproxy.cn,direct

RUN git clone --depth=1 \
    https://gitlab.torproject.org/tpo/anti-censorship/pluggable-transports/lyrebird.git \
    /tmp/lyrebird \
    && cd /tmp/lyrebird \
    && go build -o /usr/local/bin/lyrebird ./cmd/lyrebird

FROM debian:bookworm-slim

LABEL stage="tor-mesh-prototype"
LABEL node="tor1"

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG http_proxy
ARG https_proxy

RUN if [ -n "${http_proxy}" ]; then \
    echo "Acquire::http::Proxy \"${http_proxy}\";" > /etc/apt/apt.conf.d/proxy.conf; \
    echo "Acquire::https::Proxy \"${http_proxy}\";" >> /etc/apt/apt.conf.d/proxy.conf; \
    fi

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    tor \
    tor-geoipdb \
    curl \
    netcat-openbsd \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* /etc/apt/apt.conf.d/proxy.conf

COPY --from=builder /usr/local/bin/lyrebird /usr/local/bin/lyrebird
RUN /usr/local/bin/lyrebird --version

RUN mkdir -p \
    /var/lib/tor \
    /var/log/tor \
    /var/run/tor/cookies \
    /etc/tor/runtime \
    && chown -R debian-tor:debian-tor \
    /var/lib/tor \
    /var/log/tor \
    /var/run/tor \
    /etc/tor/runtime \
    && chmod 700 /var/lib/tor /var/run/tor

COPY tor1/torrc /etc/tor/runtime/torrc.template
COPY tor1/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY scripts/healthcheck_tor.sh /usr/local/bin/healthcheck_tor.sh

RUN chmod +x \
    /usr/local/bin/entrypoint.sh \
    /usr/local/bin/healthcheck_tor.sh \
    && chown debian-tor:debian-tor /etc/tor/runtime/torrc.template

USER debian-tor

EXPOSE 9050 9051

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
