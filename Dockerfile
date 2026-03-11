FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV LANG=es_ES.UTF-8
ENV LANGUAGE=es_ES:es
ENV LC_ALL=es_ES.UTF-8

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl xvfb xauth locales \
    && sed -i 's/^# *\(es_ES.UTF-8 UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen es_ES.UTF-8 \
    && update-locale LANG=es_ES.UTF-8 LC_ALL=es_ES.UTF-8 LANGUAGE=es_ES:es \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY requirements.txt .
RUN uv pip install --system -r requirements.txt
RUN playwright install --with-deps chromium

COPY src ./src
COPY docker-entrypoint.sh ./docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

EXPOSE 8000

CMD ["bash", "/app/docker-entrypoint.sh"]
