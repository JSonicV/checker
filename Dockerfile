FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC \
    ENABLE_INTERNAL_CRON=1 \
    CHECKER_CRON_SCHEDULE="0 5 * * *"

WORKDIR /app

COPY . .

RUN apt-get update \
    && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir uv \
    && uv sync --frozen --no-dev \
    && chmod +x /app/docker/entrypoint.sh /app/docker/cron-refresh.sh

EXPOSE 8501

ENTRYPOINT ["/app/docker/entrypoint.sh"]

CMD ["/app/.venv/bin/python", "main.py", "dashboard", "--allow-missing-remote-db"]
