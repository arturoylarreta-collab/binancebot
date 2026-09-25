# crypto_scalper — 24/7 trading bot + embedded dashboard (paper or Binance testnet)
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY crypto_scalper ./crypto_scalper
COPY pyproject.toml README.md ./

# Unprivileged runtime user; the state (SQLite audit trail) lives on a volume.
RUN useradd --create-home --uid 10001 bot && mkdir -p /var/data && chown bot:bot /var/data
USER bot

ENV RUN_MODE=paper \
    EXECUTION_VENUE=paper \
    PAPER_DB_PATH=/var/data/bot.db \
    LOG_DIR=/var/data/logs \
    LOG_TO_FILE=false \
    LOG_FORMAT=kv \
    PORT=8080

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
  CMD python -c "import os,urllib.request,sys; urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"PORT\",\"8080\")}/healthz', timeout=4); sys.exit(0)" || exit 1

CMD ["python", "-m", "crypto_scalper.main", "--mode", "paper"]
