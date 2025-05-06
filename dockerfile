FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-dev \
        libpq-dev \
        libpq5\
        gcc \
        g++\
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y python3-dev libpq-dev \
    && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY . .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

