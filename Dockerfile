FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

COPY kafka_analyzer/ ./kafka_analyzer/
COPY setup.py .
COPY README.md .
COPY LICENSE .

RUN pip install --user --no-cache-dir .

FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 analyzer
COPY --from=builder /root/.local /home/analyzer/.local

ENV PATH=/home/analyzer/.local/bin:$PATH
WORKDIR /home/analyzer
USER analyzer

RUN mkdir -p /home/analyzer/reports

ENTRYPOINT ["kafka-analyzer"]
CMD ["--help"]
