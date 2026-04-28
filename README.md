<h1 align="center">Kafka AxonOps Analyzer</h1>

<p align="center">
  <strong>Comprehensive Apache Kafka cluster analysis powered by the AxonOps API</strong>
</p>

> ⚠️ **DEVELOPMENT STATUS**: This project is under active development and not yet ready for production use.

---

## Overview

Kafka AxonOps Analyzer connects to the [AxonOps](https://axonops.com) monitoring platform and produces a structured diagnostic report for an Apache Kafka cluster. It pulls topic metadata, consumer groups, broker configuration, ACLs, and time-series metrics, then runs a set of analyzers that flag risks and best-practice deviations.

### Analysis categories

| Category | What it checks |
|---|---|
| **Infrastructure** | Broker count, rack awareness, host CPU/memory/disk, log-dir balance |
| **Configuration** | Broker `server.properties`, JVM args, divergence between brokers |
| **Operations** | Under-replicated/offline partitions, ISR churn, controller stability, request-handler saturation, request latency, leader skew |
| **Topics & Consumers** | Replication factor, partitions, retention, cleanup policy, consumer-group lag, schema-registry coverage |
| **Security** | Authorizer + ACLs, listener encryption (SSL/SASL_SSL), super.users, SASL mechanism, ZooKeeper ACLs |

## Requirements

- Python 3.11+
- Access to an AxonOps deployment that monitors your Kafka cluster
- An AxonOps API token

## Installation

```bash
git clone https://github.com/axonops/kafka-analyzer.git
cd kafka-analyzer

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .

kafka-analyzer --config config.yaml
```

Or run directly without installing:

```bash
python -m kafka_analyzer --config config.yaml
```

## Quick start

Create a `config.yaml`:

```yaml
cluster:
  org: "your-organization"
  cluster: "your-kafka-cluster"
  cluster_type: "kafka"

axonops:
  api_url: "https://dash.axonops.cloud/"
  token: "your-api-token"   # or set AXONOPS_API_TOKEN

analysis:
  hours: 24
```

Run:

```bash
kafka-analyzer --config config.yaml
```

Reports land in `./reports/` (Markdown + JSON; pass `--pdf` for a PDF copy if WeasyPrint is installed).

### Command-line options

```
kafka-analyzer [OPTIONS]

  --config PATH         Path to configuration file (required)
  --output-dir PATH     Output directory for reports (default: ./reports)
  --verbose             Enable debug logging
  --pdf                 Also generate a PDF report (requires WeasyPrint)
```

### Environment variables

- `AXONOPS_API_TOKEN` — API token (alternative to specifying it in the config file)

See [`example_config.yaml`](example_config.yaml) for the full set of tunables.

## Programmatic use

```python
from kafka_analyzer import KafkaAnalyzer
from kafka_analyzer.client import AxonOpsClient
from kafka_analyzer.config import Config
from datetime import UTC, datetime, timedelta
from pathlib import Path
import yaml

with open("config.yaml") as f:
    cfg = Config(**yaml.safe_load(f))

client = AxonOpsClient(api_url=cfg.axonops.api_url, token=cfg.axonops.token)
end = datetime.now(UTC)
analyzer = KafkaAnalyzer(
    client=client,
    config=cfg,
    org=cfg.cluster.org,
    cluster_type=cfg.cluster.cluster_type,
    cluster=cfg.cluster.cluster,
    start_time=end - timedelta(hours=cfg.analysis.hours),
    end_time=end,
    output_dir=Path("./reports"),
)
report_path = analyzer.analyze()
```

## Architecture

```
kafka_analyzer/
├── analyzers/    # Per-section analysis (infrastructure, configuration, ...)
├── client/       # AxonOps API client
├── collectors/   # Cluster state collection
├── models/       # Data models for brokers / topics / consumer groups
├── reports/      # Markdown / JSON / PDF report generation
└── utils/
```

## Development

```bash
make install-dev   # set up dev dependencies + pre-commit
make test          # run pytest
make lint          # flake8
make format        # black + isort
make ci            # full CI suite
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Legal

This project is independent. AxonOps is a registered trademark of AxonOps Limited. Apache, Apache Kafka, and Kafka are trademarks of the Apache Software Foundation.
