"""
Command-line interface for the Kafka AxonOps Analyzer.
"""

import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import click
import structlog
import yaml

from .analyzer import KafkaAnalyzer
from .client import AxonOpsClient
from .config import Config

logger = structlog.get_logger()


@click.command()
@click.option("--config", type=click.Path(exists=True), required=True, help="Configuration file path (required)")
@click.option("--output-dir", default="./reports", help="Output directory for reports")
@click.option("--verbose", is_flag=True, help="Enable verbose logging (logs every API request + response)")
@click.option("--curl", is_flag=True, help="Print a copy-paste curl command for every API request")
@click.option("--pdf", is_flag=True, help="Also generate PDF report (requires WeasyPrint, not available in executables)")
def main(config, output_dir, verbose, curl, pdf):
    """Analyze a Kafka cluster using the AxonOps API."""

    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    click.echo(f"Loading configuration from: {config}")
    with open(config, "r") as f:
        config_data = yaml.safe_load(f)

    if not config_data.get("axonops", {}).get("token"):
        env_token = os.getenv("AXONOPS_API_TOKEN")
        if env_token:
            config_data.setdefault("axonops", {})["token"] = env_token

    if "cluster" not in config_data:
        raise click.ClickException("'cluster' section is required in config file")

    cluster_config = config_data["cluster"]
    if "org" not in cluster_config:
        raise click.ClickException("'org' is required in cluster configuration")
    if "cluster" not in cluster_config:
        raise click.ClickException("'cluster' name is required in cluster configuration")
    cluster_config.setdefault("cluster_type", "kafka")

    analyzer_config = Config(**config_data)
    hours = analyzer_config.analysis.hours

    end_dt = datetime.now(UTC)
    start_dt = end_dt - timedelta(hours=hours)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if not analyzer_config.axonops.api_url:
        raise click.ClickException("AxonOps API URL is required in config file")
    if not analyzer_config.axonops.token:
        raise click.ClickException(
            "AxonOps API token is required. Set it in config file or AXONOPS_API_TOKEN env var"
        )

    client = AxonOpsClient(
        api_url=analyzer_config.axonops.api_url,
        token=analyzer_config.axonops.token,
        timeout=analyzer_config.axonops.timeout,
        max_retries=analyzer_config.axonops.max_retries,
        log_curl=curl,
    )

    analyzer = KafkaAnalyzer(
        client=client,
        config=analyzer_config,
        org=analyzer_config.cluster.org,
        cluster_type=analyzer_config.cluster.cluster_type,
        cluster=analyzer_config.cluster.cluster,
        start_time=start_dt,
        end_time=end_dt,
        output_dir=output_path,
    )

    click.echo(
        f"Starting analysis for Kafka cluster {analyzer_config.cluster.cluster} "
        f"in organization {analyzer_config.cluster.org}"
    )
    click.echo(f"Time range: {start_dt} to {end_dt} ({hours} hours)")
    click.echo(f"Cluster type: {analyzer_config.cluster.cluster_type}")
    click.echo(f"API URL: {analyzer_config.axonops.api_url}")

    try:
        report_path = analyzer.analyze(generate_pdf=pdf)
        click.echo(f"Analysis complete! Report saved to: {report_path}")
        if pdf:
            pdf_path = report_path.with_suffix(".pdf")
            if pdf_path.exists():
                click.echo(f"PDF report saved to: {pdf_path}")
    except Exception as e:
        logger.error("Analysis failed", error=str(e))
        click.echo(f"Error: {e}", err=True)
        raise click.ClickException(str(e))


if __name__ == "__main__":
    main()
