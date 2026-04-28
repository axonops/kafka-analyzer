"""
Main analyzer orchestrator.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict

import structlog

from .analyzers import (
    ConfigurationAnalyzer,
    ConnectAnalyzer,
    InfrastructureAnalyzer,
    OperationsAnalyzer,
    SchemaRegistryAnalyzer,
    SecurityAnalyzer,
    TopicsAnalyzer,
)
from .client import AxonOpsClient
from .collectors import ClusterDataCollector
from .config import Config
from .models import ClusterState
from .reports.generator_enhanced import EnhancedReportGenerator

logger = structlog.get_logger()


class KafkaAnalyzer:
    """Orchestrates Kafka cluster analysis."""

    def __init__(
        self,
        client: AxonOpsClient,
        config: Config,
        org: str,
        cluster_type: str,
        cluster: str,
        start_time: datetime,
        end_time: datetime,
        output_dir: Path,
    ):
        self.client = client
        self.config = config
        self.org = org
        self.cluster_type = cluster_type
        self.cluster = cluster
        self.start_time = start_time
        self.end_time = end_time
        self.output_dir = output_dir

        self.collector = ClusterDataCollector(
            client=client,
            org=org,
            cluster_type=cluster_type,
            cluster=cluster,
            collect_per_topic_partitions=config.analysis.collect_per_topic_partitions,
        )

        self.analyzers: Dict[str, Any] = {}
        sections = config.analysis.enable_sections
        if sections.get("infrastructure", True):
            self.analyzers["infrastructure"] = InfrastructureAnalyzer(config)
        if sections.get("configuration", True):
            self.analyzers["configuration"] = ConfigurationAnalyzer(config)
        if sections.get("operations", True):
            self.analyzers["operations"] = OperationsAnalyzer(config)
        if sections.get("topics", True):
            self.analyzers["topics"] = TopicsAnalyzer(config)
        if sections.get("security", True):
            self.analyzers["security"] = SecurityAnalyzer(config)
        if sections.get("connect", True):
            self.analyzers["connect"] = ConnectAnalyzer(config)
        if sections.get("schema_registry", True):
            self.analyzers["schema_registry"] = SchemaRegistryAnalyzer(config)

        self.report_generator = EnhancedReportGenerator(output_dir)

    def analyze(self, generate_pdf: bool = False) -> Path:
        logger.info(
            "Starting analysis",
            org=self.org,
            cluster=self.cluster,
            start_time=self.start_time,
            end_time=self.end_time,
        )

        logger.info("Collecting cluster data")
        cluster_state = self._collect_data()

        logger.info("Running analysis sections")
        analysis_results = self._run_analyzers(cluster_state)

        logger.info("Generating report")
        report_path = self._generate_report(cluster_state, analysis_results, generate_pdf)

        logger.info("Analysis complete", report_path=str(report_path))
        return report_path

    def _collect_data(self) -> ClusterState:
        return self.collector.collect(
            start_time=self.start_time,
            end_time=self.end_time,
            metrics_resolution=f"{self.config.analysis.metrics_resolution_seconds}s",
        )

    def _run_analyzers(self, cluster_state: ClusterState) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for name, analyzer in self.analyzers.items():
            logger.info(f"Running {name} analyzer")
            try:
                results[name] = analyzer.analyze(cluster_state)
            except Exception as e:
                import traceback
                error_details = traceback.format_exc()
                logger.error(
                    f"Error in {name} analyzer",
                    error=str(e),
                    error_type=type(e).__name__,
                    traceback=error_details,
                )
                results[name] = {"error": str(e), "recommendations": []}
        return results

    def _generate_report(
        self,
        cluster_state: ClusterState,
        analysis_results: Dict[str, Any],
        generate_pdf: bool = False,
    ) -> Path:
        report_data = {
            "cluster_info": {
                "organization": self.org,
                "cluster_type": self.cluster_type,
                "cluster_name": self.cluster,
                "analysis_time": datetime.now(UTC).isoformat(),
                "time_range": {
                    "start": self.start_time.isoformat(),
                    "end": self.end_time.isoformat(),
                },
            },
            "cluster_state": cluster_state,
            "analysis_results": analysis_results,
        }
        return self.report_generator.generate(report_data, generate_pdf=generate_pdf)
