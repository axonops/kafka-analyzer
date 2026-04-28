"""
Configuration models and defaults for the Kafka analyzer.
"""

from typing import Dict
from pydantic import BaseModel, ConfigDict, Field


class ClusterConfig(BaseModel):
    """Cluster identification."""
    org: str = Field(description="Organization name")
    cluster: str = Field(description="Cluster name")
    cluster_type: str = Field(default="kafka", description="Cluster type")


class AxonOpsConfig(BaseModel):
    """AxonOps API configuration."""
    api_url: str = Field(default="http://localhost:9090", description="AxonOps API URL")
    token: str = Field(description="API authentication token")
    timeout: int = Field(default=30, description="API request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum number of API retry attempts")


class ThresholdsConfig(BaseModel):
    """Analysis thresholds."""

    # Host / infrastructure
    cpu_usage_warn: float = Field(default=80.0, description="Broker CPU usage warning (%)")
    memory_usage_warn: float = Field(default=85.0, description="Broker memory usage warning (%)")
    disk_usage_warn: float = Field(default=80.0, description="Broker disk usage warning (%)")
    log_dir_imbalance_warn: float = Field(
        default=0.20,
        description="Allowed relative imbalance of log dir size across brokers (0.20 = 20%)",
    )

    # JVM
    heap_usage_warn: float = Field(default=75.0, description="Heap usage warning (%)")
    gc_pause_warn_ms: int = Field(default=200, description="GC pause warning (ms)")
    gc_pause_critical_ms: int = Field(default=1000, description="GC pause critical (ms)")

    # Operations (cluster health)
    under_replicated_partitions_warn: int = Field(
        default=0, description="Under-replicated partitions warning threshold"
    )
    offline_partitions_warn: int = Field(
        default=0, description="Offline partitions warning threshold"
    )
    isr_shrink_rate_warn: float = Field(
        default=0.5,
        description="ISR shrinks per second warning threshold (per broker average)",
    )
    request_handler_idle_warn: float = Field(
        default=0.30,
        description="Request handler avg idle ratio warning (lower is worse)",
    )
    network_processor_idle_warn: float = Field(
        default=0.30,
        description="Network processor avg idle ratio warning (lower is worse)",
    )
    request_p99_warn_ms: int = Field(
        default=500, description="Request total time p99 warning (ms)"
    )
    leader_skew_warn: float = Field(
        default=0.20,
        description="Allowed relative leader-count skew across brokers (0.20 = 20%)",
    )

    # Topics
    min_replication_factor: int = Field(default=3, description="Minimum recommended RF")
    min_in_sync_replicas: int = Field(default=2, description="Minimum recommended min.insync.replicas")
    max_partitions_per_broker_warn: int = Field(
        default=4000, description="Max partitions per broker warning"
    )
    max_partitions_per_topic_warn: int = Field(
        default=200, description="Suspiciously large partition count for a single topic"
    )
    min_partitions_per_topic_warn: int = Field(
        default=1,
        description="Topics with fewer than this many partitions get a note (often fine)",
    )
    retention_warn_days: int = Field(
        default=30, description="Retention longer than this is flagged for review (days)"
    )

    # Consumer lag
    consumer_lag_warn: int = Field(default=10000, description="Lag warning per group/topic")
    consumer_lag_critical: int = Field(default=100000, description="Lag critical per group/topic")


class AnalysisConfig(BaseModel):
    hours: int = Field(default=24, description="Hours of history to analyze")
    metrics_resolution_seconds: int = Field(default=60, description="Metrics step (seconds)")
    enable_sections: Dict[str, bool] = Field(
        default={
            "infrastructure": True,
            "configuration": True,
            "operations": True,
            "topics": True,
            "security": True,
            "connect": True,
            "schema_registry": True,
        },
        description="Enable/disable analysis sections",
    )
    collect_per_topic_partitions: bool = Field(
        default=True,
        description="Fetch per-topic partition details (one API call per topic; can be slow on large clusters)",
    )
    thresholds: ThresholdsConfig = Field(default_factory=ThresholdsConfig)


class Config(BaseModel):
    cluster: ClusterConfig
    axonops: AxonOpsConfig
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)

    model_config = ConfigDict(extra="allow")
