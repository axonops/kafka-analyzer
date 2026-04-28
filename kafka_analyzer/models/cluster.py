"""
Cluster state models for a Kafka cluster.
"""

from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class Broker(BaseModel):
    """A Kafka broker."""

    broker_id: int
    address: Optional[str] = None
    rack: Optional[str] = None
    is_controller: bool = False
    log_dir_size_bytes: Optional[int] = None
    primary_log_dir_size_bytes: Optional[int] = None

    # Per-broker config entries (may be empty until /broker/:id is fetched).
    configs: Dict[str, str] = Field(default_factory=dict)

    # Raw host details from /nodes-full (CPU, memory, JVM args, etc.).
    Details: Dict[str, Any] = Field(default_factory=dict)

    @property
    def host_id(self) -> Optional[str]:
        return self.Details.get("host_id")

    @property
    def kafka_version(self) -> Optional[str]:
        for key in ("comp_kafka_version", "comp_releaseVersion", "release_version"):
            v = self.Details.get(key)
            if v:
                return v
        return None

    @property
    def jvm_input_arguments(self) -> str:
        return self.Details.get("comp_jvm_input arguments", "") or ""

    @property
    def is_active(self) -> bool:
        if not self.Details:
            return self.address is not None
        active_indicators = [
            "host_uptime",
            "agent_version",
            "release_version",
            "comp_listen_address",
            "host_CPU_Percent",
            "host_Memory_Total",
        ]
        return any(self.Details.get(field) for field in active_indicators) or self.address is not None


class Partition(BaseModel):
    """A topic partition."""

    id: int
    leader: int = -1
    replicas: List[int] = Field(default_factory=list)
    in_sync_replicas: List[int] = Field(default_factory=list)
    offline_replicas: List[int] = Field(default_factory=list)
    high_water_mark: Optional[int] = None
    low_water_mark: Optional[int] = None
    error: Optional[str] = None

    @property
    def is_under_replicated(self) -> bool:
        return len(self.in_sync_replicas) < len(self.replicas)

    @property
    def is_offline(self) -> bool:
        return self.leader < 0

    @property
    def message_count(self) -> Optional[int]:
        if self.high_water_mark is None or self.low_water_mark is None:
            return None
        return max(0, self.high_water_mark - self.low_water_mark)


class Topic(BaseModel):
    """A Kafka topic."""

    name: str
    is_internal: bool = False
    partition_count: int = 0
    replication_factor: int = 0
    cleanup_policy: str = ""
    configs: Dict[str, str] = Field(default_factory=dict)
    partitions: List[Partition] = Field(default_factory=list)
    log_dir_size_bytes: Optional[int] = None

    @property
    def is_system_topic(self) -> bool:
        return self.is_internal or self.name.startswith("__") or self.name.startswith("_confluent")

    def config_value(self, name: str, default: Optional[str] = None) -> Optional[str]:
        return self.configs.get(name, default)

    def retention_ms(self) -> Optional[int]:
        v = self.configs.get("retention.ms")
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def retention_bytes(self) -> Optional[int]:
        v = self.configs.get("retention.bytes")
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def min_insync_replicas(self) -> Optional[int]:
        v = self.configs.get("min.insync.replicas")
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def under_replicated_partitions(self) -> List[Partition]:
        return [p for p in self.partitions if p.is_under_replicated]

    def offline_partitions(self) -> List[Partition]:
        return [p for p in self.partitions if p.is_offline]


class ConsumerGroup(BaseModel):
    """A Kafka consumer group."""

    group_id: str
    state: str = ""
    protocol_type: Optional[str] = None
    protocol: Optional[str] = None
    coordinator_id: Optional[int] = None
    member_count: int = 0
    total_lag: Optional[int] = None
    topic_offsets: List[Dict[str, Any]] = Field(default_factory=list)
    members: List[Dict[str, Any]] = Field(default_factory=list)


class ClusterState(BaseModel):
    """Complete observed state of a Kafka cluster."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    cluster_type: str = "kafka"

    controller_id: Optional[int] = None
    cluster_id: Optional[str] = None

    brokers: Dict[int, Broker] = Field(default_factory=dict)
    topics: Dict[str, Topic] = Field(default_factory=dict)
    consumer_groups: Dict[str, ConsumerGroup] = Field(default_factory=dict)

    acls: List[Dict[str, Any]] = Field(default_factory=list)
    is_authorizer_enabled: Optional[bool] = None

    services: Any = None
    rack_failure: Dict[str, Any] = Field(default_factory=dict)
    schema_registry_subjects: List[Any] = Field(default_factory=list)
    schema_registry_config: Dict[str, Any] = Field(default_factory=dict)
    connect_clusters: Any = None
    connectors: List[Dict[str, Any]] = Field(default_factory=list)
    agent_config: Dict[str, Any] = Field(default_factory=dict)

    metrics: Dict[str, Any] = Field(default_factory=dict)
    events: List[Dict[str, Any]] = Field(default_factory=list)
    log_events: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    collection_time: datetime = Field(default_factory=lambda: datetime.now(UTC))
    collection_duration_seconds: Optional[float] = None

    # ------------------------------------------------------------------ helpers

    def get_total_brokers(self) -> int:
        return len(self.brokers)

    def get_active_brokers(self) -> int:
        return sum(1 for b in self.brokers.values() if b.is_active)

    def get_racks(self) -> List[str]:
        racks = {b.rack for b in self.brokers.values() if b.rack}
        return sorted(racks)

    def get_brokers_by_rack(self) -> Dict[str, List[Broker]]:
        out: Dict[str, List[Broker]] = {}
        for b in self.brokers.values():
            key = b.rack or "unknown"
            out.setdefault(key, []).append(b)
        return out

    def total_partitions(self) -> int:
        return sum(t.partition_count for t in self.topics.values())

    def total_user_partitions(self) -> int:
        return sum(t.partition_count for t in self.topics.values() if not t.is_system_topic)

    def under_replicated_partition_count(self) -> int:
        return sum(len(t.under_replicated_partitions()) for t in self.topics.values())

    def offline_partition_count(self) -> int:
        return sum(len(t.offline_partitions()) for t in self.topics.values())

    def leader_counts(self) -> Dict[int, int]:
        counts: Dict[int, int] = {b.broker_id: 0 for b in self.brokers.values()}
        for t in self.topics.values():
            for p in t.partitions:
                if p.leader is not None and p.leader >= 0:
                    counts[p.leader] = counts.get(p.leader, 0) + 1
        return counts
