"""Shared pytest fixtures for kafka_analyzer tests."""

from datetime import UTC, datetime

import pytest

from kafka_analyzer.config import (
    AnalysisConfig,
    AxonOpsConfig,
    ClusterConfig,
    Config,
)
from kafka_analyzer.models import (
    Broker,
    ClusterState,
    ConsumerGroup,
    Partition,
    Topic,
)


@pytest.fixture
def config() -> Config:
    return Config(
        cluster=ClusterConfig(org="test-org", cluster="test-cluster", cluster_type="kafka"),
        axonops=AxonOpsConfig(api_url="http://localhost:9090", token="test-token"),
        analysis=AnalysisConfig(),
    )


@pytest.fixture
def healthy_cluster_state() -> ClusterState:
    state = ClusterState(name="test-cluster", cluster_type="kafka")
    state.controller_id = 1
    for i in (1, 2, 3):
        state.brokers[i] = Broker(
            broker_id=i,
            address=f"broker-{i}:9092",
            rack=f"rack-{(i % 3) + 1}",
            is_controller=(i == 1),
            log_dir_size_bytes=10_000_000_000,
            configs={
                "default.replication.factor": "3",
                "min.insync.replicas": "2",
                "unclean.leader.election.enable": "false",
                "auto.create.topics.enable": "false",
                "controlled.shutdown.enable": "true",
                "transaction.state.log.replication.factor": "3",
                "transaction.state.log.min.isr": "2",
                "num.io.threads": "8",
                "num.network.threads": "5",
                "listeners": "SASL_SSL://0.0.0.0:9093",
                "advertised.listeners": "SASL_SSL://broker:9093",
                "security.inter.broker.protocol": "SASL_SSL",
                "sasl.enabled.mechanisms": "SCRAM-SHA-512",
            },
            Details={"comp_jvm_input arguments": "-Xmx4g -XX:+UseG1GC"},
        )

    state.topics["orders"] = Topic(
        name="orders",
        partition_count=12,
        replication_factor=3,
        cleanup_policy="delete",
        configs={
            "min.insync.replicas": "2",
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),
        },
        partitions=[
            Partition(id=p, leader=(p % 3) + 1, replicas=[1, 2, 3], in_sync_replicas=[1, 2, 3])
            for p in range(12)
        ],
    )
    state.topics["__consumer_offsets"] = Topic(
        name="__consumer_offsets",
        is_internal=True,
        partition_count=50,
        replication_factor=3,
    )
    state.consumer_groups["payments-svc"] = ConsumerGroup(
        group_id="payments-svc",
        state="Stable",
        member_count=3,
        total_lag=200,
    )
    state.is_authorizer_enabled = True
    state.acls = [
        {
            "resourceType": "TOPIC",
            "resourceName": "orders",
            "acls": [
                {"principal": "User:payments-svc", "operation": "READ", "permissionType": "ALLOW"}
            ],
        }
    ]
    state.collection_time = datetime.now(UTC)
    return state


@pytest.fixture
def unhealthy_cluster_state() -> ClusterState:
    state = ClusterState(name="bad-cluster", cluster_type="kafka")
    state.controller_id = None
    state.brokers[1] = Broker(
        broker_id=1,
        address="solo:9092",
        rack=None,
        is_controller=True,
        log_dir_size_bytes=900_000_000_000,
        configs={
            "default.replication.factor": "1",
            "min.insync.replicas": "1",
            "unclean.leader.election.enable": "true",
            "auto.create.topics.enable": "true",
            "controlled.shutdown.enable": "false",
            "listeners": "PLAINTEXT://0.0.0.0:9092",
            "advertised.listeners": "PLAINTEXT://solo:9092",
            "security.inter.broker.protocol": "PLAINTEXT",
        },
    )
    state.topics["legacy"] = Topic(
        name="legacy",
        partition_count=1,
        replication_factor=1,
        cleanup_policy="delete",
        partitions=[Partition(id=0, leader=1, replicas=[1], in_sync_replicas=[])],
    )
    state.consumer_groups["stale-svc"] = ConsumerGroup(
        group_id="stale-svc",
        state="Stable",
        member_count=0,
        total_lag=500_000,
    )
    state.is_authorizer_enabled = False
    return state
