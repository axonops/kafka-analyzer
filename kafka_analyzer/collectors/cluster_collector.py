"""
Kafka cluster data collector.
"""

from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

import structlog

from ..client import AxonOpsClient, AxonOpsForbiddenError
from ..models import (
    Broker,
    ClusterState,
    ConsumerGroup,
    MetricData,
    MetricPoint,
    Partition,
    Topic,
)

logger = structlog.get_logger()


def _to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _config_entries_to_dict(entries: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not entries:
        return out
    if isinstance(entries, dict):
        # Some endpoints wrap entries under "configEntries" or "configs"
        for key in ("configEntries", "configs"):
            if key in entries and isinstance(entries[key], list):
                entries = entries[key]
                break
    if isinstance(entries, list):
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name") or entry.get("Name")
            value = entry.get("value")
            if value is None:
                value = entry.get("Value")
            if name is None:
                continue
            out[str(name)] = _to_str(value) or ""
    return out


class ClusterDataCollector:
    """Collects all observable state for a Kafka cluster from AxonOps."""

    def __init__(
        self,
        client: AxonOpsClient,
        org: str,
        cluster_type: str,
        cluster: str,
        collect_per_topic_partitions: bool = True,
    ):
        self.client = client
        self.org = org
        self.cluster_type = cluster_type
        self.cluster = cluster
        self.collect_per_topic_partitions = collect_per_topic_partitions

    # ------------------------------------------------------------------ public

    def collect(
        self,
        start_time: datetime,
        end_time: datetime,
        metrics_resolution: str = "60s",
    ) -> ClusterState:
        start_collection = datetime.now(UTC)

        state = ClusterState(name=self.cluster, cluster_type=self.cluster_type)

        logger.info("Collecting Kafka cluster info")
        self._collect_cluster_info(state)

        logger.info("Collecting host details for brokers")
        self._merge_host_details(state)

        logger.info("Collecting topics")
        self._collect_topics(state)

        logger.info("Collecting consumer groups")
        self._collect_consumer_groups(state)

        logger.info("Collecting ACLs")
        self._collect_acls(state)

        logger.info("Collecting auxiliary services / connect / schema registry")
        self._collect_aux(state)

        logger.info("Collecting agent config")
        try:
            state.agent_config = self.client.get_agent_config(
                self.org, self.cluster_type, self.cluster
            ) or {}
        except AxonOpsForbiddenError:
            logger.info(
                "Skipping agent config — token lacks permission (admin-only endpoint)"
            )
        except Exception as e:
            logger.warning("Failed to collect agent config", error=str(e))

        logger.info("Collecting metrics")
        state.metrics = self._collect_metrics(start_time, end_time, metrics_resolution)

        # Events left empty by default — the events API is heavy and timed out
        # historically. Callers can opt-in via the analyzer if needed.
        state.events = []
        state.log_events = {}

        state.collection_duration_seconds = (
            datetime.now(UTC) - start_collection
        ).total_seconds()
        logger.info(
            "Data collection complete",
            duration_seconds=state.collection_duration_seconds,
            brokers=len(state.brokers),
            topics=len(state.topics),
            consumer_groups=len(state.consumer_groups),
        )
        return state

    # -------------------------------------------------------------- collectors

    def _collect_cluster_info(self, state: ClusterState) -> None:
        try:
            info = self.client.get_kafka_cluster_info(
                self.org, self.cluster_type, self.cluster
            ) or {}
        except Exception as e:
            logger.error("Failed to collect cluster info", error=str(e))
            return

        state.controller_id = info.get("controllerId")
        state.cluster_id = info.get("clusterId") or info.get("clusterID")

        for b in info.get("brokers", []) or []:
            broker_id = b.get("brokerId")
            if broker_id is None:
                continue
            broker = Broker(
                broker_id=int(broker_id),
                address=b.get("address"),
                rack=b.get("rack"),
                is_controller=bool(b.get("isController", False)),
                log_dir_size_bytes=b.get("totalLogDirSizeBytes"),
                primary_log_dir_size_bytes=b.get("totalPrimaryLogDirSizeBytes"),
            )
            state.brokers[broker.broker_id] = broker

        # Fetch per-broker config entries.
        for broker_id in list(state.brokers.keys()):
            try:
                detail = self.client.get_kafka_broker(
                    self.org, self.cluster_type, self.cluster, broker_id
                ) or {}
                state.brokers[broker_id].configs = _config_entries_to_dict(
                    detail.get("configs", [])
                )
                # Update size fields if present in the per-broker payload.
                if detail.get("logDirSize") is not None:
                    state.brokers[broker_id].log_dir_size_bytes = detail.get("logDirSize")
                if detail.get("rack") is not None:
                    state.brokers[broker_id].rack = detail.get("rack")
                if detail.get("address"):
                    state.brokers[broker_id].address = detail.get("address")
            except Exception as e:
                logger.warning(
                    "Failed to fetch broker detail",
                    broker_id=broker_id,
                    error=str(e),
                )

    def _merge_host_details(self, state: ClusterState) -> None:
        try:
            nodes = self.client.get_nodes_full(
                self.org, self.cluster_type, self.cluster
            ) or []
        except Exception as e:
            logger.warning("Failed to fetch nodes-full", error=str(e))
            return

        # Try to match nodes to brokers using axon_node_id / broker_id / address.
        # The exact field names vary; merge by best-effort.
        for node in nodes:
            details = node.get("Details", {}) if isinstance(node, dict) else {}
            broker_id = (
                details.get("comp_broker_id")
                or details.get("broker_id")
                or details.get("comp_kafka_broker_id")
            )
            try:
                broker_id = int(broker_id) if broker_id is not None else None
            except (TypeError, ValueError):
                broker_id = None

            if broker_id is None:
                # Fall back to address match.
                listen = (
                    details.get("comp_listen_address")
                    or details.get("comp_advertised_listeners")
                    or ""
                )
                for b in state.brokers.values():
                    if b.address and listen and b.address.split(":")[0] in listen:
                        broker_id = b.broker_id
                        break

            if broker_id is None:
                continue
            broker = state.brokers.get(broker_id)
            if broker is None:
                continue
            broker.Details = details

    def _collect_topics(self, state: ClusterState) -> None:
        try:
            topics_raw = self.client.get_kafka_topics(
                self.org, self.cluster_type, self.cluster
            ) or []
        except Exception as e:
            logger.error("Failed to collect topics", error=str(e))
            return

        # Bulk topic configs (one call instead of N).
        topic_configs: Dict[str, Dict[str, str]] = {}
        try:
            bulk = self.client.get_kafka_topics_configs(
                self.org, self.cluster_type, self.cluster
            ) or []
            for entry in bulk:
                if not isinstance(entry, dict):
                    continue
                name = entry.get("topicName") or entry.get("name")
                if not name:
                    continue
                topic_configs[name] = _config_entries_to_dict(entry)
        except Exception as e:
            logger.warning("Failed to collect bulk topic configs", error=str(e))

        for raw in topics_raw:
            if not isinstance(raw, dict):
                continue
            name = raw.get("name") or raw.get("Name")
            if not name:
                continue

            partitions: List[Partition] = []
            replicas_map = raw.get("partitionReplicas") or {}
            if isinstance(replicas_map, dict):
                for pid_str, replica_ids in replicas_map.items():
                    try:
                        pid = int(pid_str)
                    except (TypeError, ValueError):
                        continue
                    rep_list = list(replica_ids) if isinstance(replica_ids, list) else []
                    partitions.append(
                        Partition(id=pid, replicas=[int(x) for x in rep_list])
                    )

            topic = Topic(
                name=name,
                is_internal=bool(raw.get("isInternal", False)),
                partition_count=int(raw.get("partitionCount", len(partitions)) or 0),
                replication_factor=int(raw.get("replicationFactor", 0) or 0),
                cleanup_policy=str(raw.get("cleanupPolicy", "")),
                configs=topic_configs.get(name, {}),
                partitions=partitions,
            )

            log_dir_info = raw.get("logDirectoryInfo") or {}
            if isinstance(log_dir_info, dict):
                topic.log_dir_size_bytes = log_dir_info.get("totalSizeBytes") or log_dir_info.get(
                    "size"
                )

            if not topic.configs:
                # Fallback: per-topic config call.
                try:
                    cfg = self.client.get_kafka_topic_configs(
                        self.org, self.cluster_type, self.cluster, name
                    )
                    topic.configs = _config_entries_to_dict(cfg)
                except Exception as e:
                    logger.debug("Failed to fetch topic configs", topic=name, error=str(e))

            if self.collect_per_topic_partitions:
                self._enrich_partitions(topic)

            state.topics[name] = topic

    def _enrich_partitions(self, topic: Topic) -> None:
        try:
            resp = self.client.get_kafka_topic_partitions(
                self.org, self.cluster_type, self.cluster, topic.name
            ) or {}
        except Exception as e:
            logger.debug("Failed to fetch partitions", topic=topic.name, error=str(e))
            return

        details = resp.get("partitions") or []
        log_dirs = resp.get("partitionLogDirs") or []
        # Index water marks/log dir size by partition id.
        size_by_pid: Dict[int, int] = {}
        for ld in log_dirs:
            if not isinstance(ld, dict):
                continue
            pid = ld.get("partitionId")
            size = ld.get("size")
            if pid is None or size is None:
                continue
            try:
                size_by_pid[int(pid)] = max(size_by_pid.get(int(pid), 0), int(size))
            except (TypeError, ValueError):
                continue

        existing_by_id = {p.id: p for p in topic.partitions}
        for raw in details:
            if not isinstance(raw, dict):
                continue
            pid_value = raw.get("id")
            if pid_value is None:
                pid_value = raw.get("partitionId")
            try:
                pid = int(pid_value)
            except (TypeError, ValueError):
                continue
            partition = existing_by_id.get(pid)
            if partition is None:
                partition = Partition(id=pid)
                topic.partitions.append(partition)
                existing_by_id[pid] = partition

            partition.replicas = [int(x) for x in (raw.get("replicas") or [])]
            partition.in_sync_replicas = [int(x) for x in (raw.get("inSyncReplicas") or [])]
            partition.offline_replicas = [int(x) for x in (raw.get("offlineReplicas") or [])]
            try:
                partition.leader = int(raw.get("leader", -1))
            except (TypeError, ValueError):
                partition.leader = -1
            partition.error = raw.get("partitionError") or raw.get("waterMarksError") or None
            if raw.get("waterMarkLow") is not None:
                try:
                    partition.low_water_mark = int(raw["waterMarkLow"])
                except (TypeError, ValueError):
                    pass
            if raw.get("waterMarkHigh") is not None:
                try:
                    partition.high_water_mark = int(raw["waterMarkHigh"])
                except (TypeError, ValueError):
                    pass

        # Apply per-partition log dir sizes (sum to topic if missing).
        if size_by_pid and topic.log_dir_size_bytes is None:
            topic.log_dir_size_bytes = sum(size_by_pid.values())

    def _collect_consumer_groups(self, state: ClusterState) -> None:
        try:
            resp = self.client.get_kafka_consumer_groups(
                self.org, self.cluster_type, self.cluster
            ) or {}
        except Exception as e:
            logger.error("Failed to collect consumer groups", error=str(e))
            return

        groups: List[Dict[str, Any]] = []
        if isinstance(resp, dict):
            for key in ("Groups", "groups", "consumerGroups", "data"):
                if key in resp and isinstance(resp[key], list):
                    groups = resp[key]
                    break
        elif isinstance(resp, list):
            groups = resp

        for raw in groups:
            if not isinstance(raw, dict):
                continue
            group_id = raw.get("groupId") or raw.get("GroupID")
            if not group_id:
                continue
            members = raw.get("members") or []
            topic_offsets = raw.get("topicOffsets") or []
            total_lag: Optional[int] = None
            if isinstance(topic_offsets, list):
                lag = 0
                seen_lag = False
                for to in topic_offsets:
                    if not isinstance(to, dict):
                        continue
                    for partition in to.get("partitionOffsets", []) or []:
                        if not isinstance(partition, dict):
                            continue
                        if "lag" in partition and partition["lag"] is not None:
                            try:
                                lag += int(partition["lag"])
                                seen_lag = True
                            except (TypeError, ValueError):
                                continue
                if seen_lag:
                    total_lag = lag

            cg = ConsumerGroup(
                group_id=str(group_id),
                state=str(raw.get("status") or raw.get("state") or ""),
                protocol_type=raw.get("protocolType"),
                protocol=raw.get("protocol"),
                coordinator_id=raw.get("coordinatorId"),
                member_count=len(members) if isinstance(members, list) else 0,
                total_lag=total_lag,
                topic_offsets=topic_offsets if isinstance(topic_offsets, list) else [],
                members=members if isinstance(members, list) else [],
            )
            state.consumer_groups[cg.group_id] = cg

    def _collect_acls(self, state: ClusterState) -> None:
        try:
            resp = self.client.get_kafka_acls(
                self.org, self.cluster_type, self.cluster
            ) or {}
        except Exception as e:
            logger.warning("Failed to collect ACLs", error=str(e))
            return

        if isinstance(resp, dict):
            state.is_authorizer_enabled = resp.get("isAuthorizerEnabled")
            resources = resp.get("aclResources") or []
            if isinstance(resources, list):
                state.acls = resources
        elif isinstance(resp, list):
            state.acls = resp

    def _collect_aux(self, state: ClusterState) -> None:
        try:
            state.services = self.client.get_kafka_services(
                self.org, self.cluster_type, self.cluster
            )
        except Exception as e:
            logger.debug("Failed to fetch services", error=str(e))

        try:
            state.rack_failure = self.client.get_kafka_rack_failure(
                self.org, self.cluster_type, self.cluster
            ) or {}
        except Exception as e:
            logger.debug("Failed to fetch rack failure config", error=str(e))

        # Kafka Connect: list clusters, then enumerate connectors + tasks for each.
        try:
            connect_clusters = self.client.get_kafka_connect_clusters(
                self.org, self.cluster_type, self.cluster
            )
            state.connect_clusters = connect_clusters
            state.connectors = self._collect_connectors(connect_clusters)
        except Exception as e:
            logger.debug("Failed to fetch Kafka Connect clusters", error=str(e))

        # Schema Registry: detailed subjects (preferred) and global config.
        try:
            subjects = self.client.get_schema_registry_subjects_detailed(
                self.org, self.cluster_type, self.cluster
            )
            if not subjects:
                subjects = self.client.get_schema_registry_subjects(
                    self.org, self.cluster_type, self.cluster
                )
            state.schema_registry_subjects = subjects if subjects else []
        except Exception as e:
            logger.debug("Failed to fetch schema registry subjects", error=str(e))

        try:
            state.schema_registry_config = self.client.get_schema_registry_config(
                self.org, self.cluster_type, self.cluster
            ) or {}
        except Exception as e:
            logger.debug("Failed to fetch schema registry config", error=str(e))

    def _collect_connectors(self, connect_clusters: Any) -> List[Dict[str, Any]]:
        """For every Connect cluster, list connectors and resolve task status."""
        out: List[Dict[str, Any]] = []
        names: List[str] = []

        if isinstance(connect_clusters, list):
            for entry in connect_clusters:
                if isinstance(entry, str):
                    names.append(entry)
                elif isinstance(entry, dict):
                    name = entry.get("name") or entry.get("clusterName") or entry.get("id")
                    if name:
                        names.append(str(name))
        elif isinstance(connect_clusters, dict):
            for key in ("clusters", "data"):
                value = connect_clusters.get(key)
                if isinstance(value, list):
                    for entry in value:
                        if isinstance(entry, str):
                            names.append(entry)
                        elif isinstance(entry, dict):
                            n = entry.get("name") or entry.get("clusterName") or entry.get("id")
                            if n:
                                names.append(str(n))
                    break

        for cluster_name in names:
            try:
                connectors = self.client.get_kafka_connect_connectors(
                    self.org, self.cluster_type, self.cluster, cluster_name
                )
            except Exception as e:
                logger.warning(
                    "Failed to fetch connectors", connect_cluster=cluster_name, error=str(e)
                )
                continue

            connector_list: List[Dict[str, Any]] = []
            if isinstance(connectors, list):
                for c in connectors:
                    if isinstance(c, str):
                        connector_list.append({"name": c})
                    elif isinstance(c, dict):
                        connector_list.append(c)
            elif isinstance(connectors, dict):
                for key in ("connectors", "data"):
                    value = connectors.get(key)
                    if isinstance(value, list):
                        for c in value:
                            if isinstance(c, str):
                                connector_list.append({"name": c})
                            elif isinstance(c, dict):
                                connector_list.append(c)
                        break

            for connector in connector_list:
                connector_name = connector.get("name") or connector.get("connector")
                if not connector_name:
                    continue
                record: Dict[str, Any] = {
                    "connect_cluster": cluster_name,
                    "name": connector_name,
                    "type": connector.get("type"),
                    "state": connector.get("state") or connector.get("status"),
                    "worker_id": connector.get("worker_id") or connector.get("workerId"),
                    "config": connector.get("config", {}),
                    "tasks": [],
                }

                # If state/tasks weren't included inline, fetch them.
                if not record["state"]:
                    try:
                        info = self.client.get_kafka_connect_connector(
                            self.org, self.cluster_type, self.cluster, cluster_name, connector_name
                        ) or {}
                        connector_status = info.get("connector") or info.get("status") or {}
                        if isinstance(connector_status, dict):
                            record["state"] = connector_status.get("state")
                            record["worker_id"] = connector_status.get("worker_id") or record["worker_id"]
                        record["type"] = record["type"] or info.get("type")
                        record["config"] = record["config"] or info.get("config", {})
                        if isinstance(info.get("tasks"), list):
                            record["tasks"] = info["tasks"]
                    except Exception as e:
                        logger.debug(
                            "Failed to fetch connector status",
                            connect_cluster=cluster_name,
                            connector=connector_name,
                            error=str(e),
                        )

                if not record["tasks"]:
                    try:
                        tasks = self.client.get_kafka_connect_connector_tasks(
                            self.org, self.cluster_type, self.cluster, cluster_name, connector_name
                        )
                        if isinstance(tasks, list):
                            record["tasks"] = tasks
                        elif isinstance(tasks, dict):
                            for key in ("tasks", "data"):
                                value = tasks.get(key)
                                if isinstance(value, list):
                                    record["tasks"] = value
                                    break
                    except Exception as e:
                        logger.debug(
                            "Failed to fetch connector tasks",
                            connect_cluster=cluster_name,
                            connector=connector_name,
                            error=str(e),
                        )

                out.append(record)

        return out

    # -------------------------------------------------------------- metrics

    def _collect_metrics(
        self, start_time: datetime, end_time: datetime, resolution: str
    ) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        org_filter = f'org="{self.org}"'
        cluster_filter = f'cluster="{self.cluster}"'
        type_filter = f'type="{self.cluster_type}"'
        common = f"{org_filter},{cluster_filter},{type_filter}"

        # Kafka-relevant metric queries. Names align with the AxonOps Kafka
        # agent emission and Prometheus-style label filters used elsewhere.
        metric_queries: Dict[str, str] = {
            # Host / system
            "cpu_usage": f'host_CPU_Percent_Merge{{{common},time="real"}}',
            "memory_usage_percent": f"host_Memory_UsedPercent{{{common}}}",
            "disk_usage_percent": f"host_Disk_UsedPercent{{{common}}}",
            "disk_read_rate": f'host_Disk_SectorsRead{{{common},axonfunction="rate"}}',
            "disk_write_rate": f'host_Disk_SectorsWrite{{{common},axonfunction="rate"}}',
            "network_in": f'host_Net_BytesIn{{{common},axonfunction="rate"}}',
            "network_out": f'host_Net_BytesOut{{{common},axonfunction="rate"}}',

            # JVM
            "heap_usage": f'jvm_Memory_{{{common},function="used",scope="HeapMemoryUsage"}}',
            "gc_pause_time": f'jvm_GarbageCollector_{{{common},function="CollectionTime",axonfunction="rate"}}',
            "gc_collection_count": f'jvm_GarbageCollector_{{{common},function="CollectionCount",axonfunction="rate"}}',

            # Kafka cluster health
            "under_replicated_partitions": f'kafka_server_ReplicaManager_UnderReplicatedPartitions{{{common}}}',
            "under_min_isr_partition_count": f'kafka_server_ReplicaManager_UnderMinIsrPartitionCount{{{common}}}',
            "offline_partitions": f'kafka_controller_KafkaController_OfflinePartitionsCount{{{common}}}',
            "active_controller_count": f'kafka_controller_KafkaController_ActiveControllerCount{{{common}}}',
            "preferred_replica_imbalance": f'kafka_controller_KafkaController_PreferredReplicaImbalanceCount{{{common}}}',
            "leader_count": f'kafka_server_ReplicaManager_LeaderCount{{{common}}}',
            "partition_count": f'kafka_server_ReplicaManager_PartitionCount{{{common}}}',

            # ISR churn
            "isr_shrinks": f'kafka_server_ReplicaManager_IsrShrinksPerSec{{{common},axonfunction="rate"}}',
            "isr_expands": f'kafka_server_ReplicaManager_IsrExpandsPerSec{{{common},axonfunction="rate"}}',

            # Throughput
            "messages_in": f'kafka_server_BrokerTopicMetrics_MessagesInPerSec{{{common},axonfunction="rate"}}',
            "bytes_in": f'kafka_server_BrokerTopicMetrics_BytesInPerSec{{{common},axonfunction="rate"}}',
            "bytes_out": f'kafka_server_BrokerTopicMetrics_BytesOutPerSec{{{common},axonfunction="rate"}}',
            "produce_request_rate": f'kafka_network_RequestMetrics_RequestsPerSec{{{common},request="Produce",axonfunction="rate"}}',
            "fetch_consumer_request_rate": f'kafka_network_RequestMetrics_RequestsPerSec{{{common},request="FetchConsumer",axonfunction="rate"}}',
            "fetch_follower_request_rate": f'kafka_network_RequestMetrics_RequestsPerSec{{{common},request="FetchFollower",axonfunction="rate"}}',

            # Request latencies
            "produce_total_time_p99": f'kafka_network_RequestMetrics_TotalTimeMs{{{common},request="Produce",function="99thPercentile"}}',
            "fetch_consumer_total_time_p99": f'kafka_network_RequestMetrics_TotalTimeMs{{{common},request="FetchConsumer",function="99thPercentile"}}',
            "fetch_follower_total_time_p99": f'kafka_network_RequestMetrics_TotalTimeMs{{{common},request="FetchFollower",function="99thPercentile"}}',

            # Resource saturation
            "request_handler_idle": f'kafka_server_KafkaRequestHandlerPool_RequestHandlerAvgIdlePercent{{{common}}}',
            "network_processor_idle": f'kafka_network_SocketServer_NetworkProcessorAvgIdlePercent{{{common}}}',
            "log_flush_rate": f'kafka_log_LogFlushStats_LogFlushRateAndTimeMs{{{common},axonfunction="rate"}}',
            "log_flush_time_p99": f'kafka_log_LogFlushStats_LogFlushRateAndTimeMs{{{common},function="99thPercentile"}}',

            # Errors
            "failed_produce_requests": f'kafka_server_BrokerTopicMetrics_FailedProduceRequestsPerSec{{{common},axonfunction="rate"}}',
            "failed_fetch_requests": f'kafka_server_BrokerTopicMetrics_FailedFetchRequestsPerSec{{{common},axonfunction="rate"}}',

            # Consumer lag (per-group/topic series)
            "consumer_lag": f"kafka_consumer_group_lag{{{common}}}",
        }

        for metric_name, query in metric_queries.items():
            try:
                result = self.client.query_range(
                    query=query, start=start_time, end=end_time, step=resolution
                )
                metrics[metric_name] = (
                    self._parse_prometheus_result(result) if result is not None else []
                )
            except Exception as e:
                logger.error(
                    f"Failed to collect metric {metric_name}", error=str(e), query=query
                )
                metrics[metric_name] = []

        return metrics

    @staticmethod
    def _parse_prometheus_result(result: Dict[str, Any]) -> List[MetricData]:
        out: List[MetricData] = []
        if not result or result.get("status") != "success":
            return out
        data = result.get("data") or {}
        if data.get("resultType") != "matrix":
            return out
        for series in data.get("result") or []:
            metric = series.get("metric", {})
            data_points: List[MetricPoint] = []
            for ts, value in series.get("values", []) or []:
                try:
                    data_points.append(
                        MetricPoint(
                            timestamp=datetime.fromtimestamp(ts),
                            value=float(value),
                        )
                    )
                except (ValueError, TypeError):
                    continue
            if data_points:
                out.append(
                    MetricData(
                        metric_name=metric.get("__name__", "unknown"),
                        labels=metric,
                        data_points=data_points,
                    )
                )
        return out
