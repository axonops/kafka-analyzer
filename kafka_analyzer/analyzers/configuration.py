"""
Broker / cluster configuration analyzer for Kafka.

Validates a curated set of broker configs against well-known best practices.
"""

from typing import Any, Dict, List, Optional

from ..models import Broker, ClusterState, Recommendation
from .base import BaseAnalyzer


def _to_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    return str(value).strip().lower() == "true"


class ConfigurationAnalyzer(BaseAnalyzer):
    """Inspects broker server.properties values and JVM arguments."""

    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}

        if not cluster_state.brokers:
            return {"recommendations": [], "summary": {}, "details": {}}

        # Spot-check inconsistencies across brokers, then validate against
        # best-practice thresholds for the first broker. Inconsistency itself
        # is the primary smell; uniformity makes the actual values trustworthy.
        config_keys_to_check = [
            "auto.create.topics.enable",
            "default.replication.factor",
            "min.insync.replicas",
            "unclean.leader.election.enable",
            "num.partitions",
            "num.io.threads",
            "num.network.threads",
            "log.retention.hours",
            "log.retention.bytes",
            "log.segment.bytes",
            "offsets.retention.minutes",
            "transaction.state.log.replication.factor",
            "transaction.state.log.min.isr",
            "delete.topic.enable",
            "broker.rack",
            "inter.broker.protocol.version",
            "log.message.format.version",
            "controlled.shutdown.enable",
            "leader.imbalance.check.interval.seconds",
            "replica.lag.time.max.ms",
        ]

        # Detect inconsistencies between brokers.
        per_key: Dict[str, Dict[int, str]] = {}
        for broker in cluster_state.brokers.values():
            for key in config_keys_to_check:
                val = broker.configs.get(key)
                if val is None:
                    continue
                per_key.setdefault(key, {})[broker.broker_id] = val

        inconsistent: List[str] = []
        for key, values in per_key.items():
            unique = set(values.values())
            # broker.rack is *expected* to differ per broker, skip it.
            if key == "broker.rack":
                continue
            if len(unique) > 1:
                inconsistent.append(key)

        if inconsistent:
            recommendations.append(
                self._create_recommendation(
                    title="Broker configurations diverge",
                    description=(
                        "The following settings are not the same on every broker: "
                        + ", ".join(sorted(inconsistent))
                    ),
                    severity="warning",
                    category="configuration",
                    impact="Inconsistent broker config can produce surprising behaviour after a leader change.",
                    recommendation="Reconcile the affected settings across all brokers.",
                )
            )
            details["inconsistent_keys"] = inconsistent

        # Use a representative broker for value-based checks.
        sample_broker: Broker = next(iter(cluster_state.brokers.values()))
        details["sample_broker_id"] = sample_broker.broker_id
        cfg = sample_broker.configs

        rf = _to_int(cfg.get("default.replication.factor"))
        if rf is not None and rf < self.thresholds.min_replication_factor:
            recommendations.append(
                self._create_recommendation(
                    title="default.replication.factor is below the recommended minimum",
                    description=f"default.replication.factor is set to {rf}.",
                    severity="warning",
                    category="configuration",
                    current_value=str(rf),
                    impact="Newly auto-created topics inherit this RF.",
                    recommendation=f"Set default.replication.factor to at least {self.thresholds.min_replication_factor}.",
                )
            )

        min_isr = _to_int(cfg.get("min.insync.replicas"))
        if min_isr is not None and min_isr < self.thresholds.min_in_sync_replicas:
            recommendations.append(
                self._create_recommendation(
                    title="min.insync.replicas is below the recommended minimum",
                    description=f"min.insync.replicas is set to {min_isr}.",
                    severity="warning",
                    category="configuration",
                    current_value=str(min_isr),
                    impact="With acks=all, the broker may accept writes that survive a single broker loss without durable replication.",
                    recommendation="Set min.insync.replicas to at least 2 (with RF >= 3) and require acks=all on producers.",
                )
            )

        unclean = _to_bool(cfg.get("unclean.leader.election.enable"))
        if unclean is True:
            recommendations.append(
                self._create_recommendation(
                    title="unclean.leader.election.enable is true",
                    description="Unclean leader election is enabled.",
                    severity="critical",
                    category="configuration",
                    current_value="true",
                    impact="A non-ISR replica can be elected leader, causing data loss.",
                    recommendation="Set unclean.leader.election.enable=false unless you explicitly accept data loss for availability.",
                )
            )

        auto_create = _to_bool(cfg.get("auto.create.topics.enable"))
        if auto_create is True:
            recommendations.append(
                self._create_recommendation(
                    title="auto.create.topics.enable is true",
                    description="Producers/consumers can implicitly create topics.",
                    severity="warning",
                    category="configuration",
                    current_value="true",
                    impact="Topics are silently created with default partition/RF settings, often suboptimal.",
                    recommendation="Disable in production and create topics explicitly via tooling.",
                )
            )

        delete_enabled = _to_bool(cfg.get("delete.topic.enable"))
        if delete_enabled is False:
            recommendations.append(
                self._create_recommendation(
                    title="delete.topic.enable is false",
                    description="Topics cannot be deleted via the admin client.",
                    severity="info",
                    category="configuration",
                    current_value="false",
                    recommendation="Modern Kafka defaults this to true; consider enabling unless you need the protection.",
                )
            )

        controlled_shutdown = _to_bool(cfg.get("controlled.shutdown.enable"))
        if controlled_shutdown is False:
            recommendations.append(
                self._create_recommendation(
                    title="controlled.shutdown.enable is false",
                    description="Brokers will not perform a controlled leader handover at shutdown.",
                    severity="warning",
                    category="configuration",
                    recommendation="Set controlled.shutdown.enable=true to avoid temporary leader gaps on planned restarts.",
                )
            )

        # Transaction state log durability.
        tx_rf = _to_int(cfg.get("transaction.state.log.replication.factor"))
        tx_isr = _to_int(cfg.get("transaction.state.log.min.isr"))
        if tx_rf is not None and tx_rf < 3 and cluster_state.get_total_brokers() >= 3:
            recommendations.append(
                self._create_recommendation(
                    title="transaction.state.log.replication.factor is below 3",
                    description=f"Set to {tx_rf}.",
                    severity="warning",
                    category="configuration",
                    current_value=str(tx_rf),
                    recommendation="Set transaction.state.log.replication.factor=3 to keep transactional state durable.",
                )
            )
        if tx_isr is not None and tx_isr < 2 and cluster_state.get_total_brokers() >= 3:
            recommendations.append(
                self._create_recommendation(
                    title="transaction.state.log.min.isr is below 2",
                    description=f"Set to {tx_isr}.",
                    severity="warning",
                    category="configuration",
                    current_value=str(tx_isr),
                    recommendation="Set transaction.state.log.min.isr=2.",
                )
            )

        # Network/IO thread sizing — very heuristic.
        io_threads = _to_int(cfg.get("num.io.threads"))
        if io_threads is not None and io_threads < 8:
            recommendations.append(
                self._create_recommendation(
                    title="num.io.threads is low",
                    description=f"num.io.threads={io_threads}.",
                    severity="info",
                    category="configuration",
                    current_value=str(io_threads),
                    recommendation="num.io.threads should be roughly the number of disks * 1-2 for I/O-bound workloads (default 8 is usually fine).",
                )
            )

        # JVM heap and GC.
        jvm_args = sample_broker.jvm_input_arguments
        details["jvm_input_arguments"] = jvm_args
        if jvm_args:
            xmx = _extract_xmx_mb(jvm_args)
            details["heap_xmx_mb"] = xmx
            if xmx is not None and xmx > 8192:
                recommendations.append(
                    self._create_recommendation(
                        title="Kafka heap is unusually large",
                        description=f"-Xmx is {xmx} MB.",
                        severity="warning",
                        category="configuration",
                        current_value=f"{xmx} MB",
                        impact="Kafka relies on the page cache; large heaps starve it and can lengthen GC pauses.",
                        recommendation="Most production Kafka brokers run with 4-8 GB heap; let the OS use the rest as page cache.",
                    )
                )
            if "UseConcMarkSweepGC" in jvm_args or "UseParallelGC" in jvm_args:
                recommendations.append(
                    self._create_recommendation(
                        title="JVM is using a legacy garbage collector",
                        description="Detected CMS or Parallel GC in the broker JVM args.",
                        severity="warning",
                        category="configuration",
                        recommendation="Move to G1GC (or ZGC on a recent JDK) for predictable pause times.",
                    )
                )

        # Inter-broker protocol / log message format.
        ibp = cfg.get("inter.broker.protocol.version")
        msg_fmt = cfg.get("log.message.format.version")
        if ibp:
            details["inter.broker.protocol.version"] = ibp
        if msg_fmt:
            details["log.message.format.version"] = msg_fmt
        if msg_fmt and ibp and msg_fmt != ibp:
            recommendations.append(
                self._create_recommendation(
                    title="inter.broker.protocol.version and log.message.format.version differ",
                    description=f"IBP={ibp}, log message format={msg_fmt}.",
                    severity="info",
                    category="configuration",
                    recommendation="During upgrades these intentionally diverge; once stable, raise both to the new version.",
                )
            )

        details["recommendation_count"] = len(recommendations)
        return {
            "recommendations": recommendations,
            "summary": {
                "issues": len(recommendations),
                "broker_count": cluster_state.get_total_brokers(),
                "inter_broker_protocol_version": ibp,
            },
            "details": details,
        }


def _extract_xmx_mb(jvm_args: str) -> Optional[int]:
    """Best-effort extraction of -Xmx from a JVM args string."""
    for token in jvm_args.split():
        if not token.startswith("-Xmx"):
            continue
        raw = token[4:].strip().lower()
        if raw.endswith("g"):
            try:
                return int(float(raw[:-1]) * 1024)
            except ValueError:
                return None
        if raw.endswith("m"):
            try:
                return int(float(raw[:-1]))
            except ValueError:
                return None
        if raw.endswith("k"):
            try:
                return int(float(raw[:-1]) / 1024)
            except ValueError:
                return None
        try:
            return int(float(raw) / (1024 * 1024))
        except ValueError:
            return None
    return None
