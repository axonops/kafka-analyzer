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


_BROKER_CFG_SOURCE = "GET /broker/{id} -> configs[]"


class ConfigurationAnalyzer(BaseAnalyzer):
    """Inspects broker server.properties values and JVM arguments."""

    category = "configuration"
    default_recommendation_category = "configuration"

    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        self._reset_checks()
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}

        if not cluster_state.brokers:
            self._record_check(
                "config.precondition.brokers",
                "Brokers are reachable for config inspection",
                _BROKER_CFG_SOURCE,
                "no_data",
                skipped_reason="no brokers in ClusterState",
            )
            return {
                "recommendations": [],
                "summary": {},
                "details": {},
                "checks": [c.model_dump() for c in self._checks],
            }

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

        per_key: Dict[str, Dict[int, str]] = {}
        for broker in cluster_state.brokers.values():
            for key in config_keys_to_check:
                val = broker.configs.get(key)
                if val is None:
                    continue
                per_key.setdefault(key, {})[broker.broker_id] = val

        inconsistent: List[str] = []
        for key, values in per_key.items():
            if key == "broker.rack":
                continue
            if len(set(values.values())) > 1:
                inconsistent.append(key)

        if inconsistent:
            rec = self._create_recommendation(
                check_id="config.consistency.cross_broker",
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
            recommendations.append(rec)
            details["inconsistent_keys"] = inconsistent
            self._record_check(
                "config.consistency.cross_broker",
                "Broker config keys are uniform across brokers",
                _BROKER_CFG_SOURCE,
                "fail",
                recommendation_id=rec.id,
                inconsistent_keys=sorted(inconsistent),
            )
        else:
            self._record_check(
                "config.consistency.cross_broker",
                "Broker config keys are uniform across brokers",
                _BROKER_CFG_SOURCE,
                "pass",
            )

        sample_broker: Broker = next(iter(cluster_state.brokers.values()))
        details["sample_broker_id"] = sample_broker.broker_id
        cfg = sample_broker.configs

        # default.replication.factor — RF=1 is qualitatively worse than RF=2.
        rf = _to_int(cfg.get("default.replication.factor"))
        if rf is None:
            self._record_check(
                "config.replication.default_rf",
                "default.replication.factor meets minimum",
                _BROKER_CFG_SOURCE + " key=default.replication.factor",
                "no_data",
                skipped_reason="key not returned by broker config",
            )
        elif rf < self.thresholds.min_replication_factor:
            rf_severity = "critical" if rf <= 1 else "warning"
            rf_impact = (
                "Newly auto-created topics inherit this RF, so any single broker loss "
                "causes permanent data loss for those topics."
                if rf <= 1
                else "Newly auto-created topics inherit this RF."
            )
            rec = self._create_recommendation(
                check_id="config.replication.default_rf",
                title="default.replication.factor is below the recommended minimum",
                description=f"default.replication.factor is set to {rf}.",
                severity=rf_severity,
                category="configuration",
                current_value=str(rf),
                impact=rf_impact,
                recommendation=f"Set default.replication.factor to at least {self.thresholds.min_replication_factor}.",
            )
            recommendations.append(rec)
            self._record_check(
                "config.replication.default_rf",
                "default.replication.factor meets minimum",
                _BROKER_CFG_SOURCE + " key=default.replication.factor",
                "fail",
                recommendation_id=rec.id,
                value=rf,
            )
        else:
            self._record_check(
                "config.replication.default_rf",
                "default.replication.factor meets minimum",
                _BROKER_CFG_SOURCE + " key=default.replication.factor",
                "pass",
                value=rf,
            )

        # min.insync.replicas
        min_isr = _to_int(cfg.get("min.insync.replicas"))
        if min_isr is None:
            self._record_check(
                "config.replication.min_isr",
                "min.insync.replicas meets minimum",
                _BROKER_CFG_SOURCE + " key=min.insync.replicas",
                "no_data",
                skipped_reason="key not returned",
            )
        elif min_isr < self.thresholds.min_in_sync_replicas:
            rec = self._create_recommendation(
                check_id="config.replication.min_isr",
                title="min.insync.replicas is below the recommended minimum",
                description=f"min.insync.replicas is set to {min_isr}.",
                severity="warning",
                category="configuration",
                current_value=str(min_isr),
                impact="With acks=all, the broker may accept writes that survive a single broker loss without durable replication.",
                recommendation="Set min.insync.replicas to at least 2 (with RF >= 3) and require acks=all on producers.",
            )
            recommendations.append(rec)
            self._record_check(
                "config.replication.min_isr",
                "min.insync.replicas meets minimum",
                _BROKER_CFG_SOURCE + " key=min.insync.replicas",
                "fail",
                recommendation_id=rec.id,
                value=min_isr,
            )
        else:
            self._record_check(
                "config.replication.min_isr",
                "min.insync.replicas meets minimum",
                _BROKER_CFG_SOURCE + " key=min.insync.replicas",
                "pass",
                value=min_isr,
            )

        # unclean.leader.election.enable
        unclean = _to_bool(cfg.get("unclean.leader.election.enable"))
        if unclean is None:
            self._record_check(
                "config.election.unclean",
                "unclean.leader.election.enable is false",
                _BROKER_CFG_SOURCE + " key=unclean.leader.election.enable",
                "no_data",
                skipped_reason="key not returned",
            )
        elif unclean:
            rec = self._create_recommendation(
                check_id="config.election.unclean",
                title="unclean.leader.election.enable is true",
                description="Unclean leader election is enabled.",
                severity="critical",
                category="configuration",
                current_value="true",
                impact="A non-ISR replica can be elected leader, causing data loss.",
                recommendation="Set unclean.leader.election.enable=false unless you explicitly accept data loss for availability.",
            )
            recommendations.append(rec)
            self._record_check(
                "config.election.unclean",
                "unclean.leader.election.enable is false",
                _BROKER_CFG_SOURCE + " key=unclean.leader.election.enable",
                "fail",
                recommendation_id=rec.id,
            )
        else:
            self._record_check(
                "config.election.unclean",
                "unclean.leader.election.enable is false",
                _BROKER_CFG_SOURCE + " key=unclean.leader.election.enable",
                "pass",
            )

        # auto.create.topics.enable
        auto_create = _to_bool(cfg.get("auto.create.topics.enable"))
        if auto_create is None:
            self._record_check(
                "config.topics.auto_create",
                "auto.create.topics.enable is false",
                _BROKER_CFG_SOURCE + " key=auto.create.topics.enable",
                "no_data",
                skipped_reason="key not returned",
            )
        elif auto_create:
            rec = self._create_recommendation(
                check_id="config.topics.auto_create",
                title="auto.create.topics.enable is true",
                description="Producers/consumers can implicitly create topics.",
                severity="warning",
                category="configuration",
                current_value="true",
                impact="Topics are silently created with default partition/RF settings, often suboptimal.",
                recommendation="Disable in production and create topics explicitly via tooling.",
            )
            recommendations.append(rec)
            self._record_check(
                "config.topics.auto_create",
                "auto.create.topics.enable is false",
                _BROKER_CFG_SOURCE + " key=auto.create.topics.enable",
                "fail",
                recommendation_id=rec.id,
            )
        else:
            self._record_check(
                "config.topics.auto_create",
                "auto.create.topics.enable is false",
                _BROKER_CFG_SOURCE + " key=auto.create.topics.enable",
                "pass",
            )

        # delete.topic.enable
        delete_enabled = _to_bool(cfg.get("delete.topic.enable"))
        if delete_enabled is None:
            self._record_check(
                "config.topics.delete_enable",
                "delete.topic.enable matches modern default",
                _BROKER_CFG_SOURCE + " key=delete.topic.enable",
                "no_data",
                skipped_reason="key not returned",
            )
        elif delete_enabled is False:
            rec = self._create_recommendation(
                check_id="config.topics.delete_enable",
                title="delete.topic.enable is false",
                description="Topics cannot be deleted via the admin client.",
                severity="info",
                category="configuration",
                current_value="false",
                recommendation="Modern Kafka defaults this to true; consider enabling unless you need the protection.",
            )
            recommendations.append(rec)
            self._record_check(
                "config.topics.delete_enable",
                "delete.topic.enable matches modern default",
                _BROKER_CFG_SOURCE + " key=delete.topic.enable",
                "fail",
                recommendation_id=rec.id,
            )
        else:
            self._record_check(
                "config.topics.delete_enable",
                "delete.topic.enable matches modern default",
                _BROKER_CFG_SOURCE + " key=delete.topic.enable",
                "pass",
            )

        # controlled.shutdown.enable
        controlled_shutdown = _to_bool(cfg.get("controlled.shutdown.enable"))
        if controlled_shutdown is None:
            self._record_check(
                "config.shutdown.controlled",
                "controlled.shutdown.enable is true",
                _BROKER_CFG_SOURCE + " key=controlled.shutdown.enable",
                "no_data",
                skipped_reason="key not returned",
            )
        elif controlled_shutdown is False:
            rec = self._create_recommendation(
                check_id="config.shutdown.controlled",
                title="controlled.shutdown.enable is false",
                description="Brokers will not perform a controlled leader handover at shutdown.",
                severity="warning",
                category="configuration",
                recommendation="Set controlled.shutdown.enable=true to avoid temporary leader gaps on planned restarts.",
            )
            recommendations.append(rec)
            self._record_check(
                "config.shutdown.controlled",
                "controlled.shutdown.enable is true",
                _BROKER_CFG_SOURCE + " key=controlled.shutdown.enable",
                "fail",
                recommendation_id=rec.id,
            )
        else:
            self._record_check(
                "config.shutdown.controlled",
                "controlled.shutdown.enable is true",
                _BROKER_CFG_SOURCE + " key=controlled.shutdown.enable",
                "pass",
            )

        # transaction state log durability
        tx_rf = _to_int(cfg.get("transaction.state.log.replication.factor"))
        tx_isr = _to_int(cfg.get("transaction.state.log.min.isr"))
        total_brokers = cluster_state.get_total_brokers()
        if total_brokers < 3:
            self._record_check(
                "config.txn.log_rf",
                "transaction.state.log.replication.factor >= 3",
                _BROKER_CFG_SOURCE + " key=transaction.state.log.replication.factor",
                "skipped",
                skipped_reason="cluster has fewer than 3 brokers; threshold not applicable",
            )
            self._record_check(
                "config.txn.log_min_isr",
                "transaction.state.log.min.isr >= 2",
                _BROKER_CFG_SOURCE + " key=transaction.state.log.min.isr",
                "skipped",
                skipped_reason="cluster has fewer than 3 brokers; threshold not applicable",
            )
        else:
            if tx_rf is None:
                self._record_check(
                    "config.txn.log_rf",
                    "transaction.state.log.replication.factor >= 3",
                    _BROKER_CFG_SOURCE + " key=transaction.state.log.replication.factor",
                    "no_data",
                    skipped_reason="key not returned",
                )
            elif tx_rf < 3:
                rec = self._create_recommendation(
                    check_id="config.txn.log_rf",
                    title="transaction.state.log.replication.factor is below 3",
                    description=f"Set to {tx_rf}.",
                    severity="warning",
                    category="configuration",
                    current_value=str(tx_rf),
                    recommendation="Set transaction.state.log.replication.factor=3 to keep transactional state durable.",
                )
                recommendations.append(rec)
                self._record_check(
                    "config.txn.log_rf",
                    "transaction.state.log.replication.factor >= 3",
                    _BROKER_CFG_SOURCE + " key=transaction.state.log.replication.factor",
                    "fail",
                    recommendation_id=rec.id,
                    value=tx_rf,
                )
            else:
                self._record_check(
                    "config.txn.log_rf",
                    "transaction.state.log.replication.factor >= 3",
                    _BROKER_CFG_SOURCE + " key=transaction.state.log.replication.factor",
                    "pass",
                    value=tx_rf,
                )
            if tx_isr is None:
                self._record_check(
                    "config.txn.log_min_isr",
                    "transaction.state.log.min.isr >= 2",
                    _BROKER_CFG_SOURCE + " key=transaction.state.log.min.isr",
                    "no_data",
                    skipped_reason="key not returned",
                )
            elif tx_isr < 2:
                rec = self._create_recommendation(
                    check_id="config.txn.log_min_isr",
                    title="transaction.state.log.min.isr is below 2",
                    description=f"Set to {tx_isr}.",
                    severity="warning",
                    category="configuration",
                    current_value=str(tx_isr),
                    recommendation="Set transaction.state.log.min.isr=2.",
                )
                recommendations.append(rec)
                self._record_check(
                    "config.txn.log_min_isr",
                    "transaction.state.log.min.isr >= 2",
                    _BROKER_CFG_SOURCE + " key=transaction.state.log.min.isr",
                    "fail",
                    recommendation_id=rec.id,
                    value=tx_isr,
                )
            else:
                self._record_check(
                    "config.txn.log_min_isr",
                    "transaction.state.log.min.isr >= 2",
                    _BROKER_CFG_SOURCE + " key=transaction.state.log.min.isr",
                    "pass",
                    value=tx_isr,
                )

        # num.io.threads
        io_threads = _to_int(cfg.get("num.io.threads"))
        if io_threads is None:
            self._record_check(
                "config.threads.io",
                "num.io.threads >= 8",
                _BROKER_CFG_SOURCE + " key=num.io.threads",
                "no_data",
                skipped_reason="key not returned",
            )
        elif io_threads < 8:
            rec = self._create_recommendation(
                check_id="config.threads.io",
                title="num.io.threads is low",
                description=f"num.io.threads={io_threads}.",
                severity="info",
                category="configuration",
                current_value=str(io_threads),
                recommendation="num.io.threads should be roughly the number of disks * 1-2 for I/O-bound workloads (default 8 is usually fine).",
            )
            recommendations.append(rec)
            self._record_check(
                "config.threads.io",
                "num.io.threads >= 8",
                _BROKER_CFG_SOURCE + " key=num.io.threads",
                "fail",
                recommendation_id=rec.id,
                value=io_threads,
            )
        else:
            self._record_check(
                "config.threads.io",
                "num.io.threads >= 8",
                _BROKER_CFG_SOURCE + " key=num.io.threads",
                "pass",
                value=io_threads,
            )

        # JVM heap & GC.
        jvm_args = sample_broker.jvm_input_arguments
        details["jvm_input_arguments"] = jvm_args
        if not jvm_args:
            self._record_check(
                "config.jvm.heap",
                "Kafka heap size is in the recommended range",
                "GET /nodes-full -> Details.comp_jvm_input_arguments",
                "no_data",
                skipped_reason="JVM input arguments not reported by agent",
            )
            self._record_check(
                "config.jvm.gc",
                "JVM uses a modern garbage collector",
                "GET /nodes-full -> Details.comp_jvm_input_arguments",
                "no_data",
                skipped_reason="JVM input arguments not reported by agent",
            )
        else:
            xmx = _extract_xmx_mb(jvm_args)
            details["heap_xmx_mb"] = xmx
            if xmx is None:
                self._record_check(
                    "config.jvm.heap",
                    "Kafka heap size is in the recommended range",
                    "GET /nodes-full -> Details.comp_jvm_input_arguments",
                    "no_data",
                    skipped_reason="-Xmx not present in JVM args",
                )
            elif xmx > 8192:
                rec = self._create_recommendation(
                    check_id="config.jvm.heap",
                    title="Kafka heap is unusually large",
                    description=f"-Xmx is {xmx} MB.",
                    severity="warning",
                    category="configuration",
                    current_value=f"{xmx} MB",
                    impact="Kafka relies on the page cache; large heaps starve it and can lengthen GC pauses.",
                    recommendation="Most production Kafka brokers run with 4-8 GB heap; let the OS use the rest as page cache.",
                )
                recommendations.append(rec)
                self._record_check(
                    "config.jvm.heap",
                    "Kafka heap size is in the recommended range",
                    "GET /nodes-full -> Details.comp_jvm_input_arguments",
                    "fail",
                    recommendation_id=rec.id,
                    xmx_mb=xmx,
                )
            else:
                self._record_check(
                    "config.jvm.heap",
                    "Kafka heap size is in the recommended range",
                    "GET /nodes-full -> Details.comp_jvm_input_arguments",
                    "pass",
                    xmx_mb=xmx,
                )
            if "UseConcMarkSweepGC" in jvm_args or "UseParallelGC" in jvm_args:
                rec = self._create_recommendation(
                    check_id="config.jvm.gc",
                    title="JVM is using a legacy garbage collector",
                    description="Detected CMS or Parallel GC in the broker JVM args.",
                    severity="warning",
                    category="configuration",
                    recommendation="Move to G1GC (or ZGC on a recent JDK) for predictable pause times.",
                )
                recommendations.append(rec)
                self._record_check(
                    "config.jvm.gc",
                    "JVM uses a modern garbage collector",
                    "GET /nodes-full -> Details.comp_jvm_input_arguments",
                    "fail",
                    recommendation_id=rec.id,
                )
            else:
                self._record_check(
                    "config.jvm.gc",
                    "JVM uses a modern garbage collector",
                    "GET /nodes-full -> Details.comp_jvm_input_arguments",
                    "pass",
                )

        # IBP / log message format alignment.
        ibp = cfg.get("inter.broker.protocol.version")
        msg_fmt = cfg.get("log.message.format.version")
        if ibp:
            details["inter.broker.protocol.version"] = ibp
        if msg_fmt:
            details["log.message.format.version"] = msg_fmt
        if not ibp or not msg_fmt:
            self._record_check(
                "config.protocol.version_alignment",
                "inter.broker.protocol.version matches log.message.format.version",
                _BROKER_CFG_SOURCE + " keys=inter.broker.protocol.version,log.message.format.version",
                "no_data",
                skipped_reason="one or both keys not returned (often absent on KRaft clusters)",
            )
        elif msg_fmt != ibp:
            rec = self._create_recommendation(
                check_id="config.protocol.version_alignment",
                title="inter.broker.protocol.version and log.message.format.version differ",
                description=f"IBP={ibp}, log message format={msg_fmt}.",
                severity="info",
                category="configuration",
                recommendation="During upgrades these intentionally diverge; once stable, raise both to the new version.",
            )
            recommendations.append(rec)
            self._record_check(
                "config.protocol.version_alignment",
                "inter.broker.protocol.version matches log.message.format.version",
                _BROKER_CFG_SOURCE + " keys=inter.broker.protocol.version,log.message.format.version",
                "fail",
                recommendation_id=rec.id,
                ibp=ibp, msg_fmt=msg_fmt,
            )
        else:
            self._record_check(
                "config.protocol.version_alignment",
                "inter.broker.protocol.version matches log.message.format.version",
                _BROKER_CFG_SOURCE + " keys=inter.broker.protocol.version,log.message.format.version",
                "pass",
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
            "checks": [c.model_dump() for c in self._checks],
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
