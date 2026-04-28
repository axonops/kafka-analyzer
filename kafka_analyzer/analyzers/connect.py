"""
Kafka Connect analyzer.

Reviews Connect cluster presence, connector states, task health, error
tolerance, dead-letter-queue setup, and connectors that target topics
which no longer exist on the Kafka cluster.
"""

from typing import Any, Dict, List, Optional

from ..models import ClusterState, Recommendation
from .base import BaseAnalyzer


_FAILED_STATES = {"FAILED"}
_PAUSED_STATES = {"PAUSED"}
_UNASSIGNED_STATES = {"UNASSIGNED", "DESTROYED"}
_HEALTHY_STATES = {"RUNNING"}


class ConnectAnalyzer(BaseAnalyzer):
    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}

        connectors = cluster_state.connectors or []
        details["connector_count"] = len(connectors)
        details["connect_cluster_count"] = self._connect_cluster_count(cluster_state)

        if not connectors:
            details["recommendation_count"] = 0
            return {
                "recommendations": [],
                "summary": {"connector_count": 0, "issues": 0},
                "details": details,
            }

        failed: List[str] = []
        paused: List[str] = []
        unassigned: List[str] = []
        connectors_with_failed_tasks: List[str] = []
        connectors_no_dlq: List[str] = []
        connectors_no_error_tolerance: List[str] = []
        connectors_no_retries: List[str] = []
        single_task_connectors: List[str] = []
        connectors_targeting_missing_topics: List[Dict[str, Any]] = []
        topic_names = set(cluster_state.topics.keys())

        state_breakdown: Dict[str, int] = {}

        for connector in connectors:
            name = connector.get("name", "<unknown>")
            state = (connector.get("state") or "").upper() or "UNKNOWN"
            state_breakdown[state] = state_breakdown.get(state, 0) + 1

            if state in _FAILED_STATES:
                failed.append(name)
            elif state in _PAUSED_STATES:
                paused.append(name)
            elif state in _UNASSIGNED_STATES:
                unassigned.append(name)

            tasks = connector.get("tasks") or []
            failed_tasks = [
                t for t in tasks
                if isinstance(t, dict)
                and (str(t.get("state") or t.get("status") or "")).upper() == "FAILED"
            ]
            if failed_tasks:
                connectors_with_failed_tasks.append(name)
            if isinstance(tasks, list) and len(tasks) <= 1:
                single_task_connectors.append(name)

            cfg = connector.get("config") or {}
            if not isinstance(cfg, dict):
                cfg = {}

            # Error tolerance / DLQ.
            error_tolerance = (cfg.get("errors.tolerance") or "").lower()
            dlq_topic = cfg.get("errors.deadletterqueue.topic.name")
            retries = cfg.get("errors.retry.timeout") or cfg.get("errors.retry.delay.max.ms")

            if error_tolerance not in {"all", "none"}:
                # Default ("none") is fine; flag only if explicitly something weird.
                pass
            if error_tolerance == "all" and not dlq_topic:
                connectors_no_dlq.append(name)
            if not error_tolerance:
                connectors_no_error_tolerance.append(name)
            if not retries:
                connectors_no_retries.append(name)

            # Topics referenced by the connector config.
            referenced = self._connector_topics(cfg)
            if referenced and topic_names:
                missing = sorted(t for t in referenced if t not in topic_names)
                if missing:
                    connectors_targeting_missing_topics.append(
                        {"connector": name, "missing_topics": missing}
                    )

        details["state_breakdown"] = state_breakdown
        details["failed_connectors"] = failed
        details["paused_connectors"] = paused
        details["unassigned_connectors"] = unassigned
        details["connectors_with_failed_tasks"] = connectors_with_failed_tasks
        details["connectors_targeting_missing_topics"] = connectors_targeting_missing_topics

        if failed:
            recommendations.append(
                self._create_recommendation(
                    title="Failed Kafka Connect connectors",
                    description=f"{len(failed)} connector(s) are in FAILED state.",
                    severity="critical",
                    category="connect",
                    impact="Failed connectors are not moving data; downstream systems are stale.",
                    recommendation="Inspect connector logs and restart once the underlying error is resolved.",
                    connectors=failed[:25],
                )
            )

        if connectors_with_failed_tasks:
            recommendations.append(
                self._create_recommendation(
                    title="Connectors with failed tasks",
                    description=f"{len(connectors_with_failed_tasks)} connector(s) have at least one task in FAILED state.",
                    severity="warning",
                    category="connect",
                    impact="A failed task means a partition / partition-range is not being processed.",
                    recommendation="Restart the failed tasks (`POST /connectors/<name>/tasks/<id>/restart`) after diagnosing the cause.",
                    connectors=connectors_with_failed_tasks[:25],
                )
            )

        if unassigned:
            recommendations.append(
                self._create_recommendation(
                    title="Unassigned connectors",
                    description=f"{len(unassigned)} connector(s) are UNASSIGNED.",
                    severity="warning",
                    category="connect",
                    recommendation="Check the Connect worker availability and the connector's last-known config.",
                    connectors=unassigned[:25],
                )
            )

        if paused:
            recommendations.append(
                self._create_recommendation(
                    title="Paused connectors",
                    description=f"{len(paused)} connector(s) are PAUSED.",
                    severity="info",
                    category="connect",
                    recommendation="Confirm the pause is intentional; resume once the maintenance window is over.",
                    connectors=paused[:25],
                )
            )

        if connectors_no_dlq:
            recommendations.append(
                self._create_recommendation(
                    title="Connectors with errors.tolerance=all but no DLQ topic",
                    description=f"{len(connectors_no_dlq)} connector(s) silently drop bad records.",
                    severity="warning",
                    category="connect",
                    impact="Without errors.deadletterqueue.topic.name, malformed records are discarded with no audit trail.",
                    recommendation="Set errors.deadletterqueue.topic.name (and errors.deadletterqueue.context.headers.enable=true).",
                    connectors=connectors_no_dlq[:25],
                )
            )

        if connectors_targeting_missing_topics:
            names = [c["connector"] for c in connectors_targeting_missing_topics]
            recommendations.append(
                self._create_recommendation(
                    title="Connectors reference topics that do not exist",
                    description=(
                        f"{len(connectors_targeting_missing_topics)} connector(s) name topics in their "
                        "config that are not present in the cluster."
                    ),
                    severity="warning",
                    category="connect",
                    impact=(
                        "Sink connectors will produce no progress; source connectors may auto-create "
                        "topics with default RF/partitions, which is usually wrong."
                    ),
                    recommendation="Reconcile topic names or pre-create the topics with proper RF and partition counts.",
                    connectors=names[:25],
                    detail=connectors_targeting_missing_topics[:10],
                )
            )

        if single_task_connectors:
            recommendations.append(
                self._create_recommendation(
                    title="Connectors running with a single task",
                    description=f"{len(single_task_connectors)} connector(s) have tasks.max effectively at 1.",
                    severity="info",
                    category="connect",
                    recommendation="Single-task connectors cap throughput and lose parallelism — verify this matches the workload.",
                    connectors=single_task_connectors[:25],
                )
            )

        details["recommendation_count"] = len(recommendations)
        return {
            "recommendations": recommendations,
            "summary": {
                "connector_count": len(connectors),
                "failed_connectors": len(failed),
                "paused_connectors": len(paused),
                "issues": len(recommendations),
            },
            "details": details,
        }

    @staticmethod
    def _connect_cluster_count(state: ClusterState) -> int:
        cc = state.connect_clusters
        if isinstance(cc, list):
            return len(cc)
        if isinstance(cc, dict):
            for key in ("clusters", "data"):
                value = cc.get(key)
                if isinstance(value, list):
                    return len(value)
        return 0

    @staticmethod
    def _connector_topics(cfg: Dict[str, Any]) -> List[str]:
        """Extract topic names referenced by a connector config."""
        out: List[str] = []
        topic = cfg.get("topic")
        if isinstance(topic, str) and topic:
            out.append(topic.strip())
        topics = cfg.get("topics")
        if isinstance(topics, str) and topics:
            out.extend(t.strip() for t in topics.split(",") if t.strip())
        # Skip topics.regex — we don't try to resolve regex against current topics.
        return out
