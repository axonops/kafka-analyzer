"""
Operations analyzer: cluster-health signals over the analysis window.

Looks at under-replicated partitions, offline partitions, ISR churn,
controller stability, request handler / network processor saturation,
request latencies, and leader skew across brokers.
"""

from typing import Any, Dict, List

from ..models import ClusterState, Recommendation
from .base import BaseAnalyzer


class OperationsAnalyzer(BaseAnalyzer):
    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}
        metrics = cluster_state.metrics

        # Under-replicated partitions (snapshot from topic metadata + metric history).
        urp_snapshot = cluster_state.under_replicated_partition_count()
        offline_snapshot = cluster_state.offline_partition_count()
        details["under_replicated_partitions_snapshot"] = urp_snapshot
        details["offline_partitions_snapshot"] = offline_snapshot

        urp_metric_max = self._get_metric_max(metrics, "under_replicated_partitions")
        offline_metric_max = self._get_metric_max(metrics, "offline_partitions")
        details["under_replicated_partitions_max"] = urp_metric_max
        details["offline_partitions_max"] = offline_metric_max

        if urp_snapshot > self.thresholds.under_replicated_partitions_warn or urp_metric_max > 0:
            recommendations.append(
                self._create_recommendation(
                    title="Under-replicated partitions observed",
                    description=(
                        f"{urp_snapshot} partitions are currently under-replicated; "
                        f"max during the window was {urp_metric_max:.0f}."
                    ),
                    severity="critical" if urp_snapshot > 0 else "warning",
                    category="operations",
                    current_value=str(urp_snapshot),
                    impact="URPs indicate a replica is not keeping up — risks data loss on broker failure.",
                    recommendation="Identify the slow follower (network, disk, GC) and resolve before more brokers are lost.",
                )
            )

        if offline_snapshot > self.thresholds.offline_partitions_warn or offline_metric_max > 0:
            recommendations.append(
                self._create_recommendation(
                    title="Offline partitions observed",
                    description=(
                        f"{offline_snapshot} partitions have no leader; "
                        f"max during window was {offline_metric_max:.0f}."
                    ),
                    severity="critical",
                    category="operations",
                    current_value=str(offline_snapshot),
                    impact="Offline partitions cannot serve produce or fetch requests.",
                    recommendation="Investigate broker availability for the affected partitions and recover the leader.",
                )
            )

        # Active controller count should be exactly 1.
        active_controller_avg = self._get_metric_average(metrics, "active_controller_count")
        active_controller_max = self._get_metric_max(metrics, "active_controller_count")
        details["active_controller_avg"] = round(active_controller_avg, 2)
        details["active_controller_max"] = round(active_controller_max, 2)
        if active_controller_max > 1:
            recommendations.append(
                self._create_recommendation(
                    title="Multiple active controllers detected",
                    description=f"ActiveControllerCount peaked at {active_controller_max:.0f}.",
                    severity="critical",
                    category="operations",
                    impact="More than one controller indicates split-brain in the metadata layer.",
                    recommendation="Investigate broker isolation, ZooKeeper/KRaft quorum health.",
                )
            )
        elif active_controller_avg < 0.95:
            recommendations.append(
                self._create_recommendation(
                    title="Controller is frequently unavailable",
                    description=f"ActiveControllerCount averaged {active_controller_avg:.2f}.",
                    severity="warning",
                    category="operations",
                    recommendation="Check for controller restarts and metadata layer instability.",
                )
            )

        # ISR churn.
        isr_shrink_avg = self._get_metric_average(metrics, "isr_shrinks")
        isr_expand_avg = self._get_metric_average(metrics, "isr_expands")
        details["isr_shrinks_per_sec_avg"] = round(isr_shrink_avg, 4)
        details["isr_expands_per_sec_avg"] = round(isr_expand_avg, 4)
        if isr_shrink_avg > self.thresholds.isr_shrink_rate_warn:
            recommendations.append(
                self._create_recommendation(
                    title="Frequent ISR shrinks",
                    description=f"Average ISR shrink rate of {isr_shrink_avg:.2f}/s across brokers.",
                    severity="warning",
                    category="operations",
                    impact="Followers are repeatedly falling behind; replication is unstable.",
                    recommendation="Look for GC pauses, network saturation, or slow disks on followers.",
                )
            )

        # Request handler / network processor idle ratio.
        rh_idle_avg = self._get_metric_average(metrics, "request_handler_idle")
        net_idle_avg = self._get_metric_average(metrics, "network_processor_idle")
        details["request_handler_idle_avg"] = round(rh_idle_avg, 3)
        details["network_processor_idle_avg"] = round(net_idle_avg, 3)
        if 0 < rh_idle_avg < self.thresholds.request_handler_idle_warn:
            recommendations.append(
                self._create_recommendation(
                    title="Request handler pool is saturated",
                    description=f"RequestHandlerAvgIdlePercent averaged {rh_idle_avg:.2f}.",
                    severity="warning",
                    category="operations",
                    current_value=f"{rh_idle_avg:.2f}",
                    impact="Below ~30% idle means brokers cannot keep up with request volume.",
                    recommendation="Increase num.io.threads, scale brokers, or rebalance leadership.",
                )
            )
        if 0 < net_idle_avg < self.thresholds.network_processor_idle_warn:
            recommendations.append(
                self._create_recommendation(
                    title="Network processors are saturated",
                    description=f"NetworkProcessorAvgIdlePercent averaged {net_idle_avg:.2f}.",
                    severity="warning",
                    category="operations",
                    current_value=f"{net_idle_avg:.2f}",
                    recommendation="Increase num.network.threads or scale brokers.",
                )
            )

        # Request latencies.
        for label, metric_key in (
            ("Produce", "produce_total_time_p99"),
            ("FetchConsumer", "fetch_consumer_total_time_p99"),
            ("FetchFollower", "fetch_follower_total_time_p99"),
        ):
            p99_max = self._get_metric_max(metrics, metric_key)
            details[f"{metric_key}_max_ms"] = round(p99_max, 1)
            if p99_max > self.thresholds.request_p99_warn_ms:
                recommendations.append(
                    self._create_recommendation(
                        title=f"High {label} request p99 latency",
                        description=f"{label} TotalTimeMs p99 peaked at {p99_max:.0f} ms.",
                        severity="warning",
                        category="operations",
                        current_value=f"{p99_max:.0f} ms",
                        recommendation=(
                            "Check network/disk on the affected broker, GC pauses, or producer/consumer batch sizes."
                        ),
                    )
                )

        # Failed produce / fetch rate.
        failed_produce = self._get_metric_max(metrics, "failed_produce_requests")
        failed_fetch = self._get_metric_max(metrics, "failed_fetch_requests")
        details["failed_produce_max"] = round(failed_produce, 2)
        details["failed_fetch_max"] = round(failed_fetch, 2)
        if failed_produce > 1:
            recommendations.append(
                self._create_recommendation(
                    title="Failed produce requests observed",
                    description=f"FailedProduceRequestsPerSec peaked at {failed_produce:.1f}.",
                    severity="warning",
                    category="operations",
                    recommendation="Inspect broker logs for the rejected requests; common causes are message-too-large, ACL denials, NotEnoughReplicas.",
                )
            )

        # Leader skew — calculated from topic metadata directly.
        leader_counts = cluster_state.leader_counts()
        details["leader_counts_per_broker"] = leader_counts
        if leader_counts and len(leader_counts) >= 2 and any(leader_counts.values()):
            values = [v for v in leader_counts.values() if v >= 0]
            if values and max(values) > 0:
                avg = sum(values) / len(values)
                if avg > 0:
                    skew = (max(values) - min(values)) / avg
                    details["leader_skew_ratio"] = round(skew, 3)
                    if skew > self.thresholds.leader_skew_warn:
                        recommendations.append(
                            self._create_recommendation(
                                title="Leadership is uneven across brokers",
                                description=(
                                    f"Leader-count skew is {skew:.0%} "
                                    f"(min={min(values)}, max={max(values)}, avg={avg:.1f})."
                                ),
                                severity="warning",
                                category="operations",
                                recommendation="Run `kafka-preferred-replica-election` or set auto.leader.rebalance.enable=true.",
                            )
                        )

        details["recommendation_count"] = len(recommendations)
        return {
            "recommendations": recommendations,
            "summary": {
                "under_replicated_partitions": urp_snapshot,
                "offline_partitions": offline_snapshot,
                "issues": len(recommendations),
            },
            "details": details,
        }
