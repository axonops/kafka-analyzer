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
    category = "operations"
    # URP/offline-partition/controller-stability/ISR-churn findings are
    # reliability concerns. Latency, request-handler saturation, and skew
    # findings override per call to performance.
    default_recommendation_category = "reliability"

    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        self._reset_checks()
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}
        metrics = cluster_state.metrics

        # ---- under-replicated partitions --------------------------------
        urp_snapshot = cluster_state.under_replicated_partition_count()
        offline_snapshot = cluster_state.offline_partition_count()
        details["under_replicated_partitions_snapshot"] = urp_snapshot
        details["offline_partitions_snapshot"] = offline_snapshot

        urp_metric_max = self._get_metric_max(metrics, "under_replicated_partitions")
        offline_metric_max = self._get_metric_max(metrics, "offline_partitions")
        details["under_replicated_partitions_max"] = urp_metric_max
        details["offline_partitions_max"] = offline_metric_max

        if urp_snapshot > self.thresholds.under_replicated_partitions_warn or urp_metric_max > 0:
            rec = self._create_recommendation(
                check_id="ops.replication.urp",
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
            recommendations.append(rec)
            self._record_check(
                "ops.replication.urp",
                "No under-replicated partitions over the analysis window",
                "topic metadata + metric: under_replicated_partitions",
                "fail",
                recommendation_id=rec.id,
                snapshot=urp_snapshot, window_max=urp_metric_max,
            )
        else:
            self._record_check(
                "ops.replication.urp",
                "No under-replicated partitions over the analysis window",
                "topic metadata + metric: under_replicated_partitions",
                "pass",
            )

        if offline_snapshot > self.thresholds.offline_partitions_warn or offline_metric_max > 0:
            rec = self._create_recommendation(
                check_id="ops.replication.offline",
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
            recommendations.append(rec)
            self._record_check(
                "ops.replication.offline",
                "No offline partitions over the analysis window",
                "topic metadata + metric: offline_partitions",
                "fail",
                recommendation_id=rec.id,
            )
        else:
            self._record_check(
                "ops.replication.offline",
                "No offline partitions over the analysis window",
                "topic metadata + metric: offline_partitions",
                "pass",
            )

        # ---- controller stability ---------------------------------------
        controller_data = metrics.get("active_controller_count")
        active_controller_avg = self._get_metric_average(metrics, "active_controller_count")
        active_controller_max = self._get_metric_max(metrics, "active_controller_count")
        details["active_controller_avg"] = round(active_controller_avg, 2)
        details["active_controller_max"] = round(active_controller_max, 2)
        if not controller_data:
            self._record_check(
                "ops.controller.stability",
                "Exactly one active controller for the window",
                "metric: active_controller_count",
                "no_data",
                skipped_reason="metric series 'active_controller_count' not returned",
            )
        elif active_controller_max > 1:
            rec = self._create_recommendation(
                check_id="ops.controller.stability",
                title="Multiple active controllers detected",
                description=f"ActiveControllerCount peaked at {active_controller_max:.0f}.",
                severity="critical",
                category="operations",
                impact="More than one controller indicates split-brain in the metadata layer.",
                recommendation="Investigate broker isolation, ZooKeeper/KRaft quorum health.",
            )
            recommendations.append(rec)
            self._record_check(
                "ops.controller.stability",
                "Exactly one active controller for the window",
                "metric: active_controller_count",
                "fail",
                recommendation_id=rec.id,
                avg=round(active_controller_avg, 2),
                max=round(active_controller_max, 2),
            )
        elif active_controller_avg < 0.95:
            rec = self._create_recommendation(
                check_id="ops.controller.stability",
                title="Controller is frequently unavailable",
                description=f"ActiveControllerCount averaged {active_controller_avg:.2f}.",
                severity="warning",
                category="operations",
                recommendation="Check for controller restarts and metadata layer instability.",
            )
            recommendations.append(rec)
            self._record_check(
                "ops.controller.stability",
                "Exactly one active controller for the window",
                "metric: active_controller_count",
                "fail",
                recommendation_id=rec.id,
                avg=round(active_controller_avg, 2),
            )
        else:
            self._record_check(
                "ops.controller.stability",
                "Exactly one active controller for the window",
                "metric: active_controller_count",
                "pass",
                avg=round(active_controller_avg, 2),
            )

        # ---- ISR churn --------------------------------------------------
        isr_data = metrics.get("isr_shrinks")
        isr_shrink_avg = self._get_metric_average(metrics, "isr_shrinks")
        isr_expand_avg = self._get_metric_average(metrics, "isr_expands")
        details["isr_shrinks_per_sec_avg"] = round(isr_shrink_avg, 4)
        details["isr_expands_per_sec_avg"] = round(isr_expand_avg, 4)
        if not isr_data:
            self._record_check(
                "ops.isr.churn",
                "ISR shrink rate is below threshold",
                "metric: isr_shrinks",
                "no_data",
                skipped_reason="metric series 'isr_shrinks' not returned",
            )
        elif isr_shrink_avg > self.thresholds.isr_shrink_rate_warn:
            rec = self._create_recommendation(
                check_id="ops.isr.churn",
                title="Frequent ISR shrinks",
                description=f"Average ISR shrink rate of {isr_shrink_avg:.2f}/s across brokers.",
                severity="warning",
                category="operations",
                impact="Followers are repeatedly falling behind; replication is unstable.",
                recommendation="Look for GC pauses, network saturation, or slow disks on followers.",
            )
            recommendations.append(rec)
            self._record_check(
                "ops.isr.churn", "ISR shrink rate is below threshold", "metric: isr_shrinks",
                "fail", recommendation_id=rec.id, avg=round(isr_shrink_avg, 4),
            )
        else:
            self._record_check(
                "ops.isr.churn", "ISR shrink rate is below threshold", "metric: isr_shrinks",
                "pass", avg=round(isr_shrink_avg, 4),
            )

        # ---- request handler / network processor ------------------------
        rh_data = metrics.get("request_handler_idle")
        net_data = metrics.get("network_processor_idle")
        rh_idle_avg = self._get_metric_average(metrics, "request_handler_idle")
        net_idle_avg = self._get_metric_average(metrics, "network_processor_idle")
        details["request_handler_idle_avg"] = round(rh_idle_avg, 3)
        details["network_processor_idle_avg"] = round(net_idle_avg, 3)
        if not rh_data:
            self._record_check(
                "ops.threads.request_handler",
                "Request handler pool has sufficient idle headroom",
                "metric: request_handler_idle",
                "no_data",
                skipped_reason="metric series 'request_handler_idle' not returned",
            )
        elif 0 < rh_idle_avg < self.thresholds.request_handler_idle_warn:
            rec = self._create_recommendation(
                check_id="ops.threads.request_handler",
                title="Request handler pool is saturated",
                description=f"RequestHandlerAvgIdlePercent averaged {rh_idle_avg:.2f}.",
                severity="warning",
                category="operations",
                current_value=f"{rh_idle_avg:.2f}",
                impact="Below ~30% idle means brokers cannot keep up with request volume.",
                recommendation="Increase num.io.threads, scale brokers, or rebalance leadership.",
            )
            recommendations.append(rec)
            self._record_check(
                "ops.threads.request_handler",
                "Request handler pool has sufficient idle headroom",
                "metric: request_handler_idle",
                "fail", recommendation_id=rec.id, avg=round(rh_idle_avg, 3),
            )
        else:
            self._record_check(
                "ops.threads.request_handler",
                "Request handler pool has sufficient idle headroom",
                "metric: request_handler_idle",
                "pass", avg=round(rh_idle_avg, 3),
            )

        if not net_data:
            self._record_check(
                "ops.threads.network_processor",
                "Network processors have sufficient idle headroom",
                "metric: network_processor_idle",
                "no_data",
                skipped_reason="metric series 'network_processor_idle' not returned",
            )
        elif 0 < net_idle_avg < self.thresholds.network_processor_idle_warn:
            rec = self._create_recommendation(
                check_id="ops.threads.network_processor",
                title="Network processors are saturated",
                description=f"NetworkProcessorAvgIdlePercent averaged {net_idle_avg:.2f}.",
                severity="warning",
                category="operations",
                current_value=f"{net_idle_avg:.2f}",
                recommendation="Increase num.network.threads or scale brokers.",
            )
            recommendations.append(rec)
            self._record_check(
                "ops.threads.network_processor",
                "Network processors have sufficient idle headroom",
                "metric: network_processor_idle",
                "fail", recommendation_id=rec.id, avg=round(net_idle_avg, 3),
            )
        else:
            self._record_check(
                "ops.threads.network_processor",
                "Network processors have sufficient idle headroom",
                "metric: network_processor_idle",
                "pass", avg=round(net_idle_avg, 3),
            )

        # ---- request latencies ------------------------------------------
        for label, metric_key, check_id in (
            ("Produce", "produce_total_time_p99", "ops.latency.produce_p99"),
            ("FetchConsumer", "fetch_consumer_total_time_p99", "ops.latency.fetch_consumer_p99"),
            ("FetchFollower", "fetch_follower_total_time_p99", "ops.latency.fetch_follower_p99"),
        ):
            metric_data = metrics.get(metric_key)
            p99_max = self._get_metric_max(metrics, metric_key)
            details[f"{metric_key}_max_ms"] = round(p99_max, 1)
            check_desc = f"{label} p99 latency below {self.thresholds.request_p99_warn_ms}ms"
            if not metric_data:
                self._record_check(
                    check_id, check_desc, f"metric: {metric_key}",
                    "no_data", skipped_reason=f"metric series '{metric_key}' not returned",
                )
            elif p99_max > self.thresholds.request_p99_warn_ms:
                rec = self._create_recommendation(
                    check_id=check_id,
                    title=f"High {label} request p99 latency",
                    description=f"{label} TotalTimeMs p99 peaked at {p99_max:.0f} ms.",
                    severity="warning",
                    category="operations",
                    current_value=f"{p99_max:.0f} ms",
                    recommendation=(
                        "Check network/disk on the affected broker, GC pauses, or producer/consumer batch sizes."
                    ),
                )
                recommendations.append(rec)
                self._record_check(
                    check_id, check_desc, f"metric: {metric_key}",
                    "fail", recommendation_id=rec.id, p99_max_ms=round(p99_max, 1),
                )
            else:
                self._record_check(
                    check_id, check_desc, f"metric: {metric_key}",
                    "pass", p99_max_ms=round(p99_max, 1),
                )

        # ---- failed requests --------------------------------------------
        failed_produce_data = metrics.get("failed_produce_requests")
        failed_produce = self._get_metric_max(metrics, "failed_produce_requests")
        failed_fetch = self._get_metric_max(metrics, "failed_fetch_requests")
        details["failed_produce_max"] = round(failed_produce, 2)
        details["failed_fetch_max"] = round(failed_fetch, 2)
        if not failed_produce_data:
            self._record_check(
                "ops.requests.failed_produce",
                "FailedProduceRequestsPerSec stays below 1",
                "metric: failed_produce_requests",
                "no_data",
                skipped_reason="metric series 'failed_produce_requests' not returned",
            )
        elif failed_produce > 1:
            rec = self._create_recommendation(
                check_id="ops.requests.failed_produce",
                title="Failed produce requests observed",
                description=f"FailedProduceRequestsPerSec peaked at {failed_produce:.1f}.",
                severity="warning",
                category="operations",
                recommendation="Inspect broker logs for the rejected requests; common causes are message-too-large, ACL denials, NotEnoughReplicas.",
            )
            recommendations.append(rec)
            self._record_check(
                "ops.requests.failed_produce",
                "FailedProduceRequestsPerSec stays below 1",
                "metric: failed_produce_requests",
                "fail", recommendation_id=rec.id, peak=round(failed_produce, 1),
            )
        else:
            self._record_check(
                "ops.requests.failed_produce",
                "FailedProduceRequestsPerSec stays below 1",
                "metric: failed_produce_requests",
                "pass", peak=round(failed_produce, 1),
            )

        # ---- leader skew -------------------------------------------------
        leader_counts = cluster_state.leader_counts()
        details["leader_counts_per_broker"] = leader_counts
        if not leader_counts or len(leader_counts) < 2 or not any(leader_counts.values()):
            self._record_check(
                "ops.leader.skew",
                "Leader distribution skew across brokers below threshold",
                "topic metadata: partition.leader",
                "no_data",
                skipped_reason="fewer than two brokers with leadership data",
            )
        else:
            values = [v for v in leader_counts.values() if v >= 0]
            avg = sum(values) / len(values) if values else 0
            if avg <= 0 or max(values) <= 0:
                self._record_check(
                    "ops.leader.skew",
                    "Leader distribution skew across brokers below threshold",
                    "topic metadata: partition.leader",
                    "no_data",
                    skipped_reason="no observed leaders",
                )
            else:
                skew = (max(values) - min(values)) / avg
                details["leader_skew_ratio"] = round(skew, 3)
                if skew > self.thresholds.leader_skew_warn:
                    rec = self._create_recommendation(
                        check_id="ops.leader.skew",
                        title="Leadership is uneven across brokers",
                        description=(
                            f"Leader-count skew is {skew:.0%} "
                            f"(min={min(values)}, max={max(values)}, avg={avg:.1f})."
                        ),
                        severity="warning",
                        category="operations",
                        recommendation="Run `kafka-preferred-replica-election` or set auto.leader.rebalance.enable=true.",
                    )
                    recommendations.append(rec)
                    self._record_check(
                        "ops.leader.skew",
                        "Leader distribution skew across brokers below threshold",
                        "topic metadata: partition.leader",
                        "fail", recommendation_id=rec.id, skew=round(skew, 3),
                    )
                else:
                    self._record_check(
                        "ops.leader.skew",
                        "Leader distribution skew across brokers below threshold",
                        "topic metadata: partition.leader",
                        "pass", skew=round(skew, 3),
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
            "checks": [c.model_dump() for c in self._checks],
        }
