"""
Infrastructure analyzer for Kafka brokers.

Reviews host-level metrics (CPU, memory, disk, network) and broker
distribution (rack placement, log dir balance).
"""

from typing import Any, Dict, List

from ..models import ClusterState, Recommendation
from .base import BaseAnalyzer


class InfrastructureAnalyzer(BaseAnalyzer):
    category = "infrastructure"
    # Broker count / rack distribution / cluster topology findings are
    # reliability concerns. CPU/memory/disk/log-dir balance findings override
    # per call to performance or capacity.
    default_recommendation_category = "reliability"

    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        self._reset_checks()
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}

        broker_count = cluster_state.get_total_brokers()
        active = cluster_state.get_active_brokers()
        racks = cluster_state.get_racks()

        details["broker_count"] = broker_count
        details["active_brokers"] = active
        details["racks"] = racks
        details["controller_id"] = cluster_state.controller_id

        # ---- broker count ------------------------------------------------
        if broker_count == 0:
            rec = self._create_recommendation(
                check_id="infra.broker_count.zero",
                title="No brokers reported",
                description="The AxonOps API returned no brokers for this cluster.",
                severity="critical",
                category="infrastructure",
                recommendation="Verify the cluster name and that AxonOps agents are connected.",
            )
            recommendations.append(rec)
            self._record_check(
                "infra.broker_count.zero",
                "Cluster reports at least one broker",
                "GET /clusters/{cluster}/info -> brokers[]",
                "fail",
                recommendation_id=rec.id,
            )
            details["recommendation_count"] = len(recommendations)
            return {
                "recommendations": recommendations,
                "summary": details,
                "details": details,
                "checks": [c.model_dump() for c in self._checks],
            }

        if broker_count < 3:
            rec = self._create_recommendation(
                check_id="infra.broker_count.below_three",
                title="Cluster has fewer than 3 brokers",
                description=f"Only {broker_count} broker(s) are reporting.",
                severity="warning",
                category="infrastructure",
                impact="A cluster with fewer than 3 brokers cannot tolerate a single broker failure while preserving RF=3.",
                recommendation="Run at least 3 brokers in production.",
                current_value=str(broker_count),
            )
            recommendations.append(rec)
            self._record_check(
                "infra.broker_count.below_three",
                "Cluster has at least 3 brokers",
                "GET /clusters/{cluster}/info -> brokers[]",
                "fail",
                recommendation_id=rec.id,
                broker_count=broker_count,
            )
        else:
            self._record_check(
                "infra.broker_count.below_three",
                "Cluster has at least 3 brokers",
                "GET /clusters/{cluster}/info -> brokers[]",
                "pass",
                broker_count=broker_count,
            )

        # ---- active brokers ----------------------------------------------
        if active < broker_count:
            rec = self._create_recommendation(
                check_id="infra.broker_count.inactive",
                title="Some brokers appear inactive",
                description=f"{broker_count - active} of {broker_count} brokers are not reporting recent host metrics.",
                severity="warning",
                category="infrastructure",
                recommendation="Check the AxonOps agent on the affected hosts.",
            )
            recommendations.append(rec)
            self._record_check(
                "infra.broker_count.inactive",
                "All brokers report recent host metrics",
                "GET /nodes-full -> Details.host_uptime / agent_version",
                "fail",
                recommendation_id=rec.id,
                active=active,
                total=broker_count,
            )
        else:
            self._record_check(
                "infra.broker_count.inactive",
                "All brokers report recent host metrics",
                "GET /nodes-full -> Details.host_uptime / agent_version",
                "pass",
            )

        # ---- rack assignment --------------------------------------------
        per_broker_rack: Dict[int, str] = {
            b.broker_id: (b.rack or "").strip() for b in cluster_state.brokers.values()
        }
        details["broker_rack_values"] = per_broker_rack
        placeholder_racks = {"", "unspecified", "norack", "none", "unknown", "default"}
        missing_brokers = [
            bid for bid, r in per_broker_rack.items() if r.lower() in placeholder_racks
        ]

        if not racks or racks == ["unknown"]:
            rec = self._create_recommendation(
                check_id="infra.rack.missing",
                title="Brokers are not rack-aware",
                description="No broker rack assignment was detected.",
                severity="warning",
                category="infrastructure",
                impact="Without broker.rack, replicas may end up co-located in the same fault zone.",
                recommendation="Set broker.rack on each broker (e.g. AZ name) and use rack-aware partition assignment.",
            )
            recommendations.append(rec)
            self._record_check(
                "infra.rack.assigned",
                "broker.rack is set on every broker",
                "GET /broker/{id} -> configs[broker.rack]",
                "fail",
                recommendation_id=rec.id,
            )
        elif missing_brokers:
            rec = self._create_recommendation(
                check_id="infra.rack.heterogeneous",
                title="Rack assignment is inconsistent across brokers",
                description=(
                    f"{len(missing_brokers)} broker(s) have a missing or placeholder "
                    f"broker.rack value while others carry a real rack ({', '.join(racks)})."
                ),
                severity="warning",
                category="infrastructure",
                impact=(
                    "Mixed rack assignment defeats rack-aware replica placement: "
                    "Kafka cannot guarantee replicas land in distinct fault zones, "
                    "so a rack failure can take an entire ISR offline."
                ),
                recommendation=(
                    "Set broker.rack to a real AZ/rack name on every broker (and KRaft controller); "
                    "avoid placeholders like 'unspecified' or 'norack'."
                ),
                brokers_missing_rack=sorted(missing_brokers),
            )
            recommendations.append(rec)
            self._record_check(
                "infra.rack.assigned",
                "broker.rack is set on every broker",
                "GET /broker/{id} -> configs[broker.rack]",
                "pass",
            )
            self._record_check(
                "infra.rack.heterogeneous",
                "broker.rack values are uniform / non-placeholder across brokers",
                "GET /broker/{id} -> configs[broker.rack]",
                "fail",
                recommendation_id=rec.id,
                brokers_missing_rack=sorted(missing_brokers),
            )
            self._record_check(
                "infra.rack.spread",
                "Brokers are distributed across at least 2 racks",
                "GET /broker/{id} -> configs[broker.rack]",
                "skipped",
                skipped_reason="cannot evaluate spread until rack assignment is consistent",
            )
        else:
            self._record_check(
                "infra.rack.assigned",
                "broker.rack is set on every broker",
                "GET /broker/{id} -> configs[broker.rack]",
                "pass",
            )
            self._record_check(
                "infra.rack.heterogeneous",
                "broker.rack values are uniform / non-placeholder across brokers",
                "GET /broker/{id} -> configs[broker.rack]",
                "pass",
            )
            if len(racks) < 2:
                rec = self._create_recommendation(
                    check_id="infra.rack.spread",
                    title="All brokers share a single rack",
                    description=f"Only one rack ({racks[0]}) is in use.",
                    severity="warning",
                    category="infrastructure",
                    recommendation="Distribute brokers across at least 2 (preferably 3) racks/AZs.",
                )
                recommendations.append(rec)
                self._record_check(
                    "infra.rack.spread",
                    "Brokers are distributed across at least 2 racks",
                    "GET /broker/{id} -> configs[broker.rack]",
                    "fail",
                    recommendation_id=rec.id,
                    rack_count=len(racks),
                )
            else:
                self._record_check(
                    "infra.rack.spread",
                    "Brokers are distributed across at least 2 racks",
                    "GET /broker/{id} -> configs[broker.rack]",
                    "pass",
                    rack_count=len(racks),
                )

        # ---- host metrics ------------------------------------------------
        cpu_avg = self._get_metric_average(cluster_state.metrics, "cpu_usage")
        cpu_max = self._get_metric_max(cluster_state.metrics, "cpu_usage")
        mem_max = self._get_metric_max(cluster_state.metrics, "memory_usage_percent")
        disk_max = self._get_metric_max(cluster_state.metrics, "disk_usage_percent")

        details["cpu_avg_pct"] = round(cpu_avg, 1)
        details["cpu_max_pct"] = round(cpu_max, 1)
        details["memory_max_pct"] = round(mem_max, 1)
        details["disk_max_pct"] = round(disk_max, 1)

        self._eval_metric_check(
            "infra.host.cpu",
            "Per-broker CPU peak below threshold",
            "metric: cpu_usage",
            cluster_state.metrics.get("cpu_usage"),
            cpu_max,
            self.thresholds.cpu_usage_warn,
            recommendations,
            title="High broker CPU usage",
            description=f"At least one broker reached {cpu_max:.1f}% CPU.",
            impact="Sustained high CPU starves request handlers and inflates request latency.",
            rec="Identify hot brokers and check partition leadership balance, or scale up CPU.",
            current_value=f"{cpu_max:.1f}%",
        )
        self._eval_metric_check(
            "infra.host.memory",
            "Per-broker memory peak below threshold",
            "metric: memory_usage_percent",
            cluster_state.metrics.get("memory_usage_percent"),
            mem_max,
            self.thresholds.memory_usage_warn,
            recommendations,
            title="High broker memory usage",
            description=f"Memory usage peaked at {mem_max:.1f}%.",
            impact=None,
            rec="Leave headroom for the page cache; Kafka relies on it for fetch performance.",
            current_value=f"{mem_max:.1f}%",
        )
        # Disk is special: escalate to critical above 90%.
        disk_data = cluster_state.metrics.get("disk_usage_percent")
        if not disk_data:
            self._record_check(
                "infra.host.disk",
                "Per-broker disk peak below threshold",
                "metric: disk_usage_percent",
                "no_data",
                skipped_reason="metric series 'disk_usage_percent' not returned",
            )
        elif disk_max > self.thresholds.disk_usage_warn:
            rec = self._create_recommendation(
                check_id="infra.host.disk",
                title="High disk usage on at least one broker",
                description=f"Disk usage peaked at {disk_max:.1f}%.",
                severity="critical" if disk_max > 90 else "warning",
                category="infrastructure",
                current_value=f"{disk_max:.1f}%",
                impact="A full log directory takes the broker offline.",
                recommendation="Reduce retention, expand the disk, or rebalance partitions.",
            )
            recommendations.append(rec)
            self._record_check(
                "infra.host.disk",
                "Per-broker disk peak below threshold",
                "metric: disk_usage_percent",
                "fail",
                recommendation_id=rec.id,
                disk_max_pct=round(disk_max, 1),
            )
        else:
            self._record_check(
                "infra.host.disk",
                "Per-broker disk peak below threshold",
                "metric: disk_usage_percent",
                "pass",
                disk_max_pct=round(disk_max, 1),
            )

        # ---- log dir balance --------------------------------------------
        sizes = {
            b.broker_id: b.log_dir_size_bytes
            for b in cluster_state.brokers.values()
            if b.log_dir_size_bytes is not None
        }
        if len(sizes) < 2:
            self._record_check(
                "infra.log_dir.balance",
                "Log directory sizes are within balance threshold across brokers",
                "GET /broker/{id} -> logDirSize / totalLogDirSizeBytes",
                "no_data",
                skipped_reason="fewer than two brokers reported a log dir size",
            )
        else:
            min_size = min(sizes.values())
            max_size = max(sizes.values())
            if max_size > 0 and (max_size - min_size) / max_size > self.thresholds.log_dir_imbalance_warn:
                details["log_dir_size_bytes_per_broker"] = sizes
                rec = self._create_recommendation(
                    check_id="infra.log_dir.balance",
                    title="Log directory sizes are imbalanced across brokers",
                    description=(
                        f"Smallest broker holds {min_size / 1e9:.1f} GB, "
                        f"largest holds {max_size / 1e9:.1f} GB."
                    ),
                    severity="warning",
                    category="infrastructure",
                    impact="Imbalanced storage indicates uneven partition assignment and risks per-broker disk pressure.",
                    recommendation="Run a partition reassignment or use Cruise Control to even out the load.",
                )
                recommendations.append(rec)
                self._record_check(
                    "infra.log_dir.balance",
                    "Log directory sizes are within balance threshold across brokers",
                    "GET /broker/{id} -> logDirSize / totalLogDirSizeBytes",
                    "fail",
                    recommendation_id=rec.id,
                )
            else:
                self._record_check(
                    "infra.log_dir.balance",
                    "Log directory sizes are within balance threshold across brokers",
                    "GET /broker/{id} -> logDirSize / totalLogDirSizeBytes",
                    "pass",
                )

        details["recommendation_count"] = len(recommendations)
        return {
            "recommendations": recommendations,
            "summary": {
                "broker_count": broker_count,
                "active_brokers": active,
                "rack_count": len([r for r in racks if r != "unknown"]),
                "issues": len(recommendations),
            },
            "details": details,
            "checks": [c.model_dump() for c in self._checks],
        }

    # ------------------------------------------------------------------ utils

    def _eval_metric_check(
        self,
        check_id: str,
        check_desc: str,
        data_source: str,
        metric_data: Any,
        observed_max: float,
        threshold: float,
        recommendations: List[Recommendation],
        *,
        title: str,
        description: str,
        impact: str,
        rec: str,
        current_value: str,
    ) -> None:
        if not metric_data:
            self._record_check(
                check_id,
                check_desc,
                data_source,
                "no_data",
                skipped_reason="metric series not returned",
            )
            return
        if observed_max > threshold:
            recommendation = self._create_recommendation(
                check_id=check_id,
                title=title,
                description=description,
                severity="warning",
                category="infrastructure",
                current_value=current_value,
                impact=impact,
                recommendation=rec,
            )
            recommendations.append(recommendation)
            self._record_check(
                check_id, check_desc, data_source, "fail",
                recommendation_id=recommendation.id, observed_max=round(observed_max, 1),
            )
        else:
            self._record_check(
                check_id, check_desc, data_source, "pass",
                observed_max=round(observed_max, 1),
            )
