"""
Infrastructure analyzer for Kafka brokers.

Reviews host-level metrics (CPU, memory, disk, network) and broker
distribution (rack placement, log dir balance).
"""

from typing import Any, Dict, List

from ..models import ClusterState, Recommendation
from .base import BaseAnalyzer


class InfrastructureAnalyzer(BaseAnalyzer):
    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}

        broker_count = cluster_state.get_total_brokers()
        active = cluster_state.get_active_brokers()
        racks = cluster_state.get_racks()

        details["broker_count"] = broker_count
        details["active_brokers"] = active
        details["racks"] = racks
        details["controller_id"] = cluster_state.controller_id

        if broker_count == 0:
            recommendations.append(
                self._create_recommendation(
                    title="No brokers reported",
                    description="The AxonOps API returned no brokers for this cluster.",
                    severity="critical",
                    category="infrastructure",
                    recommendation="Verify the cluster name and that AxonOps agents are connected.",
                )
            )
            return {"recommendations": recommendations, "summary": details, "details": details}

        if broker_count < 3:
            recommendations.append(
                self._create_recommendation(
                    title="Cluster has fewer than 3 brokers",
                    description=f"Only {broker_count} broker(s) are reporting.",
                    severity="warning",
                    category="infrastructure",
                    impact="A cluster with fewer than 3 brokers cannot tolerate a single broker failure while preserving RF=3.",
                    recommendation="Run at least 3 brokers in production.",
                    current_value=str(broker_count),
                )
            )

        if active < broker_count:
            recommendations.append(
                self._create_recommendation(
                    title="Some brokers appear inactive",
                    description=f"{broker_count - active} of {broker_count} brokers are not reporting recent host metrics.",
                    severity="warning",
                    category="infrastructure",
                    recommendation="Check the AxonOps agent on the affected hosts.",
                )
            )

        if not racks or racks == ["unknown"]:
            recommendations.append(
                self._create_recommendation(
                    title="Brokers are not rack-aware",
                    description="No broker rack assignment was detected.",
                    severity="warning",
                    category="infrastructure",
                    impact="Without broker.rack, replicas may end up co-located in the same fault zone.",
                    recommendation="Set broker.rack on each broker (e.g. AZ name) and use rack-aware partition assignment.",
                )
            )
        elif len(racks) < 2:
            recommendations.append(
                self._create_recommendation(
                    title="All brokers share a single rack",
                    description=f"Only one rack ({racks[0]}) is in use.",
                    severity="warning",
                    category="infrastructure",
                    recommendation="Distribute brokers across at least 2 (preferably 3) racks/AZs.",
                )
            )

        # Per-broker host metric checks.
        cpu_avg = self._get_metric_average(cluster_state.metrics, "cpu_usage")
        cpu_max = self._get_metric_max(cluster_state.metrics, "cpu_usage")
        mem_max = self._get_metric_max(cluster_state.metrics, "memory_usage_percent")
        disk_max = self._get_metric_max(cluster_state.metrics, "disk_usage_percent")

        details["cpu_avg_pct"] = round(cpu_avg, 1)
        details["cpu_max_pct"] = round(cpu_max, 1)
        details["memory_max_pct"] = round(mem_max, 1)
        details["disk_max_pct"] = round(disk_max, 1)

        if cpu_max > self.thresholds.cpu_usage_warn:
            recommendations.append(
                self._create_recommendation(
                    title="High broker CPU usage",
                    description=f"At least one broker reached {cpu_max:.1f}% CPU.",
                    severity="warning",
                    category="infrastructure",
                    current_value=f"{cpu_max:.1f}%",
                    impact="Sustained high CPU starves request handlers and inflates request latency.",
                    recommendation="Identify hot brokers and check partition leadership balance, or scale up CPU.",
                )
            )

        if mem_max > self.thresholds.memory_usage_warn:
            recommendations.append(
                self._create_recommendation(
                    title="High broker memory usage",
                    description=f"Memory usage peaked at {mem_max:.1f}%.",
                    severity="warning",
                    category="infrastructure",
                    current_value=f"{mem_max:.1f}%",
                    recommendation="Leave headroom for the page cache; Kafka relies on it for fetch performance.",
                )
            )

        if disk_max > self.thresholds.disk_usage_warn:
            recommendations.append(
                self._create_recommendation(
                    title="High disk usage on at least one broker",
                    description=f"Disk usage peaked at {disk_max:.1f}%.",
                    severity="critical" if disk_max > 90 else "warning",
                    category="infrastructure",
                    current_value=f"{disk_max:.1f}%",
                    impact="A full log directory takes the broker offline.",
                    recommendation="Reduce retention, expand the disk, or rebalance partitions.",
                )
            )

        # Log directory balance across brokers.
        sizes = {
            b.broker_id: b.log_dir_size_bytes
            for b in cluster_state.brokers.values()
            if b.log_dir_size_bytes is not None
        }
        if len(sizes) >= 2:
            min_size = min(sizes.values())
            max_size = max(sizes.values())
            if max_size > 0 and (max_size - min_size) / max_size > self.thresholds.log_dir_imbalance_warn:
                details["log_dir_size_bytes_per_broker"] = sizes
                recommendations.append(
                    self._create_recommendation(
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
        }
