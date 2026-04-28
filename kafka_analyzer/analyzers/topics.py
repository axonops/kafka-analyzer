"""
Topics analyzer: reviews per-topic settings (replication factor, partition
count, retention, cleanup policy, min.insync.replicas), internal-vs-user
balance, and consumer-lag posture.
"""

from typing import Any, Dict, List

from ..models import ClusterState, Recommendation, Topic
from .base import BaseAnalyzer


class TopicsAnalyzer(BaseAnalyzer):
    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}

        topics = list(cluster_state.topics.values())
        user_topics = [t for t in topics if not t.is_system_topic]
        details["topic_count_total"] = len(topics)
        details["topic_count_user"] = len(user_topics)
        details["topic_count_system"] = len(topics) - len(user_topics)
        details["partition_count_total"] = cluster_state.total_partitions()
        details["partition_count_user"] = cluster_state.total_user_partitions()

        broker_count = max(cluster_state.get_total_brokers(), 1)
        avg_partitions_per_broker = cluster_state.total_partitions() / broker_count
        details["avg_partitions_per_broker"] = round(avg_partitions_per_broker, 1)
        if avg_partitions_per_broker > self.thresholds.max_partitions_per_broker_warn:
            recommendations.append(
                self._create_recommendation(
                    title="High partition count per broker",
                    description=(
                        f"Cluster averages {avg_partitions_per_broker:.0f} partitions per broker."
                    ),
                    severity="warning",
                    category="topics",
                    impact="Per-broker partition counts above ~4000 raise controller failover and metadata-propagation cost.",
                    recommendation="Consolidate low-traffic topics or scale brokers out before the count grows further.",
                )
            )

        # Per-topic checks.
        low_rf: List[str] = []
        single_rf: List[str] = []
        no_min_isr: List[str] = []
        weak_min_isr: List[str] = []
        big_topics: List[str] = []
        single_partition_user: List[str] = []
        long_retention: List[str] = []
        bad_cleanup: List[str] = []
        urp_topics: List[str] = []

        for topic in user_topics:
            self._check_topic(
                topic,
                low_rf,
                single_rf,
                no_min_isr,
                weak_min_isr,
                big_topics,
                single_partition_user,
                long_retention,
                bad_cleanup,
                urp_topics,
                broker_count,
            )

        if single_rf:
            recommendations.append(
                self._create_recommendation(
                    title="Topics with replication factor 1",
                    description=f"{len(single_rf)} user topics have RF=1.",
                    severity="critical",
                    category="topics",
                    impact="Loss of any broker hosting these partitions = permanent data loss.",
                    recommendation="Increase RF to at least 3 (or 2 in non-prod) via partition reassignment.",
                    topics=single_rf[:25],
                )
            )

        if low_rf:
            recommendations.append(
                self._create_recommendation(
                    title=f"Topics with RF below {self.thresholds.min_replication_factor}",
                    description=f"{len(low_rf)} user topics have RF below the configured minimum.",
                    severity="warning",
                    category="topics",
                    recommendation=f"Set RF to at least {self.thresholds.min_replication_factor}.",
                    topics=low_rf[:25],
                )
            )

        if no_min_isr:
            recommendations.append(
                self._create_recommendation(
                    title="Topics without min.insync.replicas configured",
                    description=f"{len(no_min_isr)} user topics inherit the broker default.",
                    severity="info",
                    category="topics",
                    recommendation="Set min.insync.replicas explicitly for critical topics (typically RF-1).",
                    topics=no_min_isr[:25],
                )
            )

        if weak_min_isr:
            recommendations.append(
                self._create_recommendation(
                    title="Topics with weak min.insync.replicas",
                    description=f"{len(weak_min_isr)} user topics have min.insync.replicas below the recommended minimum.",
                    severity="warning",
                    category="topics",
                    recommendation=(
                        f"Set min.insync.replicas to at least {self.thresholds.min_in_sync_replicas} on topics that require durability."
                    ),
                    topics=weak_min_isr[:25],
                )
            )

        if big_topics:
            recommendations.append(
                self._create_recommendation(
                    title="Topics with very high partition counts",
                    description=f"{len(big_topics)} topics exceed {self.thresholds.max_partitions_per_topic_warn} partitions.",
                    severity="info",
                    category="topics",
                    recommendation="Verify the partition count matches actual concurrency; over-partitioning hurts metadata size and producer batching.",
                    topics=big_topics[:25],
                )
            )

        if single_partition_user:
            recommendations.append(
                self._create_recommendation(
                    title="User topics with a single partition",
                    description=f"{len(single_partition_user)} user topics have only one partition.",
                    severity="info",
                    category="topics",
                    recommendation="Single-partition topics cap throughput and consumer parallelism; size partitions for expected load.",
                    topics=single_partition_user[:25],
                )
            )

        if long_retention:
            recommendations.append(
                self._create_recommendation(
                    title="Topics with long retention",
                    description=(
                        f"{len(long_retention)} topics retain data for more than "
                        f"{self.thresholds.retention_warn_days} days."
                    ),
                    severity="info",
                    category="topics",
                    recommendation="Confirm long retention is intentional; otherwise reduce retention to free disk and speed up rebalances.",
                    topics=long_retention[:25],
                )
            )

        if bad_cleanup:
            recommendations.append(
                self._create_recommendation(
                    title="Compacted topics without min.cleanable.dirty.ratio review",
                    description=f"{len(bad_cleanup)} compacted topics may benefit from cleanup-policy review.",
                    severity="info",
                    category="topics",
                    recommendation="For log-compacted topics, verify min.cleanable.dirty.ratio and segment.ms suit your write rate.",
                    topics=bad_cleanup[:25],
                )
            )

        if urp_topics:
            recommendations.append(
                self._create_recommendation(
                    title="Topics with under-replicated partitions",
                    description=f"{len(urp_topics)} topics have at least one under-replicated partition.",
                    severity="critical",
                    category="topics",
                    recommendation="See operations section for cluster-level URP guidance.",
                    topics=urp_topics[:25],
                )
            )

        # Consumer lag posture.
        lag_warn: List[str] = []
        lag_crit: List[str] = []
        empty_groups = 0
        rebalancing_groups: List[str] = []
        for cg in cluster_state.consumer_groups.values():
            if cg.state and cg.state.lower() == "empty":
                empty_groups += 1
            if cg.state and cg.state.lower() in {"preparingrebalance", "completingrebalance"}:
                rebalancing_groups.append(cg.group_id)
            if cg.total_lag is None:
                continue
            if cg.total_lag >= self.thresholds.consumer_lag_critical:
                lag_crit.append(cg.group_id)
            elif cg.total_lag >= self.thresholds.consumer_lag_warn:
                lag_warn.append(cg.group_id)

        details["consumer_groups_total"] = len(cluster_state.consumer_groups)
        details["consumer_groups_empty"] = empty_groups
        details["consumer_groups_rebalancing"] = len(rebalancing_groups)

        if lag_crit:
            recommendations.append(
                self._create_recommendation(
                    title="Consumer groups with critical lag",
                    description=f"{len(lag_crit)} groups exceed the critical lag threshold.",
                    severity="critical",
                    category="topics",
                    recommendation="Scale the consumer group out, optimise processing, or check for stuck consumers.",
                    groups=lag_crit[:25],
                )
            )
        if lag_warn:
            recommendations.append(
                self._create_recommendation(
                    title="Consumer groups with elevated lag",
                    description=f"{len(lag_warn)} groups exceed the warning lag threshold.",
                    severity="warning",
                    category="topics",
                    recommendation="Track lag trends — short bursts are normal; sustained growth needs investigation.",
                    groups=lag_warn[:25],
                )
            )
        if rebalancing_groups:
            recommendations.append(
                self._create_recommendation(
                    title="Consumer groups stuck in a rebalance state",
                    description=f"{len(rebalancing_groups)} groups are mid-rebalance at observation time.",
                    severity="warning",
                    category="topics",
                    recommendation="Check session.timeout.ms / max.poll.interval.ms tuning on the consumers.",
                    groups=rebalancing_groups[:25],
                )
            )

        # Schema registry coverage (info-level).
        subjects = cluster_state.schema_registry_subjects or []
        details["schema_registry_subjects"] = len(subjects)
        if subjects and user_topics:
            subject_names = set()
            for s in subjects:
                if isinstance(s, str):
                    subject_names.add(s)
                elif isinstance(s, dict):
                    name = s.get("subject") or s.get("name")
                    if name:
                        subject_names.add(name)
            covered = sum(
                1
                for t in user_topics
                if f"{t.name}-value" in subject_names or f"{t.name}-key" in subject_names
            )
            details["topics_with_schema"] = covered
            if covered < len(user_topics) / 2:
                recommendations.append(
                    self._create_recommendation(
                        title="Many topics have no registered schema",
                        description=(
                            f"{covered} of {len(user_topics)} user topics appear in the schema registry."
                        ),
                        severity="info",
                        category="topics",
                        recommendation="Register schemas for produced topics to enforce contracts and enable safer evolution.",
                    )
                )

        details["recommendation_count"] = len(recommendations)
        return {
            "recommendations": recommendations,
            "summary": {
                "user_topics": len(user_topics),
                "system_topics": details["topic_count_system"],
                "user_partitions": details["partition_count_user"],
                "issues": len(recommendations),
            },
            "details": details,
        }

    def _check_topic(
        self,
        topic: Topic,
        low_rf: List[str],
        single_rf: List[str],
        no_min_isr: List[str],
        weak_min_isr: List[str],
        big_topics: List[str],
        single_partition_user: List[str],
        long_retention: List[str],
        bad_cleanup: List[str],
        urp_topics: List[str],
        broker_count: int,
    ) -> None:
        if topic.replication_factor == 1:
            single_rf.append(topic.name)
        elif 0 < topic.replication_factor < self.thresholds.min_replication_factor:
            low_rf.append(topic.name)

        min_isr = topic.min_insync_replicas()
        if min_isr is None:
            no_min_isr.append(topic.name)
        elif min_isr < self.thresholds.min_in_sync_replicas:
            weak_min_isr.append(topic.name)

        if topic.partition_count > self.thresholds.max_partitions_per_topic_warn:
            big_topics.append(topic.name)

        if topic.partition_count <= 1 and not topic.is_system_topic:
            single_partition_user.append(topic.name)

        retention_ms = topic.retention_ms()
        if retention_ms and retention_ms > self.thresholds.retention_warn_days * 86400 * 1000:
            long_retention.append(topic.name)

        cleanup = (topic.cleanup_policy or topic.config_value("cleanup.policy") or "").lower()
        if "compact" in cleanup and topic.config_value("min.cleanable.dirty.ratio") is None:
            bad_cleanup.append(topic.name)

        if topic.under_replicated_partitions():
            urp_topics.append(topic.name)
