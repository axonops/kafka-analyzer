"""
Topics analyzer: reviews per-topic settings (replication factor, partition
count, retention, cleanup policy, min.insync.replicas), internal-vs-user
balance, and consumer-lag posture.
"""

from typing import Any, Dict, List

from ..models import ClusterState, Recommendation, Topic
from .base import BaseAnalyzer


_TOPIC_SOURCE = "GET /clusters/{cluster}/topics + per-topic configs"


class TopicsAnalyzer(BaseAnalyzer):
    category = "topics"
    # Replication / ISR / consumer-lag findings are reliability. Partition-
    # count / single-partition-throughput findings override per call to
    # performance. Retention findings override to capacity. Schema coverage
    # overrides to configuration.
    default_recommendation_category = "reliability"

    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        self._reset_checks()
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
            rec = self._create_recommendation(
                check_id="topics.partitions_per_broker",
                title="High partition count per broker",
                description=(
                    f"Cluster averages {avg_partitions_per_broker:.0f} partitions per broker."
                ),
                severity="warning",
                category="topics",
                impact="Per-broker partition counts above ~4000 raise controller failover and metadata-propagation cost.",
                recommendation="Consolidate low-traffic topics or scale brokers out before the count grows further.",
            )
            recommendations.append(rec)
            self._record_check(
                "topics.partitions_per_broker",
                "Average partitions per broker is below threshold",
                _TOPIC_SOURCE,
                "fail", recommendation_id=rec.id,
                avg=round(avg_partitions_per_broker, 1),
            )
        else:
            self._record_check(
                "topics.partitions_per_broker",
                "Average partitions per broker is below threshold",
                _TOPIC_SOURCE,
                "pass", avg=round(avg_partitions_per_broker, 1),
            )

        if not user_topics:
            for cid, desc in (
                ("topics.replication.rf_one", "No user topics with RF=1"),
                ("topics.replication.rf_below_min", "No user topics with RF below recommended"),
                ("topics.replication.min_isr_present", "User topics override min.insync.replicas where critical"),
                ("topics.replication.min_isr_weak", "min.insync.replicas at or above recommended"),
                ("topics.partitions.high_count", "No topics exceeding partition-count warning"),
                ("topics.partitions.single_user", "User topics have more than one partition"),
                ("topics.retention.long", "Retention windows within recommended range"),
                ("topics.compaction.dirty_ratio", "Compacted topics review min.cleanable.dirty.ratio"),
                ("topics.replication.urp_topic", "No user topics with under-replicated partitions"),
                ("topics.schema.coverage", "Most user topics have a registered schema"),
            ):
                self._record_check(
                    cid, desc, _TOPIC_SOURCE,
                    "skipped", skipped_reason="no user topics present",
                )
        else:
            self._evaluate_topic_checks(user_topics, broker_count, recommendations)

        # ---- consumer lag -----------------------------------------------
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

        cg_source = "GET /clusters/{cluster}/consumer-groups"
        if not cluster_state.consumer_groups:
            for cid, desc in (
                ("topics.consumer.lag_critical", "No consumer groups with critical lag"),
                ("topics.consumer.lag_warning", "No consumer groups with elevated lag"),
                ("topics.consumer.rebalancing", "No consumer groups stuck rebalancing"),
            ):
                self._record_check(
                    cid, desc, cg_source,
                    "no_data", skipped_reason="no consumer groups returned",
                )
        else:
            if lag_crit:
                rec = self._create_recommendation(
                    check_id="topics.consumer.lag_critical",
                    title="Consumer groups with critical lag",
                    description=f"{len(lag_crit)} groups exceed the critical lag threshold.",
                    severity="critical",
                    category="topics",
                    recommendation="Scale the consumer group out, optimise processing, or check for stuck consumers.",
                    groups=lag_crit[:25],
                )
                recommendations.append(rec)
                self._record_check(
                    "topics.consumer.lag_critical",
                    "No consumer groups with critical lag",
                    cg_source,
                    "fail", recommendation_id=rec.id, count=len(lag_crit),
                )
            else:
                self._record_check(
                    "topics.consumer.lag_critical",
                    "No consumer groups with critical lag",
                    cg_source, "pass",
                )
            if lag_warn:
                rec = self._create_recommendation(
                    check_id="topics.consumer.lag_warning",
                    title="Consumer groups with elevated lag",
                    description=f"{len(lag_warn)} groups exceed the warning lag threshold.",
                    severity="warning",
                    category="topics",
                    recommendation="Track lag trends — short bursts are normal; sustained growth needs investigation.",
                    groups=lag_warn[:25],
                )
                recommendations.append(rec)
                self._record_check(
                    "topics.consumer.lag_warning",
                    "No consumer groups with elevated lag",
                    cg_source, "fail", recommendation_id=rec.id, count=len(lag_warn),
                )
            else:
                self._record_check(
                    "topics.consumer.lag_warning",
                    "No consumer groups with elevated lag",
                    cg_source, "pass",
                )
            if rebalancing_groups:
                rec = self._create_recommendation(
                    check_id="topics.consumer.rebalancing",
                    title="Consumer groups stuck in a rebalance state",
                    description=f"{len(rebalancing_groups)} groups are mid-rebalance at observation time.",
                    severity="warning",
                    category="topics",
                    recommendation="Check session.timeout.ms / max.poll.interval.ms tuning on the consumers.",
                    groups=rebalancing_groups[:25],
                )
                recommendations.append(rec)
                self._record_check(
                    "topics.consumer.rebalancing",
                    "No consumer groups stuck rebalancing",
                    cg_source, "fail", recommendation_id=rec.id,
                )
            else:
                self._record_check(
                    "topics.consumer.rebalancing",
                    "No consumer groups stuck rebalancing",
                    cg_source, "pass",
                )

        # ---- schema coverage --------------------------------------------
        subjects = cluster_state.schema_registry_subjects or []
        details["schema_registry_subjects"] = len(subjects)
        sr_source = "GET /clusters/{cluster}/schema-registry/subjects"
        if not subjects:
            self._record_check(
                "topics.schema.coverage",
                "Most user topics have a registered schema",
                sr_source, "no_data",
                skipped_reason="schema registry not deployed or no subjects returned",
            )
        elif not user_topics:
            self._record_check(
                "topics.schema.coverage",
                "Most user topics have a registered schema",
                sr_source, "skipped",
                skipped_reason="no user topics present",
            )
        else:
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
                rec = self._create_recommendation(
                    check_id="topics.schema.coverage",
                    title="Many topics have no registered schema",
                    description=(
                        f"{covered} of {len(user_topics)} user topics appear in the schema registry."
                    ),
                    severity="info",
                    category="topics",
                    recommendation="Register schemas for produced topics to enforce contracts and enable safer evolution.",
                )
                recommendations.append(rec)
                self._record_check(
                    "topics.schema.coverage",
                    "Most user topics have a registered schema",
                    sr_source, "fail", recommendation_id=rec.id,
                    covered=covered, total=len(user_topics),
                )
            else:
                self._record_check(
                    "topics.schema.coverage",
                    "Most user topics have a registered schema",
                    sr_source, "pass",
                    covered=covered, total=len(user_topics),
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
            "checks": [c.model_dump() for c in self._checks],
        }

    # ------------------------------------------------------------------ helpers

    def _evaluate_topic_checks(
        self,
        user_topics: List[Topic],
        broker_count: int,
        recommendations: List[Recommendation],
    ) -> None:
        single_rf: List[str] = []
        low_rf: List[str] = []
        no_min_isr: List[str] = []
        weak_min_isr: List[str] = []
        big_topics: List[str] = []
        single_partition_user: List[str] = []
        long_retention: List[str] = []
        bad_cleanup: List[str] = []
        urp_topics: List[str] = []
        compacted_topic_count = 0

        for topic in user_topics:
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
            if "compact" in cleanup:
                compacted_topic_count += 1
                if topic.config_value("min.cleanable.dirty.ratio") is None:
                    bad_cleanup.append(topic.name)

            if topic.under_replicated_partitions():
                urp_topics.append(topic.name)

        # RF=1
        if single_rf:
            rec = self._create_recommendation(
                check_id="topics.replication.rf_one",
                title="Topics with replication factor 1",
                description=f"{len(single_rf)} user topics have RF=1.",
                severity="critical",
                category="topics",
                impact="Loss of any broker hosting these partitions = permanent data loss.",
                recommendation="Increase RF to at least 3 (or 2 in non-prod) via partition reassignment.",
                topics=single_rf[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "topics.replication.rf_one", "No user topics with RF=1",
                _TOPIC_SOURCE, "fail", recommendation_id=rec.id, count=len(single_rf),
            )
        else:
            self._record_check(
                "topics.replication.rf_one", "No user topics with RF=1",
                _TOPIC_SOURCE, "pass",
            )

        # RF below min (>1)
        if low_rf:
            rec = self._create_recommendation(
                check_id="topics.replication.rf_below_min",
                title=f"Topics with RF below {self.thresholds.min_replication_factor}",
                description=f"{len(low_rf)} user topics have RF below the configured minimum.",
                severity="warning",
                category="topics",
                recommendation=f"Set RF to at least {self.thresholds.min_replication_factor}.",
                topics=low_rf[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "topics.replication.rf_below_min", "No user topics with RF below recommended",
                _TOPIC_SOURCE, "fail", recommendation_id=rec.id, count=len(low_rf),
            )
        else:
            self._record_check(
                "topics.replication.rf_below_min", "No user topics with RF below recommended",
                _TOPIC_SOURCE, "pass",
            )

        # min.insync.replicas not set
        if no_min_isr:
            rec = self._create_recommendation(
                check_id="topics.replication.min_isr_present",
                title="Topics without min.insync.replicas configured",
                description=f"{len(no_min_isr)} user topics inherit the broker default.",
                severity="info",
                category="topics",
                recommendation="Set min.insync.replicas explicitly for critical topics (typically RF-1).",
                topics=no_min_isr[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "topics.replication.min_isr_present",
                "User topics override min.insync.replicas where critical",
                _TOPIC_SOURCE, "fail", recommendation_id=rec.id, count=len(no_min_isr),
            )
        else:
            self._record_check(
                "topics.replication.min_isr_present",
                "User topics override min.insync.replicas where critical",
                _TOPIC_SOURCE, "pass",
            )

        # min.insync.replicas weak
        if weak_min_isr:
            rec = self._create_recommendation(
                check_id="topics.replication.min_isr_weak",
                title="Topics with weak min.insync.replicas",
                description=f"{len(weak_min_isr)} user topics have min.insync.replicas below the recommended minimum.",
                severity="warning",
                category="topics",
                recommendation=(
                    f"Set min.insync.replicas to at least {self.thresholds.min_in_sync_replicas} on topics that require durability."
                ),
                topics=weak_min_isr[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "topics.replication.min_isr_weak",
                "min.insync.replicas at or above recommended",
                _TOPIC_SOURCE, "fail", recommendation_id=rec.id, count=len(weak_min_isr),
            )
        else:
            self._record_check(
                "topics.replication.min_isr_weak",
                "min.insync.replicas at or above recommended",
                _TOPIC_SOURCE, "pass",
            )

        # high partition count
        if big_topics:
            rec = self._create_recommendation(
                check_id="topics.partitions.high_count",
                title="Topics with very high partition counts",
                description=f"{len(big_topics)} topics exceed {self.thresholds.max_partitions_per_topic_warn} partitions.",
                severity="info",
                category="topics",
                recommendation="Verify the partition count matches actual concurrency; over-partitioning hurts metadata size and producer batching.",
                topics=big_topics[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "topics.partitions.high_count",
                "No topics exceeding partition-count warning",
                _TOPIC_SOURCE, "fail", recommendation_id=rec.id, count=len(big_topics),
            )
        else:
            self._record_check(
                "topics.partitions.high_count",
                "No topics exceeding partition-count warning",
                _TOPIC_SOURCE, "pass",
            )

        # single-partition user topics
        if single_partition_user:
            rec = self._create_recommendation(
                check_id="topics.partitions.single_user",
                title="User topics with a single partition",
                description=f"{len(single_partition_user)} user topics have only one partition.",
                severity="info",
                category="topics",
                recommendation="Single-partition topics cap throughput and consumer parallelism; size partitions for expected load.",
                topics=single_partition_user[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "topics.partitions.single_user",
                "User topics have more than one partition",
                _TOPIC_SOURCE, "fail", recommendation_id=rec.id, count=len(single_partition_user),
            )
        else:
            self._record_check(
                "topics.partitions.single_user",
                "User topics have more than one partition",
                _TOPIC_SOURCE, "pass",
            )

        # long retention
        if long_retention:
            rec = self._create_recommendation(
                check_id="topics.retention.long",
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
            recommendations.append(rec)
            self._record_check(
                "topics.retention.long",
                "Retention windows within recommended range",
                _TOPIC_SOURCE, "fail", recommendation_id=rec.id, count=len(long_retention),
            )
        else:
            self._record_check(
                "topics.retention.long",
                "Retention windows within recommended range",
                _TOPIC_SOURCE, "pass",
            )

        # cleanup policy review
        if compacted_topic_count == 0:
            self._record_check(
                "topics.compaction.dirty_ratio",
                "Compacted topics review min.cleanable.dirty.ratio",
                _TOPIC_SOURCE, "skipped",
                skipped_reason="no compacted topics in cluster",
            )
        elif bad_cleanup:
            rec = self._create_recommendation(
                check_id="topics.compaction.dirty_ratio",
                title="Compacted topics without min.cleanable.dirty.ratio review",
                description=f"{len(bad_cleanup)} compacted topics may benefit from cleanup-policy review.",
                severity="info",
                category="topics",
                recommendation="For log-compacted topics, verify min.cleanable.dirty.ratio and segment.ms suit your write rate.",
                topics=bad_cleanup[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "topics.compaction.dirty_ratio",
                "Compacted topics review min.cleanable.dirty.ratio",
                _TOPIC_SOURCE, "fail", recommendation_id=rec.id, count=len(bad_cleanup),
            )
        else:
            self._record_check(
                "topics.compaction.dirty_ratio",
                "Compacted topics review min.cleanable.dirty.ratio",
                _TOPIC_SOURCE, "pass",
            )

        # URP topics
        if urp_topics:
            rec = self._create_recommendation(
                check_id="topics.replication.urp_topic",
                title="Topics with under-replicated partitions",
                description=f"{len(urp_topics)} topics have at least one under-replicated partition.",
                severity="critical",
                category="topics",
                recommendation="See operations section for cluster-level URP guidance.",
                topics=urp_topics[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "topics.replication.urp_topic",
                "No user topics with under-replicated partitions",
                _TOPIC_SOURCE, "fail", recommendation_id=rec.id, count=len(urp_topics),
            )
        else:
            self._record_check(
                "topics.replication.urp_topic",
                "No user topics with under-replicated partitions",
                _TOPIC_SOURCE, "pass",
            )
