"""
Schema Registry analyzer.

Checks subject coverage of user topics, compatibility configuration,
soft-deleted subjects, and detects subjects that no longer match a topic.
"""

from typing import Any, Dict, List, Optional

from ..models import ClusterState, Recommendation
from .base import BaseAnalyzer


_RECOMMENDED_COMPATIBILITY = {"BACKWARD", "BACKWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE"}
_SR_SOURCE = "GET /clusters/{cluster}/schema-registry/subjects + /config"


def _subject_to_topic(subject: str) -> Optional[str]:
    """Strip a TopicNameStrategy suffix to recover the source topic."""
    if subject.endswith("-value"):
        return subject[: -len("-value")]
    if subject.endswith("-key"):
        return subject[: -len("-key")]
    return None


class SchemaRegistryAnalyzer(BaseAnalyzer):
    category = "schema_registry"
    # Compatibility / orphan-subjects / topic-coverage are configuration
    # discipline concerns.
    default_recommendation_category = "configuration"

    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        self._reset_checks()
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}

        subjects_raw = cluster_state.schema_registry_subjects or []
        registry_config = cluster_state.schema_registry_config or {}

        details["subject_count"] = len(subjects_raw)

        if not subjects_raw:
            for cid, desc in (
                ("schema_registry.global_compatibility", "Global compatibility level is non-permissive"),
                ("schema_registry.subject_compatibility", "Subject-level compatibility overrides are non-permissive"),
                ("schema_registry.orphan_subjects", "Subjects map to existing topics"),
                ("schema_registry.topic_coverage", "At least half of user topics have a registered schema"),
            ):
                self._record_check(
                    cid, desc, _SR_SOURCE,
                    "no_data", skipped_reason="schema registry not deployed or no subjects returned",
                )
            details["recommendation_count"] = 0
            return {
                "recommendations": [],
                "summary": {"subject_count": 0, "issues": 0},
                "details": details,
                "checks": [c.model_dump() for c in self._checks],
            }

        subject_records = self._normalize_subjects(subjects_raw)
        details["soft_deleted_subjects"] = [
            s["name"] for s in subject_records if s["soft_deleted"]
        ]

        global_compat = self._extract_compatibility(registry_config)
        details["global_compatibility"] = global_compat
        if global_compat is None:
            self._record_check(
                "schema_registry.global_compatibility",
                "Global compatibility level is non-permissive",
                _SR_SOURCE, "no_data",
                skipped_reason="schema-registry config not returned",
            )
        elif global_compat.upper() not in _RECOMMENDED_COMPATIBILITY:
            rec = self._create_recommendation(
                check_id="schema_registry.global_compatibility",
                title="Schema Registry global compatibility is permissive",
                description=f"Default compatibility level is {global_compat}.",
                severity="warning",
                category="schema_registry",
                current_value=global_compat,
                impact=(
                    "NONE / FORWARD-only compatibility allows breaking schema changes "
                    "to be published, which can break consumers."
                ),
                recommendation="Default to BACKWARD or FULL compatibility, and override per subject only when justified.",
            )
            recommendations.append(rec)
            self._record_check(
                "schema_registry.global_compatibility",
                "Global compatibility level is non-permissive",
                _SR_SOURCE, "fail", recommendation_id=rec.id, value=global_compat,
            )
        else:
            self._record_check(
                "schema_registry.global_compatibility",
                "Global compatibility level is non-permissive",
                _SR_SOURCE, "pass", value=global_compat,
            )

        permissive_subjects: List[str] = []
        orphan_subjects: List[str] = []
        topic_names = set(cluster_state.topics.keys())
        unique_topics_with_schemas = set()

        for record in subject_records:
            if record["soft_deleted"]:
                continue
            compat = record["compatibility"]
            if compat and compat.upper() not in _RECOMMENDED_COMPATIBILITY:
                permissive_subjects.append(record["name"])

            inferred_topic = _subject_to_topic(record["name"])
            if inferred_topic is not None:
                if inferred_topic in topic_names:
                    unique_topics_with_schemas.add(inferred_topic)
                else:
                    orphan_subjects.append(record["name"])

        details["permissive_subject_count"] = len(permissive_subjects)
        details["orphan_subject_count"] = len(orphan_subjects)
        if orphan_subjects:
            details["orphan_subjects"] = orphan_subjects

        details["topics_with_schemas"] = len(unique_topics_with_schemas)
        user_topic_count = sum(1 for t in cluster_state.topics.values() if not t.is_system_topic)
        details["user_topic_count"] = user_topic_count

        if permissive_subjects:
            rec = self._create_recommendation(
                check_id="schema_registry.subject_compatibility",
                title="Subjects with permissive compatibility levels",
                description=f"{len(permissive_subjects)} subject(s) override compatibility to NONE / FORWARD.",
                severity="warning",
                category="schema_registry",
                recommendation="Review permissive overrides — they are usually only safe for short, controlled migrations.",
                subjects=permissive_subjects[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "schema_registry.subject_compatibility",
                "Subject-level compatibility overrides are non-permissive",
                _SR_SOURCE, "fail", recommendation_id=rec.id,
                count=len(permissive_subjects),
            )
        else:
            self._record_check(
                "schema_registry.subject_compatibility",
                "Subject-level compatibility overrides are non-permissive",
                _SR_SOURCE, "pass",
            )

        if orphan_subjects:
            rec = self._create_recommendation(
                check_id="schema_registry.orphan_subjects",
                title="Schema subjects reference topics that no longer exist",
                description=(
                    f"{len(orphan_subjects)} subject(s) follow the TopicNameStrategy "
                    "(<topic>-key / <topic>-value) but the underlying topic is missing."
                ),
                severity="warning",
                category="schema_registry",
                impact=(
                    "Stale subjects clutter the registry and can re-attach silently if a "
                    "topic with the same name is recreated."
                ),
                recommendation="Soft-delete or hard-delete obsolete subjects after confirming no consumer still depends on them.",
                subjects=orphan_subjects[:25],
            )
            recommendations.append(rec)
            self._record_check(
                "schema_registry.orphan_subjects",
                "Subjects map to existing topics",
                _SR_SOURCE, "fail", recommendation_id=rec.id,
                count=len(orphan_subjects),
            )
        else:
            self._record_check(
                "schema_registry.orphan_subjects",
                "Subjects map to existing topics",
                _SR_SOURCE, "pass",
            )

        if not user_topic_count:
            self._record_check(
                "schema_registry.topic_coverage",
                "At least half of user topics have a registered schema",
                _SR_SOURCE, "skipped",
                skipped_reason="no user topics present",
            )
        elif unique_topics_with_schemas:
            covered_ratio = len(unique_topics_with_schemas) / user_topic_count
            details["topic_schema_coverage_ratio"] = round(covered_ratio, 3)
            if covered_ratio < 0.5:
                rec = self._create_recommendation(
                    check_id="schema_registry.topic_coverage",
                    title="Less than half of user topics have a registered schema",
                    description=(
                        f"{len(unique_topics_with_schemas)} of {user_topic_count} user topics "
                        "have a key or value subject in the registry."
                    ),
                    severity="info",
                    category="schema_registry",
                    recommendation="Register schemas for produced topics to enforce contracts and enable safer evolution.",
                )
                recommendations.append(rec)
                self._record_check(
                    "schema_registry.topic_coverage",
                    "At least half of user topics have a registered schema",
                    _SR_SOURCE, "fail", recommendation_id=rec.id,
                    covered=len(unique_topics_with_schemas), total=user_topic_count,
                )
            else:
                self._record_check(
                    "schema_registry.topic_coverage",
                    "At least half of user topics have a registered schema",
                    _SR_SOURCE, "pass",
                    covered=len(unique_topics_with_schemas), total=user_topic_count,
                )
        else:
            # Subjects exist but none map to current topic names — covered=0 < 50%.
            rec = self._create_recommendation(
                check_id="schema_registry.topic_coverage",
                title="No user topics have a registered schema",
                description=(
                    f"0 of {user_topic_count} user topics have a key or value subject in the registry."
                ),
                severity="info",
                category="schema_registry",
                recommendation="Register schemas for produced topics to enforce contracts and enable safer evolution.",
            )
            recommendations.append(rec)
            self._record_check(
                "schema_registry.topic_coverage",
                "At least half of user topics have a registered schema",
                _SR_SOURCE, "fail", recommendation_id=rec.id,
                covered=0, total=user_topic_count,
            )

        details["recommendation_count"] = len(recommendations)
        return {
            "recommendations": recommendations,
            "summary": {
                "subject_count": len(subjects_raw),
                "topics_with_schemas": len(unique_topics_with_schemas),
                "issues": len(recommendations),
            },
            "details": details,
            "checks": [c.model_dump() for c in self._checks],
        }

    @staticmethod
    def _normalize_subjects(subjects_raw: List[Any]) -> List[Dict[str, Any]]:
        """Coerce mixed subject payloads into a uniform record set."""
        records: List[Dict[str, Any]] = []
        for entry in subjects_raw:
            if isinstance(entry, str):
                records.append({"name": entry, "compatibility": None, "soft_deleted": False})
                continue
            if not isinstance(entry, dict):
                continue
            name = (
                entry.get("subject")
                or entry.get("name")
                or entry.get("subjectName")
            )
            if not name:
                continue
            compat = (
                entry.get("compatibility")
                or entry.get("compatibilityLevel")
                or (entry.get("config") or {}).get("compatibility")
            )
            soft_deleted = bool(
                entry.get("softDeleted")
                or entry.get("deleted")
                or entry.get("isDeleted")
            )
            records.append({"name": name, "compatibility": compat, "soft_deleted": soft_deleted})
        return records

    @staticmethod
    def _extract_compatibility(registry_config: Any) -> Optional[str]:
        if not isinstance(registry_config, dict):
            return None
        for key in ("compatibility", "compatibilityLevel"):
            value = registry_config.get(key)
            if isinstance(value, str) and value:
                return value
        return None
