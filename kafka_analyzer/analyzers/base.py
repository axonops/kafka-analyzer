"""
Base analyzer class.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..config import Config
from ..models import (
    AffectedResources,
    Check,
    CheckStatus,
    ClusterState,
    Recommendation,
)


def _infer_affected_resources(context: Dict[str, Any]) -> "AffectedResources":
    """Best-effort lift of standard scoping keys from a recommendation's
    ``**context`` dict into the typed ``AffectedResources`` field.

    Recognised keys (matching conventions used across the kafka analyzers):

    - ``topic`` (str) → ``topics=[value]``
    - ``topics`` (list) → ``topics=[...]``
    - ``broker`` / ``broker_id`` (str) → ``brokers=[value]``
    - ``brokers`` / ``affected_brokers`` (list) → ``brokers=[...]``
    - ``consumer_group`` (str) → ``consumer_groups=[value]``
    - ``consumer_groups`` (list) → ``consumer_groups=[...]``
    - ``connect_cluster`` (str) → ``connect_clusters=[value]``
    - ``schema_subject`` (str) → ``schema_subjects=[value]``
    - ``schema_subjects`` (list) → ``schema_subjects=[...]``

    Call sites needing richer scoping pass ``affected_resources=`` explicitly.
    """

    def _collect(keys_scalar: tuple, keys_list: tuple) -> List[str]:
        out: List[str] = []
        for k in keys_scalar:
            v = context.get(k)
            if isinstance(v, (str, int)) and str(v):
                s = str(v)
                if s not in out:
                    out.append(s)
        for k in keys_list:
            v = context.get(k)
            if isinstance(v, list):
                for entry in v:
                    if isinstance(entry, (str, int)) and str(entry):
                        s = str(entry)
                        if s not in out:
                            out.append(s)
        return out

    return AffectedResources(
        topics=_collect(("topic",), ("topics",)),
        brokers=_collect(("broker", "broker_id"), ("brokers", "affected_brokers")),
        consumer_groups=_collect(("consumer_group",), ("consumer_groups",)),
        connect_clusters=_collect(("connect_cluster",), ("connect_clusters",)),
        schema_subjects=_collect(("schema_subject",), ("schema_subjects",)),
    )


class BaseAnalyzer(ABC):
    """Base class for all analyzers."""

    # Default category surfaced on Check entries created by this analyzer
    # if `_record_check` is called without an explicit category.
    category: str = ""

    def __init__(self, config: Config):
        self.config = config
        self.thresholds = config.analysis.thresholds
        self._checks: List[Check] = []

    @abstractmethod
    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        """Analyze and return {recommendations, summary, details, checks}."""

    # ------------------------------------------------------------------ checks

    def _reset_checks(self) -> None:
        """Clear the per-run check buffer. Call at the start of analyze()."""
        self._checks = []

    def _record_check(
        self,
        check_id: str,
        description: str,
        data_source: str,
        status: str,
        *,
        category: Optional[str] = None,
        skipped_reason: Optional[str] = None,
        recommendation_id: Optional[str] = None,
        **context: Any,
    ) -> Check:
        """Append a Check entry to this analyzer's coverage manifest.

        Use the four-status model:
        - "pass": ran cleanly
        - "fail": ran and produced a recommendation (set recommendation_id)
        - "skipped": precondition not met (set skipped_reason)
        - "no_data": data source absent / empty (set skipped_reason explaining what was missing)
        """
        check = Check(
            id=check_id,
            description=description,
            category=category or self.category or "unknown",
            data_source=data_source,
            status=CheckStatus(status),
            skipped_reason=skipped_reason,
            recommendation_id=recommendation_id,
            context=context,
        )
        self._checks.append(check)
        return check

    def _create_recommendation(
        self,
        title: str,
        description: str,
        severity: str,
        category: str,
        impact: Optional[str] = None,
        recommendation: Optional[str] = None,
        current_value: Optional[str] = None,
        reference_url: Optional[str] = None,
        check_id: Optional[str] = None,
        recommendation_category: Optional[str] = None,
        affected_resources: Any = None,
        **context,
    ) -> Recommendation:
        """Helper method to create recommendations.

        ``recommendation_category`` is the downstream LLM-service vocabulary
        (one of performance/reliability/configuration/capacity/security). Each
        analyzer subclass sets ``default_recommendation_category`` so this
        helper has a sensible fallback; per-call overrides take precedence.

        ``affected_resources`` accepts either an ``AffectedResources`` model
        or a dict mapping fields (``topics=``, ``brokers=``, ...). When None,
        the helper auto-infers from standard scoping keys in ``context`` so
        existing call sites passing ``topic=`` / ``broker=`` get scoping data
        on their output without requiring a per-site rewrite.
        """
        effective_recommendation_category = (
            recommendation_category
            or getattr(self, "default_recommendation_category", None)
            or "configuration"
        )

        if affected_resources is None:
            affected_resources_obj = _infer_affected_resources(context)
        elif isinstance(affected_resources, AffectedResources):
            affected_resources_obj = affected_resources
        elif isinstance(affected_resources, dict):
            affected_resources_obj = AffectedResources(**affected_resources)
        else:
            raise TypeError(
                f"affected_resources must be AffectedResources, dict, or None; "
                f"got {type(affected_resources).__name__}"
            )

        return Recommendation(
            id=check_id,
            title=title,
            description=description,
            severity=severity,
            category=category,
            recommendation_category=effective_recommendation_category,
            affected_resources=affected_resources_obj,
            impact=impact,
            recommendation=recommendation,
            current_value=current_value,
            reference_url=reference_url,
            context=context,
        )

    def _get_metric_average(self, metrics: Dict[str, Any], metric_name: str) -> float:
        metric_data = metrics.get(metric_name, [])
        if not metric_data:
            return 0.0
        total_points = 0
        total_value = 0.0
        for metric in metric_data:
            if hasattr(metric, "data_points"):
                for point in metric.data_points:
                    total_value += point.value
                    total_points += 1
        return total_value / total_points if total_points > 0 else 0.0

    def _get_metric_max(self, metrics: Dict[str, Any], metric_name: str) -> float:
        metric_data = metrics.get(metric_name, [])
        if not metric_data:
            return 0.0
        max_value = 0.0
        for metric in metric_data:
            if hasattr(metric, "data_points"):
                for point in metric.data_points:
                    if point.value > max_value:
                        max_value = point.value
        return max_value

    @staticmethod
    def _is_internal_topic(topic_name: str) -> bool:
        return topic_name.startswith("__") or topic_name.startswith("_confluent")
