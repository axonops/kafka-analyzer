"""
Base analyzer class.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..config import Config
from ..models import Check, CheckStatus, ClusterState, Recommendation


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
        **context,
    ) -> Recommendation:
        return Recommendation(
            id=check_id,
            title=title,
            description=description,
            severity=severity,
            category=category,
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
