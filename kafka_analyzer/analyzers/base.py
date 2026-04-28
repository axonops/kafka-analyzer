"""
Base analyzer class.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from ..config import Config
from ..models import ClusterState, Recommendation


class BaseAnalyzer(ABC):
    """Base class for all analyzers."""

    def __init__(self, config: Config):
        self.config = config
        self.thresholds = config.analysis.thresholds

    @abstractmethod
    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        """Analyze and return {recommendations, summary, details}."""

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
        **context,
    ) -> Recommendation:
        return Recommendation(
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
