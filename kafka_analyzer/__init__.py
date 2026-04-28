"""
Kafka AxonOps Analyzer

A Python tool that analyzes Apache Kafka clusters using the AxonOps API
as the data source.
"""

__version__ = "0.1.0"
__author__ = "AxonOps"

from .analyzer import KafkaAnalyzer

__all__ = ["KafkaAnalyzer"]
