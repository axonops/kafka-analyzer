from kafka_analyzer.analyzers.connect import ConnectAnalyzer


def _titles(result):
    return {r.title for r in result["recommendations"]}


def test_no_connectors_no_findings(config, healthy_cluster_state):
    analyzer = ConnectAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    assert result["recommendations"] == []
    assert result["details"]["connector_count"] == 0


def test_failed_connector_flagged_critical(config, healthy_cluster_state):
    healthy_cluster_state.connectors = [
        {
            "connect_cluster": "main",
            "name": "orders-sink",
            "type": "sink",
            "state": "FAILED",
            "config": {"topics": "orders"},
            "tasks": [{"id": 0, "state": "FAILED"}],
        }
    ]
    analyzer = ConnectAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    titles = _titles(result)
    assert "Failed Kafka Connect connectors" in titles
    assert "Connectors with failed tasks" in titles


def test_connector_targeting_missing_topic_flagged(config, healthy_cluster_state):
    healthy_cluster_state.connectors = [
        {
            "connect_cluster": "main",
            "name": "missing-topic-sink",
            "type": "sink",
            "state": "RUNNING",
            "config": {"topics": "orders,does-not-exist"},
            "tasks": [{"id": 0, "state": "RUNNING"}, {"id": 1, "state": "RUNNING"}],
        }
    ]
    analyzer = ConnectAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    titles = _titles(result)
    assert "Connectors reference topics that do not exist" in titles
    detail = result["details"]["connectors_targeting_missing_topics"]
    assert detail and detail[0]["missing_topics"] == ["does-not-exist"]


def test_errors_tolerance_all_without_dlq_flagged(config, healthy_cluster_state):
    healthy_cluster_state.connectors = [
        {
            "connect_cluster": "main",
            "name": "lossy",
            "type": "sink",
            "state": "RUNNING",
            "config": {"topics": "orders", "errors.tolerance": "all"},
            "tasks": [{"id": 0, "state": "RUNNING"}, {"id": 1, "state": "RUNNING"}],
        }
    ]
    analyzer = ConnectAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Connectors with errors.tolerance=all but no DLQ topic" in titles


def test_running_connector_with_existing_topic_passes(config, healthy_cluster_state):
    healthy_cluster_state.connectors = [
        {
            "connect_cluster": "main",
            "name": "orders-mirror",
            "type": "source",
            "state": "RUNNING",
            "config": {
                "topic": "orders",
                "errors.tolerance": "all",
                "errors.deadletterqueue.topic.name": "orders.dlq",
                "errors.retry.timeout": "30000",
            },
            "tasks": [{"id": 0, "state": "RUNNING"}, {"id": 1, "state": "RUNNING"}],
        }
    ]
    analyzer = ConnectAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Failed Kafka Connect connectors" not in titles
    assert "Connectors reference topics that do not exist" not in titles
    assert "Connectors with errors.tolerance=all but no DLQ topic" not in titles
