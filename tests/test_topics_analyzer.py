from kafka_analyzer.analyzers.topics import TopicsAnalyzer
from kafka_analyzer.models import ConsumerGroup, Topic


def _titles(result):
    return {r.title for r in result["recommendations"]}


def test_healthy_topics_pass(config, healthy_cluster_state):
    analyzer = TopicsAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Topics with replication factor 1" not in titles


def test_rf1_topic_flagged_critical(config, unhealthy_cluster_state):
    analyzer = TopicsAnalyzer(config)
    titles = _titles(analyzer.analyze(unhealthy_cluster_state))
    assert "Topics with replication factor 1" in titles


def test_critical_consumer_lag(config, healthy_cluster_state):
    healthy_cluster_state.consumer_groups["loaded"] = ConsumerGroup(
        group_id="loaded", state="Stable", member_count=1, total_lag=1_000_000
    )
    analyzer = TopicsAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Consumer groups with critical lag" in titles


def test_long_retention_flagged(config, healthy_cluster_state):
    healthy_cluster_state.topics["log-archive"] = Topic(
        name="log-archive",
        partition_count=6,
        replication_factor=3,
        cleanup_policy="delete",
        configs={
            "min.insync.replicas": "2",
            "retention.ms": str(60 * 24 * 60 * 60 * 1000),  # 60 days
        },
    )
    analyzer = TopicsAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Topics with long retention" in titles
