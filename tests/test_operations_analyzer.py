from kafka_analyzer.analyzers.operations import OperationsAnalyzer
from kafka_analyzer.models import Partition


def _titles(result):
    return {r.title for r in result["recommendations"]}


def test_healthy_cluster_no_urp(config, healthy_cluster_state):
    analyzer = OperationsAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Under-replicated partitions observed" not in titles


def test_under_replicated_partitions_detected(config, healthy_cluster_state):
    healthy_cluster_state.topics["orders"].partitions[0] = Partition(
        id=0, leader=1, replicas=[1, 2, 3], in_sync_replicas=[1, 2]
    )
    analyzer = OperationsAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Under-replicated partitions observed" in titles


def test_offline_partitions_detected(config, unhealthy_cluster_state):
    analyzer = OperationsAnalyzer(config)
    titles = _titles(analyzer.analyze(unhealthy_cluster_state))
    assert "Offline partitions observed" in titles or "Under-replicated partitions observed" in titles


def test_leader_skew_detected(config, healthy_cluster_state):
    # All leaders on broker 1 → max skew.
    for partition in healthy_cluster_state.topics["orders"].partitions:
        partition.leader = 1
    analyzer = OperationsAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Leadership is uneven across brokers" in titles
