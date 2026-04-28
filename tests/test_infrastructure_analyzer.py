from kafka_analyzer.analyzers.infrastructure import InfrastructureAnalyzer


def _titles(result):
    return {r.title for r in result["recommendations"]}


def test_healthy_cluster_has_no_critical_findings(config, healthy_cluster_state):
    analyzer = InfrastructureAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    severities = {r.severity.value for r in result["recommendations"]}
    assert "critical" not in severities


def test_unhealthy_cluster_flags_few_brokers(config, unhealthy_cluster_state):
    analyzer = InfrastructureAnalyzer(config)
    result = analyzer.analyze(unhealthy_cluster_state)
    titles = _titles(result)
    assert "Cluster has fewer than 3 brokers" in titles
    assert "Brokers are not rack-aware" in titles


def test_log_dir_imbalance_detected(config, healthy_cluster_state):
    healthy_cluster_state.brokers[1].log_dir_size_bytes = 1_000_000_000
    healthy_cluster_state.brokers[3].log_dir_size_bytes = 50_000_000_000
    analyzer = InfrastructureAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    assert "Log directory sizes are imbalanced across brokers" in _titles(result)
