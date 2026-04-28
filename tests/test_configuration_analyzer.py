from kafka_analyzer.analyzers.configuration import ConfigurationAnalyzer


def _titles(result):
    return {r.title for r in result["recommendations"]}


def test_healthy_config_passes(config, healthy_cluster_state):
    analyzer = ConfigurationAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "unclean.leader.election.enable is true" not in titles
    assert "auto.create.topics.enable is true" not in titles


def test_unclean_leader_election_flagged(config, unhealthy_cluster_state):
    analyzer = ConfigurationAnalyzer(config)
    titles = _titles(analyzer.analyze(unhealthy_cluster_state))
    assert "unclean.leader.election.enable is true" in titles
    assert "auto.create.topics.enable is true" in titles
    assert "default.replication.factor is below the recommended minimum" in titles


def test_inconsistent_configs_detected(config, healthy_cluster_state):
    healthy_cluster_state.brokers[2].configs["min.insync.replicas"] = "1"
    analyzer = ConfigurationAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Broker configurations diverge" in titles
