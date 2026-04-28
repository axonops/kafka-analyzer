from kafka_analyzer.analyzers.schema_registry import SchemaRegistryAnalyzer


def _titles(result):
    return {r.title for r in result["recommendations"]}


def test_no_subjects_no_findings(config, healthy_cluster_state):
    analyzer = SchemaRegistryAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    assert result["recommendations"] == []
    assert result["details"]["subject_count"] == 0


def test_orphan_subject_detected(config, healthy_cluster_state):
    healthy_cluster_state.schema_registry_subjects = [
        {"subject": "orders-value", "compatibility": "BACKWARD"},
        {"subject": "deleted-topic-value", "compatibility": "BACKWARD"},
    ]
    healthy_cluster_state.schema_registry_config = {"compatibility": "BACKWARD"}
    analyzer = SchemaRegistryAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    titles = _titles(result)
    assert "Schema subjects reference topics that no longer exist" in titles
    assert result["details"]["orphan_subject_count"] == 1
    assert "deleted-topic-value" in result["details"]["orphan_subjects"]


def test_permissive_global_compatibility_flagged(config, healthy_cluster_state):
    healthy_cluster_state.schema_registry_subjects = [
        {"subject": "orders-value", "compatibility": "BACKWARD"}
    ]
    healthy_cluster_state.schema_registry_config = {"compatibility": "NONE"}
    analyzer = SchemaRegistryAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Schema Registry global compatibility is permissive" in titles


def test_permissive_subject_compatibility_flagged(config, healthy_cluster_state):
    healthy_cluster_state.schema_registry_subjects = [
        {"subject": "orders-value", "compatibility": "NONE"},
        {"subject": "orders-key", "compatibility": "BACKWARD"},
    ]
    healthy_cluster_state.schema_registry_config = {"compatibility": "BACKWARD"}
    analyzer = SchemaRegistryAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Subjects with permissive compatibility levels" in titles


def test_string_subjects_normalized(config, healthy_cluster_state):
    healthy_cluster_state.schema_registry_subjects = ["orders-value", "orders-key"]
    healthy_cluster_state.schema_registry_config = {"compatibility": "BACKWARD"}
    analyzer = SchemaRegistryAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    assert result["details"]["orphan_subject_count"] == 0
    assert result["details"]["topics_with_schemas"] == 1


def test_soft_deleted_subjects_excluded_from_orphan_check(config, healthy_cluster_state):
    healthy_cluster_state.schema_registry_subjects = [
        {"subject": "ghost-value", "compatibility": "BACKWARD", "softDeleted": True},
    ]
    healthy_cluster_state.schema_registry_config = {"compatibility": "BACKWARD"}
    analyzer = SchemaRegistryAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    assert result["details"]["orphan_subject_count"] == 0
    assert "ghost-value" in result["details"]["soft_deleted_subjects"]
