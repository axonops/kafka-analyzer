from kafka_analyzer.analyzers.security import SecurityAnalyzer


def _titles(result):
    return {r.title for r in result["recommendations"]}


def test_secure_cluster_passes(config, healthy_cluster_state):
    analyzer = SecurityAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Authorizer is disabled" not in titles
    assert "Brokers serve plaintext only" not in titles


def test_unsecured_cluster_flags_findings(config, unhealthy_cluster_state):
    analyzer = SecurityAnalyzer(config)
    titles = _titles(analyzer.analyze(unhealthy_cluster_state))
    assert "Authorizer is disabled" in titles
    assert "Brokers serve plaintext only" in titles
    assert "Inter-broker traffic is unencrypted" in titles


def test_orphan_topic_acl_detected(config, healthy_cluster_state):
    healthy_cluster_state.acls.append(
        {
            "resourceType": "TOPIC",
            "resourceName": "topic-that-no-longer-exists",
            "resourcePatternType": "LITERAL",
            "acls": [
                {"principal": "User:legacy-svc", "operation": "READ", "permissionType": "ALLOW"}
            ],
        }
    )
    analyzer = SecurityAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    titles = _titles(result)
    assert "Orphan ACLs reference topics that no longer exist" in titles
    assert result["details"]["orphan_topic_acl_count"] == 1


def test_existing_topic_acl_not_orphan(config, healthy_cluster_state):
    # The healthy fixture already has an ACL for the "orders" topic.
    analyzer = SecurityAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    assert result["details"]["orphan_topic_acl_count"] == 0
    assert "Orphan ACLs reference topics that no longer exist" not in _titles(result)


def test_prefixed_acl_pattern_matches_existing_topic(config, healthy_cluster_state):
    healthy_cluster_state.acls.append(
        {
            "resourceType": "TOPIC",
            "resourceName": "orde",
            "resourcePatternType": "PREFIXED",
            "acls": [
                {"principal": "User:svc", "operation": "READ", "permissionType": "ALLOW"}
            ],
        }
    )
    analyzer = SecurityAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    assert result["details"]["orphan_topic_acl_count"] == 0


def test_prefixed_acl_pattern_with_no_matching_topic_is_orphan(config, healthy_cluster_state):
    healthy_cluster_state.acls.append(
        {
            "resourceType": "TOPIC",
            "resourceName": "deprecated-",
            "resourcePatternType": "PREFIXED",
            "acls": [
                {"principal": "User:svc", "operation": "READ", "permissionType": "ALLOW"}
            ],
        }
    )
    analyzer = SecurityAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    assert result["details"]["orphan_topic_acl_count"] == 1


def test_wildcard_topic_acl_is_not_orphan(config, healthy_cluster_state):
    healthy_cluster_state.acls.append(
        {
            "resourceType": "TOPIC",
            "resourceName": "*",
            "resourcePatternType": "LITERAL",
            "acls": [
                {"principal": "User:admin", "operation": "ALL", "permissionType": "ALLOW"}
            ],
        }
    )
    analyzer = SecurityAnalyzer(config)
    result = analyzer.analyze(healthy_cluster_state)
    assert result["details"]["orphan_topic_acl_count"] == 0


def test_wildcard_acl_principal_detected(config, healthy_cluster_state):
    healthy_cluster_state.acls.append(
        {
            "resourceType": "TOPIC",
            "resourceName": "*",
            "acls": [
                {"principal": "User:*", "operation": "ALL", "permissionType": "ALLOW"}
            ],
        }
    )
    analyzer = SecurityAnalyzer(config)
    titles = _titles(analyzer.analyze(healthy_cluster_state))
    assert "Wildcard ACL principals detected" in titles
