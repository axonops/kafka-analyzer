"""
Security analyzer for Kafka clusters.

Reviews authorizer state, ACL coverage, listener encryption, super.users,
and SASL/TLS-related broker config.
"""

from typing import Any, Dict, List, Optional

from ..models import ClusterState, Recommendation
from .base import BaseAnalyzer


_BROKER_CFG_SOURCE = "GET /broker/{id} -> configs[]"
_ACL_SOURCE = "GET /clusters/{cluster}/acls"


def _to_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    return str(value).strip().lower() == "true"


class SecurityAnalyzer(BaseAnalyzer):
    category = "security"

    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        self._reset_checks()
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}

        # ---- authorizer + ACLs ------------------------------------------
        authorizer_enabled = cluster_state.is_authorizer_enabled
        details["authorizer_enabled"] = authorizer_enabled
        details["acl_resource_count"] = len(cluster_state.acls or [])

        if authorizer_enabled is None:
            self._record_check(
                "security.authorizer.enabled",
                "Authorizer is enabled on the cluster",
                "GET /clusters/{cluster}/acls (presence implies authorizer)",
                "no_data",
                skipped_reason="authorizer state not reported",
            )
        elif authorizer_enabled is False:
            rec = self._create_recommendation(
                check_id="security.authorizer.enabled",
                title="Authorizer is disabled",
                description="No authorizer is configured on the brokers.",
                severity="critical",
                category="security",
                impact="Any authenticated client (or any client at all on plaintext listeners) can produce, consume, or admin any topic.",
                recommendation="Enable the AclAuthorizer (or equivalent) and apply least-privilege ACLs.",
            )
            recommendations.append(rec)
            self._record_check(
                "security.authorizer.enabled",
                "Authorizer is enabled on the cluster",
                "GET /clusters/{cluster}/acls",
                "fail",
                recommendation_id=rec.id,
            )
            self._record_check(
                "security.acls.present",
                "Authorizer-enabled cluster has ACLs configured",
                _ACL_SOURCE,
                "skipped",
                skipped_reason="authorizer is disabled",
            )
        else:
            self._record_check(
                "security.authorizer.enabled",
                "Authorizer is enabled on the cluster",
                "GET /clusters/{cluster}/acls",
                "pass",
            )
            if not cluster_state.acls:
                rec = self._create_recommendation(
                    check_id="security.acls.present",
                    title="Authorizer is enabled but no ACLs are configured",
                    description="The cluster has an authorizer but zero ACL resources.",
                    severity="warning",
                    category="security",
                    impact="Without ACLs, behaviour depends on allow.everyone.if.no.acl.found.",
                    recommendation="Define explicit ACLs per principal/topic/group.",
                )
                recommendations.append(rec)
                self._record_check(
                    "security.acls.present",
                    "Authorizer-enabled cluster has ACLs configured",
                    _ACL_SOURCE,
                    "fail",
                    recommendation_id=rec.id,
                )
            else:
                self._record_check(
                    "security.acls.present",
                    "Authorizer-enabled cluster has ACLs configured",
                    _ACL_SOURCE,
                    "pass",
                )

        # ---- listener / TLS / SASL config -------------------------------
        if not cluster_state.brokers:
            for cid in (
                "security.listener.plaintext",
                "security.listener.inter_broker_protocol",
                "security.sasl.mechanism",
                "security.sasl.super_users",
                "security.tls.client_auth",
                "security.zookeeper.acl",
                "security.role_consistency.cross_role",
            ):
                self._record_check(
                    cid, "depends on broker config snapshot", _BROKER_CFG_SOURCE,
                    "no_data", skipped_reason="no brokers in ClusterState",
                )
        else:
            sample = next(iter(cluster_state.brokers.values()))
            cfg = sample.configs

            listeners = cfg.get("listeners", "")
            advertised = cfg.get("advertised.listeners", "")
            security_map = cfg.get("listener.security.protocol.map", "")
            inter_broker_protocol = cfg.get("security.inter.broker.protocol")

            details["listeners"] = listeners
            details["listener.security.protocol.map"] = security_map
            details["security.inter.broker.protocol"] = inter_broker_protocol

            plaintext_present = (
                "PLAINTEXT" in (listeners or "") or "PLAINTEXT" in (advertised or "")
            )
            tls_present = any(
                proto in (listeners or "") + (advertised or "") + (security_map or "")
                for proto in ("SSL", "SASL_SSL", "TLS")
            )
            details["plaintext_listener_detected"] = plaintext_present
            details["tls_listener_detected"] = tls_present

            if plaintext_present and not tls_present:
                rec = self._create_recommendation(
                    check_id="security.listener.plaintext",
                    title="Brokers serve plaintext only",
                    description="Listener configuration only advertises PLAINTEXT.",
                    severity="critical",
                    category="security",
                    impact="Client traffic and broker-to-broker traffic is unencrypted.",
                    recommendation="Add an SSL or SASL_SSL listener and migrate clients off plaintext.",
                )
                recommendations.append(rec)
                self._record_check(
                    "security.listener.plaintext",
                    "Brokers expose at least one TLS listener; plaintext is decommissioned",
                    _BROKER_CFG_SOURCE + " keys=listeners,advertised.listeners",
                    "fail",
                    recommendation_id=rec.id,
                )
            elif plaintext_present and tls_present:
                rec = self._create_recommendation(
                    check_id="security.listener.plaintext",
                    title="A plaintext listener is still active",
                    description="Brokers expose both encrypted and plaintext listeners.",
                    severity="warning",
                    category="security",
                    recommendation="Decommission the plaintext listener once all clients have migrated.",
                )
                recommendations.append(rec)
                self._record_check(
                    "security.listener.plaintext",
                    "Brokers expose at least one TLS listener; plaintext is decommissioned",
                    _BROKER_CFG_SOURCE + " keys=listeners,advertised.listeners",
                    "fail",
                    recommendation_id=rec.id,
                )
            else:
                self._record_check(
                    "security.listener.plaintext",
                    "Brokers expose at least one TLS listener; plaintext is decommissioned",
                    _BROKER_CFG_SOURCE + " keys=listeners,advertised.listeners",
                    "pass",
                )

            if inter_broker_protocol is None:
                self._record_check(
                    "security.listener.inter_broker_protocol",
                    "security.inter.broker.protocol uses an encrypted transport",
                    _BROKER_CFG_SOURCE + " key=security.inter.broker.protocol",
                    "no_data",
                    skipped_reason="key not returned",
                )
            elif "PLAINTEXT" in inter_broker_protocol:
                rec = self._create_recommendation(
                    check_id="security.listener.inter_broker_protocol",
                    title="Inter-broker traffic is unencrypted",
                    description=f"security.inter.broker.protocol={inter_broker_protocol}.",
                    severity="warning",
                    category="security",
                    recommendation="Set security.inter.broker.protocol to SSL or SASL_SSL.",
                )
                recommendations.append(rec)
                self._record_check(
                    "security.listener.inter_broker_protocol",
                    "security.inter.broker.protocol uses an encrypted transport",
                    _BROKER_CFG_SOURCE + " key=security.inter.broker.protocol",
                    "fail",
                    recommendation_id=rec.id,
                    value=inter_broker_protocol,
                )
            else:
                self._record_check(
                    "security.listener.inter_broker_protocol",
                    "security.inter.broker.protocol uses an encrypted transport",
                    _BROKER_CFG_SOURCE + " key=security.inter.broker.protocol",
                    "pass",
                    value=inter_broker_protocol,
                )

            sasl_mechanism = cfg.get("sasl.enabled.mechanisms")
            if sasl_mechanism is None:
                self._record_check(
                    "security.sasl.mechanism",
                    "SASL mechanisms are SCRAM/GSSAPI/OAUTHBEARER (not PLAIN-only)",
                    _BROKER_CFG_SOURCE + " key=sasl.enabled.mechanisms",
                    "no_data",
                    skipped_reason="key not returned",
                )
            else:
                details["sasl.enabled.mechanisms"] = sasl_mechanism
                if "PLAIN" in sasl_mechanism and "SCRAM" not in sasl_mechanism:
                    rec = self._create_recommendation(
                        check_id="security.sasl.mechanism",
                        title="SASL/PLAIN is enabled without SCRAM",
                        description=f"sasl.enabled.mechanisms={sasl_mechanism}.",
                        severity="warning",
                        category="security",
                        impact="SASL/PLAIN sends credentials in cleartext, relying entirely on TLS for confidentiality.",
                        recommendation="Prefer SASL/SCRAM-SHA-512 (or OAUTHBEARER / GSSAPI) over PLAIN.",
                    )
                    recommendations.append(rec)
                    self._record_check(
                        "security.sasl.mechanism",
                        "SASL mechanisms are SCRAM/GSSAPI/OAUTHBEARER (not PLAIN-only)",
                        _BROKER_CFG_SOURCE + " key=sasl.enabled.mechanisms",
                        "fail",
                        recommendation_id=rec.id,
                        value=sasl_mechanism,
                    )
                else:
                    self._record_check(
                        "security.sasl.mechanism",
                        "SASL mechanisms are SCRAM/GSSAPI/OAUTHBEARER (not PLAIN-only)",
                        _BROKER_CFG_SOURCE + " key=sasl.enabled.mechanisms",
                        "pass",
                        value=sasl_mechanism,
                    )

            super_users = cfg.get("super.users", "")
            details["super.users"] = super_users
            if super_users is None:
                self._record_check(
                    "security.sasl.super_users",
                    "super.users list is short and audited",
                    _BROKER_CFG_SOURCE + " key=super.users",
                    "no_data",
                    skipped_reason="key not returned",
                )
            elif super_users:
                count = sum(1 for s in super_users.split(";") if s.strip())
                if count > 5:
                    rec = self._create_recommendation(
                        check_id="security.sasl.super_users",
                        title="Many super.users configured",
                        description=f"{count} entries in super.users.",
                        severity="info",
                        category="security",
                        recommendation="super.users bypass all ACLs; keep the list short and audited.",
                    )
                    recommendations.append(rec)
                    self._record_check(
                        "security.sasl.super_users",
                        "super.users list is short and audited",
                        _BROKER_CFG_SOURCE + " key=super.users",
                        "fail",
                        recommendation_id=rec.id,
                        count=count,
                    )
                else:
                    self._record_check(
                        "security.sasl.super_users",
                        "super.users list is short and audited",
                        _BROKER_CFG_SOURCE + " key=super.users",
                        "pass",
                        count=count,
                    )
            else:
                self._record_check(
                    "security.sasl.super_users",
                    "super.users list is short and audited",
                    _BROKER_CFG_SOURCE + " key=super.users",
                    "pass",
                    count=0,
                )

            ssl_client_auth = cfg.get("ssl.client.auth")
            if ssl_client_auth is None:
                self._record_check(
                    "security.tls.client_auth",
                    "ssl.client.auth=required when TLS is in use",
                    _BROKER_CFG_SOURCE + " key=ssl.client.auth",
                    "no_data",
                    skipped_reason="key not returned",
                )
            elif not tls_present:
                self._record_check(
                    "security.tls.client_auth",
                    "ssl.client.auth=required when TLS is in use",
                    _BROKER_CFG_SOURCE + " key=ssl.client.auth",
                    "skipped",
                    skipped_reason="no TLS listener detected",
                )
            elif ssl_client_auth.lower() == "none":
                rec = self._create_recommendation(
                    check_id="security.tls.client_auth",
                    title="SSL client authentication is disabled",
                    description="ssl.client.auth=none.",
                    severity="info",
                    category="security",
                    recommendation="Set ssl.client.auth=required if you rely on mTLS for client identity.",
                )
                recommendations.append(rec)
                self._record_check(
                    "security.tls.client_auth",
                    "ssl.client.auth=required when TLS is in use",
                    _BROKER_CFG_SOURCE + " key=ssl.client.auth",
                    "fail",
                    recommendation_id=rec.id,
                    value=ssl_client_auth,
                )
            else:
                self._record_check(
                    "security.tls.client_auth",
                    "ssl.client.auth=required when TLS is in use",
                    _BROKER_CFG_SOURCE + " key=ssl.client.auth",
                    "pass",
                    value=ssl_client_auth,
                )

            zookeeper_secure = _to_bool(cfg.get("zookeeper.set.acl"))
            zk_connect = cfg.get("zookeeper.connect")
            if not zk_connect:
                self._record_check(
                    "security.zookeeper.acl",
                    "zookeeper.set.acl=true on ZK-mode clusters",
                    _BROKER_CFG_SOURCE + " key=zookeeper.set.acl",
                    "skipped",
                    skipped_reason="cluster is KRaft (no ZooKeeper)",
                )
            elif zookeeper_secure is None:
                self._record_check(
                    "security.zookeeper.acl",
                    "zookeeper.set.acl=true on ZK-mode clusters",
                    _BROKER_CFG_SOURCE + " key=zookeeper.set.acl",
                    "no_data",
                    skipped_reason="key not returned",
                )
            elif zookeeper_secure is False:
                rec = self._create_recommendation(
                    check_id="security.zookeeper.acl",
                    title="ZooKeeper ACLs are not enabled",
                    description="zookeeper.set.acl=false.",
                    severity="warning",
                    category="security",
                    recommendation="Enable zookeeper.set.acl=true so Kafka znodes are protected.",
                )
                recommendations.append(rec)
                self._record_check(
                    "security.zookeeper.acl",
                    "zookeeper.set.acl=true on ZK-mode clusters",
                    _BROKER_CFG_SOURCE + " key=zookeeper.set.acl",
                    "fail",
                    recommendation_id=rec.id,
                )
            else:
                self._record_check(
                    "security.zookeeper.acl",
                    "zookeeper.set.acl=true on ZK-mode clusters",
                    _BROKER_CFG_SOURCE + " key=zookeeper.set.acl",
                    "pass",
                )

            # KRaft role-consistency comparison.
            controllers = [b for b in cluster_state.brokers.values() if b.is_controller]
            data_brokers = [b for b in cluster_state.brokers.values() if not b.is_controller]
            if not controllers or not data_brokers:
                self._record_check(
                    "security.role_consistency.cross_role",
                    "Security-relevant configs match across controllers and brokers (KRaft)",
                    _BROKER_CFG_SOURCE,
                    "skipped",
                    skipped_reason=(
                        "ZooKeeper-mode cluster, or controllers/brokers are not separated as KRaft roles"
                    ),
                )
            else:
                role_compare_keys = (
                    "sasl.mechanism.inter.broker.protocol",
                    "security.inter.broker.protocol",
                    "sasl.enabled.mechanisms",
                    "listener.security.protocol.map",
                )
                divergent: List[str] = []
                for key in role_compare_keys:
                    ctrl_vals = {b.configs.get(key) for b in controllers if b.configs.get(key) is not None}
                    brk_vals = {b.configs.get(key) for b in data_brokers if b.configs.get(key) is not None}
                    if not ctrl_vals or not brk_vals or ctrl_vals == brk_vals:
                        continue
                    divergent.append(key)
                    ctrl_repr = ", ".join(sorted(str(v) for v in ctrl_vals))
                    brk_repr = ", ".join(sorted(str(v) for v in brk_vals))
                    if key == "listener.security.protocol.map":
                        title = "listener.security.protocol.map differs between controllers and brokers"
                        impact = (
                            "Divergent listener maps make it easy to introduce silent "
                            "misconfiguration when adding listeners or rotating security policies."
                        )
                        rec_text = (
                            "Reconcile listener.security.protocol.map so controllers and brokers "
                            "share a single, intentional set of listener-to-protocol mappings."
                        )
                    else:
                        title = f"{key} differs between controllers and brokers"
                        impact = (
                            "Inconsistent inter-role security settings cause authentication or "
                            "encryption mismatches during failover and inter-process traffic."
                        )
                        rec_text = f"Set {key} to the same value on all controllers and brokers."
                    rec = self._create_recommendation(
                        check_id=f"security.role_consistency.{key.replace('.', '_')}",
                        title=title,
                        description=f"controllers: {ctrl_repr}; brokers: {brk_repr}.",
                        severity="warning",
                        category="security",
                        impact=impact,
                        recommendation=rec_text,
                    )
                    recommendations.append(rec)
                if divergent:
                    self._record_check(
                        "security.role_consistency.cross_role",
                        "Security-relevant configs match across controllers and brokers (KRaft)",
                        _BROKER_CFG_SOURCE,
                        "fail",
                        divergent_keys=divergent,
                    )
                else:
                    self._record_check(
                        "security.role_consistency.cross_role",
                        "Security-relevant configs match across controllers and brokers (KRaft)",
                        _BROKER_CFG_SOURCE,
                        "pass",
                    )

        # ---- ACL details (wildcard / orphan) ----------------------------
        wildcard_principals = 0
        orphan_topic_acls: List[str] = []
        topic_names = set(cluster_state.topics.keys())

        for resource in cluster_state.acls or []:
            if not isinstance(resource, dict):
                continue
            for acl in resource.get("acls", []) or []:
                principal = acl.get("principal", "")
                if principal in {"User:*", "*"}:
                    wildcard_principals += 1

            resource_type = (resource.get("resourceType") or "").upper()
            resource_name = resource.get("resourceName") or ""
            pattern_type = (resource.get("resourcePatternType") or "LITERAL").upper()
            if resource_type == "TOPIC" and resource_name and resource_name != "*":
                matched = False
                if pattern_type == "PREFIXED":
                    matched = any(name.startswith(resource_name) for name in topic_names)
                else:
                    matched = resource_name in topic_names
                if not matched:
                    orphan_topic_acls.append(resource_name)

        details["wildcard_acl_count"] = wildcard_principals
        details["orphan_topic_acl_count"] = len(orphan_topic_acls)
        if orphan_topic_acls:
            details["orphan_topic_acls"] = orphan_topic_acls

        if not cluster_state.acls:
            self._record_check(
                "security.acls.wildcard_principal",
                "ACLs do not grant access to wildcard principals (User:*)",
                _ACL_SOURCE,
                "skipped",
                skipped_reason="no ACLs to evaluate",
            )
            self._record_check(
                "security.acls.orphan_topic",
                "Topic ACLs reference existing topics",
                _ACL_SOURCE,
                "skipped",
                skipped_reason="no ACLs to evaluate",
            )
        else:
            if wildcard_principals > 0:
                rec = self._create_recommendation(
                    check_id="security.acls.wildcard_principal",
                    title="Wildcard ACL principals detected",
                    description=f"{wildcard_principals} ACL entries grant access to User:* (any authenticated user).",
                    severity="warning",
                    category="security",
                    recommendation="Replace wildcard principals with named ones following least privilege.",
                )
                recommendations.append(rec)
                self._record_check(
                    "security.acls.wildcard_principal",
                    "ACLs do not grant access to wildcard principals (User:*)",
                    _ACL_SOURCE,
                    "fail",
                    recommendation_id=rec.id,
                    count=wildcard_principals,
                )
            else:
                self._record_check(
                    "security.acls.wildcard_principal",
                    "ACLs do not grant access to wildcard principals (User:*)",
                    _ACL_SOURCE,
                    "pass",
                )
            if orphan_topic_acls:
                rec = self._create_recommendation(
                    check_id="security.acls.orphan_topic",
                    title="Orphan ACLs reference topics that no longer exist",
                    description=(
                        f"{len(orphan_topic_acls)} ACL resource(s) target topics that are not "
                        "present in the cluster (literal names not found, or PREFIXED patterns "
                        "that no current topic matches)."
                    ),
                    severity="warning",
                    category="security",
                    impact=(
                        "Orphan ACLs are stale grants. If a topic with the same name is "
                        "recreated later, the old ACL silently re-applies — often to the wrong "
                        "principal."
                    ),
                    recommendation=(
                        "Audit and delete ACLs whose target topic no longer exists; tighten "
                        "PREFIXED patterns that match no current topic."
                    ),
                    orphan_topics=orphan_topic_acls[:25],
                )
                recommendations.append(rec)
                self._record_check(
                    "security.acls.orphan_topic",
                    "Topic ACLs reference existing topics",
                    _ACL_SOURCE,
                    "fail",
                    recommendation_id=rec.id,
                    orphan_count=len(orphan_topic_acls),
                )
            else:
                self._record_check(
                    "security.acls.orphan_topic",
                    "Topic ACLs reference existing topics",
                    _ACL_SOURCE,
                    "pass",
                )

        details["recommendation_count"] = len(recommendations)
        return {
            "recommendations": recommendations,
            "summary": {
                "authorizer_enabled": authorizer_enabled,
                "acl_resource_count": len(cluster_state.acls or []),
                "issues": len(recommendations),
            },
            "details": details,
            "checks": [c.model_dump() for c in self._checks],
        }
