"""
Security analyzer for Kafka clusters.

Reviews authorizer state, ACL coverage, listener encryption, super.users,
and SASL/TLS-related broker config.
"""

from typing import Any, Dict, List, Optional

from ..models import ClusterState, Recommendation
from .base import BaseAnalyzer


def _to_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    return str(value).strip().lower() == "true"


class SecurityAnalyzer(BaseAnalyzer):
    def analyze(self, cluster_state: ClusterState) -> Dict[str, Any]:
        recommendations: List[Recommendation] = []
        details: Dict[str, Any] = {}

        # Authorizer + ACLs.
        authorizer_enabled = cluster_state.is_authorizer_enabled
        details["authorizer_enabled"] = authorizer_enabled
        details["acl_resource_count"] = len(cluster_state.acls or [])

        if authorizer_enabled is False:
            recommendations.append(
                self._create_recommendation(
                    title="Authorizer is disabled",
                    description="No authorizer is configured on the brokers.",
                    severity="critical",
                    category="security",
                    impact="Any authenticated client (or any client at all on plaintext listeners) can produce, consume, or admin any topic.",
                    recommendation="Enable the AclAuthorizer (or equivalent) and apply least-privilege ACLs.",
                )
            )
        elif authorizer_enabled is True and not cluster_state.acls:
            recommendations.append(
                self._create_recommendation(
                    title="Authorizer is enabled but no ACLs are configured",
                    description="The cluster has an authorizer but zero ACL resources.",
                    severity="warning",
                    category="security",
                    impact="Without ACLs, behaviour depends on allow.everyone.if.no.acl.found.",
                    recommendation="Define explicit ACLs per principal/topic/group.",
                )
            )

        # Listener / TLS / SASL config from any broker (settings are usually identical).
        if cluster_state.brokers:
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
                recommendations.append(
                    self._create_recommendation(
                        title="Brokers serve plaintext only",
                        description="Listener configuration only advertises PLAINTEXT.",
                        severity="critical",
                        category="security",
                        impact="Client traffic and broker-to-broker traffic is unencrypted.",
                        recommendation="Add an SSL or SASL_SSL listener and migrate clients off plaintext.",
                    )
                )
            elif plaintext_present and tls_present:
                recommendations.append(
                    self._create_recommendation(
                        title="A plaintext listener is still active",
                        description="Brokers expose both encrypted and plaintext listeners.",
                        severity="warning",
                        category="security",
                        recommendation="Decommission the plaintext listener once all clients have migrated.",
                    )
                )

            if inter_broker_protocol and "PLAINTEXT" in inter_broker_protocol:
                recommendations.append(
                    self._create_recommendation(
                        title="Inter-broker traffic is unencrypted",
                        description=f"security.inter.broker.protocol={inter_broker_protocol}.",
                        severity="warning",
                        category="security",
                        recommendation="Set security.inter.broker.protocol to SSL or SASL_SSL.",
                    )
                )

            sasl_mechanism = cfg.get("sasl.enabled.mechanisms")
            if sasl_mechanism:
                details["sasl.enabled.mechanisms"] = sasl_mechanism
                if "PLAIN" in sasl_mechanism and "SCRAM" not in sasl_mechanism:
                    recommendations.append(
                        self._create_recommendation(
                            title="SASL/PLAIN is enabled without SCRAM",
                            description=f"sasl.enabled.mechanisms={sasl_mechanism}.",
                            severity="warning",
                            category="security",
                            impact="SASL/PLAIN sends credentials in cleartext, relying entirely on TLS for confidentiality.",
                            recommendation="Prefer SASL/SCRAM-SHA-512 (or OAUTHBEARER / GSSAPI) over PLAIN.",
                        )
                    )

            super_users = cfg.get("super.users", "")
            details["super.users"] = super_users
            if super_users:
                count = sum(1 for s in super_users.split(";") if s.strip())
                if count > 5:
                    recommendations.append(
                        self._create_recommendation(
                            title="Many super.users configured",
                            description=f"{count} entries in super.users.",
                            severity="info",
                            category="security",
                            recommendation="super.users bypass all ACLs; keep the list short and audited.",
                        )
                    )

            ssl_client_auth = cfg.get("ssl.client.auth")
            if ssl_client_auth and ssl_client_auth.lower() == "none" and tls_present:
                recommendations.append(
                    self._create_recommendation(
                        title="SSL client authentication is disabled",
                        description="ssl.client.auth=none.",
                        severity="info",
                        category="security",
                        recommendation="Set ssl.client.auth=required if you rely on mTLS for client identity.",
                    )
                )

            zookeeper_secure = _to_bool(cfg.get("zookeeper.set.acl"))
            zk_connect = cfg.get("zookeeper.connect")
            if zk_connect and zookeeper_secure is False:
                recommendations.append(
                    self._create_recommendation(
                        title="ZooKeeper ACLs are not enabled",
                        description="zookeeper.set.acl=false.",
                        severity="warning",
                        category="security",
                        recommendation="Enable zookeeper.set.acl=true so Kafka znodes are protected.",
                    )
                )

        # Public/wildcard ACL detection + orphan topic ACLs.
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

            # Orphan topic ACL: a TOPIC-resource ACL whose name does not
            # match any current topic. Skip the cluster-wildcard "*", which
            # is by definition not orphan.
            resource_type = (resource.get("resourceType") or "").upper()
            resource_name = resource.get("resourceName") or ""
            pattern_type = (resource.get("resourcePatternType") or "LITERAL").upper()
            if resource_type == "TOPIC" and resource_name and resource_name != "*":
                matched = False
                if pattern_type == "PREFIXED":
                    matched = any(name.startswith(resource_name) for name in topic_names)
                else:  # LITERAL (default) and any other unknown pattern
                    matched = resource_name in topic_names
                if not matched:
                    orphan_topic_acls.append(resource_name)

        details["wildcard_acl_count"] = wildcard_principals
        details["orphan_topic_acl_count"] = len(orphan_topic_acls)
        if orphan_topic_acls:
            details["orphan_topic_acls"] = orphan_topic_acls

        if wildcard_principals > 0:
            recommendations.append(
                self._create_recommendation(
                    title="Wildcard ACL principals detected",
                    description=f"{wildcard_principals} ACL entries grant access to User:* (any authenticated user).",
                    severity="warning",
                    category="security",
                    recommendation="Replace wildcard principals with named ones following least privilege.",
                )
            )

        if orphan_topic_acls:
            recommendations.append(
                self._create_recommendation(
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
        }
