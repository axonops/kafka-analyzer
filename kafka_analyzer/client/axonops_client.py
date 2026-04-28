"""
AxonOps API client implementation (Kafka)
"""

from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

import requests
import structlog
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from .exceptions import (
    AxonOpsAPIError,
    AxonOpsAuthError,
    AxonOpsConnectionError,
    AxonOpsForbiddenError,
    AxonOpsNotFoundError,
)

logger = structlog.get_logger()


class AxonOpsClient:
    """Client for the AxonOps API for Kafka clusters."""

    def __init__(
        self,
        api_url: str,
        token: str,
        timeout: int = 30,
        max_retries: int = 3,
        log_curl: bool = False,
    ):
        self.api_url = api_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.log_curl = log_curl

        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "kafka-analyzer/1.0.0",
        })

    def _request(self, method: str, endpoint: str, org: Optional[str] = None, **kwargs) -> Any:
        url = f"{self.api_url}/{endpoint.lstrip('/')}"

        headers = kwargs.get("headers", {})
        if org:
            headers["X-Grafana-Org-Id"] = org
        if headers:
            kwargs["headers"] = headers

        logger.debug(
            "API Request",
            method=method,
            url=url,
            params=kwargs.get("params", {}),
            json=kwargs.get("json", {}),
        )

        if self.log_curl:
            logger.info("curl_equivalent", curl=self._build_curl(method, url, headers, kwargs))

        try:
            response = self.session.request(
                method=method, url=url, timeout=self.timeout, **kwargs
            )

            logger.debug(
                "API Response",
                status_code=response.status_code,
                content_length=len(response.text) if response.text else 0,
                content_preview=response.text[:500] if response.text else "",
            )

            if response.status_code == 401:
                raise AxonOpsAuthError("Authentication failed")
            elif response.status_code == 403:
                # Caller decides whether to swallow this — log at debug, not error,
                # so optional admin endpoints don't pollute the output.
                logger.debug(
                    "API Forbidden",
                    status_code=response.status_code,
                    response_text=response.text,
                    url=url,
                    method=method,
                )
                raise AxonOpsForbiddenError(
                    f"Forbidden: {response.status_code} - {response.text}"
                )
            elif response.status_code == 404:
                raise AxonOpsNotFoundError(f"Resource not found: {endpoint}")
            elif response.status_code >= 400:
                logger.error(
                    "API Error Response",
                    status_code=response.status_code,
                    response_text=response.text,
                    url=url,
                    method=method,
                )
                raise AxonOpsAPIError(
                    f"API error: {response.status_code} - {response.text}"
                )

            return response.json() if response.text else {}

        except requests.exceptions.ConnectionError as e:
            raise AxonOpsConnectionError(f"Failed to connect to API: {e}")
        except requests.exceptions.Timeout as e:
            raise AxonOpsConnectionError(f"Request timed out: {e}")
        except requests.exceptions.RequestException as e:
            raise AxonOpsAPIError(f"Request failed: {e}")

    def _build_curl(
        self,
        method: str,
        url: str,
        extra_headers: Dict[str, str],
        kwargs: Dict[str, Any],
    ) -> str:
        import json as _json
        import urllib.parse

        params = kwargs.get("params") or {}
        if params:
            url_with_qs = f"{url}?{urllib.parse.urlencode(params)}"
        else:
            url_with_qs = url

        parts = [f"curl -X {method} '{url_with_qs}'"]
        parts.append("  -H 'Authorization: Bearer <REDACTED>'")
        parts.append("  -H 'Content-Type: application/json'")
        for k, v in extra_headers.items():
            if k == "Authorization":
                continue
            parts.append(f"  -H '{k}: {v}'")
        body = kwargs.get("json")
        if body is not None:
            parts.append(f"  -d '{_json.dumps(body)}'")
        return " \\\n".join(parts)

    # Organization / generic cluster info ------------------------------------

    def get_organizations(self) -> List[Dict[str, Any]]:
        result = self._request("GET", "/api/v1/orgs")
        return result.get("orgs", []) if isinstance(result, dict) else []

    def get_cluster_settings(self, org: str, cluster_type: str, cluster: str) -> Dict[str, Any]:
        return self._request(
            "GET", f"/api/v1/clusterSettings/{org}/{cluster_type}/{cluster}", org=org
        )

    def get_nodes(self, org: str, cluster_type: str, cluster: str) -> List[Dict[str, Any]]:
        return self._request(
            "GET", f"/api/v1/nodes/{org}/{cluster_type}/{cluster}", org=org
        )

    def get_nodes_full(self, org: str, cluster_type: str, cluster: str) -> List[Dict[str, Any]]:
        return self._request(
            "GET", f"/api/v1/nodes-full/{org}/{cluster_type}/{cluster}", org=org
        )

    def get_agent_config(self, org: str, cluster_type: str, cluster: str) -> Dict[str, Any]:
        return self._request(
            "GET", f"/api/v1/agentconfig/{org}/{cluster_type}/{cluster}", org=org
        )

    # Metrics (Prometheus-compatible) ----------------------------------------

    def query(self, query: str, time: Optional[datetime] = None) -> Dict[str, Any]:
        if time is None:
            time = datetime.now(UTC)
        params = {
            "query": query,
            "start": int(time.timestamp()),
            "end": int(time.timestamp()),
            "time": int(time.timestamp()),
        }
        return self._request("GET", "/api/v1/query", params=params)

    def query_range(
        self, query: str, start: datetime, end: datetime, step: str = "60s"
    ) -> Dict[str, Any]:
        params = {
            "query": query,
            "start": int(start.timestamp()),
            "end": int(end.timestamp()),
            "step": step,
        }
        return self._request("GET", "/api/v1/query_range", params=params)

    def get_metric_names(self, org: str, cluster_type: str, cluster: str) -> List[str]:
        result = self._request(
            "GET", f"/api/v1/metricNames/{org}/{cluster_type}/{cluster}", org=org
        )
        return result if isinstance(result, list) else []

    # Kafka cluster + brokers ------------------------------------------------

    def get_kafka_cluster_info(self, org: str, cluster_type: str, cluster: str) -> Dict[str, Any]:
        return self._request(
            "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/clusterInfo", org=org
        )

    def get_kafka_broker(
        self, org: str, cluster_type: str, cluster: str, broker_id: int
    ) -> Dict[str, Any]:
        return self._request(
            "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/broker/{broker_id}", org=org
        )

    def get_kafka_services(self, org: str, cluster_type: str, cluster: str) -> Any:
        return self._request(
            "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/services", org=org
        )

    def get_kafka_rack_failure(self, org: str, cluster_type: str, cluster: str) -> Dict[str, Any]:
        try:
            return self._request(
                "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/rackfailure", org=org
            )
        except (AxonOpsNotFoundError, AxonOpsForbiddenError):
            return {}

    # Topics -----------------------------------------------------------------

    def get_kafka_topics(self, org: str, cluster_type: str, cluster: str) -> List[Dict[str, Any]]:
        result = self._request(
            "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/topics", org=org
        )
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            for key in ("topics", "data", "Topics"):
                if key in result and isinstance(result[key], list):
                    return result[key]
        return []

    def get_kafka_topic(
        self, org: str, cluster_type: str, cluster: str, topic_name: str
    ) -> Dict[str, Any]:
        return self._request(
            "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/topics/{topic_name}", org=org
        )

    def get_kafka_topics_configs(
        self, org: str, cluster_type: str, cluster: str
    ) -> List[Dict[str, Any]]:
        result = self._request(
            "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/topics/configs", org=org
        )
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            for key in ("configs", "data", "topics"):
                if key in result and isinstance(result[key], list):
                    return result[key]
        return []

    def get_kafka_topic_configs(
        self, org: str, cluster_type: str, cluster: str, topic_name: str
    ) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/{org}/{cluster_type}/{cluster}/topics/{topic_name}/configs",
            org=org,
        )

    def get_kafka_topic_partitions(
        self, org: str, cluster_type: str, cluster: str, topic_name: str
    ) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/{org}/{cluster_type}/{cluster}/topics/{topic_name}/partitions",
            org=org,
        )

    def get_kafka_topic_consumer_groups(
        self, org: str, cluster_type: str, cluster: str, topic_name: str
    ) -> Any:
        return self._request(
            "GET",
            f"/api/v1/{org}/{cluster_type}/{cluster}/topics/{topic_name}/consumers",
            org=org,
        )

    # Consumer groups --------------------------------------------------------

    def get_kafka_consumer_groups(
        self, org: str, cluster_type: str, cluster: str
    ) -> Dict[str, Any]:
        return self._request(
            "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/consumerGroups", org=org
        )

    def get_kafka_consumer_groups_list(
        self, org: str, cluster_type: str, cluster: str
    ) -> Dict[str, Any]:
        return self._request(
            "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/consumerGroupsList", org=org
        )

    def get_kafka_consumer_group(
        self, org: str, cluster_type: str, cluster: str, group_id: str
    ) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/{org}/{cluster_type}/{cluster}/consumerGroups/{group_id}",
            org=org,
        )

    # Security / ACLs --------------------------------------------------------

    def get_kafka_acls(self, org: str, cluster_type: str, cluster: str) -> Dict[str, Any]:
        return self._request(
            "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/acls", org=org
        )

    # Kafka Connect ----------------------------------------------------------

    def get_kafka_connect_clusters(
        self, org: str, cluster_type: str, cluster: str
    ) -> Any:
        try:
            return self._request(
                "GET", f"/api/v1/{org}/{cluster_type}/{cluster}/connect/clusters", org=org
            )
        except (AxonOpsNotFoundError, AxonOpsForbiddenError):
            return []

    def get_kafka_connect_connectors(
        self, org: str, cluster_type: str, cluster: str, connect_cluster: str
    ) -> Any:
        return self._request(
            "GET",
            f"/api/v1/{org}/{cluster_type}/{cluster}/connect/{connect_cluster}/connectors",
            org=org,
        )

    def get_kafka_connect_connector(
        self, org: str, cluster_type: str, cluster: str, connect_cluster: str, connector: str
    ) -> Any:
        return self._request(
            "GET",
            f"/api/v1/{org}/{cluster_type}/{cluster}/connect/{connect_cluster}/{connector}",
            org=org,
        )

    def get_kafka_connect_connector_tasks(
        self, org: str, cluster_type: str, cluster: str, connect_cluster: str, connector: str
    ) -> Any:
        return self._request(
            "GET",
            f"/api/v1/{org}/{cluster_type}/{cluster}/connect/{connect_cluster}/{connector}/tasks",
            org=org,
        )

    def get_kafka_connect_connector_config(
        self, org: str, cluster_type: str, cluster: str, connect_cluster: str, connector: str
    ) -> Any:
        return self._request(
            "GET",
            f"/api/v1/{org}/{cluster_type}/{cluster}/connect/{connect_cluster}/{connector}/config",
            org=org,
        )

    # Schema Registry --------------------------------------------------------

    def get_schema_registry_subjects(
        self, org: str, cluster_type: str, cluster: str
    ) -> Any:
        try:
            return self._request(
                "GET",
                f"/api/v1/{org}/{cluster_type}/{cluster}/registry/subjects",
                org=org,
            )
        except (AxonOpsNotFoundError, AxonOpsForbiddenError):
            return []

    def get_schema_registry_subjects_detailed(
        self, org: str, cluster_type: str, cluster: str
    ) -> Any:
        try:
            return self._request(
                "GET",
                f"/api/v1/{org}/{cluster_type}/{cluster}/registry/subjects-detailed",
                org=org,
            )
        except (AxonOpsNotFoundError, AxonOpsForbiddenError):
            return []

    def get_schema_registry_config(
        self, org: str, cluster_type: str, cluster: str
    ) -> Any:
        try:
            return self._request(
                "GET",
                f"/api/v1/{org}/{cluster_type}/{cluster}/registry/configs",
                org=org,
            )
        except (AxonOpsNotFoundError, AxonOpsForbiddenError):
            return {}

    # Events / logs ----------------------------------------------------------

    def get_events(
        self,
        org: str,
        cluster_type: str,
        cluster: str,
        start_time: datetime,
        end_time: datetime,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        params = {
            "start": int(start_time.timestamp()),
            "end": int(end_time.timestamp()),
            "sort": "desc",
        }
        endpoint = f"/api/v1/events/{org}/{cluster_type}/{cluster}"
        payload = {
            "f1": "",
            "f2": "",
            "host_id": "",
            "bucket": 25,
            "type": "",
            "level": "",
            "source": "",
            "message": "",
            "search_after": None,
        }
        if filters:
            payload.update(filters)

        try:
            response = self._request("POST", endpoint, org=org, params=params, json=payload)
        except Exception as e:
            logger.error("Events API request failed", error=str(e))
            raise

        if isinstance(response, dict) and "data" in response:
            data = response["data"]
            return data if isinstance(data, list) else []
        if isinstance(response, list):
            return response
        return []

    def search_logs(
        self,
        org: str,
        cluster_type: str,
        cluster: str,
        start_time: datetime,
        end_time: datetime,
        message_filter: str,
        level: str = "",
        event_type: str = "",
        host_id: str = "",
        source: str = "",
        bucket: int = 25,
    ) -> List[Dict[str, Any]]:
        params = {
            "start": int(start_time.timestamp()),
            "end": int(end_time.timestamp()),
            "sort": "desc",
        }
        payload = {
            "type": event_type,
            "f1": "",
            "f2": "",
            "host_id": host_id,
            "level": level,
            "source": source,
            "message": message_filter,
            "bucket": bucket,
            "search_after": None,
        }
        endpoint = f"/api/v1/events/{org}/{cluster_type}/{cluster}"
        response = self._request("POST", endpoint, org=org, params=params, json=payload)

        if isinstance(response, dict) and "data" in response:
            data = response["data"]
            return data if isinstance(data, list) else []
        if isinstance(response, list):
            return response
        return []

    def get_logs_histogram(
        self,
        org: str,
        cluster_type: str,
        cluster: str,
        start_time: datetime,
        end_time: datetime,
        message_filter: str,
        level: str = "",
        event_type: str = "",
        host_id: str = "",
        source: str = "",
        bucket: int = 25,
    ) -> Dict[str, Any]:
        params = {
            "start": int(start_time.timestamp()),
            "end": int(end_time.timestamp()),
        }
        payload = {
            "type": event_type,
            "f1": "",
            "f2": "",
            "host_id": host_id,
            "level": level,
            "source": source,
            "message": message_filter,
            "bucket": bucket,
        }
        endpoint = f"/api/v1/histogram/{org}/{cluster_type}/{cluster}"
        response = self._request("POST", endpoint, org=org, params=params, json=payload)
        return response if response else {}

    # Health -----------------------------------------------------------------

    def health_check(self) -> bool:
        try:
            self._request("GET", "/api/v1/healthz")
            return True
        except Exception:
            return False

    def get_server_time(self) -> int:
        result = self._request("GET", "/api/v1/time")
        return result.get("timeUTC", 0) if isinstance(result, dict) else 0
