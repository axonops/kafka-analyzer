from unittest.mock import patch

import pytest

from kafka_analyzer.client import AxonOpsAuthError, AxonOpsClient
from kafka_analyzer.client.exceptions import AxonOpsAPIError, AxonOpsNotFoundError


@pytest.fixture
def client():
    return AxonOpsClient(api_url="http://localhost:9090", token="test-token")


def _mock_response(status_code=200, json_data=None, text="{}"):
    class _R:
        def __init__(self):
            self.status_code = status_code
            self.text = text
            self.headers = {}

        def json(self):
            return json_data if json_data is not None else {}

    return _R()


def test_client_sets_auth_header(client):
    assert client.session.headers["Authorization"] == "Bearer test-token"


def test_client_strips_trailing_slash():
    c = AxonOpsClient(api_url="http://example.com/", token="x")
    assert c.api_url == "http://example.com"


def test_get_kafka_topics_returns_list(client):
    with patch.object(client.session, "request") as mock_req:
        mock_req.return_value = _mock_response(200, [{"name": "orders"}], text="[]")
        result = client.get_kafka_topics("org", "kafka", "c")
    assert result == [{"name": "orders"}]


def test_get_kafka_topics_unwraps_dict_payload(client):
    with patch.object(client.session, "request") as mock_req:
        mock_req.return_value = _mock_response(
            200, {"topics": [{"name": "t1"}, {"name": "t2"}]}, text="{}"
        )
        result = client.get_kafka_topics("org", "kafka", "c")
    assert [t["name"] for t in result] == ["t1", "t2"]


def test_auth_error_raised_on_401(client):
    with patch.object(client.session, "request") as mock_req:
        mock_req.return_value = _mock_response(401, text="unauthorized")
        with pytest.raises(AxonOpsAuthError):
            client.get_kafka_cluster_info("org", "kafka", "c")


def test_not_found_raised_on_404(client):
    with patch.object(client.session, "request") as mock_req:
        mock_req.return_value = _mock_response(404, text="missing")
        with pytest.raises(AxonOpsNotFoundError):
            client.get_kafka_cluster_info("org", "kafka", "c")


def test_api_error_raised_on_500(client):
    with patch.object(client.session, "request") as mock_req:
        mock_req.return_value = _mock_response(500, text="boom")
        with pytest.raises(AxonOpsAPIError):
            client.get_kafka_cluster_info("org", "kafka", "c")


def test_org_header_added(client):
    captured = {}

    def fake_request(method, url, **kwargs):  # noqa: ARG001
        captured["headers"] = kwargs.get("headers", {})
        return _mock_response(200, {}, text="{}")

    with patch.object(client.session, "request", side_effect=fake_request):
        client.get_kafka_cluster_info("acme", "kafka", "c")
    assert captured["headers"].get("X-Grafana-Org-Id") == "acme"
