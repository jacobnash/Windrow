"""windrow.tracing exports only when a collector is configured, so an edge
node without one doesn't retry localhost:4318 forever."""

import pytest

from windrow import tracing


@pytest.mark.parametrize(
    "env, want",
    [
        ({}, None),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": ""}, None),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4318/v1/traces"}, "http://jaeger:4318/v1/traces"),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4318/v1/traces", "OTEL_TRACES_EXPORTER": "none"}, None),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://jaeger:4318/v1/traces", "OTEL_TRACES_EXPORTER": "otlp"},
         "http://jaeger:4318/v1/traces"),
    ],
)
def test_exporter_endpoint(monkeypatch, env, want):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_TRACES_EXPORTER", raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert tracing.exporter_endpoint() == want
