"""
Shared OpenTelemetry setup - mirrors baler/src/baler/tracing.py, except
that exporting is off when no collector is configured (exporter_endpoint). Deliberately duplicated (12 lines) rather than imported across
repos - sharing code here would violate the Windrow/Baler decoupling
rule harder than duplicating this does.
"""

import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_initialized = False


def exporter_endpoint() -> str | None:
    """Where spans go, or None for nowhere. Off unless a collector is named
    (OTEL_EXPORTER_OTLP_ENDPOINT) and OTEL_TRACES_EXPORTER isn't `none`: an
    edge node with no collector used to retry localhost:4318 every few
    seconds forever. Baler's deploys set the endpoint explicitly, so this is
    the one place Windrow differs from baler/tracing.py on purpose."""
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if not endpoint or os.environ.get("OTEL_TRACES_EXPORTER", "").strip().lower() == "none":
        return None
    return endpoint


def init_tracing(service_name: str = "windrow") -> None:
    global _initialized
    if _initialized:
        return
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    endpoint = exporter_endpoint()
    if endpoint is not None:
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    _initialized = True


def get_tracer(name: str):
    return trace.get_tracer(name)
