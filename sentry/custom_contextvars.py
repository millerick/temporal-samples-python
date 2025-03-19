from contextvars import ContextVar
from uuid import UUID

_CV_SENTRY_TRACE: ContextVar[str] = ContextVar("sentry_trace_id")
"""
Represent the Sentry trace ID.
"""
_CV_SENTRY_SPAN: ContextVar[str] = ContextVar("sentry_span_id")
"""
Represent the Sentry span ID.
"""

_ALL_CONTEXTVARS: set[ContextVar] = set()
_ALL_SEARCH_ATTRIBUTES: set[ContextVar] = set()


def register_temporal_contextvars(
    *contextvars: ContextVar,
    add_as_search_attribute: bool = False,
) -> None:
    """Register contextvars to be used in Temporal workflows."""
    _ALL_CONTEXTVARS.update(contextvars)
    if add_as_search_attribute:
        _ALL_SEARCH_ATTRIBUTES.update(contextvars)

register_temporal_contextvars(
    _CV_SENTRY_TRACE,
    _CV_SENTRY_SPAN,
    add_as_search_attribute=False,
)
