import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, NoReturn

import temporalio
import temporalio.workflow
from temporalio.client import (
    Interceptor as ClientInterceptor,
)
from temporalio.client import (
    OutboundInterceptor,
    QueryWorkflowInput,
    SignalWorkflowInput,
    StartWorkflowInput,
    StartWorkflowUpdateInput,
    WorkflowHandle,
    WorkflowUpdateHandle,
)
from temporalio.converter import (
    PayloadConverter,
)
from temporalio.worker import (
    ActivityInboundInterceptor,
    ContinueAsNewInput,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    SignalChildWorkflowInput,
    SignalExternalWorkflowInput,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)
from temporalio.worker import (
    Interceptor as WorkerInterceptor,
)
from temporalio.workflow import (
    ActivityHandle,
    ChildWorkflowHandle,
    Info,
)

with temporalio.workflow.unsafe.imports_passed_through():
    import sentry_sdk

if TYPE_CHECKING:
    from temporalio.activity import (
        Info as ActivityInfo,
    )


logger = logging.getLogger(__name__)

_SENTRY_TRACE_HEADER: str = "_sentry-trace-data"
_PAYLOAD_CONVERTER = PayloadConverter.default


def _join_trace(
    scope: sentry_sdk.Scope,
    headers: Mapping[str, temporalio.api.common.v1.Payload],
    op: str,
    name: str,
) -> sentry_sdk.tracing.Transaction:
    """Join the current trace or start a new transaction."""
    try:
        trace_headers = headers.get(_SENTRY_TRACE_HEADER)
        if trace_headers:
            # existing headers available - join trace and start transaction
            trace_headers = _PAYLOAD_CONVERTER.from_payload(trace_headers)
            tx = scope.continue_trace(trace_headers, op=op, name=name)
            return scope.start_transaction(transaction=tx)
    except Exception as e:  # noqa: BLE001
        msg = f"Error extracting Sentry trace headers: {e}"
        logger.warning(msg)

    # no existing headers? start a new transaction
    return scope.start_transaction(op=op, name=name)


def _inject_trace_headers(
    headers: Mapping[str, temporalio.api.common.v1.Payload],
) -> Mapping[str, temporalio.api.common.v1.Payload]:
    """Inject the current trace into the headers."""
    try:
        return {
            **headers,
            _SENTRY_TRACE_HEADER: _PAYLOAD_CONVERTER.to_payload(
                {
                    "sentry-trace": sentry_sdk.get_traceparent(),
                    "baggage": sentry_sdk.get_baggage(),
                },
            ),
        }
    except Exception as e:  # noqa: BLE001
        msg = f"Error injecting Sentry trace headers: {e}"
        logger.warning(msg)
        return headers


class SentryTracingInterceptor(ClientInterceptor, WorkerInterceptor):
    """Temporal interceptor to trace workflows and activities with Sentry."""

    def intercept_client(
        self,
        next: OutboundInterceptor,
    ) -> OutboundInterceptor:
        """Return a new outbound interceptor that wraps the given one."""
        return _SentryOutboundInterceptor(next)

    def intercept_activity(
        self,
        next: ActivityInboundInterceptor,
    ) -> ActivityInboundInterceptor:
        """Return a new inbound interceptor that wraps the given one."""
        return _SentryActivityInboundInterceptor(next)

    def workflow_interceptor_class(
        self,
        _input: WorkflowInterceptorClassInput,
    ) -> type[WorkflowInboundInterceptor]:
        """Return the workflow interceptor class to use."""
        return _SentryWorkflowInboundInterceptor


class _SentryOutboundInterceptor(OutboundInterceptor):
    """Outbound interceptor to wrap workflow creation, signal, and query handling."""

    def __init__(self, next: OutboundInterceptor) -> None:
        self.next = next

    async def start_workflow(
        self,
        input: StartWorkflowInput,
    ) -> WorkflowHandle[Any, Any]:
        """Called for every `Client.start_workflow` call."""
        # Inject the current sentry trace details into the workflow input
        input.headers = _inject_trace_headers(input.headers)
        prefix = (
            "StartWorkflow"
            if not input.start_signal
            else "SignalWithStartWorkflow"
        )
        with (
            sentry_sdk.isolation_scope() as scope,
            scope.start_span(op=prefix, name=input.workflow) as span,
        ):
            scope.set_context(
                "Temporal Start Workflow",
                {
                    "workflow": input.workflow,
                    "workflow_id": input.id,
                    "task_queue": input.task_queue,
                },
            )
            return await super().start_workflow(input)

    async def query_workflow(self, input: QueryWorkflowInput) -> Any:  # noqa:ANN401
        """Called for every `WorkflowHandle.query` call."""
        input.headers = _inject_trace_headers(input.headers)
        with (
            sentry_sdk.isolation_scope() as scope,
            scope.start_span(
                op="QueryWorkflow",
                name=input.query,
            ) as span,
        ):
            scope.set_context(
                "Temporal Query Workflow",
                {
                    "workflow_id": input.id,
                    "workflow_run_id": input.run_id,
                    "query": input.query,
                },
            )
            return await super().query_workflow(input)

    async def signal_workflow(self, input: SignalWorkflowInput) -> None:
        """Called for every `WorkflowHandle.signal` call."""
        with (
            sentry_sdk.isolation_scope() as scope,
            scope.start_span(
                op="SignalWorkflow",
                name=input.signal,
            ) as span,
        ):
            input.headers = _inject_trace_headers(input.headers)
            scope.set_context(
                "Temporal Signal Workflow",
                {
                    "workflow_id": input.id,
                    "workflow_run_id": input.run_id,
                    "signal": input.signal,
                },
            )
            return await super().signal_workflow(input)

    async def start_workflow_update(
        self,
        input: StartWorkflowUpdateInput,
    ) -> WorkflowUpdateHandle[Any]:
        """Called for every `WorkflowHandle.update` and `WorkflowHandle.start_update` call."""
        input.headers = _inject_trace_headers(input.headers)
        with (
            sentry_sdk.isolation_scope() as scope,
            scope.start_span(
                op="StartWorkflowUpdate",
                name=input.update,
            ) as span,
        ):
            scope.set_context(
                "Temporal Workflow Update",
                {
                    "workflow_id": input.id,
                    "workflow_run_id": input.run_id,
                    "update": input.update,
                },
            )
            return await super().start_workflow_update(input)


class _SentryWorkflowInboundInterceptor(WorkflowInboundInterceptor):
    """Inbound interceptor to wrap outbound creation, workflow execution, and signal/query handling."""

    def __init__(self, next: WorkflowInboundInterceptor) -> None:
        """Initialize a tracing workflow interceptor."""
        super().__init__(next)

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        """Initialize the outbound interceptor."""
        super().init(_SentryWorkflowOutboundInterceptor(outbound))

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:  # noqa:ANN401
        """Called to run the workflow."""
        info: Info = temporalio.workflow.info()
        with temporalio.workflow.unsafe.sandbox_unrestricted():
            scope = sentry_sdk.get_isolation_scope()
        with (
            _join_trace(
                scope,
                input.headers,
                op="RunWorkflow",
                name=info.workflow_type,
            ) as transaction,
        ):
            transaction.set_tag("temporal.task_queue", info.task_queue)
            transaction.set_tag("temporal.workflow_id", info.workflow_id)
            transaction.set_tag("temporal.workflow_run_id", info.run_id)
            transaction.set_tag("temporal.workflow", info.workflow_type)
            transaction.set_context(
                "Temporal Workflow",
                {
                    "workflow": info.workflow_type,
                    "workflow_id": info.workflow_id,
                    "task_queue": info.task_queue,
                    "attempt": info.attempt,
                    "workflow_run_id": info.run_id,
                    "namespace": info.namespace,
                },
            )
            return await super().execute_workflow(input)


class _SentryWorkflowOutboundInterceptor(WorkflowOutboundInterceptor):
    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        """Called for every continue_as_new call."""
        input.headers = _inject_trace_headers(input.headers)
        super().continue_as_new(input)

    async def signal_child_workflow(
        self,
        input: SignalChildWorkflowInput,
    ) -> None:
        """Called for every signal_child_workflow call."""
        input.headers = _inject_trace_headers(input.headers)
        return await super().signal_child_workflow(input)

    async def signal_external_workflow(
        self,
        input: SignalExternalWorkflowInput,
    ) -> None:
        """Called for every signal_external_workflow call."""
        input.headers = _inject_trace_headers(input.headers)
        return await super().signal_external_workflow(input)

    def start_activity(
        self,
        input: StartActivityInput,
    ) -> ActivityHandle:
        """Called for every :py:func:`temporalio.workflow.start_activity`."""
        input.headers = _inject_trace_headers(input.headers)
        return super().start_activity(input)

    async def start_child_workflow(
        self,
        input: StartChildWorkflowInput,
    ) -> ChildWorkflowHandle:
        """Called for every :py:func:`temporalio.workflow.start_child_workflow` call."""
        input.headers = _inject_trace_headers(input.headers)
        return await super().start_child_workflow(input)

    def start_local_activity(
        self,
        input: StartLocalActivityInput,
    ) -> ActivityHandle:
        """Called for every :py:func:`temporalio.workflow.start_local_activity` call."""
        input.headers = _inject_trace_headers(input.headers)
        return super().start_local_activity(input)


class _SentryActivityInboundInterceptor(ActivityInboundInterceptor):
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:  # noqa:ANN401
        """Called to invoke the activity."""
        info: ActivityInfo = temporalio.activity.info()
        with (
            sentry_sdk.isolation_scope() as scope,
            _join_trace(
                scope,
                input.headers,
                op="RunActivity",
                name=info.activity_type,
            ) as transaction,
        ):
            transaction.set_tag("temporal.activity_id", info.activity_id)
            transaction.set_tag("temporal.activity_type", info.activity_type)
            transaction.set_tag("temporal.task_queue", info.task_queue)
            transaction.set_tag("temporal.workflow_id", info.workflow_id)
            transaction.set_tag(
                "temporal.workflow_run_id",
                info.workflow_run_id,
            )
            transaction.set_tag("temporal.workflow", info.workflow_type)
            transaction.set_context(
                "Temporal Activity",
                {
                    "activity_type": info.activity_type,
                    "activity_id": info.activity_id,
                    "attempt": info.attempt,
                    "workflow_id": info.workflow_id,
                    "workflow_run_id": info.workflow_run_id,
                    "task_queue": info.task_queue,
                    "workflow": info.workflow_type,
                    "namespace": info.workflow_namespace,
                },
            )
            return await super().execute_activity(input)
