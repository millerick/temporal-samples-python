from collections.abc import Coroutine
from dataclasses import asdict, is_dataclass

from temporalio import activity, workflow
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    Interceptor,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
)
from typing_extensions import override

# with workflow.unsafe.imports_passed_through():
from sentry_sdk import Scope, isolation_scope


def _set_common_workflow_tags(
    scope: Scope,
    info: workflow.Info | activity.Info,
) -> None:
    scope.set_tag("temporal.workflow.type", info.workflow_type)
    scope.set_tag("temporal.workflow.id", info.workflow_id)


class _SentryExceptionActivityInboundInterceptor(ActivityInboundInterceptor):
    @override
    async def execute_activity(self, input: ExecuteActivityInput) -> Coroutine:
        # https://docs.sentry.io/platforms/python/troubleshooting/#addressing-concurrency-issues
        with isolation_scope() as scope:
            scope.set_tag("temporal.execution_type", "activity")
            scope.set_tag(
                "module",
                input.fn.__module__ + "." + input.fn.__qualname__,
            )

            activity_info = activity.info()
            _set_common_workflow_tags(scope, activity_info)
            scope.set_tag("temporal.activity.id", activity_info.activity_id)
            scope.set_tag(
                "temporal.activity.type",
                activity_info.activity_type,
            )
            scope.set_tag(
                "temporal.activity.task_queue",
                activity_info.task_queue,
            )
            scope.set_tag(
                "temporal.workflow.namespace",
                activity_info.workflow_namespace,
            )
            scope.set_tag(
                "temporal.workflow.run_id",
                activity_info.workflow_run_id,
            )
            try:
                return await super().execute_activity(input)
            except Exception:
                if len(input.args) == 1 and is_dataclass(input.args[0]):
                    scope.set_context(
                        "temporal.activity.input",
                        asdict(input.args[0]),  # type: ignore  # noqa: PGH003
                    )
                scope.set_context(
                    "temporal.activity.info",
                    activity.info().__dict__,
                )
                scope.capture_exception()
                raise


class _SentryExceptionWorkflowInterceptor(WorkflowInboundInterceptor):
    @override
    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Coroutine:
        # https://docs.sentry.io/platforms/python/troubleshooting/#addressing-concurrency-issues
        with isolation_scope() as scope:
            scope.set_tag("temporal.execution_type", "workflow")
            scope.set_tag(
                "module",
                input.run_fn.__module__ + "." + input.run_fn.__qualname__,
            )
            workflow_info = workflow.info()
            _set_common_workflow_tags(scope, workflow_info)
            scope.set_tag(
                "temporal.workflow.task_queue",
                workflow_info.task_queue,
            )
            scope.set_tag(
                "temporal.workflow.namespace",
                workflow_info.namespace,
            )
            scope.set_tag("temporal.workflow.run_id", workflow_info.run_id)
            try:
                return await super().execute_workflow(input)
            except Exception:
                if len(input.args) == 1 and is_dataclass(input.args[0]):
                    scope.set_context(
                        "temporal.workflow.input",
                        asdict(input.args[0]),  # type: ignore  # noqa: PGH003
                    )
                scope.set_context(
                    "temporal.workflow.info",
                    workflow.info().__dict__,
                )

                if not workflow.unsafe.is_replaying():
                    with (
                        workflow.unsafe.sandbox_unrestricted(),
                        workflow.unsafe.imports_passed_through(),
                    ):
                        scope.capture_exception()
                raise


class SentryInterceptor(Interceptor):
    """Temporal Interceptor class which will report workflow & activity exceptions to Sentry."""

    @override
    def intercept_activity(
        self,
        next: ActivityInboundInterceptor,
    ) -> ActivityInboundInterceptor:
        """Implementation of :py:meth:`temporalio.worker.Interceptor.intercept_activity`."""
        return _SentryExceptionActivityInboundInterceptor(
            super().intercept_activity(next),
        )

    @override
    def workflow_interceptor_class(
        self,
        input: WorkflowInterceptorClassInput,
    ) -> type[WorkflowInboundInterceptor] | None:
        return _SentryExceptionWorkflowInterceptor
