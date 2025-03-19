from temporalio import workflow
from datetime import timedelta
import threading

with workflow.unsafe.imports_passed_through():
    from activities import compose_greeting, ComposeGreetingInput

@workflow.defn
class GreetingWorkflow:
    failure_exception_types=[Exception]
    @workflow.run
    async def run(self, name: str) -> str:
        a = threading.Lock()
        workflow.logger.error("Running workflow with parameter %s" % name)
        return await workflow.execute_activity(
            compose_greeting,
            ComposeGreetingInput("Hello", name),
            start_to_close_timeout=timedelta(seconds=10),
        )
