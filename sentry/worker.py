import asyncio
import logging
import os

import sentry_sdk
from temporalio.client import Client
from temporalio.worker import Worker
from activities import compose_greeting
from workflows import GreetingWorkflow
from sentry_sdk.integrations.asyncio import AsyncioIntegration

from sentry.interceptor import SentryInterceptor
from sentry.tracing_interceptor import SentryTracingInterceptor

async def main():
    # Uncomment the line below to see logging
    logging.basicConfig(level=logging.INFO)

    # Initialize the Sentry SDK
    sentry_sdk.init(
        environment="dev",
        # Tracing
        enable_tracing=True,
        dsn=os.environ.get("SENTRY_DSN"),
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
        send_default_pii=True,
        integrations=[
            AsyncioIntegration(),
        ],
        max_request_body_size="always",
        max_value_length=10240,
    )

    # Start client
    client = await Client.connect("localhost:7233")

    # Run a worker for the workflow
    worker = Worker(
        client,
        task_queue="sentry-task-queue",
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
        interceptors=[SentryInterceptor(), SentryTracingInterceptor()],  # Use SentryInterceptor for error reporting
    )

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
