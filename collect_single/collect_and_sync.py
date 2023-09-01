# fix-collect-single
# Copyright (C) 2023  Some Engineering
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import logging
from typing import List, Optional, Any

from fixcloudutils.redis.event_stream import RedisStreamPublisher
from fixcloudutils.redis.pub_sub import RedisPubSubPublisher
from fixcloudutils.service import Service
from fixcloudutils.util import utc, utc_str
from redis.asyncio import Redis
from resotoclient.async_client import ResotoClient

from collect_single.process import ProcessWrapper

log = logging.getLogger("resoto.coordinator")


class CollectAndSync(Service):
    def __init__(
        self,
        redis: Redis,
        tenant_id: str,
        job_id: str,
        core_args: List[str],
        worker_args: List[str],
        core_url: str = "http://localhost:8980",
    ) -> None:
        self.tenant_id = tenant_id
        self.job_id = job_id
        self.core_args = ["resotocore", "--no-scheduling", "--ignore-interrupted-tasks"] + core_args
        self.worker_args = ["resotoworker"] + worker_args
        self.core_url = core_url
        self.task_id: Optional[str] = None
        publisher = "collect-and-sync"
        self.progress_update_publisher = RedisPubSubPublisher(redis, "progress-updates", publisher)
        self.collect_done_publisher = RedisStreamPublisher(redis, "collect-done", publisher)
        self.report_publisher = RedisStreamPublisher(redis, "report", publisher)
        self.started_at = utc()

    async def start(self) -> Any:
        await self.progress_update_publisher.start()
        await self.collect_done_publisher.start()
        await self.report_publisher.start()

    async def stop(self) -> None:
        await self.progress_update_publisher.stop()
        await self.collect_done_publisher.stop()
        await self.report_publisher.stop()

    async def core_client(self) -> ResotoClient:
        client = ResotoClient(self.core_url)
        while True:
            try:
                await client.ping()
                return client
            except Exception:
                await asyncio.sleep(1)

    async def listen_to_events_until_collect_done(self, client: ResotoClient) -> bool:
        async for event in client.events({"progress", "task_end"}):
            message_type = event.get("message_type", "")
            data = event.get("data", {})
            if message_type == "progress":
                log.info("Received progress event. Publish via redis")
                await self.progress_update_publisher.publish("progress", data, f"progress.{self.tenant_id}")
            elif message_type == "task_end" and self.task_id and data.get("task_id", "") == self.task_id:
                log.info("Received Task End event. Exit.")
                return True
            else:
                log.info(f"Received event. Waiting for task {self.task_id}. Ignore: {event}")
        return False

    async def wait_for_worker_connected(self, client: ResotoClient) -> None:
        while True:
            res = await client.subscribers_for_event("collect")
            if len(res) > 0:
                log.info(f"Found subscribers for collect event: {res}")
                return
            log.info("Wait for worker to connect.")
            await asyncio.sleep(1)

    async def wait_for_collect_tasks_to_finish(self, client: ResotoClient) -> None:
        while True:
            running = [
                entry async for entry in client.cli_execute("workflows running") if entry.get("workflow") != "collect"
            ]
            if len(running) == 0:
                return
            else:
                log.info(f"Wait for running workflows to finish. Running: {running}")
                await asyncio.sleep(5)

    async def start_collect(self, client: ResotoClient) -> None:
        running = [
            entry async for entry in client.cli_execute("workflows running") if entry.get("workflow") == "collect"
        ]
        if not running:
            log.info("No collect workflow running. Start one.")
            # start a workflow
            async for result in client.cli_execute("workflow run collect"):
                pass
            running = [
                entry async for entry in client.cli_execute("workflows running") if entry.get("workflow") == "collect"
            ]
        log.info(f"All running collect workflows: {running}")
        if running:
            self.task_id = running[0]["task-id"]
        else:
            raise Exception("Could not start collect workflow")

    async def send_result_events(self) -> None:
        # get information about all accounts that have been collected/updated
        async with await asyncio.wait_for(self.core_client(), timeout=60) as client:
            account_info = {
                info["id"]: info
                async for info in client.cli_execute(
                    "search is(account) and /metadata.exported_age<1h | jq {cloud: ./ancestors.cloud.reported.name, id:.id, name: .name, exported_at: ./metadata.exported_at, summary: ./metadata.descendant_summary}"  # noqa: E501
                )
            }
            # send a collect done event for the tenant
            await self.collect_done_publisher.publish(
                "collect-done",
                {
                    "job_id": self.job_id,
                    "tenant_id": self.tenant_id,
                    "account_info": account_info,
                    "started_at": utc_str(self.started_at),
                    "duration": int((utc() - self.started_at).total_seconds()),
                },
            )

            if account_info:  # only create reports, if account data was collected
                benchmark_results = {}
                benchmarks = [
                    cfg.split("resoto.report.benchmark.", maxsplit=1)[-1]
                    async for cfg in client.cli_execute("configs list")
                    if cfg.startswith("resoto.report.benchmark.")
                ]
                for benchmark in benchmarks:
                    report = [
                        n async for n in client.cli_execute(f"report benchmark run {benchmark} | format --ndjson")
                    ]
                    benchmark_results[benchmark] = report

                # send a report event for this tenant
                await self.report_publisher.publish(
                    "report",
                    {
                        "job_id": self.job_id,
                        "tenant_id": self.tenant_id,
                        "account_info": account_info,
                        "started_at": utc_str(self.started_at),
                        "duration": int((utc() - self.started_at).total_seconds()),
                        "reports": benchmark_results,
                    },
                )

    async def sync(self) -> None:
        async with ProcessWrapper(self.core_args):
            log.info("Core started.")
            async with await asyncio.wait_for(self.core_client(), timeout=60) as client:
                log.info("Core client connected")
                # wait up to 5 minutes for all running workflows to finish
                await asyncio.wait_for(self.wait_for_collect_tasks_to_finish(client), timeout=300)
                log.info("All collect workflows finished")
                async with ProcessWrapper(self.worker_args):
                    log.info("Worker started")
                    try:
                        # wait for worker to be connected
                        event_listener = asyncio.create_task(self.listen_to_events_until_collect_done(client))
                        # wait for worker to be connected
                        await asyncio.wait_for(self.wait_for_worker_connected(client), timeout=60)
                        log.info("Worker connected")
                        await self.start_collect(client)
                        log.info("Collect started. wait for the collect to finish")
                        await asyncio.wait_for(event_listener, 3600)  # wait up to 1 hour
                        log.info("Event listener done")
                        await asyncio.wait_for(self.send_result_events(), 300)  # wait up to 5 minutes
                    except Exception as ex:
                        log.info(f"Got exception {ex}. Giving up", exc_info=ex)
                        raise
