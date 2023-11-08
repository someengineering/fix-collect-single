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
        account_id: Optional[str],
        job_id: str,
        core_args: List[str],
        worker_args: List[str],
        core_url: str = "http://localhost:8980",
    ) -> None:
        self.tenant_id = tenant_id
        self.account_id = account_id
        self.job_id = job_id
        self.core_args = ["resotocore", "--no-scheduling", "--ignore-interrupted-tasks"] + core_args
        self.worker_args = ["resotoworker"] + worker_args
        self.core_url = core_url
        self.task_id: Optional[str] = None
        publisher = "collect-and-sync"
        self.progress_update_publisher = RedisPubSubPublisher(redis, f"tenant-events::{tenant_id}", publisher)
        self.collect_done_publisher = RedisStreamPublisher(redis, "collect-events", publisher)
        self.started_at = utc()

    async def start(self) -> Any:
        await self.progress_update_publisher.start()
        await self.collect_done_publisher.start()

    async def stop(self) -> None:
        await self.progress_update_publisher.stop()
        await self.collect_done_publisher.stop()

    async def core_client(self) -> ResotoClient:
        client = ResotoClient(self.core_url)
        while True:
            try:
                await client.ping()
                return client
            except Exception:
                await asyncio.sleep(1)

    async def listen_to_events_until_collect_done(self, client: ResotoClient) -> bool:
        async for event in client.events({"progress", "error", "task_end"}):
            message_type = event.get("message_type", "")
            data = event.get("data", {})
            if message_type == "progress":
                log.info("Received progress event. Publish via redis")
                await self.progress_update_publisher.publish("collect-progress", data)
            elif message_type == "error":
                log.info("Received info message. Publish via redis")
                await self.progress_update_publisher.publish("collect-error", data)
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
            # fetch account information for last collect run (account nodes updated in the last hour).
            account_filter = f" and id=={self.account_id}" if self.account_id else ""
            account_info = {
                info["id"]: info
                async for info in client.cli_execute(
                    f"search is(account){account_filter} and /metadata.exported_age<1h | jq {{cloud: ./ancestors.cloud.reported.name, id:.id, name: .name, exported_at: ./metadata.exported_at, summary: ./metadata.descendant_summary}}"  # noqa: E501
                )
            }
            # check if there were errors
            messages = []
            if self.task_id:
                messages = [
                    info
                    async for info in client.cli_execute(f"workflows log {self.task_id} | head 100")
                    if info != "No error messages for this run."
                ]
            # Synchronize security section, if account data was collected
            if account_info:
                benchmarks = [cfg async for cfg in client.cli_execute("report benchmark list")]
                if benchmarks:
                    bn = " ".join(benchmarks)
                    accounts = " ".join(account_info.keys())
                    command = (
                        f"report benchmark run {bn} --accounts {accounts} --sync-security-section "
                        f"--run-id {self.task_id} | count"
                    )
                    log.info(
                        f"Create reports for following benchmarks: {bn} for accounts: {accounts}. Command: {command}"
                    )
                    async for _ in client.cli_execute(command, headers={"Accept": "application/json"}):
                        pass  # ignore the result

            # send a collect done event for the tenant
            await self.collect_done_publisher.publish(
                "collect-done",
                {
                    "job_id": self.job_id,
                    "task_id": self.task_id,
                    "tenant_id": self.tenant_id,
                    "account_info": account_info,
                    "messages": messages,
                    "started_at": utc_str(self.started_at),
                    "duration": int((utc() - self.started_at).total_seconds()),
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
                    except Exception as ex:
                        log.info(f"Got exception {ex}. Giving up", exc_info=ex)
                        raise
                    finally:
                        await asyncio.wait_for(self.send_result_events(), 600)  # wait up to 10 minutes
