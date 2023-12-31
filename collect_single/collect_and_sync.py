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
from typing import List, Optional, Any, Tuple, Dict

from fixcloudutils.redis.event_stream import RedisStreamPublisher, Json
from fixcloudutils.redis.pub_sub import RedisPubSubPublisher
from fixcloudutils.service import Service
from fixcloudutils.util import utc, utc_str
from redis.asyncio import Redis
import prometheus_client

from collect_single.core_client import CoreClient
from collect_single.process import ProcessWrapper

log = logging.getLogger("resoto.coordinator")

SNAPSHOT_METRICS = {
    # count the number of infected resources by severity, cloud and account
    "infected_resources": "aggregate(/security.severity as severity, "
    "/ancestors.account.reported.id as account_id, "
    "/ancestors.cloud.reported.id as cloud_id: sum(1) as count): /security.has_issues==true",
    # count the number of resources by kind, cloud and account
    "resources": "aggregate(/reported.kind as kind, "
    "/ancestors.account.reported.id as account_id,"
    "/ancestors.cloud.reported.id as cloud_id: sum(1) as count): all",
}


class CollectAndSync(Service):
    def __init__(
        self,
        *,
        redis: Redis,
        tenant_id: str,
        account_id: Optional[str],
        job_id: str,
        core_args: List[str],
        worker_args: List[str],
        logging_context: Dict[str, str],
        push_gateway_url: Optional[str] = None,
        core_url: str = "http://localhost:8980",
    ) -> None:
        self.tenant_id = tenant_id
        self.account_id = account_id
        self.job_id = job_id
        self.core_args = ["resotocore", "--no-scheduling", "--ignore-interrupted-tasks"] + core_args
        self.worker_args = ["resotoworker"] + worker_args
        self.logging_context = logging_context
        self.core_client = CoreClient(core_url)
        self.task_id: Optional[str] = None
        self.push_gateway_url = push_gateway_url
        publisher = "collect-and-sync"
        self.progress_update_publisher = RedisPubSubPublisher(redis, f"tenant-events::{tenant_id}", publisher)
        self.collect_done_publisher = RedisStreamPublisher(redis, "collect-events", publisher)
        self.started_at = utc()
        self.worker_connected = asyncio.Event()

    async def start(self) -> Any:
        await self.progress_update_publisher.start()
        await self.collect_done_publisher.start()

    async def stop(self) -> None:
        await self.progress_update_publisher.stop()
        await self.collect_done_publisher.stop()

    async def listen_to_events_until_collect_done(self) -> bool:
        async for event in self.core_client.client.events():
            msg_type = event.get("message_type", "")
            kind = event.get("kind", "")
            data = event.get("data", {})
            if kind == "action":
                log.info(f"Received action event. Ignore. {msg_type}")
            elif msg_type == "progress":
                log.info("Received progress event. Publish via redis")
                await self.progress_update_publisher.publish("collect-progress", data)
            elif msg_type == "error":
                log.info("Received info message.")
                # await self.progress_update_publisher.publish("collect-error", data)
                # Ignore errors for now
                pass
            elif msg_type == "task_end" and self.task_id and data.get("task_id", "") == self.task_id:
                log.info("Received Task End event. Exit.")
                return True
            elif msg_type == "message-listener-connected" and data.get("subscriber_id") == "resoto.worker-collector":
                log.info("Received worker connected event. Mark.")
                self.worker_connected.set()
            else:
                log.info(f"Received event. Ignore: {event}")
        return False

    async def start_collect(self) -> None:
        running = await self.core_client.workflows_running("collect")
        if not running:
            log.info("No collect workflow running. Start one.")
            # start a workflow
            await self.core_client.start_workflow("collect")
            running = await self.core_client.workflows_running("collect")
        log.info(f"All running collect workflows: {running}")
        if running:
            self.task_id = running[0]["task-id"]
        else:
            raise Exception("Could not start collect workflow")

    async def post_process(self) -> Tuple[Json, List[str]]:
        # get information about all accounts that have been collected/updated
        account_info = await self.core_client.account_info(self.account_id)
        # check if there were errors
        messages = await self.core_client.workflow_log(self.task_id) if self.task_id else []
        # post process the data, if something has been collected
        if account_info and (account_id := self.account_id):
            # synchronize the security section
            benchmarks = await self.core_client.list_benchmarks()
            if benchmarks:
                await self.core_client.create_benchmark_reports(account_id, benchmarks, self.task_id)
            # create metrics
            for name, query in SNAPSHOT_METRICS.items():
                res = await self.core_client.timeseries_snapshot(name, query, account_id)
                log.info(f"Created timeseries snapshot: {name} created {res} entries")
                ds = await self.core_client.timeseries_downsample()
                log.info(f"Sampled down all timeseries. Result: {ds}")
        else:
            raise ValueError("No account info found. Give up!")

        return account_info, messages

    async def send_result_events(self, read_from_process: bool, error_messages: Optional[List[str]] = None) -> None:
        account_info, messages = await self.post_process() if read_from_process else ({}, error_messages or [])
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

    async def push_metrics(self) -> None:
        if gateway := self.push_gateway_url:
            # Possible future option: retrieve metrics from core and worker and push them to prometheus
            prometheus_client.push_to_gateway(
                gateway=gateway, job="collect_single", registry=prometheus_client.REGISTRY
            )
            log.info("Metrics pushed to gateway")

    async def sync(self, send_on_failed: bool) -> bool:
        result_send = False
        try:
            async with ProcessWrapper(self.core_args, self.logging_context):
                log.info("Core started.")
                async with await asyncio.wait_for(self.core_client.wait_connected(), timeout=60):
                    log.info("Core client connected")
                    # wait up to 5 minutes for all running workflows to finish
                    await asyncio.wait_for(self.core_client.wait_for_collect_tasks_to_finish(), timeout=300)
                    log.info("All collect workflows finished")
                    async with ProcessWrapper(self.worker_args, self.logging_context):
                        log.info("Worker started")
                        try:
                            # wait for worker to be connected
                            event_listener = asyncio.create_task(self.listen_to_events_until_collect_done())
                            # wait for worker to be connected
                            subs = await asyncio.wait_for(self.core_client.wait_for_worker_subscribed(), timeout=60)
                            log.info(f"Found subscribers for collect event: {subs}. Wait for worker to connect.")
                            await asyncio.wait_for(self.worker_connected.wait(), timeout=60)
                            log.info("Worker connected")
                            await self.start_collect()
                            log.info("Collect started. wait for the collect to finish")
                            await asyncio.wait_for(event_listener, 3600)  # wait up to 1 hour
                            log.info("Event listener done")
                            await self.push_metrics()
                        except Exception as ex:
                            log.info(f"Got exception {ex}. Giving up", exc_info=ex)
                            raise
                        finally:
                            await asyncio.wait_for(self.send_result_events(True), 600)  # wait up to 10 minutes
                            result_send = True
        except Exception as ex:
            log.info(f"Got Exception during sync: {ex}")
            if send_on_failed and not result_send:
                await asyncio.wait_for(self.send_result_events(False, [str(ex)]), 600)  # wait up to 10 minutes
                result_send = True
        return result_send
