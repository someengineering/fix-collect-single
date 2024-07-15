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
import os
import sys
from argparse import Namespace, ArgumentParser
from datetime import timedelta
from itertools import takewhile
from pathlib import Path
from typing import List, Tuple, Dict
from typing import Optional, Any, cast

import yaml
from arango.cursor import Cursor
from fixcloudutils.logging import setup_logger
from fixcloudutils.redis.event_stream import Json
from fixcloudutils.redis.lock import Lock
from fixcloudutils.util import utc
from fixcloudutils.util import utc_str
from fixcore.core_config import parse_config
from fixcore.db.async_arangodb import AsyncArangoDB
from fixcore.db.db_access import DbAccess
from fixcore.db.timeseriesdb import TimeSeriesDB
from fixcore.system_start import parse_args as core_parse_args
from redis.asyncio import Redis

from collect_single.job import Job
from collect_single.model import MetricQuery
from collect_single.process import ProcessWrapper

log = logging.getLogger("fix.coordinator")


class CollectSingle(Job):
    def __init__(
        self,
        *,
        redis: Redis,  # type: ignore
        tenant_id: str,
        cloud: Optional[str],
        account_id: Optional[str],
        job_id: str,
        core_args: List[str],
        worker_args: List[str],
        logging_context: Dict[str, str],
        push_gateway_url: Optional[str] = None,
    ) -> None:
        super().__init__(redis=redis, job_id=job_id, tenant_id=tenant_id, push_gateway_url=push_gateway_url)
        self.cloud = cloud
        self.account_id = account_id
        self.core_args = ["--no-scheduling", "--ignore-interrupted-tasks"] + core_args
        self.worker_args = worker_args
        self.logging_context = logging_context
        self.task_id: Optional[str] = None
        self.worker_connected = asyncio.Event()
        self.metrics: List[MetricQuery] = []

    async def start(self) -> Any:
        await super().start()
        self.metrics = self.load_metrics()

    @staticmethod
    def load_metrics() -> List[MetricQuery]:
        with open(Path(__file__).parent / "metrics.yaml", "r") as f:
            yml = yaml.safe_load(f)
            return [MetricQuery.from_json(k, v) for k, v in yml.items() if "search" in v]

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
            elif msg_type == "message-listener-connected" and data.get("subscriber_id") == "fix.worker-collector":
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
            benchmarks = await self.core_client.list_benchmarks(providers=[self.cloud] if self.cloud else None)
            if benchmarks:
                await self.core_client.create_benchmark_reports([account_id], benchmarks, self.task_id)
            # create metrics
            for metric in self.metrics:
                res = await self.core_client.timeseries_snapshot(metric, account_id)
                if res:
                    log.info(f"Created timeseries snapshot: {metric.name} created {res} entries")
            # downsample all timeseries
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

    async def migrate_ts_data(self) -> None:
        ts_with_account = "for doc in ts filter doc.group.account!=null"
        update = (
            ts_with_account + " let account_id = NOT_NULL(@accounts[doc.group.account], doc.group.account) "
            'UPDATE doc WITH { group: MERGE(UNSET(doc.group, "account"), { account_id: account_id }) }  '
            "IN ts OPTIONS { mergeObjects: false }"
        )

        args = core_parse_args(self.core_args)
        # TODO: silent insecure request warning
        _, _, sdb = DbAccess.connect(args, timedelta(seconds=120), verify=False)

        async def migrate_ts() -> None:
            log.info("Redis lock taken. Migrate data.")
            config = parse_config(args, {}, lambda: None)
            tsdb = TimeSeriesDB(AsyncArangoDB(sdb), "ts", config)
            name_by_id = await self.core_client.account_id_by_name()
            async with tsdb._lock():  # also take the tsdb lock, so no one else is allowed to change ts data
                log.info("TS Lock taken. Migrate TS data to account id")
                sdb.aql.execute(update, bind_vars={"accounts": name_by_id})
                log.info("TS data migrated")

        result = cast(Cursor, sdb.aql.execute(ts_with_account + " LIMIT 1 RETURN doc", count=True))
        if result.count():
            # pick a global lock
            lock = Lock(self.redis, "collect_single_" + self.tenant_id, timedelta(minutes=15).total_seconds())
            await lock.with_lock(migrate_ts())

    async def sync(self, send_on_failed: bool) -> bool:
        result_send = False
        try:
            async with ProcessWrapper(["fixcore", *self.core_args], self.logging_context):
                log.info("Core started.")
                async with await asyncio.wait_for(self.core_client.wait_connected(), timeout=60):
                    log.info("Core client connected")
                    # wait up to 5 minutes for all running workflows to finish
                    await asyncio.wait_for(self.core_client.wait_for_collect_tasks_to_finish(), timeout=300)
                    log.info("All collect workflows finished")
                    async with ProcessWrapper(["fixworker", *self.worker_args], self.logging_context):
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


async def startup(
    args: Namespace, core_args: List[str], worker_args: List[str], logging_context: Dict[str, str]
) -> None:
    redis_args = {}
    if args.redis_password:
        redis_args["password"] = args.redis_password
    if args.redis_url.startswith("rediss://") and args.ca_cert:
        redis_args["ssl_ca_certs"] = args.ca_cert
    async with Redis.from_url(args.redis_url, decode_responses=True, **redis_args) as redis:

        async def collect_and_sync(send_on_failed: bool) -> bool:
            async with CollectSingle(
                redis=redis,
                tenant_id=args.tenant_id,
                cloud=args.cloud,
                account_id=args.account_id,
                job_id=args.job_id,
                core_args=core_args,
                worker_args=worker_args,
                push_gateway_url=args.push_gateway_url,
                logging_context=logging_context,
            ) as cas:
                return await cas.sync(send_on_failed)

        if retry := args.retry_failed_for:
            log.info(f"Collect job with retry enabled for {retry}.")
            has_result = False
            deadline = utc() + retry
            while not has_result and utc() < deadline:
                # collect and do not send a message in the failing case
                has_result = await collect_and_sync(False)
                if not has_result:
                    log.info("Failed collect with retry enabled. Retrying in 30s.")
                    await asyncio.sleep(30)
            # if we come here without a result, collect and also send a message in the failing case
            if not has_result:
                log.info("Last attempt to collect with retry enabled.")
                await collect_and_sync(True)
        else:
            await collect_and_sync(True)


def kv_pairs(s: str) -> Tuple[str, str]:
    return tuple(s.split("=", maxsplit=1))  # type: ignore


def main() -> None:
    # 3 argument sets delimited by "---": <coordinator args> --- <core args> --- <worker args>
    # coordinator --main-arg1 --main-arg2 --- --core-arg1 --core-arg2 --- --worker-arg1 --worker-arg2
    args = iter(sys.argv[1:])
    coordinator_args = list(takewhile(lambda x: x != "---", args))
    core_args = list(takewhile(lambda x: x != "---", args))
    worker_args = list(args)
    # handle coordinator args
    parser = ArgumentParser()
    parser.add_argument(
        "--write",
        type=kv_pairs,
        help="Write config files in home dir from env vars. Format: --write path/in/home/dir=env-var-name",
        default=[],
        action="append",
    )
    parser.add_argument("--job-id", required=True, help="Job Id of the coordinator")
    parser.add_argument("--tenant-id", required=True, help="Id of the tenant")
    parser.add_argument("--account-id", help="Id of the account")
    parser.add_argument("--cloud", help="Cloud provider.")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis host.")
    parser.add_argument("--redis-password", default=os.environ.get("REDIS_PASSWORD"), help="Redis password")
    parser.add_argument("--push-gateway-url", help="Prometheus push gateway url")
    parser.add_argument("--ca-cert", help="Path to CA cert file")
    parser.add_argument(
        "--retry-failed-for", type=lambda x: timedelta(seconds=float(x)), help="Seconds to retry failed jobs."
    )
    parsed = parser.parse_args(coordinator_args)

    # setup logging
    logging_context = dict(job_id=parsed.job_id, workspace_id=parsed.tenant_id, cloud_account_id=parsed.account_id)
    setup_logger("collect-single", get_logging_context=lambda: {"process": "coordinator", **logging_context})

    # write config files from env vars
    env_vars = {k.lower(): v for k, v in os.environ.items()}
    for home_path, env_var_name in parsed.write:
        path = (Path.home() / Path(home_path)).absolute()
        content = env_vars.get(env_var_name.lower())
        assert content is not None, f"Env var {env_var_name} not found"
        log.info(f"Writing file: {path} from env var: {env_var_name}")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w+") as f:
            f.write(content)

    log.info(f"Coordinator args:({coordinator_args}) Core args:({core_args}) Worker args:({worker_args})")
    asyncio.run(startup(parsed, core_args, worker_args, logging_context))


if __name__ == "__main__":
    main()
