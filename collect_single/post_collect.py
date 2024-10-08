from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from argparse import ArgumentParser, Namespace
from itertools import takewhile
from pathlib import Path
from time import sleep
from typing import List, Dict
from typing import Optional

import cattrs
import yaml
from attr import define
from fixcloudutils.logging import setup_logger
from fixcloudutils.util import utc, utc_str
from redis.asyncio.client import Redis

from collect_single.job import Job
from collect_single.model import MetricQuery
from collect_single.process import ProcessWrapper

log = logging.getLogger("fix.coordinator")


class PostCollect(Job):
    def __init__(
        self,
        *,
        redis: Redis,
        tenant_id: str,
        job_id: str,
        accounts_collected: List[AccountCollected],
        core_args: List[str],
        logging_context: Dict[str, str],
        push_gateway_url: Optional[str] = None,
    ) -> None:
        super().__init__(redis=redis, job_id=job_id, tenant_id=tenant_id, push_gateway_url=push_gateway_url)
        self.accounts_collected = accounts_collected
        self.core_args = core_args
        self.logging_context = logging_context

    @staticmethod
    def load_metrics() -> List[MetricQuery]:
        with open(Path(__file__).parent / "metrics.yaml", "r") as f:
            yml = yaml.safe_load(f)
            return [MetricQuery.from_json(k, v) for k, v in yml.items() if "search" in v]

    async def send_result_events(self, exception: Optional[Exception] = None) -> None:
        # send a collect done event for the tenant
        await self.collect_done_publisher.publish(
            "post-collect-done",
            {
                "job_id": self.job_id,
                "tenant_id": self.tenant_id,
                "started_at": utc_str(self.started_at),
                "duration": int((utc() - self.started_at).total_seconds()),
                "success": exception is None,
                "exception": str(exception) if exception else None,
            },
        )

    async def create_timeseries(self) -> None:
        # create metrics
        account_ids = [ac.account_id for ac in self.accounts_collected]
        for metric in self.load_metrics():
            on_accounts = account_ids if metric.only_on_collected_accounts else None
            res = await self.core_client.timeseries_snapshot(metric, on_accounts)
            if res:
                log.info(f"Created timeseries snapshot: {metric.name} created {res} entries")
        # downsample all timeseries
        ds = await self.core_client.timeseries_downsample()
        log.info(f"Sampled down all timeseries. Result: {ds}")

    async def merge_deferred_edges(self) -> None:
        await self.core_client.merge_deferred_edges([ac.task_id for ac in self.accounts_collected])

    async def security_report(self) -> None:
        for acc in self.accounts_collected:
            benchmarks = await self.core_client.list_benchmarks(providers=[acc.cloud])
            if benchmarks:
                await self.core_client.create_benchmark_reports([acc.account_id], benchmarks, acc.task_id)

    async def sync(self) -> None:
        try:
            if self.accounts_collected:  # Don't do anything if no accounts were collected
                aids = ", ".join([ac.account_id for ac in self.accounts_collected])
                log.info(f"Sync tenant {self.tenant_id}: with {len(self.accounts_collected)} accounts: {aids}.")
                async with ProcessWrapper(["fixcore", *self.core_args], self.logging_context):
                    log.info("Core started.")
                    await asyncio.wait_for(self.core_client.wait_connected(), timeout=60)
                    log.info("Core Client connected.")
                    await self.merge_deferred_edges()
                    log.info("All deferred edges have been updated.")
                    await self.security_report()
                    log.info("Security reports have been synchronized.")
                    sleep(10)  # wait for the view to become ready
                    await self.create_timeseries()
                    log.info("Time series have been updated.")
            await asyncio.wait_for(self.send_result_events(), 600)  # wait up to 10 minutes
        except Exception as ex:
            log.info(f"Got Exception during sync: {ex}")
            await asyncio.wait_for(self.send_result_events(ex), 600)  # wait up to 10 minutes


async def startup(args: Namespace, core_args: List[str], logging_context: Dict[str, str]) -> None:
    redis_args = {}
    if args.redis_password:
        redis_args["password"] = args.redis_password
    if args.redis_url.startswith("rediss://") and args.ca_cert:
        redis_args["ssl_ca_certs"] = args.ca_cert
    async with Redis.from_url(args.redis_url, decode_responses=True, **redis_args) as redis:
        async with PostCollect(
            redis=redis,
            tenant_id=args.tenant_id,
            job_id=args.job_id,
            core_args=core_args,
            accounts_collected=args.accounts_collected,
            logging_context=logging_context,
            push_gateway_url=args.push_gateway_url,
        ) as post_collect:
            await post_collect.sync()


@define
class AccountCollected:
    cloud: str
    account_id: str
    task_id: str

    @staticmethod
    def from_string(json_str: str) -> List[AccountCollected]:
        return cattrs.structure(json.loads(json_str), List[AccountCollected])


def main() -> None:
    args = iter(sys.argv[1:])
    post_process_args = list(takewhile(lambda x: x != "---", args))
    core_args = list(takewhile(lambda x: x != "---", args))
    parser = ArgumentParser()
    parser.add_argument("--job-id", required=True, help="Job Id of the coordinator")
    parser.add_argument("--tenant-id", required=True, help="Id of the tenant")
    parser.add_argument(
        "--accounts-collected", default=os.environ.get("ACCOUNTS_COLLECTED"), type=AccountCollected.from_string
    )
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis host.")
    parser.add_argument("--redis-password", default=os.environ.get("REDIS_PASSWORD"), help="Redis password")
    parser.add_argument("--push-gateway-url", help="Prometheus push gateway url")
    parser.add_argument("--ca-cert", help="Path to CA cert file")
    parsed = parser.parse_args(post_process_args)

    # setup logging
    logging_context = dict(job_id=parsed.job_id, workspace_id=parsed.tenant_id)
    setup_logger("post-collect", get_logging_context=lambda: {"process": "post-collect", **logging_context})
    asyncio.run(startup(parsed, core_args, logging_context))


if __name__ == "__main__":
    main()
