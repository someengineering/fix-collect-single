from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Optional, Dict, List, Set, cast

from fixclient import Subscriber
from fixclient.async_client import FixInventoryClient
from fixcloudutils.service import Service
from fixcloudutils.types import Json, JsonElement
from fixcore.query import query_parser, Query
from fixcore.query.model import P
from fixlib.json import value_in_path

from collect_single.model import MetricQuery

log = logging.getLogger("fix.coordinator")


class CoreClient(Service):
    def __init__(self, url: str) -> None:
        self.client = FixInventoryClient(url)

    async def start(self) -> None:
        await self.client.start()

    async def stop(self) -> None:
        await self.client.shutdown()

    async def wait_connected(self) -> CoreClient:
        while True:
            try:
                await self.client.ping()
                return self
            except Exception:
                await asyncio.sleep(1)

    async def account_info(self, account_id: Optional[str] = None, since: str = "1h") -> Dict[str, Json]:
        account_filter = f" and id=={account_id}" if account_id else ""
        return {
            info["id"]: info
            async for info in self.client.cli_execute(
                f"search is(account){account_filter} and /metadata.exported_age<{since} | "
                "jq {cloud: ./ancestors.cloud.reported.name, id:.id, name: .name, "
                "exported_at: ./metadata.exported_at, summary: ./metadata.descendant_summary}"
            )
        }

    async def workflows_running(self, name: str) -> List[Json]:
        return [entry async for entry in self.client.cli_execute("workflows running") if entry.get("workflow") == name]

    async def start_workflow(self, name: str) -> None:
        async for _ in self.client.cli_execute(f"workflow run {name}"):
            pass

    async def workflow_log(self, task_id: str, limit: int = 100) -> List[str]:
        return [
            info
            async for info in self.client.cli_execute(f"workflows log {task_id} | head {limit}")
            if info != "No error messages for this run."
        ]

    async def list_benchmarks(self, *, providers: Optional[List[str]] = None) -> List[str]:
        params = dict(ids_only="true")
        if providers:
            params["providers"] = " ".join(providers)
        response = await self.client._get("/report/benchmarks", params)
        if response.status_code == 200:
            return cast(List[str], await response.json())
        else:
            raise AttributeError(await response.text())

    async def create_benchmark_reports(
        self, account_ids: List[str], benchmarks: List[str], run_id: Optional[str]
    ) -> None:
        bn = " ".join(benchmarks)
        an = " ".join(account_ids)
        rid = run_id or str(uuid.uuid4())
        command = f"report benchmark run {bn} --accounts {an} --sync-security-section --run-id {rid} | count"
        log.info(f"Create reports for following benchmarks: {bn} for accounts: {an}. Command: {command}")
        async for _ in self.client.cli_execute(command, headers={"Accept": "application/json"}):
            pass  # ignore the result

    async def wait_for_collect_tasks_to_finish(self) -> None:
        while True:
            running = [
                entry
                async for entry in self.client.cli_execute("workflows running")
                if entry.get("workflow") != "collect"
            ]
            if len(running) == 0:
                return
            else:
                log.info(f"Wait for running workflows to finish. Running: {running}")
                await asyncio.sleep(5)

    async def wait_for_worker_subscribed(self) -> List[Subscriber]:
        while True:
            res = await self.client.subscribers_for_event("collect")
            if len(res) > 0:
                log.info(f"Found subscribers for collect event: {res}. Wait for worker to connect.")
                return res  # type: ignore
            log.info("Wait for worker to connect.")
            await asyncio.sleep(1)

    async def timeseries_snapshot(self, metric: MetricQuery, account_ids: Optional[List[str]] = None) -> int:
        # create query
        query = query_parser.parse_query(metric.search)
        if account_ids:
            query = query.combine(Query.by(P("/ancestors.account.reported.id").is_in(account_ids)))
        # create command
        command = f"timeseries snapshot --name {metric.name} "
        if metric.factor:
            command += f"--avg-factor {metric.factor} "
        async for single in self.client.cli_execute(f"{command} {query}"):
            try:
                first, rest = single.split(" ", maxsplit=1)
                return int(first)
            except Exception:
                log.error(f"Failed to parse timeseries snapshot result: {single}")
        return 0

    async def timeseries_downsample(self) -> List[JsonElement]:
        return [s async for s in self.client.cli_execute("timeseries downsample")]

    async def graphs(self) -> Set[str]:
        return {g async for g in self.client.cli_execute("graph list")}

    async def copy_graph(self, from_graph: str, to_graph: str, *, force: bool = False) -> None:
        cmd = f"graph copy {from_graph} {to_graph}"
        if force:
            cmd += " --force"
        async for _ in self.client.cli_execute(cmd):
            pass

    async def delete_graph(self, graph: str) -> None:
        async for _ in self.client.cli_execute(f"graph delete {graph}"):
            pass

    async def account_id_by_name(self) -> Dict[Optional[str], Optional[str]]:
        return {
            value_in_path(r, "reported.name"): value_in_path(r, "reported.id")
            async for r in self.client.cli_execute("search is(account) | dump")
        }

    async def merge_deferred_edges(self, task_ids: List[str], *, graph: str = "fix") -> None:
        response = await self.client._post(f"/graph/{graph}/merge/deferred_edges", json=task_ids)
        if response.status_code == 200:
            return await response.json()  # type: ignore
        else:
            raise AttributeError(await response.text())
