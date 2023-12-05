from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Optional, Dict, List

from fixcloudutils.types import Json
from resotoclient import Subscriber
from resotoclient.async_client import ResotoClient

log = logging.getLogger("resoto.coordinator")


class CoreClient:
    def __init__(self, url: str) -> None:
        self.client = ResotoClient(url)

    async def __aenter__(self) -> None:
        await self.client.start()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
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

    async def list_benchmarks(self) -> List[str]:
        return [cfg async for cfg in self.client.cli_execute("report benchmark list")]

    async def create_benchmark_reports(
        self, account_ids: List[str], benchmarks: List[str], task_id: Optional[str]
    ) -> None:
        assert isinstance(account_ids, List), "account_ids must be a collection"
        bn = " ".join(benchmarks)
        ac = " ".join(account_ids)
        run_id = task_id or str(uuid.uuid4())
        command = f"report benchmark run {bn} --accounts {ac} --sync-security-section --run-id {run_id} | count"
        log.info(f"Create reports for following benchmarks: {bn} for accounts: {ac}. Command: {command}")
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
