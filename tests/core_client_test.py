import asyncio
import os
import uuid
from typing import AsyncIterator

import pytest

from collect_single.core_client import CoreClient
from collect_single.post_collect import PostCollect


@pytest.fixture
async def core_client() -> AsyncIterator[CoreClient]:
    async with CoreClient("http://localhost:8980") as client:
        yield client


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_wait_connected(core_client: CoreClient) -> None:
    await asyncio.wait_for(core_client.wait_connected(), timeout=1)


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_wait_account_info(core_client: CoreClient) -> None:
    infos = await core_client.account_info(since="1s")
    assert len(infos) == 0
    infos = await core_client.account_info(since="5y")
    assert len(infos) > 0
    account_id = next(iter(infos))
    infos = await core_client.account_info(account_id=account_id, since="5y")
    assert len(infos) > 0


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_workflows_running(core_client: CoreClient) -> None:
    cleanup = await core_client.workflows_running("cleanup")
    assert len(cleanup) == 0
    await core_client.start_workflow("cleanup")
    cleanup = await core_client.workflows_running("cleanup")
    assert len(cleanup) == 1
    task_id = cleanup[0]["task-id"]
    log = await core_client.workflow_log(task_id)
    assert len(log) == 0


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_benchmarks(core_client: CoreClient) -> None:
    assert "aws_cis_1_5" in await core_client.list_benchmarks()


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_create_benchmark_report(core_client: CoreClient) -> None:
    accounts = [a async for a in core_client.client.search_list("is(aws_account) limit 1")]
    single = accounts[0]["reported"]["id"]
    task_id = str(uuid.uuid4())
    await core_client.create_benchmark_reports([single], ["aws_cis_1_5"], task_id)
    res = [
        a
        async for a in core_client.client.cli_execute(
            f'search account.id=="{single}" and /security.has_issues==true and /security.run_id=="{task_id}" | '
            f"aggregate sum(1) as count | dump"
        )
    ]
    assert res[0]["count"] > 10


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_wait_for_collect_task_to_finish(core_client: CoreClient) -> None:
    await core_client.start_workflow("collect")
    await asyncio.wait_for(core_client.wait_for_collect_tasks_to_finish(), timeout=600)


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_timeseries_snapshot(core_client: CoreClient) -> None:
    accounts = [a async for a in core_client.client.search_list("is(aws_account) limit 1")]
    single = accounts[0]["reported"]["id"]
    for query in PostCollect.load_metrics():
        res = await core_client.timeseries_snapshot(query, single)
        assert res >= 0


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_timeseries_downsample(core_client: CoreClient) -> None:
    result = await core_client.timeseries_downsample()
    assert result


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_list_graphs(core_client: CoreClient) -> None:
    result = await core_client.graphs()
    assert isinstance(result, set)
    for i in result:
        assert isinstance(i, str)


@pytest.mark.skipif(os.environ.get("CORE_RUNNING") is None, reason="No core running")
async def test_account_by_id(core_client: CoreClient) -> None:
    result = await core_client.account_id_by_name()
    assert isinstance(result, dict)
    assert len(result) > 0
    for k, v in result.items():
        assert isinstance(k, str)
        assert isinstance(v, str)
