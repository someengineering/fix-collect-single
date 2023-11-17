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
import json

import pytest
from resotoclient.async_client import ResotoClient
from redis.asyncio import Redis

from collect_single.collect_and_sync import CollectAndSync


@pytest.mark.asyncio
@pytest.mark.skip(reason="Only for manual testing")
async def test_client(core_client: ResotoClient) -> None:
    benchmark_results = {}
    benchmarks = ["aws_test"]
    for benchmark in benchmarks:
        report = [
            n
            async for n in core_client.cli_execute(
                f"report benchmark run {benchmark} | dump", headers={"Accept": "application/json"}
            )
        ]
        benchmark_results[benchmark] = report[0]
    print(json.dumps(benchmark_results, indent=2))


@pytest.mark.asyncio
@pytest.mark.skip(reason="Only for manual testing")
async def test_collect_and_sync(redis: Redis, core_client: ResotoClient) -> None:
    cs = CollectAndSync(
        redis=redis, tenant_id="tenant_id", account_id="account_id", job_id="job_id", core_args=[], worker_args=[]
    )
    await cs.send_result_events(True)


def test_true() -> None:
    assert True
