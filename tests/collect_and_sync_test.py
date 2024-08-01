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
from fixclient.async_client import FixInventoryClient
from fixcore.query import query_parser

from collect_single.collect_single import CollectSingle
from collect_single.post_collect import PostCollect


@pytest.mark.asyncio
@pytest.mark.skip(reason="Only for manual testing")
async def test_client(core_client: FixInventoryClient) -> None:
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
async def test_collect_and_sync(collect_and_sync: CollectSingle) -> None:
    await collect_and_sync.send_result_events(True)


@pytest.mark.asyncio
@pytest.mark.skip(reason="Only for manual testing")
async def test_migrate_ts(collect_and_sync: CollectSingle) -> None:
    await collect_and_sync.migrate_ts_data()


def test_load_metrics() -> None:
    metrics = PostCollect.load_metrics()
    assert len(metrics) >= 14
    for query in metrics:
        # make sure the query parser does not explode
        query_parser.parse_query(query.search)
