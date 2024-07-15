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
from typing import AsyncIterator

from pytest import fixture
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from fixclient.async_client import FixInventoryClient

from collect_single.collect_single import CollectSingle


@fixture
def redis() -> Redis:  # type: ignore
    backoff = ExponentialBackoff()
    return Redis(host="localhost", port=6379, decode_responses=True, retry=Retry(backoff, 10))


@fixture
async def core_client() -> AsyncIterator[FixInventoryClient]:
    client = FixInventoryClient("http://localhost:8980")
    flag = True
    while flag:
        print("Trying to connect to core...")
        try:
            await client.ping()
            flag = False
            yield client
        except Exception:
            await asyncio.sleep(1)


@fixture
async def collect_and_sync(redis: Redis) -> AsyncIterator[CollectSingle]:  # type: ignore
    async with CollectSingle(
        redis=redis,
        tenant_id="tenant_id",
        cloud="aws",
        account_id="account_id",
        job_id="job_id",
        core_args=[],
        worker_args=[],
        logging_context={},
    ) as collect_and_sync:
        yield collect_and_sync
