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
from resotoclient.async_client import ResotoClient


@fixture
async def core_client() -> AsyncIterator[ResotoClient]:
    client = ResotoClient("http://localhost:8980")
    flag = True
    while flag:
        try:
            await client.ping()
            flag = False
            yield client
        except Exception:
            await asyncio.sleep(1)


# @mark.asyncio
# async def test_client(core_client: ResotoClient) -> None:
#     configs = [
#         cfg.split("resoto.report.benchmark.", maxsplit=1)[-1]
#         async for cfg in core_client.cli_execute("configs list")
#         if cfg.startswith("resoto.report.benchmark.")
#     ]
#     print(configs)


def test_true() -> None:
    assert True
