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
