from pytest import fixture
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff


@fixture
def redis() -> Redis:
    backoff = ExponentialBackoff()  # type: ignore
    return Redis(host="localhost", port=6379, decode_responses=True, retry=Retry(backoff, 10))
