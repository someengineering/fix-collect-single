import logging
from abc import ABC
from typing import Optional, Any

import prometheus_client
from fixcloudutils.redis.event_stream import RedisStreamPublisher
from fixcloudutils.redis.pub_sub import RedisPubSubPublisher
from fixcloudutils.service import Service
from fixcloudutils.util import utc
from redis.asyncio.client import Redis

from collect_single.core_client import CoreClient

log = logging.getLogger("fix.coordinator")


class Job(Service, ABC):
    def __init__(
        self,
        *,
        redis: Redis,
        job_id: str,
        tenant_id: str,
        push_gateway_url: Optional[str] = None,
        core_url: str = "http://localhost:8980",
    ) -> None:
        self.redis = redis
        self.job_id = job_id
        self.tenant_id = tenant_id
        self.push_gateway_url = push_gateway_url
        self.core_client = CoreClient(core_url)
        publisher = "collect-and-sync"
        self.progress_update_publisher = RedisPubSubPublisher(redis, f"tenant-events::{tenant_id}", publisher)
        self.collect_done_publisher = RedisStreamPublisher(redis, "collect-events", publisher)
        self.started_at = utc()

    async def start(self) -> Any:
        await self.progress_update_publisher.start()
        await self.collect_done_publisher.start()
        # note: the client is not started (core is not running and no certificate required)

    async def stop(self) -> None:
        await self.progress_update_publisher.stop()
        await self.collect_done_publisher.stop()
        await self.core_client.stop()

    async def push_metrics(self) -> None:
        if gateway := self.push_gateway_url:
            # Possible future option: retrieve metrics from core and worker and push them to prometheus
            prometheus_client.push_to_gateway(
                gateway=gateway, job="collect_single", registry=prometheus_client.REGISTRY
            )
            log.info("Metrics pushed to gateway")
