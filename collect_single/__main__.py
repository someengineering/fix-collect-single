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
import logging
import os
import sys
from argparse import Namespace, ArgumentParser
from itertools import takewhile
from pathlib import Path
from typing import List, Tuple, Dict

from redis.asyncio import Redis

from collect_single.collect_and_sync import CollectAndSync
from fixcloudutils.logging import setup_logger

log = logging.getLogger("resoto.coordinator")


def kv_pairs(s: str) -> Tuple[str, str]:
    return tuple(s.split("=", maxsplit=1))  # type: ignore


async def startup(
    args: Namespace, core_args: List[str], worker_args: List[str], logging_context: Dict[str, str]
) -> None:
    redis_args = {}
    if args.redis_password:
        redis_args["password"] = args.redis_password
    if args.redis_url.startswith("rediss://") and args.ca_cert:
        redis_args["ssl_ca_certs"] = args.ca_cert
    async with Redis.from_url(args.redis_url, decode_responses=True, **redis_args) as redis:
        collect_and_sync = CollectAndSync(
            redis=redis,
            tenant_id=args.tenant_id,
            account_id=args.account_id,
            job_id=args.job_id,
            core_args=core_args,
            worker_args=worker_args,
            push_gateway_url=args.push_gateway_url,
            logging_context=logging_context,
        )
        await collect_and_sync.sync()


def main() -> None:
    # 3 argument sets delimited by "---": <coordinator args> --- <core args> --- <worker args>
    # coordinator --main-arg1 --main-arg2 --- --core-arg1 --core-arg2 --- --worker-arg1 --worker-arg2
    args = iter(sys.argv[1:])
    coordinator_args = list(takewhile(lambda x: x != "---", args))
    core_args = list(takewhile(lambda x: x != "---", args))
    worker_args = list(args)
    # handle coordinator args
    parser = ArgumentParser()
    parser.add_argument(
        "--write",
        type=kv_pairs,
        help="Write config files in home dir from env vars. Format: --write path/in/home/dir=env-var-name",
        default=[],
        action="append",
    )
    parser.add_argument("--job-id", required=True, help="Job Id of the coordinator")
    parser.add_argument("--tenant-id", required=True, help="Id of the tenant")
    parser.add_argument("--account-id", help="Id of the account")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis host.")
    parser.add_argument("--redis-password", default=os.environ.get("REDIS_PASSWORD"), help="Redis password")
    parser.add_argument("--push-gateway-url", help="Prometheus push gateway url")
    parser.add_argument("--ca-cert", help="Path to CA cert file")
    parsed = parser.parse_args(coordinator_args)

    # setup logging
    logging_context = dict(job_id=parsed.job_id, workspace_id=parsed.tenant_id, cloud_account_id=parsed.account_id)
    setup_logger("collect-single", get_logging_context=lambda: {"process": "coordinator", **logging_context})

    # write config files from env vars
    env_vars = {k.lower(): v for k, v in os.environ.items()}
    for home_path, env_var_name in parsed.write:
        path = (Path.home() / Path(home_path)).absolute()
        content = env_vars.get(env_var_name.lower())
        assert content is not None, f"Env var {env_var_name} not found"
        log.info(f"Writing file: {path} from env var: {env_var_name}")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w+") as f:
            f.write(content)

    log.info(f"Coordinator args:({coordinator_args}) Core args:({core_args}) Worker args:({worker_args})")
    asyncio.run(startup(parsed, core_args, worker_args, logging_context))


if __name__ == "__main__":
    main()
