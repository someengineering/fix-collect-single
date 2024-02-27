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
import json
import logging
from asyncio import Future, streams
from asyncio.subprocess import Process
from contextlib import suppress
from signal import SIGKILL
from typing import List, Optional, Any, Dict

from fixcloudutils.service import Service
from fixcloudutils.logging.prometheus_counter import LogRecordCounter
from fixlib.proc import kill_children

log = logging.getLogger("fix.coordinator")


class ProcessWrapper(Service):
    def __init__(self, cmd: List[str], logging_context: Dict[str, str]) -> None:
        self.cmd = cmd
        self.logging_context = logging_context
        self.process: Optional[Process] = None
        self.reader: Optional[Future[Any]] = None

    async def read_stream(self, stream: streams.StreamReader) -> None:
        while line := await stream.readline():
            try:
                # try handling json
                log_js = json.loads(line)
                if level := log_js.get("level"):
                    LogRecordCounter.labels(component="collect-single", level=level).inc()
                log_js.update(self.logging_context)
                print(json.dumps(log_js))
            except Exception:
                # if we come here, something went wrong: print the line as is
                print(line.decode("utf-8").strip())

    async def start(self) -> None:
        process = await asyncio.create_subprocess_exec(
            *self.cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        self.process = process
        self.reader = asyncio.gather(
            asyncio.create_task(self.read_stream(process.stdout)),  # type: ignore
            asyncio.create_task(self.read_stream(process.stderr)),  # type: ignore
        )

    async def stop(self) -> None:
        try:
            logging.disable(logging.CRITICAL)  # ignore log messages during shutdown
            if self.reader:
                self.reader.cancel()
                with suppress(asyncio.CancelledError):
                    await self.reader
            if self.process:
                self.process.terminate()
                await asyncio.sleep(1)
                if self.process.returncode is not None:
                    with suppress(Exception):
                        kill_children(SIGKILL, process_pid=self.process.pid)
        finally:
            logging.disable(logging.NOTSET)
            self.process = None
            self.reader = None
