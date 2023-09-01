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
from asyncio import Future, streams
from asyncio.subprocess import Process
from contextlib import suppress
from signal import SIGKILL
from typing import List, Optional, Any

from fixcloudutils.service import Service
from resotolib.proc import kill_children

log = logging.getLogger("resoto.coordinator")


class ProcessWrapper(Service):
    def __init__(self, cmd: List[str]) -> None:
        self.cmd = cmd
        self.process: Optional[Process] = None
        self.reader: Optional[Future[Any]] = None

    async def read_stream(self, stream: streams.StreamReader) -> None:
        while True:
            line = await stream.readline()
            if line:
                print(line.decode("utf-8").strip())
            else:
                await asyncio.sleep(0.1)

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
