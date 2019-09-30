import json
import asyncio
from os import environ
from functools import partial

from aiohttp import ClientSession, ClientConnectionError
from pyee import AsyncIOEventEmitter
from aiohttp_sse_client.client import EventSource

DEFAULT_STREAM_URL = 'https://stream.flowdock.com/flows'
__all__ = ["EventStream"]


class EventStream(AsyncIOEventEmitter):
    def __init__(self, auth, flows, url=None, session=None, params=None, loop=None):
        super().__init__(loop or asyncio.get_event_loop())
        self._evt = None
        self.auth = auth
        self.flows = flows
        self.params = params or dict()
        self.session = session or ClientSession()
        self.url = url or environ.get("FLOWDOCK_STREAM_URL", DEFAULT_STREAM_URL)

    async def connect(self, retry=3):
        if self._evt is not None:
            return
        self._evt = EventSource(self.url, session=self.session,
                                timeout=-1,
                                on_open=partial(self.emit, 'connected'),
                                on_error=partial(self.emit, 'error'),
                                **self._options())
        retry = 0 if retry < 0 else retry
        await self._evt.connect(retry)

        async def _process_data(event_source, emit, loop):
            try:
                async for evt in event_source:
                    emit("rawdata", evt)
                    msg = await loop.run_in_executor(None, json.loads, evt.data)
                    emit("message", msg)
            except ClientConnectionError as e:
                emit("disconnected", e)
            except Exception as e:
                emit("clientError", e)

        coro = _process_data(self._evt, self.emit, self._loop)
        self._loop.create_task(coro)

    async def end(self):
        if self._evt is not None:
            await self._evt.close()
            self._evt = None

    def _options(self):
        qs = dict(filter=",".join(self.flows))
        qs.update(self.params)
        options = {
            "params": qs,
            "headers": {
                "Authorization": self.auth
            }
        }

        return options
