import json
import asyncio
from os import environ
from base64 import b64encode
from functools import partial

from aiohttp import ClientSession, ClientConnectionError
from pyee import AsyncIOEventEmitter
from aiohttp_sse_client.client import EventSource

DEFAULT_URL = "https://api.flowdock.com"
DEFAULT_STREAM_URL = 'https://stream.flowdock.com/flows'


class Session:
    def __init__(self, email, password="", url="", session=None):
        self.email = email
        self.password = password
        self.session = session or ClientSession()
        self.url = url or environ.get("FLOWDOCK_API_URL", DEFAULT_URL)
        secret = ("%s:%s" % (email, password)).encode()
        self.auth = "Basic " + b64encode(secret).decode()

    def stream(self, flows, options=None):
        options = options or dict()
        flows = [flows] if not isinstance(flows, list) else flows
        stream = EventStream(self.auth, flows, session=self.session, params=options)
        return stream

    async def flows(self):
        return await self.get('/flows', dict(users=1))

    async def send(self, path, message):
        return await self.post(path, message)

    async def message(self, flow_id, msg, tags=None):
        tags = tags or list()
        data = dict(flow=flow_id, event='message', content=msg, tags=tags)
        return await self.send('/messages', data)

    async def thread_message(self, flow_id, thread_id, msg, tags=None):
        tags = tags or list()
        data = dict(flow_id, thread_id, event='message', content=msg, tags=tags)
        return await self.send("/messages", data)

    async def comment(self, flow_id, parent_id, comment, tags=None):
        data = {
            "event": "comment",
            "flow": flow_id,
            "message": parent_id,
            "content": comment,
            "tags": tags or list()
        }
        return await self.send("/comments", data)

    async def private_message(self, user_id, msg, tags=None):
        data = dict(event='message', content=msg, tags=tags or list())
        return await self.send("/private/%s/messages" % user_id, data)

    async def status(self, flow_id, status):
        data = dict(event='status', content=status, flow=flow_id)
        return await self.send("/messages", data)

    async def invite(self, flow_id, org_id, email, msg):
        data = dict(email=email, message=msg)
        path = "/flows/%s/%s/invitations" % (org_id, flow_id)
        return await self.send(path, data)

    async def edit_message(self, flow_id, org_id, msg_id, data):
        path = "/flows/{}/{}/message/{}".format(org_id, flow_id, msg_id)
        return await self.put(path, data)

    async def post(self, path, data):
        return await self.request("post", path, data)

    async def get(self, path, data):
        return await self.request("get", path, data)

    async def put(self, path, data):
        return await self.request("put", path, data)

    async def delete(self, path):
        return await self.request("delete", path)

    async def request(self, method, path, data=None):
        data = data or dict()
        url = "/".join([self.url, path])
        header = {
            "Authorization": self.auth,
            "Accept": "application/json",
            "Content-Type": 'application/json'
        }

        options = dict(headers=header)
        if method.lower() == 'get':
            options.update(params=data)
        else:
            options.update(json=data)

        return await self.session.request(method, url, **options)


class EventStream(AsyncIOEventEmitter):
    def __init__(self, auth, flows, url=None, session=None, loop=None, params=None):
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
                                on_open=partial(self.emit, 'connected', self),
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
