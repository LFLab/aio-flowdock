from os import environ
from base64 import b64encode
from asyncio import get_event_loop

from aiohttp import ClientSession
from pyee import AsyncIOEventEmitter

from .stream import EventStream

DEFAULT_URL = "https://api.flowdock.com"
__all__ = ["Session"]


class Session(AsyncIOEventEmitter):
    def __init__(self, email, password="", url="", session=None, loop=None):
        super().__init__(loop or get_event_loop())
        self.email = email
        self.password = password
        self._session = session
        self.url = url or environ.get("FLOWDOCK_API_URL", DEFAULT_URL)
        secret = ("%s:%s" % (email, password)).encode()
        self.auth = "Basic " + b64encode(secret).decode()
        
    @property
    def session(self):
        if self._session is None:
            self._session = ClientSession()
        return self._session

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
        data = dict(flow=flow_id, thread_id=thread_id, event='message',
                    content=msg, tags=tags)
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

        try:
            resp = await self.session.request(method, url, **options)
            if resp.status >= 300:
                raise ValueError("[%s] %s" % (resp.status, await resp.text()))
            return await resp.json(), resp
        except Exception as e:
            self.emit("error", e)
            return e
