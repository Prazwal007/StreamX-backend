from typing import Set
from fastapi import WebSocket
import asyncio
import json


class WSManager:
    def __init__(self):
        self.connections: Set[WebSocket] = set()
        self.lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self.lock:
            self.connections.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self.lock:
            self.connections.discard(ws)

    async def broadcast(self, payload: dict):
        message = json.dumps(payload)
        dead = []

        async with self.lock:
            for ws in self.connections:
                try:
                    await ws.send_text(message)
                except Exception:
                    dead.append(ws)

            for ws in dead:
                self.connections.discard(ws)
