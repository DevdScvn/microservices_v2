import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from redis.asyncio import Redis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import db_helper
from settings.config import settings

from gateway.router import router as gateway_router
from gateway.ws_router import router as ws_router

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db_redis.cache,
    )

    yield

    log.warning("dispose_engine")
    await db_helper.dispose()
    await redis.close()

main_app = FastAPI(
    lifespan=lifespan,
)


# origins = [
#     "http://0.0.0.0:8000",
#     "http://localhost:8000"
#
# ]

main_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

main_app.include_router(gateway_router)
main_app.include_router(ws_router)

from starlette.types import Scope, Receive, Send
from starlette.websockets import WebSocket as StarletteWebSocket, WebSocket


# Кастомная реализация WebSocket для обхода CORS
class CustomWebSocket(StarletteWebSocket):
    async def accept(self, subprotocol=None, headers=None):
        # Всегда принимаем WebSocket соединение
        await super().accept(subprotocol=subprotocol, headers=headers)

@main_app.websocket("/ws/test")
async def test_websocket(websocket: WebSocket):
    custom_ws = CustomWebSocket(websocket.scope, websocket.receive, websocket.send)
    await custom_ws.accept()

    await custom_ws.send_text("✅ Test WebSocket connection successful!")

    for i in range(5):
        await asyncio.sleep(1)
        await custom_ws.send_text(f"Test message {i + 1}")

    await custom_ws.send_text("Test completed")
    await custom_ws.close()

if __name__ == "__main__":
    uvicorn.run("api_gateway.main:main_app",
                host=settings.run.host,
                port=settings.run.port,
                reload=True)
