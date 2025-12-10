import logging
from contextlib import asynccontextmanager

import uvicorn
from redis.asyncio import Redis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import db_helper
from settings.config import settings

from gateway.router import router as gateway_router


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
    allow_origins=["*"],  # <- все домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

main_app.include_router(gateway_router)

if __name__ == "__main__":
    uvicorn.run("api_gateway.main:main_app",
                host=settings.run.host,
                port=settings.run.port,
                reload=True)
