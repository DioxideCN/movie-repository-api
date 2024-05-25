from contextlib import asynccontextmanager

from fastapi import FastAPI

from util import logger
from infra.init_app import init_app
from router.fetch_router import router as fetch_router


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 初始化
    await init_app()
    logger.info('Starting fast api server...')
    # 组合路由
    _app.include_router(fetch_router, prefix='/fetch', tags=['fetch'])
    yield

# 启动FastAPI应用
app = FastAPI(lifespan=lifespan)
