import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from infra.init_app import init_app
from router.fetch_router import router as fetch_router
from util import logger


# 使 FastAPI 使用你的日志记录器
logging.getLogger("uvicorn.access").handlers = logger.handlers
logging.getLogger("uvicorn.error").handlers = logger.handlers
# 使 FastAPI 的日志级别与自定义日志记录器一致
logging.getLogger("uvicorn.access").setLevel(logger.level)
logging.getLogger("uvicorn.error").setLevel(logger.level)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 初始化
    await init_app()
    logger.info(f'Starting Movie Repository API 0.0.1-SNAPSHOT FastAPI server on port(s): 8000.')
    # 组合路由
    _app.include_router(fetch_router, prefix='/fetch')
    yield

# 启动FastAPI应用
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
