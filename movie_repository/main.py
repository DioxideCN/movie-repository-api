# main.py
import asyncio

import uvicorn
from fastapi import FastAPI

from infra.initializer_app import init_app
from router.fetch_router import router as fetch_router

# 启动FastAPI应用
app = FastAPI()

# 组合路由
app.include_router(fetch_router, prefix='/fetch', tags=['fetch'])


# 使用startup事件来运行异步初始化任务
@app.on_event("startup")
async def startup_event():
    await init_app()
