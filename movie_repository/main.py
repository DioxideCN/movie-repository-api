# main.py
from fastapi import FastAPI

from infra.initializer_app import init_app
from router.fetch_router import router as fetch_router

# 初始化应用配置
init_app()

# 启动FastAPI应用
app = FastAPI()


# 组合所有路由
app.include_router(fetch_router, prefix='/fetch', tags=['fetch'])
