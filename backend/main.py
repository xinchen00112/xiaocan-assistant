"""
小蚕小帮手 (XiaoCan Assistant)
基于 FastAPI + APScheduler + 逆向 RPC 协议驱动
"""
import os
import sys

# 排除宿主机或外部代理工具注入的 SOCKS 代理环境变量污染，保证本地服务与国内接口纯净直连
for proxy_k in ["ALL_PROXY", "all_proxy"]:
    if proxy_k in os.environ and "socks" in os.environ[proxy_k].lower():
        os.environ.pop(proxy_k, None)

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.endpoints import router as api_router
from app.api.user_endpoints import router_user as user_api_router
from app.core.scheduler import start_scheduler, shutdown_scheduler
from app.core.auth import seed_jwt_secret_if_needed
from app.models.database import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("xiaocan.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("正在启动小蚕助手系统...")
    init_db()
    seed_jwt_secret_if_needed()
    start_scheduler()
    yield
    logger.info("正在关闭小蚕助手系统...")
    shutdown_scheduler()


app = FastAPI(
    title="小蚕小帮手",
    description="自动化挂机、秒杀与抢单小帮手",
    version="1.0.0",
    lifespan=lifespan
)

# 允许跨域请求 (支持前端 Vite 开发服务器 http://localhost:5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载 API 路由
app.include_router(api_router)
app.include_router(user_api_router)


from fastapi.responses import RedirectResponse

@app.get("/health")
async def health_check():
    return {"status": "ok", "app": "xiaocan-assistant", "version": "2.0.0", "license": "free"}


@app.get("/m/bind")
async def root_m_bind(ticket: str = ""):
    return RedirectResponse(f"/api/m/bind?ticket={ticket}")


# 先挂载更具体的路径，避免被 / catch-all 吞掉
SLIM_DIST = os.path.join(os.path.dirname(os.path.dirname(__file__)), "slim-frontend-dist")
if os.path.exists(SLIM_DIST):
    app.mount("/slim", StaticFiles(directory=SLIM_DIST, html=True), name="slim-frontend")

# 主前端：完整管理后台 (原 xiaocan-assistant 前端)
FRONTEND_DIST = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "dist")
if os.path.exists(FRONTEND_DIST):
    app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    # run.bat 设置 XIAOCAN_RELOAD=0 以生产模式运行；dev.bat 设置为 1 启用代码热重载
    reload_enabled = os.environ.get("XIAOCAN_RELOAD", "1") != "0"
    logger.info(f"启动本地 Web 服务: http://0.0.0.0:8690 (热重载: {'开启' if reload_enabled else '关闭'})")
    uvicorn.run("main:app", host="0.0.0.0", port=8690, reload=reload_enabled)
