"""
精简前端用户登录路由
POST /api/user/login — 用户名密码登录，返回 JWT
GET  /api/user/me    — 验证当前 JWT 并返回用户信息
"""
import logging
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Body, Depends

from ..core.auth import (
    hash_password,
    verify_password,
    create_user_jwt,
    decode_user_jwt,
    get_current_user,
)
from ..models import users as users_db

logger = logging.getLogger("xiaocan.user-api")

router_user = APIRouter(prefix="/api/user")


@router_user.post("/login")
async def user_login(data: Dict[str, Any] = Body(...)):
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        raise HTTPException(400, "用户名和密码不能为空")

    user = users_db.get_user_by_username(username)
    if not user or not verify_password(password, user["password_hash"]):
        raise HTTPException(401, "用户名或密码错误")

    token = create_user_jwt(
        username=user["username"],
        role=user.get("role") or "user",
        default_account_key=user.get("default_account_key") or "",
    )

    return {
        "ok": True,
        "token": token,
        "username": user["username"],
        "role": user.get("role") or "user",
        "default_account_key": user.get("default_account_key") or "",
    }


@router_user.get("/me")
async def user_me(user: Dict[str, Any] = Depends(get_current_user)):
    return {
        "ok": True,
        "username": user["username"],
        "role": user.get("role", "user"),
        "default_account_key": user.get("default_account_key", ""),
    }
