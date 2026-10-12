"""
用户认证模块：密码哈希 / JWT 签发解码 / FastAPI Depends
与小蚕账号体系解耦，登录成功后返回 default_account_key 给前端
"""
import hashlib
import hmac
import os
import time
import logging
from typing import Optional, Dict, Any

import jwt
from fastapi import Header, HTTPException

logger = logging.getLogger("xiaocan.auth")

# JWT 密钥延迟加载：启动时由 main.py 调 seed_jwt_secret_if_needed() 写入 settings 表
_JWT_SECRET: Optional[str] = None


def hash_password(password: str) -> str:
    """pbkdf2_sha256 哈希，零第三方依赖"""
    salt = os.urandom(16).hex()
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), 120000)
    return f"pbkdf2_sha256$120000${salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iters, salt, hex_hash = stored.split("$")
        if algo != "pbkdf2_sha256":
            return False
        dk = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), bytes.fromhex(salt), int(iters)
        )
        return hmac.compare_digest(dk.hex(), hex_hash)
    except Exception:
        return False


def _get_jwt_secret() -> str:
    global _JWT_SECRET
    if _JWT_SECRET:
        return _JWT_SECRET
    # 延迟从 settings 表读取
    try:
        from ..models import database as db

        secret = db.get_setting("jwt_secret")
        if secret and isinstance(secret, str):
            _JWT_SECRET = secret
            return _JWT_SECRET
    except Exception:
        pass
    # 兜底：临时生成一个（首次启动前）
    _JWT_SECRET = os.urandom(32).hex()
    logger.warning("JWT secret 未在 settings 表中找到，使用临时生成值")
    return _JWT_SECRET


def seed_jwt_secret_if_needed() -> None:
    """启动时调用：确保 settings 表存在 jwt_secret"""
    global _JWT_SECRET
    try:
        from ..models import database as db

        existing = db.get_setting("jwt_secret")
        if existing and isinstance(existing, str) and len(existing) >= 16:
            _JWT_SECRET = existing
            return
        secret = os.urandom(32).hex()
        db.set_setting("jwt_secret", secret)
        _JWT_SECRET = secret
        logger.info("已生成并保存 JWT secret 到 settings 表")
    except Exception as e:
        logger.error(f"seed_jwt_secret 失败: {e}")


def create_user_jwt(
    username: str, role: str, default_account_key: str, exp_hours: int = 72
) -> str:
    payload = {
        "sub": username,
        "role": role,
        "default_account_key": default_account_key,
        "iat": int(time.time()),
        "exp": int(time.time()) + exp_hours * 3600,
    }
    return jwt.encode(payload, _get_jwt_secret(), algorithm="HS256")


def decode_user_jwt(token: str) -> Dict[str, Any]:
    return jwt.decode(token, _get_jwt_secret(), algorithms=["HS256"])


# ---------------- FastAPI 依赖 ----------------


async def get_current_user(
    authorization: str = Header(default="", alias="authorization"),
) -> Dict[str, Any]:
    """强制登录：无 token 或无效时抛 401"""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "未登录，请先登录")
    token = authorization[7:].strip()
    if not token:
        raise HTTPException(401, "未登录，请先登录")
    try:
        payload = decode_user_jwt(token)
        return {
            "username": payload.get("sub", ""),
            "role": payload.get("role", "user"),
            "default_account_key": payload.get("default_account_key", ""),
        }
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "登录已过期，请重新登录")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "无效的登录凭证")
