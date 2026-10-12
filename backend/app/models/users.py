"""
users 表 CRUD：极简，只为精简前端的用户名密码登录服务
"""
import time
import logging
from typing import Optional, Dict, Any, List

from . import database as db_mod

logger = logging.getLogger("xiaocan.users")


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    with db_mod.get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE username = ?", (username.strip(),)
        ).fetchone()
        return dict(row) if row else None


def get_user_by_id(uid: int) -> Optional[Dict[str, Any]]:
    with db_mod.get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
        return dict(row) if row else None


def list_users() -> List[Dict[str, Any]]:
    with db_mod.get_conn() as conn:
        rows = conn.execute(
            "SELECT id, username, role, default_account_key, created_at, updated_at FROM users ORDER BY id ASC"
        ).fetchall()
        return [dict(r) for r in rows]


def create_user(
    username: str,
    password_hash: str,
    role: str = "user",
    default_account_key: str = "",
) -> Optional[Dict[str, Any]]:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with db_mod.get_conn() as conn:
        try:
            conn.execute(
                """INSERT INTO users (username, password_hash, role, default_account_key, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (username.strip(), password_hash, role, default_account_key, now, now),
            )
            conn.commit()
        except Exception as e:
            logger.error(f"create_user 失败: {e}")
            return None
    return get_user_by_username(username)


def update_user_default_account_key(uid: int, account_key: str) -> bool:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with db_mod.get_conn() as conn:
        conn.execute(
            "UPDATE users SET default_account_key = ?, updated_at = ? WHERE id = ?",
            (account_key, now, uid),
        )
        conn.commit()
        return True


def delete_user(uid: int) -> bool:
    with db_mod.get_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (uid,))
        conn.commit()
        return True


def seed_initial_user() -> None:
    """数据库初始化时调用：如果 users 表为空，自动创建 admin/admin123
    default_account_key 取 accounts 表里最新的一个。
    每次启动都会检查并补关联空 default_account_key 的用户。
    """
    from ..core.auth import hash_password

    with db_mod.get_conn() as conn:
        # -------- 1. 创建初始用户（users 为空时）----------
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if count == 0:
            acc_row = conn.execute(
                "SELECT key FROM accounts ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            default_key = acc_row[0] if acc_row else ""
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            pw_hash = hash_password("admin123")
            conn.execute(
                """INSERT INTO users (username, password_hash, role, default_account_key, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ("admin", pw_hash, "admin", default_key, now, now),
            )
            conn.commit()
            logger.info(f"已创建初始用户 admin，default_account_key={default_key}")

        # -------- 2. 每次启动自动补关联（accounts 后来才有数据的场景）----------
        acc_row = conn.execute(
            "SELECT key FROM accounts ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if acc_row:
            latest_key = acc_row[0]
            conn.execute(
                "UPDATE users SET default_account_key = ?, updated_at = datetime('now') WHERE (default_account_key IS NULL OR default_account_key = '') AND role = 'admin'",
                (latest_key,)
            )
            if conn.total_changes > 0:
                conn.commit()
                logger.info(f"已自动关联 admin 用户到最新 account_key={latest_key}")
