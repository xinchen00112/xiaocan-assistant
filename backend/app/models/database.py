"""
SQLite 数据库初始化与轻量 ORM 封装
单文件零运维，自动建表
"""
import sqlite3
import json
import os
import time
import uuid
from typing import List, Dict, Any, Optional

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "xiaocan.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 15000")
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    # 企业级 SQLite 高并发调优：WAL 无锁并发读写、15s 防锁死超时、NORMAL 刷盘与 8MB 页缓存
    c.execute("PRAGMA journal_mode = WAL")
    c.execute("PRAGMA synchronous = NORMAL")
    c.execute("PRAGMA busy_timeout = 15000")
    c.execute("PRAGMA cache_size = -8000")

    # 1. 账号表 (无车位上限，永久激活)
    c.execute("""
    CREATE TABLE IF NOT EXISTS accounts (
        key TEXT PRIMARY KEY,
        silk_id TEXT,
        nickname TEXT,
        avatar TEXT,
        token TEXT,
        vip_level INTEGER DEFAULT 5,
        city_code INTEGER DEFAULT 420100,
        city_name TEXT DEFAULT '武汉',
        longitude TEXT DEFAULT '114.305393',
        latitude TEXT DEFAULT '30.593099',
        expires_at TEXT DEFAULT '2099-12-31',
        is_active INTEGER DEFAULT 1,
        created_at TEXT,
        updated_at TEXT
    )
    """)

    # 动态检查 accounts 补充官方原生字段 (严格对齐小蚕官方用户模型，不臆造自定义字段)
    acc_cols = [r[1] for r in c.execute("PRAGMA table_info(accounts)").fetchall()]
    if "user_id" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN user_id TEXT DEFAULT ''")
    if "is_plus" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN is_plus INTEGER DEFAULT 0")
    if "phone" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN phone TEXT DEFAULT ''")
    if "real_name" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN real_name TEXT DEFAULT ''")
    if "vip_score" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN vip_score INTEGER DEFAULT 0")
    if "vip_expired_at" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN vip_expired_at INTEGER DEFAULT 0")
    if "silk" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN silk INTEGER DEFAULT 0")
    if "withdrawing" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN withdrawing INTEGER DEFAULT 0")
    if "withdraw_total" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN withdraw_total INTEGER DEFAULT 0")
    if "completed_number" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN completed_number INTEGER DEFAULT 0")
    if "yb_point" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN yb_point INTEGER DEFAULT 0")
    if "unreceived_points" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN unreceived_points INTEGER DEFAULT 0")
    if "notify_mode" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN notify_mode TEXT DEFAULT 'global'")
    if "notify_config" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN notify_config TEXT DEFAULT '{}'")
    if "client_mode" not in acc_cols:
        c.execute("ALTER TABLE accounts ADD COLUMN client_mode TEXT DEFAULT 'app'")

    # 2. 自动化任务开关与调度表
    c.execute("""
    CREATE TABLE IF NOT EXISTS task_configs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_key TEXT,
        task_id TEXT,
        enabled INTEGER DEFAULT 0,
        cron_time TEXT,
        params TEXT,
        UNIQUE(account_key, task_id)
    )
    """)

    # 3. 店铺预约与抢单队列
    c.execute("""
    CREATE TABLE IF NOT EXISTS store_appointments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_key TEXT,
        store_id TEXT,
        store_name TEXT,
        promotion_id TEXT,
        status TEXT DEFAULT 'pending',
        early_ms INTEGER DEFAULT 500,
        rebate_card_id TEXT,
        redpack_mode INTEGER DEFAULT 0,
        outcome TEXT,
        created_at TEXT
    )
    """)

    # 4. 运行日志表
    c.execute("""
    CREATE TABLE IF NOT EXISTS job_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT,
        account_key TEXT,
        task_id TEXT,
        status TEXT,
        output TEXT,
        created_at TEXT
    )
    """)

    # 5. 系统与通知配置表
    c.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    # 7. 精简前端用户表 (用户名密码登录，关联小蚕 account_key)
    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT DEFAULT 'user',
        default_account_key TEXT,
        created_at TEXT,
        updated_at TEXT
    )
    """)

    # 6. 霸王餐订单表
    c.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_key TEXT,
        order_sn TEXT UNIQUE,
        platform_order_id TEXT,
        store_id TEXT,
        store_name TEXT,
        platform TEXT DEFAULT 'meituan',
        order_money REAL DEFAULT 0,
        rebate_money REAL DEFAULT 0,
        status TEXT DEFAULT 'pending',
        condition TEXT DEFAULT '无需评价',
        receipt_img TEXT,
        reject_reason TEXT,
        expire_time TEXT,
        created_at TEXT,
        updated_at TEXT,
        promotion_order_id INTEGER DEFAULT 0
    )
    """)

    # 动态检查补充 orders 字段
    cols = [r[1] for r in c.execute("PRAGMA table_info(orders)").fetchall()]
    if "promotion_order_id" not in cols:
        c.execute("ALTER TABLE orders ADD COLUMN promotion_order_id INTEGER DEFAULT 0")
    if "order_time" not in cols:
        c.execute("ALTER TABLE orders ADD COLUMN order_time INTEGER DEFAULT 0")
    if "store_icon" not in cols:
        c.execute("ALTER TABLE orders ADD COLUMN store_icon TEXT DEFAULT ''")
    if "redpack_reward_num" not in cols:
        c.execute("ALTER TABLE orders ADD COLUMN redpack_reward_num REAL DEFAULT 0")
    if "timeout_time" not in cols:
        c.execute("ALTER TABLE orders ADD COLUMN timeout_time INTEGER DEFAULT 0")
    if "original_user_rebate" not in cols:
        c.execute("ALTER TABLE orders ADD COLUMN original_user_rebate REAL DEFAULT 0")

    # 自动自愈 orders 中缺失 store_icon 的记录 (从已有历史同名/同 store_id 记录回填)
    try:
        c.execute("""
            UPDATE orders
            SET store_icon = (
                SELECT o2.store_icon FROM orders o2
                WHERE (o2.store_id = orders.store_id OR o2.store_name = orders.store_name)
                  AND o2.store_icon IS NOT NULL
                  AND o2.store_icon != ''
                LIMIT 1
            )
            WHERE (store_icon IS NULL OR store_icon = '')
        """)
    except Exception:
        pass

    # 动态检查补充 store_appointments 调度与监控字段
    apt_cols = [r[1] for r in c.execute("PRAGMA table_info(store_appointments)").fetchall()]
    if "task_type" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN task_type TEXT DEFAULT 'countdown'")
    if "start_time" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN start_time TEXT")
    if "until_time" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN until_time TEXT")
    if "notified_31m" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN notified_31m INTEGER DEFAULT 0")
    if "notified_1m" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN notified_1m INTEGER DEFAULT 0")
    if "check_interval" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN check_interval INTEGER DEFAULT 30")
    if "platform" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN platform TEXT DEFAULT 'meituan'")
    if "order_money" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN order_money REAL DEFAULT 0")
    if "rebate_price" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN rebate_price REAL DEFAULT 0")
    if "rebate_desc" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN rebate_desc TEXT")
    if "rebate_type" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN rebate_type TEXT DEFAULT 'fixed'")
    if "last_checked_at" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN last_checked_at TEXT")
    if "log_id" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN log_id INTEGER DEFAULT 0")
    if "store_icon" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN store_icon TEXT DEFAULT ''")
    if "use_advance_card" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN use_advance_card INTEGER DEFAULT 0")
    if "redpack_id" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN redpack_id TEXT DEFAULT ''")
    if "redpack_name" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN redpack_name TEXT DEFAULT ''")
    if "keyword" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN keyword TEXT DEFAULT ''")
    if "match_mode" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN match_mode TEXT DEFAULT 'contains'")
    if "min_rebate_price" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN min_rebate_price REAL DEFAULT 0")
    if "min_rebate_rate" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN min_rebate_rate REAL DEFAULT 0")
    if "rebate_mode_filter" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN rebate_mode_filter TEXT DEFAULT 'all'")
    if "auto_stop_on_success" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN auto_stop_on_success INTEGER DEFAULT 1")
    if "max_order_money" not in apt_cols:
        c.execute("ALTER TABLE store_appointments ADD COLUMN max_order_money REAL DEFAULT 0")

    # 自动自愈 store_appointments 中缺失 store_icon 的记录 (从 orders 表或同店名同ID回填)
    try:
        c.execute("""
            UPDATE store_appointments
            SET store_icon = (
                SELECT o.store_icon FROM orders o
                WHERE (o.store_id = store_appointments.store_id OR o.store_name = store_appointments.store_name)
                  AND o.store_icon IS NOT NULL
                  AND o.store_icon != ''
                LIMIT 1
            )
            WHERE (store_icon IS NULL OR store_icon = '')
        """)
    except Exception:
        pass

    # 历史 daily 综合任务平滑迁移：自动迁移至独立原子的 yb_sign 与 yb_task，彻底消除重复调用
    try:
        daily_enabled_keys = [
            r[0] for r in c.execute("SELECT account_key FROM task_configs WHERE task_id = 'daily' AND enabled = 1").fetchall()
        ]
        for a_key in daily_enabled_keys:
            c.execute("""
                INSERT INTO task_configs (account_key, task_id, enabled, cron_time, params)
                VALUES (?, 'yb_sign', 1, '08:10', '{}')
                ON CONFLICT(account_key, task_id) DO UPDATE SET enabled = 1
            """, (a_key,))
            c.execute("""
                INSERT INTO task_configs (account_key, task_id, enabled, cron_time, params)
                VALUES (?, 'yb_task', 1, '08:05', '{}')
                ON CONFLICT(account_key, task_id) DO UPDATE SET enabled = 1
            """, (a_key,))
        c.execute("UPDATE task_configs SET enabled = 0 WHERE task_id = 'daily'")
    except Exception:
        pass

    conn.commit()
    conn.close()

    # 种子：初始用户 admin/admin123
    try:
        from . import users as users_db
        users_db.seed_initial_user()
    except Exception as e:
        pass


# 初始化建表
init_db()


# ---------------- 数据库便捷操作接口 ---------------- #

def get_all_accounts() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM accounts ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]


def get_account_by_key(key: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM accounts WHERE key = ?", (key,)).fetchone()
        return dict(row) if row else None


get_account = get_account_by_key


def find_account(
    key: Optional[str] = None,
    silk_id: Optional[str] = None,
    user_id: Optional[str] = None,
    token: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """根据 key、user_id、silk_id 或 token 查找已存在的小蚕账号，避免重复入库"""
    with get_conn() as conn:
        if key and str(key).strip():
            row = conn.execute("SELECT * FROM accounts WHERE key = ?", (str(key).strip(),)).fetchone()
            if row:
                return dict(row)
        if user_id and str(user_id).strip():
            row = conn.execute("SELECT * FROM accounts WHERE user_id = ?", (str(user_id).strip(),)).fetchone()
            if row:
                return dict(row)
        if silk_id and str(silk_id).strip():
            row = conn.execute("SELECT * FROM accounts WHERE silk_id = ?", (str(silk_id).strip(),)).fetchone()
            if row:
                return dict(row)
        if token and str(token).strip():
            row = conn.execute("SELECT * FROM accounts WHERE token = ?", (str(token).strip(),)).fetchone()
            if row:
                return dict(row)
            # 尝试从传入的 token 解码 UserId 并查重
            try:
                from ..core.jwt_utils import decode_jwt_payload, extract_user_id_from_payload
                _, p, _ = decode_jwt_payload(str(token).strip())
                jwt_uid = extract_user_id_from_payload(p)
                if jwt_uid:
                    row = conn.execute("SELECT * FROM accounts WHERE user_id = ?", (jwt_uid,)).fetchone()
                    if row:
                        return dict(row)
            except Exception:
                pass
    return None


def save_account(data: Dict[str, Any]) -> Dict[str, Any]:
    """保存或更新小蚕账号，具备完善的查重合并与用户配置保护能力"""
    now = time.strftime("%Y-%m-%d %H:%M:%S")

    # 尝试自动从 JWT 提取真实的 UserId、Silk ID、有效期
    token = (data.get("token") or "").strip()
    user_id = str(data.get("user_id") or "").strip()
    silk_id = str(data.get("silk_id") or "").strip()
    exp_time = data.get("expires_at")

    if token:
        try:
            from ..core.jwt_utils import decode_jwt_payload, extract_user_id_from_payload, extract_silk_id_from_payload
            is_valid, payload, _ = decode_jwt_payload(token)
            if is_valid and payload:
                if not user_id:
                    user_id = extract_user_id_from_payload(payload)
                if not silk_id:
                    silk_id = extract_silk_id_from_payload(payload)
                exp = payload.get("exp")
                if exp and not exp_time:
                    exp_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(exp)))
        except Exception:
            pass

    # 查找已有账号以实现无缝去重更新 (通过 key / user_id / silk_id / token 全方位查重)
    existing = find_account(
        key=data.get("key"),
        silk_id=silk_id,
        user_id=user_id,
        token=token
    )

    # 确定唯一稳定的 key：优先沿用已有 key，否则以 user_id 或 silk_id 命名保证幂等
    if existing:
        key = existing["key"]
    elif data.get("key"):
        key = data["key"]
    elif user_id:
        key = f"acc_{user_id}"
    elif silk_id:
        key = f"acc_{silk_id}"
    else:
        key = f"acc_{uuid.uuid4().hex[:8]}"

    # 昵称保护：如果新昵称为默认占位符而已有真实昵称，优先保留已有昵称
    nickname = data.get("nickname") or (existing.get("nickname") if existing else "小蚕用户")
    if (not nickname or nickname.startswith("小蚕用户") or nickname == "小蚕微信用户") and existing and existing.get("nickname") and not existing["nickname"].startswith("小蚕用户"):
        nickname = existing["nickname"]

    # 头像保护
    avatar = data.get("avatar") or (existing.get("avatar") if existing else "")

    # 会员等级与 PLUS 标识（严格对齐小蚕官方 vip_level_info 结构）
    vip_level = data.get("vip_level") or (existing.get("vip_level") if existing else 1)
    is_plus = int(data.get("is_plus", existing.get("is_plus", 0) if existing else 0))
    vip_score = data.get("vip_score") or (existing.get("vip_score") if existing else 0)
    vip_expired_at = data.get("vip_expired_at") or (existing.get("vip_expired_at") if existing else 0)
    phone = data.get("phone") or (existing.get("phone") if existing else "")
    real_name = data.get("real_name") or (existing.get("real_name") if existing else "")

    # 城市与定位保护 (优先保留用户已设置的城市与经纬度，避免被默认值覆盖)
    city_code = data.get("city_code") or (existing.get("city_code") if existing else 420100)
    city_name = data.get("city_name") or (existing.get("city_name") if existing else "武汉")
    longitude = data.get("longitude") or (existing.get("longitude") if existing else "114.305393")
    latitude = data.get("latitude") or (existing.get("latitude") if existing else "30.593099")

    # 真实官方资产字段保护
    silk = int(data.get("silk", existing.get("silk", 0) if existing else 0))
    withdrawing = int(data.get("withdrawing", existing.get("withdrawing", 0) if existing else 0))
    withdraw_total = int(data.get("withdraw_total", existing.get("withdraw_total", 0) if existing else 0))
    completed_number = int(data.get("completed_number", existing.get("completed_number", 0) if existing else 0))
    yb_point = int(data.get("yb_point", existing.get("yb_point", 0) if existing else 0))
    unreceived_points = int(data.get("unreceived_points", existing.get("unreceived_points", 0) if existing else 0))

    # Silk ID 补全保护：若已有存在 silk_id，新包缺失时予以保留
    final_silk_id = silk_id or (existing.get("silk_id") if existing else "")
    final_user_id = user_id or (existing.get("user_id") if existing else "")
    final_token = token or (existing.get("token") if existing else "")
    final_expires_at = exp_time or (existing.get("expires_at") if existing else "2099-12-31")
    client_mode = data.get("client_mode") or (existing.get("client_mode") if existing else "app")
    created_at = existing.get("created_at") if existing else data.get("created_at", now)

    with get_conn() as conn:
        conn.execute("""
        INSERT INTO accounts (key, user_id, silk_id, nickname, avatar, token, vip_level, is_plus, phone, real_name, vip_score, vip_expired_at, silk, withdrawing, withdraw_total, completed_number, yb_point, unreceived_points, city_code, city_name, longitude, latitude, expires_at, client_mode, is_active, created_at, updated_at)
        VALUES (:key, :user_id, :silk_id, :nickname, :avatar, :token, :vip_level, :is_plus, :phone, :real_name, :vip_score, :vip_expired_at, :silk, :withdrawing, :withdraw_total, :completed_number, :yb_point, :unreceived_points, :city_code, :city_name, :longitude, :latitude, :expires_at, :client_mode, :is_active, :created_at, :updated_at)
        ON CONFLICT(key) DO UPDATE SET
            user_id=excluded.user_id,
            silk_id=CASE WHEN excluded.silk_id != '' THEN excluded.silk_id ELSE accounts.silk_id END,
            nickname=excluded.nickname,
            avatar=excluded.avatar,
            token=excluded.token,
            vip_level=excluded.vip_level,
            is_plus=excluded.is_plus,
            phone=excluded.phone,
            real_name=excluded.real_name,
            vip_score=excluded.vip_score,
            vip_expired_at=excluded.vip_expired_at,
            silk=excluded.silk,
            withdrawing=excluded.withdrawing,
            withdraw_total=excluded.withdraw_total,
            completed_number=excluded.completed_number,
            yb_point=excluded.yb_point,
            unreceived_points=excluded.unreceived_points,
            city_code=excluded.city_code,
            city_name=excluded.city_name,
            longitude=excluded.longitude,
            latitude=excluded.latitude,
            expires_at=excluded.expires_at,
            client_mode=COALESCE(excluded.client_mode, accounts.client_mode),
            is_active=1,
            updated_at=excluded.updated_at
        """, {
            "key": key,
            "user_id": final_user_id,
            "silk_id": final_silk_id,
            "nickname": nickname,
            "avatar": avatar,
            "token": final_token,
            "vip_level": vip_level,
            "is_plus": is_plus,
            "phone": phone,
            "real_name": real_name,
            "vip_score": vip_score,
            "vip_expired_at": vip_expired_at,
            "silk": silk,
            "withdrawing": withdrawing,
            "withdraw_total": withdraw_total,
            "completed_number": completed_number,
            "yb_point": yb_point,
            "unreceived_points": unreceived_points,
            "city_code": city_code,
            "city_name": city_name,
            "longitude": longitude,
            "latitude": latitude,
            "expires_at": final_expires_at,
            "client_mode": client_mode,
            "is_active": 1,
            "created_at": created_at,
            "updated_at": now
        })
        # 如果本次写入有 user_id，同时清理其他可能同属于同一个 user_id 的旧 key 重复行！
        if final_user_id:
            conn.execute("DELETE FROM accounts WHERE user_id = ? AND key != ?", (final_user_id, key))
        conn.commit()

    saved = get_account_by_key(key)
    if saved:
        saved["is_updated"] = bool(existing)
    return saved


def update_account_profile(key: str, profile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """更新账号资料字段 (严格对齐官方字段)"""
    allowed_fields = [
        "silk_id", "nickname", "avatar", "vip_level", "is_plus",
        "phone", "real_name", "vip_score", "vip_expired_at",
        "silk", "withdrawing", "withdraw_total", "completed_number",
        "yb_point", "unreceived_points",
        "city_code", "city_name", "expires_at", "longitude", "latitude",
        "notify_mode", "notify_config", "client_mode"
    ]
    updates = []
    params = {"key": key, "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    for field in allowed_fields:
        if field in profile:
            updates.append(f"{field} = :{field}")
            params[field] = profile[field]
    if not updates:
        return get_account_by_key(key)

    sql = f"UPDATE accounts SET {', '.join(updates)}, updated_at = :updated_at WHERE key = :key"
    with get_conn() as conn:
        conn.execute(sql, params)
        conn.commit()
    return get_account_by_key(key)


def get_account_notify_config(key: str) -> Dict[str, Any]:
    """获取指定账号的通知模式及配置"""
    acc = get_account_by_key(key)
    if not acc:
        return {"notify_mode": "global", "notify_config": {}}
    mode = acc.get("notify_mode") or "global"
    raw_cfg = acc.get("notify_config") or "{}"
    cfg = {}
    if isinstance(raw_cfg, str) and raw_cfg.strip():
        try:
            cfg = json.loads(raw_cfg)
        except Exception:
            cfg = {}
    elif isinstance(raw_cfg, dict):
        cfg = raw_cfg
    return {"notify_mode": mode, "notify_config": cfg}


def set_account_notify_config(key: str, mode: str, config: Dict[str, Any]) -> bool:
    """更新指定账号的通知模式及配置"""
    cfg_str = json.dumps(config, ensure_ascii=False) if isinstance(config, dict) else str(config or "{}")
    res = update_account_profile(key, {
        "notify_mode": mode or "global",
        "notify_config": cfg_str
    })
    return res is not None



def delete_account(key: str) -> bool:
    with get_conn() as conn:
        conn.execute("DELETE FROM accounts WHERE key = ?", (key,))
        conn.execute("DELETE FROM task_configs WHERE account_key = ?", (key,))
        conn.commit()
        return True


def get_task_configs(account_key: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM task_configs WHERE account_key = ?", (account_key,)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if isinstance(d.get("params"), str):
                try:
                    d["params"] = json.loads(d["params"])
                except Exception:
                    d["params"] = {}
            elif not d.get("params"):
                d["params"] = {}
            result.append(d)
        return result


def save_task_config(account_key: str, task_id: str, enabled: bool, cron_time: Optional[str] = None, params: Optional[Dict[str, Any]] = None):
    with get_conn() as conn:
        params_str = json.dumps(params) if params is not None else None
        conn.execute("""
        INSERT INTO task_configs (account_key, task_id, enabled, cron_time, params)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(account_key, task_id) DO UPDATE SET
            enabled=excluded.enabled,
            cron_time=COALESCE(excluded.cron_time, task_configs.cron_time),
            params=COALESCE(excluded.params, task_configs.params)
        """, (account_key, task_id, 1 if enabled else 0, cron_time, params_str))
        conn.commit()


def add_job_log(job_id: str, account_key: str, task_id: str, status: str, output: str) -> int:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        cur = conn.execute("""
        INSERT INTO job_logs (job_id, account_key, task_id, status, output, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (job_id, account_key, task_id, status, output, now))
        conn.commit()
        return cur.lastrowid


def update_job_log(log_id: int, status: Optional[str] = None, output: Optional[str] = None) -> bool:
    """动态更新某条日志的状态与详细步骤输出"""
    if not log_id:
        return False
    updates = []
    vals = []
    if status is not None:
        updates.append("status = ?")
        vals.append(status)
    if output is not None:
        updates.append("output = ?")
        vals.append(output)
    if not updates:
        return False
    vals.append(log_id)
    with get_conn() as conn:
        conn.execute(f"UPDATE job_logs SET {', '.join(updates)} WHERE id = ?", vals)
        conn.commit()
        return True


def get_job_logs(account_key: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        if account_key:
            rows = conn.execute("SELECT * FROM job_logs WHERE account_key = ? ORDER BY id DESC LIMIT ?", (account_key, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM job_logs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]


def clear_job_logs(account_key: Optional[str] = None):
    with get_conn() as conn:
        if account_key:
            conn.execute("DELETE FROM job_logs WHERE account_key = ?", (account_key,))
        else:
            conn.execute("DELETE FROM job_logs")
        conn.commit()


def get_appointments(account_key: Optional[str] = None) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        if account_key:
            rows = conn.execute("SELECT * FROM store_appointments WHERE account_key = ? ORDER BY id DESC", (account_key,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM store_appointments ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]


def get_appointment_by_id(aid: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM store_appointments WHERE id = ?", (aid,)).fetchone()
        return dict(row) if row else None


def get_active_appointments() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute("""
        SELECT * FROM store_appointments 
        WHERE status IN ('pending', 'scheduled', 'primed', 'monitoring')
        ORDER BY id ASC
        """).fetchall()
        return [dict(r) for r in rows]


def add_appointment(data: Dict[str, Any]) -> int:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    status = data.get("status")
    if not status:
        status = "monitoring" if data.get("task_type") in ("monitor", "name_monitor", "keyword") else "scheduled"

    store_icon = (data.get("store_icon") or data.get("icon") or "").strip()
    with get_conn() as conn:
        if not store_icon:
            s_id = str(data.get("store_id", "")).strip()
            s_name = (data.get("store_name") or "").strip()
            row = conn.execute(
                "SELECT store_icon FROM orders WHERE (store_id = ? OR store_name = ?) AND store_icon IS NOT NULL AND store_icon != '' LIMIT 1",
                (s_id, s_name)
            ).fetchone()
            if row and row["store_icon"]:
                store_icon = row["store_icon"]

        cur = conn.execute("""
        INSERT INTO store_appointments (
            account_key, store_id, store_name, store_icon, promotion_id, status, 
            early_ms, rebate_card_id, redpack_mode, outcome, created_at,
            task_type, start_time, until_time, notified_31m, notified_1m,
            check_interval, platform, order_money, rebate_price, rebate_desc, rebate_type, log_id,
            use_advance_card, redpack_id, redpack_name,
            keyword, match_mode, min_rebate_price, min_rebate_rate, rebate_mode_filter, auto_stop_on_success, max_order_money
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["account_key"],
            str(data.get("store_id", "")),
            data.get("store_name", "店铺活动"),
            store_icon,
            str(data["promotion_id"]),
            status,
            int(data.get("early_ms", 500)),
            data.get("rebate_card_id", ""),
            int(data.get("redpack_mode", 0)),
            data.get("outcome", ""),
            now,
            data.get("task_type", "countdown"),
            data.get("start_time", ""),
            data.get("until_time", ""),
            int(data.get("notified_31m", 0)),
            int(data.get("notified_1m", 0)),
            int(data.get("check_interval", 5)),
            data.get("platform", "meituan"),
            float(data.get("order_money", 0.0)),
            float(data.get("rebate_price", 0.0)),
            data.get("rebate_desc", ""),
            data.get("rebate_type", "fixed"),
            int(data.get("log_id") or 0),
            int(data.get("use_advance_card") or 0),
            str(data.get("redpack_id") or ""),
            str(data.get("redpack_name") or ""),
            str(data.get("keyword") or ""),
            str(data.get("match_mode") or "contains"),
            float(data.get("min_rebate_price") or 0.0),
            float(data.get("min_rebate_rate") or 0.0),
            str(data.get("rebate_mode_filter") or "all"),
            int(data.get("auto_stop_on_success") if data.get("auto_stop_on_success") is not None else 1),
            float(data.get("max_order_money") or 0.0)
        ))
        conn.commit()
        return cur.lastrowid


def update_appointment(aid: int, fields: Dict[str, Any]) -> bool:
    if not fields:
        return False
    keys = list(fields.keys())
    set_clause = ", ".join([f"{k} = ?" for k in keys])
    vals = [fields[k] for k in keys] + [aid]
    with get_conn() as conn:
        conn.execute(f"UPDATE store_appointments SET {set_clause} WHERE id = ?", vals)
        conn.commit()
        return True


def delete_appointment(aid: int) -> bool:
    with get_conn() as conn:
        conn.execute("DELETE FROM store_appointments WHERE id = ?", (aid,))
        conn.commit()
        return True


def get_setting(key: str, default: Any = None) -> Any:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return json.loads(row["value"]) if row else default


def set_setting(key: str, value: Any):
    with get_conn() as conn:
        conn.execute("""
        INSERT INTO settings (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, json.dumps(value)))
        conn.commit()


# ---------------- 霸王餐订单操作接口 ---------------- #

def auto_cancel_expired_orders(account_key: Optional[str] = None) -> int:
    """
    自动检测并批量更新所有已超时的待处理订单为已取消 (cancelled)。
    判定规则：
    1. status == 'pending' 且 (timeout_time > 0 且 timeout_time < 当前时间戳)
    2. 或 status == 'pending' 且 (expire_time != '' 且 expire_time < 当前日期时间)
    """
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    now_ts = int(time.time())
    sql = """
        UPDATE orders
        SET status = 'cancelled', updated_at = :now_str
        WHERE status = 'pending' AND (
            (timeout_time > 0 AND timeout_time < :now_ts) OR
            (expire_time != '' AND expire_time < :now_str)
        )
    """
    params: Dict[str, Any] = {"now_str": now_str, "now_ts": now_ts}
    if account_key:
        sql += " AND account_key = :account_key"
        params["account_key"] = account_key
    try:
        with get_conn() as conn:
            cur = conn.execute(sql, params)
            conn.commit()
            return cur.rowcount
    except Exception:
        return 0


def get_orders(
    account_key: Optional[str] = None,
    status: Optional[str] = None,
    platform: Optional[str] = None,
    keyword: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> List[Dict[str, Any]]:
    # 查询前先执行过期超时订单自动自愈与取消
    auto_cancel_expired_orders(account_key=account_key)

    sql = "SELECT * FROM orders WHERE 1=1"
    params: Dict[str, Any] = {}
    if account_key:
        sql += " AND account_key = :account_key"
        params["account_key"] = account_key
    if status and status != "all":
        sql += " AND status = :status"
        params["status"] = status
    if platform and platform != "all":
        if platform in ("jingdong", "jd"):
            sql += " AND platform IN ('jingdong', 'jd')"
        elif platform in ("eleme", "taobao"):
            sql += " AND platform IN ('eleme', 'taobao')"
        else:
            sql += " AND platform = :platform"
            params["platform"] = platform
    if keyword:
        sql += " AND (store_name LIKE :kw OR order_sn LIKE :kw OR platform_order_id LIKE :kw)"
        params["kw"] = f"%{keyword}%"
    sql += " ORDER BY order_time DESC, created_at DESC, id DESC LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_order_by_id(order_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        return dict(row) if row else None


def save_order(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        cursor = conn.execute("""
        INSERT INTO orders (account_key, order_sn, promotion_order_id, platform_order_id, store_id, store_name, store_icon, platform, order_money, rebate_money, original_user_rebate, redpack_reward_num, timeout_time, status, condition, receipt_img, reject_reason, expire_time, created_at, updated_at, order_time)
        VALUES (:account_key, :order_sn, :promotion_order_id, :platform_order_id, :store_id, :store_name, :store_icon, :platform, :order_money, :rebate_money, :original_user_rebate, :redpack_reward_num, :timeout_time, :status, :condition, :receipt_img, :reject_reason, :expire_time, :created_at, :updated_at, :order_time)
        ON CONFLICT(order_sn) DO UPDATE SET
            promotion_order_id = excluded.promotion_order_id,
            platform_order_id = excluded.platform_order_id,
            store_name = excluded.store_name,
            store_icon = excluded.store_icon,
            status = excluded.status,
            order_money = excluded.order_money,
            rebate_money = excluded.rebate_money,
            original_user_rebate = excluded.original_user_rebate,
            redpack_reward_num = excluded.redpack_reward_num,
            timeout_time = excluded.timeout_time,
            receipt_img = excluded.receipt_img,
            reject_reason = excluded.reject_reason,
            order_time = excluded.order_time,
            expire_time = excluded.expire_time,
            condition = excluded.condition,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
        """, {
            "account_key": data.get("account_key", ""),
            "order_sn": data.get("order_sn", f"XC{int(time.time()*1000)}"),
            "promotion_order_id": int(data.get("promotion_order_id") or 0),
            "platform_order_id": data.get("platform_order_id", ""),
            "store_id": str(data.get("store_id", "")),
            "store_name": data.get("store_name", ""),
            "store_icon": data.get("store_icon", ""),
            "platform": data.get("platform", "meituan"),
            "order_money": float(data.get("order_money", 0)),
            "rebate_money": float(data.get("rebate_money", 0)),
            "original_user_rebate": float(data.get("original_user_rebate", 0)),
            "redpack_reward_num": float(data.get("redpack_reward_num", 0)),
            "timeout_time": int(data.get("timeout_time") or 0),
            "status": data.get("status", "pending"),
            "condition": data.get("condition", "无需评价"),
            "receipt_img": data.get("receipt_img", ""),
            "reject_reason": data.get("reject_reason", ""),
            "expire_time": data.get("expire_time", ""),
            "created_at": data.get("created_at", now),
            "updated_at": now,
            "order_time": int(data.get("order_time") or 0)
        })
        conn.commit()
        last_id = cursor.lastrowid
        return get_order_by_id(last_id) if last_id else None


def update_order(order_id: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    allowed = [
        "platform_order_id", "status", "receipt_img", "reject_reason",
        "order_money", "rebate_money", "original_user_rebate",
        "redpack_reward_num", "timeout_time", "store_icon"
    ]
    updates = []
    params: Dict[str, Any] = {"id": order_id, "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    for k in allowed:
        if k in data:
            updates.append(f"{k} = :{k}")
            params[k] = data[k]
    if not updates:
        return get_order_by_id(order_id)
    sql = f"UPDATE orders SET {', '.join(updates)}, updated_at = :updated_at WHERE id = :id"
    with get_conn() as conn:
        conn.execute(sql, params)
        conn.commit()
    return get_order_by_id(order_id)


def delete_order(order_id: int) -> bool:
    with get_conn() as conn:
        conn.execute("DELETE FROM orders WHERE id = ?", (order_id,))
        conn.commit()
    return True


def get_order_stats(account_key: Optional[str] = None) -> Dict[str, Any]:
    auto_cancel_expired_orders(account_key=account_key)
    with get_conn() as conn:
        sql = "SELECT status, order_money, rebate_money FROM orders WHERE 1=1"
        params: Dict[str, Any] = {}
        if account_key:
            sql += " AND account_key = :account_key"
            params["account_key"] = account_key
        rows = conn.execute(sql, params).fetchall()

    total_orders = len([r for r in rows if r["status"] != "cancelled"])
    completed_orders = sum(1 for r in rows if r["status"] == "completed")
    pending_orders = sum(1 for r in rows if r["status"] in ("pending", "auditing"))
    total_rebate = sum(r["rebate_money"] for r in rows if r["status"] == "completed")
    pending_rebate = sum(r["rebate_money"] for r in rows if r["status"] in ("pending", "auditing"))
    total_spent = sum(r["order_money"] for r in rows if r["status"] == "completed")

    return {
        "total_orders": total_orders,
        "completed_orders": completed_orders,
        "pending_orders": pending_orders,
        "total_rebate": round(total_rebate, 2),
        "pending_rebate": round(pending_rebate, 2),
        "total_spent": round(total_spent, 2)
    }


def get_dashboard_chart_data(account_key: Optional[str] = None) -> Dict[str, Any]:
    auto_cancel_expired_orders(account_key=account_key)
    import datetime
    with get_conn() as conn:
        sql = "SELECT status, platform, order_money, rebate_money, created_at, order_time FROM orders WHERE 1=1"
        params: Dict[str, Any] = {}
        if account_key:
            sql += " AND account_key = :account_key"
            params["account_key"] = account_key
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]

    today = datetime.date.today()
    days_map = {}
    for i in range(6, -1, -1):
        d = today - datetime.timedelta(days=i)
        d_str = d.strftime("%m-%d")
        days_map[d.strftime("%Y-%m-%d")] = {
            "date": d_str,
            "full_date": d.strftime("%Y-%m-%d"),
            "rebate": 0.0,
            "orders": 0,
            "spent": 0.0
        }

    status_counts = {
        "completed": 0,
        "auditing": 0,
        "pending": 0,
        "rejected": 0,
        "cancelled": 0
    }
    platform_counts = {
        "meituan": 0,
        "eleme": 0,
        "jingdong": 0
    }

    today_str = today.strftime("%Y-%m-%d")
    today_rebate = 0.0
    today_orders = 0
    today_savings_retail = 0.0
    today_completed_orders = 0
    today_completed_rebate = 0.0
    today_pending_orders = 0
    today_pending_rebate = 0.0

    for r in rows:
        st = r.get("status") or "pending"
        plat = r.get("platform") or "meituan"
        if plat == "jd":
            plat = "jingdong"
        elif plat == "taobao":
            plat = "eleme"
        rebate = float(r.get("rebate_money") or 0.0)
        spent = float(r.get("order_money") or 0.0)

        if st in status_counts:
            status_counts[st] += 1
        else:
            status_counts["pending"] += 1

        if plat in platform_counts:
            platform_counts[plat] += 1
        else:
            platform_counts["meituan"] += 1

        row_date = None
        c_at = r.get("created_at") or ""
        if len(c_at) >= 10:
            row_date = c_at[:10]
        elif r.get("order_time"):
            try:
                row_date = datetime.datetime.fromtimestamp(r["order_time"]).strftime("%Y-%m-%d")
            except Exception:
                pass

        if row_date in days_map:
            if st != "cancelled":
                days_map[row_date]["orders"] += 1
                days_map[row_date]["spent"] = round(days_map[row_date]["spent"] + spent, 2)
            if st == "completed":
                days_map[row_date]["rebate"] = round(days_map[row_date]["rebate"] + rebate, 2)

        if row_date == today_str:
            if st != "cancelled":
                today_orders += 1
            if st == "completed":
                today_completed_orders += 1
                today_completed_rebate = round(today_completed_rebate + rebate, 2)
                today_savings_retail = round(today_savings_retail + spent, 2)
            elif st in ("pending", "auditing"):
                today_pending_orders += 1
                today_pending_rebate = round(today_pending_rebate + rebate, 2)

    trend_list = [days_map[k] for k in sorted(days_map.keys())]

    status_dist = [
        {"name": "已到账", "value": status_counts["completed"], "color": "#10B981"},
        {"name": "审核中", "value": status_counts["auditing"], "color": "#3B82F6"},
        {"name": "待提交", "value": status_counts["pending"], "color": "#F59E0B"},
        {"name": "已驳回", "value": status_counts["rejected"], "color": "#EF4444"},
        {"name": "已取消", "value": status_counts["cancelled"], "color": "#9CA3AF"},
    ]

    platform_dist = [
        {"name": "美团外卖", "value": platform_counts["meituan"], "color": "#FFC300"},
        {"name": "饿了么/淘宝", "value": platform_counts["eleme"], "color": "#0091FF"},
        {"name": "京东外卖", "value": platform_counts["jingdong"], "color": "#E1251B"},
    ]

    return {
        "trend": trend_list,
        "status_distribution": status_dist,
        "platform_distribution": platform_dist,
        "today_summary": {
            "today_rebate": today_completed_rebate,
            "today_orders": today_orders,
            "today_completed_orders": today_completed_orders,
            "today_pending_orders": today_pending_orders,
            "today_savings": today_completed_rebate,
            "today_savings_retail": today_savings_retail,
        }
    }


