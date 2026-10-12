"""
FastAPI RESTful 路由
完全对齐前端调用，彻底废弃卡密、积分与到期限制（全免开放）
"""
import time
from datetime import datetime, timedelta, timezone
import uuid
import re
import math
import json
import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from fastapi import APIRouter, HTTPException, Query, Body
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

logger = logging.getLogger("xiaocan.api")

TZ_BJ = timezone(timedelta(hours=8))

from ..models import database as db
from ..core import scheduler, notifier, clawbot_client, time_service
from ..core.jwt_utils import extract_token_from_text, decode_jwt_payload, extract_city_code_from_text, extract_silk_id_from_payload, extract_user_id_from_payload, extract_silk_id_from_text
from ..core.proxy_sniffer import sniffer
import sys
_IS_WINDOWS = sys.platform.startswith("win")
if _IS_WINDOWS:
    from ..core.wechat_scanner import scan_wechat_credentials, wechat_listener
else:
    # Linux/macOS: 微信内存扫描是 Windows 专属，提供空实现
    def scan_wechat_credentials(): return None
    class _DummyListener:
        def start(self, *a, **kw): pass
        def stop(self, *a, **kw): pass
        def get_status(self): return {"status": "unsupported", "is_running": False}
    wechat_listener = _DummyListener()
from ..core.tianditu import tianditu_client
from ..protocol.client import XiaoCanClient, is_waf_blocked, get_waf_cooldown_seconds

router = APIRouter(prefix="/api")
client = XiaoCanClient()

# 任务元数据定义 (涵盖日常、秒杀与资产守护共 15 项全量对齐)
TASK_META = {
    # --- 日常打卡与元宝任务 (daily) ---
    "yb_task": {"id": "yb_task", "label": "领天天赚元宝", "tip": "自动完成社群打卡、电商浏览、领券加赠及天天打卡，一键聚拢元宝", "category": "daily", "default_time": "08:05"},
    "yb_sign": {"id": "yb_sign", "label": "天天赚元宝签到", "tip": "每日天天赚元宝独立签到打卡", "category": "daily", "default_time": "08:10"},
    "collect_points": {"id": "collect_points", "label": "收取未收元宝", "tip": "晚间睡前自动收取成熟气泡元宝与已完成任务奖励，防过期", "category": "daily", "default_time": "23:59"},
    "redpack_rain": {"id": "redpack_rain", "label": "整点红包雨", "tip": "每日六场整点放量红包雨自动接入与高频额度抓取", "category": "daily", "default_time": "10:00", "fixed_time": True, "time_label": "六场 10/11/12/14/16/19点"},
    "group_lottery": {"id": "group_lottery", "label": "免费开红包与抽奖", "tip": "自动领取全部7类免费开红包机会并开启，支持阶梯进度与防风控间隔", "category": "daily", "default_time": "07:40"},
    "flash_sale": {"id": "flash_sale", "label": "元宝秒杀抢券", "tip": "元宝商城限量秒杀抢券，支持自定义商品ID", "category": "daily", "default_time": "10:00"},

    # --- 会员秒杀与特权抢券 (member) ---
    "svip_rebate": {"id": "svip_rebate", "label": "抢SVIP专属返利券", "tip": "SVIP专属大额返利券每日 09:00:00 准点秒杀", "category": "member", "vip": "SVIP2-6", "default_time": "09:00", "fixed_time": True, "time_label": "09:00 固定"},
    "brand_flash": {"id": "brand_flash", "label": "抢SVIP大牌券", "tip": "每日 09:30:00 准点抢额外放量大牌神券（独立通道，绝不消耗每月保底张数）", "category": "member", "vip": "SVIP2-6 / VIP5+", "default_time": "09:30", "fixed_time": True, "time_label": "09:30 固定"},
    "media_vip": {"id": "media_vip", "label": "抢每月影音VIP", "tip": "影音会员VIP周卡(网易云音乐/腾讯视频等)每日三场 (10/17/20点)，智能检测本月余量与历史记录", "category": "member", "vip": "SVIP4-6 / VIP5-6", "default_time": "10:00", "fixed_time": True, "time_label": "三场 10/17/20点"},
    "free_order": {"id": "free_order", "label": "抢每月免单券", "tip": "外卖霸王餐全额免单券每日 14:00:00 准点秒杀", "category": "member", "vip": "SVIP5-6 / VIP6", "default_time": "14:00", "fixed_time": True, "time_label": "14:00 固定"},
    "vip_expand": {"id": "vip_expand", "label": "会员每日签到", "tip": "会员每日打卡 + VIP专属签到抽奖 + 里程碑核验 + 专属成长礼包", "category": "member", "vip": "VIP2+ / SVIP", "default_time": "08:30"},

    # --- 资产守护与双返利监控 (custom) ---
    "expire_remind": {"id": "expire_remind", "label": "凭据/JWT到期预警", "tip": "监控小蚕凭据与JWT有效期，提前推送防掉线", "category": "custom", "default_time": "08:00"},
    "coupon_remind": {"id": "coupon_remind", "label": "卡券/红包到期提醒", "tip": "每日早间核查账户中即将失效的特权券并主动推送", "category": "custom", "default_time": "08:30"},
    "dual_rebate_monitor": {
        "id": "dual_rebate_monitor",
        "label": "美团同店双返利监控",
        "tip": "准点扫描美团同店双返利商户，对齐每半小时放量开抢整点(:00/:30)，支持下架二次核验防抖",
        "category": "custom",
        "default_time": "*/10 9-22 * * *",
        "fixed_time": True,
        "time_label": "30分钟整点准时巡检"
    },
}

TASK_PARAM_DEFS = {
    "dual_rebate_monitor": [
        {"key": "interval_minutes", "label": "巡检周期间隔（分钟）", "def": 10, "type": "number", "step": "5", "min": 5, "max": 30, "hint": "巡检间隔周期：默认 10 分钟（整点00/30分必测，并在10/20分补充捡漏），可选 15、30 或 5 分钟，均保证00/30分准点开火"},
        {"key": "active_hours_only", "label": "仅在营业时段巡检 (09:00~23:00)", "def": True, "type": "check", "hint": "开启后仅在营业时段运行 (09:00~23:00)，夜间 23:00~09:00 自动休眠静默，防风控与打扰"},
        {"key": "confirm_removal", "label": "下架二次核验保护", "def": True, "type": "check", "hint": "商户缺失时必须连续 2 轮巡检均未搜出才确认为已下架，杜绝接口偶发异常导致的误报"},
        {"key": "max_stores", "label": "扫描商户范围上限（家）", "def": 300, "type": "number", "step": "50", "min": 100, "max": 800, "hint": "按距离由近及远流式扫描的美团商户数上限（建议 200~500 家）"},
        {"key": "keyword", "label": "关键词优先过滤（可选）", "def": "", "type": "string", "hint": "指定优先扫描的品类或品牌关键词，留空则扫描全商圈"},
        {"key": "notify_on_initial", "label": "首轮全量基准推送", "def": True, "type": "check", "hint": "任务开启或跨天重置后的第一轮扫描，是否推送完整基准商户清单"}
    ],
    "redpack_rain": [
        {"key": "click_num", "label": "上报抓取点击数", "def": 15, "type": "number", "step": "1", "min": 1, "max": 99, "hint": "模拟在红包雨过程中成功点击并抓取的红包数量（参考默认值15）"},
        {"key": "jitter", "label": "点击随机抖动值", "def": 5, "type": "number", "step": "1", "min": 0, "max": 20, "hint": "实际提交数量将在 click_num ± jitter 之间浮动（如 10~20 次），拟真防风控"},
        {"key": "game_wait_sec", "label": "雨落拟真等待时长（秒）", "def": 28, "type": "number", "step": "1", "min": 10, "max": 45, "hint": "成功接入场次后模拟红包下落和交互等待的时间（实测约 28~35 秒）后再上报结算"}
    ],
    "group_lottery": [
        {"key": "draw_sec", "label": "转盘抽奖间隔秒数", "def": 0.35, "type": "number", "step": "0.05", "min": 0.1, "max": 5.0, "hint": "每次抽奖间隔时间（建议 0.2~0.5 秒），兼顾极速执行与拟真防风控"}
    ],
    "vip_expand": [
        {"key": "receive_help", "label": "膨胀助力（可被助力）", "def": True, "type": "check", "hint": "开启后允许参与会员膨胀金互助助力"}
    ],
    "expire_remind": [
        {"key": "warn_days", "label": "提前预警天数", "def": 3, "type": "number", "step": "1", "min": 0, "hint": "凭据剩余有效天数小于等于此值时触发预警"},
        {"key": "jwt_check", "label": "核验 JWT 有效期", "def": True, "type": "check", "hint": "主动解码并核验小蚕 JWT 真实生命周期"}
    ],
    "coupon_remind": [
        {"key": "warn_hours", "label": "提前预警小时数", "def": 24, "type": "number", "step": "1", "min": 1, "hint": "券过期前多少小时发送到期提醒"}
    ],
    "flash_sale": [
        {"key": "goods_ids", "label": "秒杀商品ID（逗号分隔）", "def": "", "type": "string", "hint": "指定优先秒杀的商品/券ID，留空则默认场次"}
    ]
}


# ---------------- 1. 账号托管接口 ---------------- #

@router.get("/accounts")
async def get_accounts():
    """获取所有托管小蚕账号 (无车位上限，自动增量补齐未同步账号资产与 Silk ID)"""
    accounts = db.get_all_accounts()
    for acc in accounts:
        # 自愈修复：若账号缺少 silk_id，自动直连小蚕官方拉取并补全
        if acc.get("token") and not acc.get("silk_id"):
            try:
                enriched = await _enrich_and_save_account(acc)
                acc.update(enriched)
            except Exception:
                pass
        elif acc.get("token") and acc.get("withdraw_total", 0) == 0 and acc.get("completed_number", 0) == 0:
            try:
                enriched = await _enrich_and_save_account(acc)
                acc.update(enriched)
            except Exception:
                pass
    return {"ok": True, "accounts": accounts, "total": len(accounts)}


@router.post("/accounts")
async def create_or_update_account(data: Dict[str, Any] = Body(...)):
    """手动添加或更新小蚕账号（支持自动去重合并，防止重复生成多账号）"""
    token = (data.get("token") or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="Token 不能为空")

    # 尝试真实解码 JWT Payload 提取真实 Silk ID 与到期时间
    is_valid, payload, msg = decode_jwt_payload(token)
    if is_valid and payload:
        if not data.get("silk_id"):
            data["silk_id"] = extract_silk_id_from_payload(payload)
        exp = payload.get("exp")
        if exp and not data.get("expires_at"):
            try:
                data["expires_at"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(exp)))
            except Exception:
                pass

    if not data.get("client_mode"):
        data["client_mode"] = "app"

    saved = await _enrich_and_save_account(data)
    key = saved["key"]

    # 默认初始化开启核心任务
    configs = db.get_task_configs(key)
    if not configs:
        for tid, meta in TASK_META.items():
            default_on = tid in ("yb_sign", "yb_task", "vip_expand", "brand_flash")
            db.save_task_config(key, tid, enabled=default_on, cron_time=meta["default_time"])

    scheduler.reload_schedules()
    action_desc = "更新" if saved.get("is_updated") else "接入"
    return {
        "ok": True,
        "account": saved,
        "is_updated": saved.get("is_updated", False),
        "message": f"成功{action_desc}小蚕账号【{saved.get('nickname', '小蚕用户')}】！"
    }


@router.put("/accounts/{key}")
@router.post("/accounts/{key}/profile")
async def update_account_profile_endpoint(key: str, data: Dict[str, Any] = Body(...)):
    """更新小蚕账号资料 (自定义昵称、Silk ID、城市、会员等级、到期时间等)"""
    acc = db.get_account_by_key(key)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")
    updated = db.update_account_profile(key, data)
    return {"ok": True, "account": updated, "message": "账号资料已更新"}


@router.get("/accounts/{key}/notify-config")
async def get_account_notify_config_endpoint(key: str):
    """获取指定账号的通知模式与独立通道配置"""
    acc = db.get_account_by_key(key)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")
    cfg_data = db.get_account_notify_config(key)
    return {"ok": True, "data": cfg_data}


@router.post("/accounts/{key}/notify-config")
async def save_account_notify_config_endpoint(key: str, data: Dict[str, Any] = Body(...)):
    """保存指定账号的通知模式与独立通道配置"""
    acc = db.get_account_by_key(key)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")
    mode = str(data.get("notify_mode") or "global").strip()
    config = data.get("notify_config") or {}
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except Exception:
            config = {}
    ok = db.set_account_notify_config(key, mode, config)
    if ok:
        return {"ok": True, "message": "账号专属通知设置已保存"}
    raise HTTPException(status_code=500, detail="保存通知配置失败")


@router.post("/accounts/{key}/test-notify")
async def test_account_notify_endpoint(key: str, data: Dict[str, Any] = Body(...)):
    """即时测试指定账号的专属通知通道"""
    acc = db.get_account_by_key(key)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")
    mode = str(data.get("notify_mode") or "global").strip()
    config = data.get("notify_config") or {}
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except Exception:
            config = {}
    res = await notifier.test_account_channel(key, mode, config)
    return res


@router.post("/accounts/{key}/sync")
async def sync_account_endpoint(key: str):
    """一键同步指定小蚕账号的官方档案与钱包资产"""
    acc = db.get_account_by_key(key)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")
    try:
        updated = await _enrich_and_save_account(acc)
        return {"ok": True, "account": updated, "message": "账号资产与官方档案已成功同步！"}
    except Exception as e:
        logger.error(f"同步账号资产失败: {e}")
        return {"ok": False, "message": f"同步失败: {str(e)}"}


@router.get("/accounts/{key}/detail")
async def get_account_detail_endpoint(key: str):
    """获取指定小蚕账号的全面详情（包含个人信息、元宝状态、特权卡券列表与可用红包列表）"""
    acc = db.get_account_by_key(key)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")

    token = acc.get("token")
    silk_id = acc.get("silk_id")
    user_id = acc.get("user_id")
    city_code = acc.get("city_code") or 440303

    card_stats = {"can_use_number": 0, "expiring_soon_number": 0}
    cards = []
    redpack_stats = {"num": 0}
    redpacks = []
    user_info = {}
    task_info = {}

    if token:
        # 1. 抓取用户卡券数量与卡券列表
        try:
            cnum_res = await client.get_user_card_number(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code)
            if isinstance(cnum_res, dict):
                card_stats["can_use_number"] = cnum_res.get("can_use_number", 0)
                card_stats["expiring_soon_number"] = cnum_res.get("expiring_soon_number", 0)
        except Exception as e:
            logger.warning(f"获取账号卡券统计失败: {e}")

        try:
            clist_res = await client.get_user_card_list(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code, status=0, offset=0, number=100)
            if isinstance(clist_res, dict) and clist_res.get("list"):
                cards = clist_res["list"]
        except Exception as e:
            logger.warning(f"获取账号可用卡券列表失败: {e}")

        # 2. 抓取红包数量与红包列表
        try:
            rp_res = await client.get_user_redpack_num(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code)
            if isinstance(rp_res, dict):
                redpack_stats["num"] = rp_res.get("num", 0)
        except Exception as e:
            logger.warning(f"获取账号红包数量失败: {e}")

        try:
            rplist_res = await client.get_app_redpack_list(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code, page=1, page_size=50)
            if isinstance(rplist_res, dict) and rplist_res.get("unused_items"):
                redpacks = rplist_res["unused_items"]
        except Exception as e:
            logger.warning(f"获取账号红包列表失败: {e}")

        # 3. 抓取用户详情 (VIP, 余额, 头像)
        try:
            uinfo_res = await client.get_user_info(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code)
            if isinstance(uinfo_res, dict) and uinfo_res.get("user_info"):
                user_info = uinfo_res["user_info"]

                # 实时同步最新会员与资产信息至数据库
                up_fields = {}
                vinfo = user_info.get("vip_level_info") or {}
                if vinfo.get("new_level") is not None:
                    up_fields["vip_level"] = int(vinfo["new_level"])
                    up_fields["is_plus"] = 1 if vinfo.get("is_plus") else 0
                    up_fields["vip_score"] = int(vinfo.get("score") or 0)
                    up_fields["vip_expired_at"] = int(vinfo.get("expired_at") or 0)
                elif user_info.get("client_vip", {}).get("level") is not None:
                    up_fields["vip_level"] = int(user_info["client_vip"]["level"])
                elif user_info.get("now_vip_info", {}).get("level") is not None:
                    up_fields["vip_level"] = int(user_info["now_vip_info"]["level"])

                if user_info.get("silk") is not None:
                    up_fields["silk"] = int(user_info["silk"])
                if user_info.get("withdrawing") is not None:
                    up_fields["withdrawing"] = int(user_info["withdrawing"])
                if user_info.get("withdraw_total") is not None:
                    up_fields["withdraw_total"] = int(user_info["withdraw_total"])
                if user_info.get("completed_number") is not None:
                    up_fields["completed_number"] = int(user_info["completed_number"])
                if user_info.get("nickname"):
                    up_fields["nickname"] = user_info["nickname"]
                if user_info.get("avatar"):
                    up_fields["avatar"] = user_info["avatar"]
                if user_info.get("phone"):
                    up_fields["phone"] = user_info["phone"]
                if user_info.get("real_name"):
                    up_fields["real_name"] = user_info["real_name"]

                if up_fields:
                    updated_acc = db.update_account_profile(key, up_fields)
                    if updated_acc:
                        acc = updated_acc
        except Exception as e:
            logger.warning(f"获取账号用户详情失败: {e}")

        # 4. 抓取天天赚元宝数据
        try:
            t_res = await client.get_user_task_v2(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code)
            if isinstance(t_res, dict) and t_res.get("data"):
                task_info = t_res["data"]
        except Exception as e:
            logger.warning(f"获取账号元宝数据失败: {e}")

    return {
        "ok": True,
        "account": acc,
        "user_info": user_info,
        "task_info": task_info,
        "card_stats": card_stats,
        "cards": cards,
        "redpack_stats": redpack_stats,
        "redpacks": redpacks
    }


@router.get("/accounts/{key}/cards")
async def get_account_cards_endpoint(key: str, status: int = Query(0, description="0未使用, 1已使用, 2已过期")):
    """按状态获取特权卡券列表 (0未使用, 1已使用, 2已过期)"""
    acc = db.get_account_by_key(key)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")
    token = acc.get("token")
    silk_id = acc.get("silk_id")
    user_id = acc.get("user_id")
    city_code = acc.get("city_code") or 440303
    cards = []
    if token:
        try:
            res = await client.get_user_card_list(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code, status=status, offset=0, number=100)
            cards = res.get("list") or []
        except Exception as e:
            logger.warning(f"获取卡券失败: {e}")
    return {"ok": True, "cards": cards, "status": status}


@router.delete("/accounts/{key}")
async def remove_account(key: str):
    """删除/退出小蚕账号，彻底清除内存会话与关联数据"""
    db.delete_account(key)
    remaining = db.get_all_accounts()
    if not remaining:
        with db.get_conn() as conn:
            conn.execute("DELETE FROM orders WHERE account_key = ? OR account_key = 'acc_default'", (key,))
            conn.execute("DELETE FROM store_appointments WHERE account_key = ?", (key,))
            conn.commit()
    scheduler.reload_schedules()
    return {"ok": True, "message": "账号已安全退出并清除数据"}




# ---------------- 2. 真实 Token 智能解析与校验 ---------------- #

@router.post("/accounts/parse-token")
async def parse_token_endpoint(data: Dict[str, Any] = Body(...)):
    """从用户粘贴的任意抓包文本、cURL 命令或请求头中智能提取真实 Token 并解码，自动补全真实 Silk ID"""
    raw_text = data.get("raw_text") or ""
    token = extract_token_from_text(raw_text)
    if not token:
        return {
            "ok": False,
            "message": "未能从输入文本中检测到有效的小蚕凭证（需包含 eyJ 开头的 JWT 或 x-Sivir 字段）"
        }

    is_valid, payload, msg = decode_jwt_payload(token)
    silk_id = extract_silk_id_from_text(raw_text) or extract_silk_id_from_payload(payload)
    user_id = extract_user_id_from_payload(payload)
    exp = payload.get("exp")
    exp_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(exp))) if exp else "长期有效"
    city_code = extract_city_code_from_text(raw_text) or 420100

    nickname = ""
    avatar = ""
    vip_level = 5

    # 尝试直连小蚕官方接口获取真实官方资料与精确 Silk ID (解决 Linux/手动添加缺失 silk_id 问题)
    if is_valid and token:
        try:
            info_res = await client.get_user_info(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code)
            uinfo = info_res.get("user_info") or {}
            official_silk_id = str(
                uinfo.get("silk_id")
                or uinfo.get("id")
                or info_res.get("silk_id")
                or ""
            ).strip()
            if official_silk_id and official_silk_id != "0":
                silk_id = official_silk_id
            if uinfo.get("nickname"):
                nickname = uinfo["nickname"]
            if uinfo.get("avatar"):
                avatar = uinfo["avatar"]
            vinfo = uinfo.get("vip_level_info") or {}
            if vinfo.get("new_level") is not None:
                vip_level = vinfo["new_level"]
        except Exception as e:
            logger.info(f"解析凭证时直连小蚕查询官方资料跳过: {e}")

    # 优先查重匹配已有账号资料（若已存在则直接读取昵称、头像、城市）
    existing = db.find_account(silk_id=silk_id, user_id=user_id, token=token)
    if existing:
        nickname = existing.get("nickname") or nickname or (f"小蚕用户_{silk_id[-4:]}" if silk_id else "小蚕用户")
        avatar = existing.get("avatar") or avatar
        city_code = existing.get("city_code") or city_code
        vip_level = existing.get("vip_level") or vip_level
    else:
        if not nickname:
            nickname = f"小蚕用户_{silk_id[-4:]}" if silk_id else (f"小蚕用户_{user_id[-4:]}" if user_id else "小蚕微信用户")

    return {
        "ok": True,
        "token": token,
        "silk_id": silk_id,
        "user_id": user_id,
        "is_valid": is_valid,
        "message": msg,
        "exp_date": exp_date,
        "city_code": city_code,
        "nickname": nickname,
        "avatar": avatar,
        "vip_level": vip_level,
        "client_mode": "app"
    }


@router.post("/accounts/verify-token")
async def verify_token_endpoint(data: Dict[str, Any] = Body(...)):
    """验证 Token 真实有效性与过期状态"""
    token = (data.get("token") or "").strip()
    raw_text = data.get("raw_text") or ""
    if not token:
        raise HTTPException(status_code=400, detail="Token 不能为空")

    is_valid, payload, msg = decode_jwt_payload(token)
    if not is_valid:
        return {"ok": False, "valid": False, "message": msg}

    silk_id = extract_silk_id_from_text(raw_text) or extract_silk_id_from_payload(payload)
    user_id = extract_user_id_from_payload(payload)
    if not silk_id and token:
        try:
            info_res = await client.get_user_info(token=token, silk_id="", user_id=user_id)
            uinfo = info_res.get("user_info") or {}
            official_silk_id = str(uinfo.get("silk_id") or uinfo.get("id") or info_res.get("silk_id") or "").strip()
            if official_silk_id and official_silk_id != "0":
                silk_id = official_silk_id
        except Exception:
            pass

    exp = payload.get("exp")
    exp_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(exp))) if exp else "长期有效"
    return {
        "ok": True,
        "valid": True,
        "message": f"小蚕凭据有效（Silk ID: {silk_id or '已解析'}，有效期至: {exp_str}）",
        "silk_id": silk_id,
        "expires_at": exp_str
    }


# ---------------- 3. 电脑微信无感内存直连 (免配置·首选) ---------------- #

async def _enrich_and_save_account(cred: Dict[str, Any]) -> Dict[str, Any]:
    token = cred.get("token", "")
    silk_id = cred.get("silk_id") or ""
    user_id = cred.get("user_id") or ""
    expires_at = cred.get("expires_at") or "长期有效"
    raw_nick = cred.get("nickname") or ""
    if not raw_nick or raw_nick in ("xcmap", "小蚕用户", "小蚕微信用户"):
        nickname = f"小蚕用户_{silk_id[-4:]}" if (silk_id and len(silk_id) >= 4) else "小蚕微信用户"
    else:
        nickname = raw_nick

    avatar = cred.get("avatar") or ""
    vip_level = cred.get("vip_level") or 1
    is_plus = int(cred.get("is_plus", 0))
    phone = cred.get("phone") or ""
    real_name = cred.get("real_name") or ""
    vip_score = cred.get("vip_score") or 0
    vip_expired_at = cred.get("vip_expired_at") or 0
    silk = int(cred.get("silk") or 0)
    withdrawing = int(cred.get("withdrawing") or 0)
    withdraw_total = int(cred.get("withdraw_total") or 0)
    completed_number = int(cred.get("completed_number") or 0)
    yb_point = int(cred.get("yb_point") or 0)
    unreceived_points = int(cred.get("unreceived_points") or 0)

    # 尝试直连小蚕官方获取真实微信昵称、头像和官方会员档案及原生资产 (自动补全真实 Silk ID)
    if token:
        try:
            info_res = await client.get_user_info(token=token, silk_id=silk_id, user_id=user_id)
            uinfo = info_res.get("user_info") or {}

            # 关键：从官方用户详情中提取真正的 Silk ID 回填入库
            official_silk_id = str(
                uinfo.get("silk_id")
                or uinfo.get("id")
                or info_res.get("silk_id")
                or ""
            ).strip()
            if official_silk_id and official_silk_id != "0":
                silk_id = official_silk_id
                if not raw_nick or raw_nick in ("xcmap", "小蚕用户", "小蚕微信用户"):
                    if not uinfo.get("nickname"):
                        nickname = f"小蚕用户_{silk_id[-4:]}"

            if uinfo.get("nickname"):
                nickname = uinfo["nickname"]
            if uinfo.get("avatar"):
                avatar = uinfo["avatar"]
            if uinfo.get("phone"):
                phone = uinfo["phone"]
            if uinfo.get("real_name"):
                real_name = uinfo["real_name"]
            if uinfo.get("silk") is not None:
                silk = int(uinfo["silk"])
            if uinfo.get("withdrawing") is not None:
                withdrawing = int(uinfo["withdrawing"])
            if uinfo.get("withdraw_total") is not None:
                withdraw_total = int(uinfo["withdraw_total"])
            if uinfo.get("completed_number") is not None:
                completed_number = int(uinfo["completed_number"])

            # 优先从小蚕官方 vip_level_info 结构读取原生会员属性
            vinfo = uinfo.get("vip_level_info") or {}
            if vinfo.get("new_level") is not None:
                vip_level = vinfo["new_level"]
                is_plus = 1 if vinfo.get("is_plus") else 0
                vip_score = vinfo.get("score") or 0
                vip_expired_at = vinfo.get("expired_at") or 0
            elif uinfo.get("client_vip", {}).get("level") is not None:
                vip_level = uinfo["client_vip"]["level"]
            elif uinfo.get("now_vip_info", {}).get("level") is not None:
                vip_level = uinfo["now_vip_info"]["level"]
        except Exception as e:
            logger.info(f"同步小蚕官方用户详情失败 (使用默认): {e}")

        # 同步天天赚元宝原生资产
        try:
            task_res = await client.get_user_task_v2(token=token, silk_id=silk_id, user_id=user_id)
            tdata = task_res.get("data") or {}
            if tdata.get("yb_point") is not None:
                yb_point = int(tdata["yb_point"])
            if tdata.get("unreceived_points") is not None:
                unreceived_points = int(tdata["unreceived_points"])
        except Exception as e:
            logger.info(f"同步小蚕官方元宝资产失败: {e}")

    acc_payload = {
        "silk_id": silk_id,
        "user_id": user_id,
        "nickname": nickname,
        "avatar": avatar,
        "token": token,
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
        "client_mode": cred.get("client_mode") or "app",
        "city_code": cred.get("city_code") or 440303,
        "city_name": cred.get("city_name") or "深圳",
        "longitude": cred.get("longitude") or "114.13166",
        "latitude": cred.get("latitude") or "22.548361",
        "expires_at": expires_at
    }
    saved = db.save_account(acc_payload)
    key = saved["key"]

    configs = db.get_task_configs(key)
    if not configs:
        for tid, meta in TASK_META.items():
            default_on = tid in ("yb_sign", "yb_task", "vip_expand", "brand_flash")
            db.save_task_config(key, tid, enabled=default_on, cron_time=meta["default_time"])
    scheduler.reload_schedules()
    return saved


@router.post("/accounts/{key}/sync")
async def sync_account_endpoint(key: str):
    """一键从官方网关深度同步账号资产与资料 (微信昵称、头像、蚕豆余额、累计提现、成单量、元宝)"""
    acc = db.get_account_by_key(key)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")
    token = acc.get("token")
    if not token:
        raise HTTPException(status_code=400, detail="账号未配置有效 Token")

    enriched = await _enrich_and_save_account(acc)
    return {
        "ok": True,
        "account": enriched,
        "message": f"成功同步账号【{enriched.get('nickname')}】的最新官方资产与资料！"
    }


def _save_scanned_account_sync(cred: Dict[str, Any]) -> Dict[str, Any]:
    """供后台线程安全同步调用的保存账号方法 (保证自动调用官方接口同步真实微信资料)"""
    import asyncio
    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            asyncio.create_task(_enrich_and_save_account(cred))
        else:
            return asyncio.run(_enrich_and_save_account(cred))
    except Exception as e:
        logger.error(f"后台线程同步账号详情异常: {e}")

    token = cred.get("token", "")
    silk_id = cred.get("silk_id") or ""
    return db.save_account({
        "silk_id": silk_id,
        "user_id": cred.get("user_id") or "",
        "nickname": cred.get("nickname") or f"小蚕用户_{silk_id[-4:] if silk_id else '8888'}",
        "avatar": cred.get("avatar") or "",
        "token": token,
        "vip_level": 1,
        "city_code": cred.get("city_code") or 440303,
        "city_name": cred.get("city_name") or "深圳",
        "longitude": cred.get("longitude") or "114.13166",
        "latitude": cred.get("latitude") or "22.548361",
        "expires_at": cred.get("expires_at") or "长期有效"
    })


@router.post("/accounts/wechat-scan")
async def scan_wechat_endpoint():
    """
    立即从当前电脑微信小程序进程中提取小蚕真实鉴权凭证
    0 配置、0 证书、0 代理、零断网风险
    """
    cred = scan_wechat_credentials()
    if not cred or not cred.get("token"):
        return {
            "ok": False,
            "message": "未能从电脑微信中检测到【小蚕霸王餐】小程序凭证。请先在电脑微信中点击打开一次小蚕小程序，然后点击重试。"
        }

    saved = await _enrich_and_save_account(cred)
    nick = saved.get("nickname", "小蚕用户")
    silk = saved.get("silk_id")
    silk_text = f" (Silk ID: {silk})" if silk else ""
    return {
        "ok": True,
        "account": saved,
        "message": f"成功从电脑微信提取到小蚕账号【{nick}】{silk_text}！"
    }


@router.post("/accounts/wechat-listener/start")
async def start_wechat_listener(data: Optional[Dict[str, Any]] = Body(None)):
    """开启电脑微信后台自动感知监听（当用户打开微信小程序时，秒级自动抓取并入库）"""
    timeout = int((data or {}).get("timeout", 120))
    wechat_listener.start(timeout=timeout, on_captured_callback=_save_scanned_account_sync)
    return {
        "ok": True,
        "message": "已开启微信小程序自动监听，请在电脑微信中点击打开一次【小蚕霸王餐】小程序",
        "status": wechat_listener.get_status()
    }


@router.post("/accounts/wechat-listener/stop")
async def stop_wechat_listener():
    """停止电脑微信自动感知监听"""
    wechat_listener.stop()
    return {"ok": True, "message": "已停止微信自动监听", "status": wechat_listener.get_status()}


@router.get("/accounts/wechat-listener/status")
async def get_wechat_listener_status():
    """获取电脑微信自动监听状态"""
    return {"ok": True, "status": wechat_listener.get_status()}


# 兼容旧版 win_capture 与 proxy-capture 接口（无缝桥接至内存嗅探模式）
@router.post("/accounts/win_capture/start")
async def compat_win_capture_start(data: Optional[Dict[str, Any]] = Body(None)):
    timeout = int((data or {}).get("timeout", 120))
    wechat_listener.start(timeout=timeout, on_captured_callback=_save_scanned_account)
    return {
        "ok": True,
        "message": "微信直连监听已就绪，请在电脑微信中打开一次【小蚕霸王餐】小程序",
        "status": wechat_listener.get_status()
    }


@router.post("/accounts/win_capture/stop")
async def compat_win_capture_stop():
    wechat_listener.stop()
    return {"ok": True, "message": "微信抓取已停止", "status": wechat_listener.get_status()}


@router.get("/accounts/win_capture/status")
async def compat_win_capture_status():
    st = wechat_listener.get_status()
    return {
        "ok": True,
        "status": {
            "is_running": st.get("is_running", False),
            "status": st.get("status", "idle"),
            "account": st.get("account"),
            "is_win_proxy_active": False,
            "ca_installed": True
        }
    }


@router.post("/accounts/proxy-capture/start")
async def compat_proxy_start(data: Dict[str, Any] = Body(default={})):
    return await compat_win_capture_start(data)


@router.post("/accounts/proxy-capture/stop")
async def compat_proxy_stop():
    return await compat_win_capture_stop()


@router.get("/accounts/proxy-capture/status")
async def compat_proxy_status():
    return await compat_win_capture_status()


@router.get("/accounts/win_capture/ca_cert")
async def download_ca_cert():
    """下载小蚕助手 CA 根证书 (用于 HTTPS 信任)"""
    import os
    from fastapi.responses import FileResponse
    from ..core.cert_manager import cert_mgr
    ca_path = cert_mgr.get_ca_cert_path()
    if os.path.exists(ca_path):
        return FileResponse(ca_path, media_type="application/x-x509-ca-cert", filename="XiaoCan_Assistant_CA.crt")
    return {"ok": False, "message": "CA 证书尚未生成"}


@router.post("/accounts/win_capture/install_ca")
async def install_ca_cert():
    """一键触发 Windows 根证书安装确认"""
    from ..core.cert_manager import cert_mgr
    ok = cert_mgr.install_ca_cert()
    return {
        "ok": ok,
        "message": "已在系统桌面弹出 Windows 安全警告窗口，请点击【是(Y)】完成安装" if ok else "唤起证书安装失败，请手动下载证书安装",
        "ca_installed": cert_mgr.is_ca_installed()
    }


# ---------------- 3. 自动化任务开关与配置 ---------------- #

@router.get("/tasks")
async def get_tasks(account_key: str = Query(...)):
    """获取指定账号的任务配置列表 (包含动态参数与参数元数据模板)"""
    configs = {c["task_id"]: c for c in db.get_task_configs(account_key)}
    result = []

    for tid, meta in TASK_META.items():
        cfg = configs.get(tid, {})
        param_defs = TASK_PARAM_DEFS.get(tid, [])
        # 构建默认参数并合并已存储的参数
        merged_params = {}
        for pd in param_defs:
            merged_params[pd["key"]] = pd["def"]
        if isinstance(cfg.get("params"), dict):
            merged_params.update(cfg["params"])

        # 查询 APScheduler 中此任务的下一次真实触发时刻
        job_id = f"{account_key}_{tid}"
        next_run_time = None
        try:
            job = scheduler.scheduler.get_job(job_id)
            if job and getattr(job, "next_run_time", None):
                next_run_time = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

        result.append({
            "task_id": tid,
            "label": meta["label"],
            "tip": meta["tip"],
            "category": meta["category"],
            "vip": meta.get("vip", ""),
            "enabled": bool(cfg.get("enabled", 0)),
            "cron_time": cfg.get("cron_time") or meta["default_time"],
            "fixed_time": meta.get("fixed_time", False),
            "time_label": meta.get("time_label", ""),
            "next_run_time": next_run_time,
            "params": merged_params,
            "param_defs": param_defs
        })

    return {"ok": True, "tasks": result}


@router.post("/tasks/toggle")
async def toggle_task(data: Dict[str, Any] = Body(...)):
    """切换任务开关、保存执行时间与高级参数"""
    account_key = data["account_key"]
    task_id = data["task_id"]
    enabled = bool(data.get("enabled", False))
    cron_time = data.get("cron_time")
    params = data.get("params")

    if task_id == "dual_rebate_monitor" and enabled:
        from ..core.notifier import is_any_notify_channel_enabled
        if not is_any_notify_channel_enabled():
            raise HTTPException(
                status_code=400,
                detail="开启【美团同店双返利监控】任务前，必须先在【系统设置】中配置并启用至少一种通知渠道！"
            )

    db.save_task_config(account_key, task_id, enabled, cron_time, params)
    scheduler.reload_schedules()
    return {"ok": True, "message": f"任务 [{TASK_META.get(task_id, {}).get('label', task_id)}] 配置已更新生效"}


@router.post("/tasks/run")
async def run_task_immediately(data: Dict[str, Any] = Body(...)):
    """立即手动触发一次任务"""
    account_key = data.get("account_key")
    task_id = data.get("task_id")
    if not account_key:
        return {"ok": False, "msg": "未指定执行账号，请先选择或登录账号"}

    account = db.get_account_by_key(account_key)
    if not account:
        return {"ok": False, "msg": "当前账号不存在或已退出登录，无法执行自动化任务！"}

    res = await scheduler.execute_task_job(account_key, task_id, trigger_type="manual")
    return res


@router.post("/tasks/batch-run-daily")
async def batch_run_daily_tasks(data: Dict[str, Any] = Body(...)):
    """一键执行当前账号的高频日常任务 (元宝签到 + 领天天赚元宝 + 会员打卡 + 极速抽奖开红包)"""
    account_key = data.get("account_key")
    if not account_key:
        return {"ok": False, "message": "未指定执行账号"}

    account = db.get_account_by_key(account_key)
    if not account:
        return {"ok": False, "message": "账号不存在或未托管"}

    daily_task_ids = ["yb_sign", "yb_task", "vip_expand", "group_lottery"]
    results = []
    
    for tid in daily_task_ids:
        label = TASK_META.get(tid, {}).get("label", tid)
        try:
            res = await scheduler.execute_task_job(account_key, tid, trigger_type="batch_daily")
            results.append({
                "task_id": tid,
                "label": label,
                "ok": res.get("ok", False),
                "output": res.get("output", "")
            })
        except Exception as e:
            results.append({
                "task_id": tid,
                "label": label,
                "ok": False,
                "output": f"执行失败: {str(e)}"
            })

    all_ok = all(r["ok"] for r in results)
    nick = account.get("nickname") or "当前账号"
    return {
        "ok": True,
        "all_ok": all_ok,
        "results": results,
        "message": f"账号【{nick}】已完成全部日常打卡任务！" if all_ok else f"账号【{nick}】部分日常任务已执行，请查看执行结果"
    }



def _safe_val(v, default=None):
    """提取 Query 参数或默认值，兼容内部直接函数调用与 FastAPI 注入"""
    if hasattr(v, "default"):
        return v.default if v.default is not ... else default
    return v if v is not None else default


def _clean_emoji(text: Any) -> str:
    """去除文本中的所有 Emoji 符号、闪电符号与杂项表情字符，确保输出纯净文本"""
    if not text:
        return ""
    # 过滤 Unicode Emoji (\U00010000-\U0010ffff)、杂项符号与闪电 (\u2600-\u27bf, 包含 \u26a1 闪电等)、表情符号
    cleaned = re.sub(r'[\U00010000-\U0010ffff]|[\u2600-\u27bf]|[\u2300-\u23ff]|[\u2b50-\u2b55]|[\u200d]|[\ufe0f]', '', str(text))
    return cleaned.strip()


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
    """计算两经纬度之间的球面大圆直线距离 (米)"""
    R = 6371000  # 地球半径（米）
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return int(round(R * c))


def _parse_store_distance(
    p: Dict[str, Any],
    store_obj: Dict[str, Any],
    user_lat: Optional[float] = None,
    user_lon: Optional[float] = None,
    is_brand: bool = False
) -> Tuple[int, str]:
    """
    智能解析店铺距离：
    1. 优先提取接口返回的各种可能距离字段：
       p['distance'], p['delivery_distance'], p['store_distance'], store_obj['distance']
    2. 支持字符串与各种格式解析，例如 "3.1km", "4.3km", "800m", "1500", 3.1
    3. 若接口无直出距离但存在经纬度坐标 (store.latitude / store.longitude) 以及当前定位，使用 Haversine 球面大圆公式测距
    4. 若最终无法解析出有效正距离：
       - 若属于品牌券/连锁门店 (is_brand 或 store_brand_type == 1)：返回 (0, "周边门店通用")
       - 若普通商家：返回 (0, "附近")
       彻底消除不合理的 "0m" 显示与 0m 置顶霸榜问题。
    """
    dist_m = 0

    # 候选字段列表（按可靠度排列）
    candidates = [
        p.get("distance"),
        p.get("delivery_distance"),
        p.get("store_distance"),
        store_obj.get("distance"),
        store_obj.get("delivery_distance"),
    ]

    for val in candidates:
        if val is None or val == "" or val == 0 or val == "0":
            continue
        if isinstance(val, (int, float)):
            num = float(val)
            if num > 0:
                # 若数值小于 50 (例如 3.1, 4.5)，通常单位为公里(km)；若 >= 50，单位通常为米(m)
                dist_m = int(round(num * 1000)) if num < 50 else int(round(num))
                break
        elif isinstance(val, str):
            val_clean = val.strip().lower()
            m_km = re.match(r'^([\d\.]+)\s*km$', val_clean)
            if m_km:
                dist_m = int(round(float(m_km.group(1)) * 1000))
                break
            m_m = re.match(r'^([\d\.]+)\s*m$', val_clean)
            if m_m:
                dist_m = int(round(float(m_m.group(1))))
                break
            try:
                num = float(val_clean)
                if num > 0:
                    dist_m = int(round(num * 1000)) if num < 50 else int(round(num))
                    break
            except Exception:
                pass

    # 若仍未解析出距离，尝试用商户经纬度与用户当前经纬度进行大圆测距
    if dist_m <= 0:
        store_lat = store_obj.get("latitude") or p.get("latitude")
        store_lon = store_obj.get("longitude") or p.get("longitude")
        if store_lat and store_lon and user_lat and user_lon:
            try:
                slat = float(store_lat)
                slon = float(store_lon)
                ulat = float(user_lat)
                ulon = float(user_lon)
                # 过滤掉经纬度为 0 或超出国内地理范围的异常值
                if slat > 1 and slon > 1 and ulat > 1 and ulon > 1:
                    calc_d = haversine_distance(ulat, ulon, slat, slon)
                    if calc_d > 0:
                        dist_m = calc_d
            except Exception:
                pass

    # 格式化展示文本
    if dist_m > 0:
        if dist_m < 1000:
            dist_text = f"{dist_m}m"
        else:
            km_val = round(dist_m / 1000.0, 1)
            dist_text = f"{int(km_val) if km_val.is_integer() else km_val}km"
    else:
        # 绝不输出 "0m"
        if is_brand or store_obj.get("store_brand_type") == 1:
            dist_text = "周边门店通用"
        else:
            dist_text = "附近"

    return dist_m, dist_text


# ---------------- 4. 店铺列表与抢单预约 ---------------- #

def _format_time_pair(
    hour_val: Any = None, 
    min_val: Any = None, 
    direct_str: Any = None, 
    ts_val: Any = None, 
    default: str = "00:00"
) -> str:
    """
    高精度营业与活动抢单时段解析 (严格支持字符串HH:MM、小时/分钟数值、Unix秒级时间戳、时段范围等多形态数据)
    """
    # 1. 优先解析 hour_val (官方接口通常直接给出 '10:30'、'08:00'、'14:00' 等字符串，或纯数字 10、14)
    if hour_val is not None:
        s = str(hour_val).strip()
        if ":" in s:
            parts = s.split(":")
            try:
                h = int(parts[0])
                m = int(parts[1]) if len(parts) > 1 else 0
                return f"{h:02d}:{m:02d}"
            except Exception:
                pass
        elif s.isdigit() or (s.replace(".", "", 1).isdigit()):
            try:
                h = int(float(s))
                m = 0
                if min_val is not None and str(min_val).strip() != "":
                    m_s = str(min_val).strip()
                    if m_s.isdigit() or (m_s.replace(".", "", 1).isdigit()):
                        m = int(float(m_s))
                return f"{h:02d}:{m:02d}"
            except Exception:
                pass

    # 2. 其次解析 direct_str (可能为 '10:30'、'10:30-14:00' 或独立时间字符串)
    if direct_str is not None:
        s = str(direct_str).strip()
        if ":" in s:
            if "-" in s and default.startswith("23"):
                s = s.split("-")[1].strip()
            elif "-" in s:
                s = s.split("-")[0].strip()
            parts = s.split(":")
            try:
                h = int(parts[0])
                m = int(parts[1]) if len(parts) > 1 else 0
                return f"{h:02d}:{m:02d}"
            except Exception:
                pass

    # 3. 再次解析 Unix 秒级时间戳 (如 start_timestamp: 1789525800 -> 10:30)
    if ts_val is not None:
        try:
            ts = int(float(str(ts_val).strip()))
            if ts > 100000000:
                dt = datetime.fromtimestamp(ts, TZ_BJ)
                return dt.strftime("%H:%M")
        except Exception:
            pass

    return default


def _parse_store_promotion_item(
    p: Dict[str, Any],
    user_lat: Optional[float] = None,
    user_lon: Optional[float] = None,
    is_search: bool = False
) -> Dict[str, Any]:
    """标准化解析官方店铺霸王餐活动数据 (严格对齐 SilkwormFusion.FusionService 规范与实测抓包)"""
    store_obj = p.get("store") or {}
    store_id = str(store_obj.get("id") or store_obj.get("store_id") or p.get("store_id") or p.get("wm_poi_id") or "")
    store_name = store_obj.get("name") or p.get("name") or p.get("store_name") or "小蚕店铺"
    promotion_id = str(p.get("promotion_id") or p.get("poi_event_id") or store_id)
    icon = store_obj.get("icon") or p.get("picture") or p.get("store_icon") or ""

    plans = p.get("plan_activity_info_list") or []
    plan0 = plans[0] if plans else {}

    raw_ratio = (
        p.get("ratio_pct") or 
        p.get("user_ratio") or 
        plan0.get("user_ratio") or 
        p.get("ratio") or 
        plan0.get("ratio") or 
        0
    )
    pct = 0.0
    try:
        r_val = float(raw_ratio)
        if r_val > 0:
            pct = r_val if r_val <= 100 else (r_val / 100.0)
    except Exception:
        pct = 0.0

    raw_cap = (
        p.get("max_commission_yuan") or 
        p.get("max_commission") or 
        plan0.get("user_max_commission") or 
        plan0.get("max_commission") or 
        0
    )
    cap = 0.0
    try:
        c_val = float(raw_cap)
        if c_val > 0:
            cap = (c_val / 100.0) if c_val > 50 else c_val
    except Exception:
        cap = 0.0

    # 平台解析：1=美团, 2=饿了么, 3=京东
    tp = p.get("tp_promotion") or {}
    platform_code = p.get("store_platform") or store_obj.get("store_platform") or tp.get("store_platform")
    p_plat = str(p.get("platform") or store_obj.get("platform") or "").lower().strip()
    if p_plat in ("jingdong", "jd"):
        platform_code = 3
    elif p_plat in ("eleme", "taobao", "ele"):
        platform_code = 2
    elif p_plat == "meituan":
        platform_code = 1

    if not platform_code:
        if p.get("eleme_status") and not p.get("meituan_status"):
            platform_code = 2
        elif p.get("jingdong_status"):
            platform_code = 3
        else:
            platform_code = 1

    if platform_code == 3:
        plat = "jingdong"
    elif platform_code == 2:
        plat = "eleme"
    else:
        plat = "meituan"

    raw_order_money = p.get("order_money")
    if raw_order_money is None:
        raw_order_money = p.get("min_price_tip")
    if raw_order_money is None:
        if plat == "eleme":
            raw_order_money = p.get("eleme_order_money") or tp.get("tp_order_money") or p.get("meituan_order_money")
        else:
            raw_order_money = p.get("meituan_order_money") or tp.get("tp_order_money") or p.get("eleme_order_money")
    if raw_order_money is None:
        raw_order_money = 0
    try:
        raw_om = float(raw_order_money)
        order_money = (raw_om / 100.0) if raw_om >= 100 else raw_om
    except Exception:
        order_money = 0.0

    raw_rebate = p.get("user_rebate")
    if raw_rebate is None:
        raw_rebate = p.get("discount_amount")
    if raw_rebate is None:
        if plat == "eleme":
            raw_rebate = p.get("eleme_user_rebate") or tp.get("tp_user_rebate") or p.get("meituan_user_rebate")
        else:
            raw_rebate = p.get("meituan_user_rebate") or tp.get("tp_user_rebate") or p.get("eleme_user_rebate")
    if raw_rebate is None:
        raw_rebate = 0
    try:
        raw_rb = float(raw_rebate)
        rebate = (raw_rb / 100.0) if raw_rb >= 100 else raw_rb
    except Exception:
        rebate = 0.0

    # 评价条件解析 (优先使用官方接口直出的完整字符串)
    cond_str = p.get("rebate_condition_str")
    if not cond_str:
        cond_val = p.get("rebate_condition")
        cond_str = "无需评价" if cond_val == 99 else ("随心好评" if cond_val == 2 else "图文好评")

    left_num = p.get("left_number")
    if left_num is None:
        left_num = p.get("inventory")
    if left_num is None:
        if plat == "eleme":
            left_num = p.get("eleme_left_number") if p.get("eleme_left_number") is not None else p.get("meituan_left_number")
        else:
            left_num = p.get("meituan_left_number") if p.get("meituan_left_number") is not None else p.get("eleme_left_number")
    if left_num is None:
        left_num = tp.get("tp_left_number")
    if left_num is None:
        left_num = 0

    # 营业与抢单时间字符串高精度处理 (严格调用全局 _format_time_pair 并接入时间戳)
    start_time_str = _format_time_pair(
        hour_val=p.get("start_time_hour"),
        min_val=p.get("start_time_minute"),
        direct_str=p.get("start_time"),
        ts_val=p.get("start_timestamp") or p.get("start_date_timestamp"),
        default="00:00"
    )
    end_time_str = _format_time_pair(
        hour_val=p.get("end_time_hour"),
        min_val=p.get("end_time_minute"),
        direct_str=p.get("end_time"),
        ts_val=p.get("end_timestamp") or p.get("end_date_timestamp"),
        default="23:59"
    )

    # 兜底：若有时段列表 business_time 且无明确时段
    biz_time = p.get("business_time") or store_obj.get("business_time")
    if biz_time and isinstance(biz_time, list) and len(biz_time) > 0 and isinstance(biz_time[0], dict):
        bt = biz_time[0]
        if start_time_str == "00:00" and bt.get("start_time") is not None:
            try:
                sec = int(bt["start_time"])
                if sec < 86400:
                    start_time_str = f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}"
            except Exception:
                pass
        if end_time_str == "23:59" and bt.get("end_time") is not None:
            try:
                sec = int(bt["end_time"])
                if sec < 86400:
                    end_time_str = f"{sec // 3600:02d}:{(sec % 3600) // 60:02d}"
            except Exception:
                pass

    opening_hours_str = store_obj.get("opening_hours") or p.get("opening_hours") or "00:00-23:59"
    delivery_time_tip_str = str(p.get("delivery_time_tip") or store_obj.get("delivery_time_tip") or "")
    if_can_advance_order_val = bool(p.get("if_can_advance_order", False))
    start_date_ts = p.get("start_date_timestamp") or p.get("start_timestamp")
    end_date_ts = p.get("end_date_timestamp") or p.get("end_timestamp")

    # 满返与按比例判定
    tags = p.get("tags") or []
    tag_str = "".join(str(t) for t in tags)
    is_percent = (
        pct > 0 or 
        (order_money == 0 and cap > 0) or 
        ("比例" in tag_str) or 
        (p.get("bwc_type") == 1 and pct > 0)
    )

    if is_percent:
        rebate_type = "percent"
        rebate_type_text = "按比例返"
        cap_val = cap if cap > 0 else rebate
        pct_fmt = int(pct) if pct.is_integer() else round(pct, 1)
        cap_fmt = int(cap_val) if cap_val.is_integer() else round(cap_val, 1)
        if pct > 0 and cap_val > 0:
            rebate_desc = f"返{pct_fmt}% (最高¥{cap_fmt})"
        elif pct > 0:
            rebate_desc = f"返{pct_fmt}%"
        elif cap_val > 0:
            rebate_desc = f"按实付比例返 (最高¥{cap_fmt})"
        else:
            rebate_desc = "按比例返"
        final_order_money = 0.0
        final_rebate_price = round(cap_val, 2)
        rebate_rate = pct_fmt
    else:
        rebate_type = "fixed"
        rebate_type_text = "实付满返"
        om_fmt = int(order_money) if order_money.is_integer() else order_money
        rb_fmt = int(rebate) if rebate.is_integer() else rebate
        rebate_desc = f"满{om_fmt}返{rb_fmt}元"
        final_order_money = round(order_money, 2)
        final_rebate_price = round(rebate, 2)
        rebate_rate = round(rebate / order_money * 100, 1) if order_money > 0 else 0

    need_brand_coupon = bool(
        p.get("if_has_brand_promotion") or 
        p.get("brand_sign") or 
        p.get("is_vip_brand") or 
        (p.get("brand_left_number", 0) > 0) or 
        p.get("top_brand_activity_id") or
        (store_obj.get("store_brand_type") == 1)
    )
    brand_left_number = int(p.get("brand_left_number") or (p.get("promotion_condition") or {}).get("bln") or 0)
    is_vip_brand = bool(p.get("is_vip_brand") or (store_obj.get("store_brand_type") == 1) or (p.get("vip_level", 0) >= 3))
    
    # 限制频次解析
    store_info_limit = (store_obj.get("store_info") or {}).get("store_limit") or {}
    days_limit = int(p.get("days_limit") or p.get("user_limit_days") or (p.get("activity_limit") or {}).get("days_limit") or store_info_limit.get("days_limit") or 0)
    days_order_limit = int(p.get("days_order_limit") or (p.get("activity_limit") or {}).get("days_order_limit") or store_info_limit.get("days_order_limit") or 1)

    dist, dist_text = _parse_store_distance(
        p=p,
        store_obj=store_obj,
        user_lat=user_lat,
        user_lon=user_lon,
        is_brand=need_brand_coupon
    )

    same_group_id = int(p.get("same_group_id") or 0)
    if_use_red_pack = bool(p.get("if_use_red_pack", True))
    address = (
        store_obj.get("address") or 
        store_obj.get("address_detail") or 
        store_obj.get("store_address") or 
        store_obj.get("poi_address") or 
        p.get("address") or 
        p.get("address_detail") or 
        p.get("store_address") or 
        ""
    )

    return {
        "store_id": store_id,
        "name": _clean_emoji(store_name),
        "promotion_id": promotion_id,
        "icon": icon,
        "distance": dist,
        "distance_text": dist_text,
        "left_number": int(left_num or 0),
        "order_money": final_order_money,
        "rebate_price": final_rebate_price,
        "rebate_rate": rebate_rate,
        "rebate_desc": rebate_desc,
        "rebate_type": rebate_type,
        "rebate_type_text": rebate_type_text,
        "platform": plat,
        "store_platform": int(platform_code or (3 if plat == "jingdong" else (2 if plat == "eleme" else 1))),
        "start_time": start_time_str,
        "end_time": end_time_str,
        "opening_hours": opening_hours_str,
        "delivery_time_tip": delivery_time_tip_str,
        "if_can_advance_order": if_can_advance_order_val,
        "start_date_timestamp": start_date_ts,
        "end_date_timestamp": end_date_ts,
        "condition": cond_str,
        "need_brand_coupon": need_brand_coupon,
        "brand_left_number": brand_left_number,
        "is_vip_brand": is_vip_brand,
        "days_limit": days_limit,
        "days_order_limit": days_order_limit,
        "same_group_id": same_group_id,
        "if_use_red_pack": if_use_red_pack,
        "address": address
    }


def _normalize_shangjin_item(poi: Dict[str, Any], user_lat: Optional[float] = None, user_lon: Optional[float] = None) -> List[Dict[str, Any]]:
    """标准化解析官方美团赏金按比例返现 POI 数据 (SilkwormRcsService.MeituanShangjinGetPoiList)"""
    name = str(poi.get("name") or "").strip()
    icon = poi.get("picture") or ""
    dist_str = str(poi.get("delivery_distance") or "").strip()
    dist = 0
    dist_text = dist_str or "附近"
    if "km" in dist_str:
        try:
            dist = int(float(dist_str.replace("km", "").strip()) * 1000)
        except Exception:
            dist = 0
    elif "m" in dist_str:
        try:
            dist = int(float(dist_str.replace("m", "").strip()))
        except Exception:
            dist = 0

    wm_poi_id = str(poi.get("wm_poi_id") or "")
    action_url = poi.get("action_url") or {}
    dp_url = action_url.get("dp_url") or ""
    poi_id_m = re.search(r"poi_id=(\d+)", dp_url)
    store_id = poi_id_m.group(1) if poi_id_m else wm_poi_id

    plans = poi.get("plan_activity_info_list") or []
    if not plans:
        ratio_raw = float(poi.get("ratio") or 0)
        cap_raw = float(poi.get("max_commission") or 0)
        plans = [{
            "user_ratio": ratio_raw,
            "user_max_commission": cap_raw,
            "inventory": poi.get("inventory") or 0,
            "poi_event_id": poi.get("poi_event_id") or wm_poi_id,
            "rebate_condition": 0
        }]

    items = []
    for idx, plan in enumerate(plans, start=1):
        ratio_raw = float(plan.get("user_ratio") or plan.get("ratio") or 0)
        pct = (ratio_raw / 100.0) if ratio_raw > 100 else ratio_raw
        cap_raw = float(plan.get("user_max_commission") or plan.get("max_commission") or 0)
        cap_val = (cap_raw / 100.0) if cap_raw > 50 else cap_raw
        pct_fmt = int(pct) if pct.is_integer() else round(pct, 1)
        cap_fmt = int(cap_val) if cap_val.is_integer() else round(cap_val, 1)

        rebate_desc = f"返{pct_fmt}% (最高¥{cap_fmt})" if cap_val > 0 else f"返{pct_fmt}%"
        cond_code = plan.get("rebate_condition")
        cond_str = "无需评价" if cond_code == 99 else "用餐反馈"
        left_num = int(plan.get("inventory") or 0)
        pid = str(plan.get("poi_event_id") or f"{store_id}_p{idx}")

        item = {
            "store_id": str(store_id),
            "name": _clean_emoji(name),
            "promotion_id": pid,
            "icon": icon,
            "distance": dist,
            "distance_text": dist_text,
            "left_number": left_num,
            "order_money": 0.0,
            "rebate_price": round(cap_val, 2),
            "rebate_rate": pct_fmt,
            "rebate_desc": rebate_desc,
            "rebate_type": "percent",
            "rebate_type_text": "按比例返",
            "platform": "meituan",
            "store_platform": 1,
            "start_time": "00:00",
            "end_time": "23:59",
            "opening_hours": poi.get("opening_hours") or "00:00-23:59",
            "delivery_time_tip": str(poi.get("delivery_time_tip") or ""),
            "if_can_advance_order": False,
            "condition": cond_str,
            "need_brand_coupon": False,
            "brand_left_number": 0,
            "is_vip_brand": False,
            "days_limit": 0,
            "days_order_limit": 1,
            "same_group_id": 0,
            "if_use_red_pack": True,
            "address": ""
        }
        items.append(item)
    return items


def _normalize_feed_item(p: Dict[str, Any], user_lat: Optional[float] = None, user_lon: Optional[float] = None) -> Dict[str, Any]:
    """标准化解析官方首页 Feed 推荐店铺数据 (FusionService.GetFeedPromotions)"""
    return _parse_store_promotion_item(p, user_lat=user_lat, user_lon=user_lon, is_search=False)


def _normalize_search_item(p: Dict[str, Any], user_lat: Optional[float] = None, user_lon: Optional[float] = None) -> Dict[str, Any]:
    """标准化解析官方微服务搜索结果数据 (FusionService.SearchPromotions)"""
    return _parse_store_promotion_item(p, user_lat=user_lat, user_lon=user_lon, is_search=True)

def _get_store_branch_key(splat: str, name: str) -> str:
    """提取店铺在平台内的同店分店唯一聚合标识（准确保留主品牌与核心分店，杜绝跨分店混淆）"""
    if not name:
        return f"{splat or 'meituan'}_unknown"
    n = name.replace("（", "(").replace("）", ")").replace("【", "[").replace("】", "]")
    n = re.sub(r'\s+', '', n)
    m = re.match(r'^([^(\[]+)(?:[(\[](.*?)[)\]])?', n)
    if not m:
        core = n.lower()
    else:
        main_brand = m.group(1).strip().lower()
        branch_info = (m.group(2) or "").strip().lower()
        branch_core = ""
        if branch_info:
            bm = re.search(r'([^·・,，.、]+?店)', branch_info)
            if bm:
                branch_core = bm.group(1)
            else:
                branch_core = re.split(r'[·・,，.、]', branch_info)[0]
        core = f"{main_brand}_{branch_core}" if branch_core else main_brand
    return f"{splat or 'meituan'}_{core}"


@router.get("/store/list")
async def get_stores(
    city_code: int = Query(440303),
    longitude: str = Query("114.13166"),
    latitude: str = Query("22.548361"),
    account_key: Optional[str] = Query(None),
    keyword: Optional[str] = Query(None),
    platform: Optional[str] = Query("all"),
    condition: Optional[str] = Query("all"),
    rebate_type: Optional[str] = Query("all"),
    sort_by: Optional[str] = Query("distance"),
    offset: int = Query(0),
    limit: int = Query(50),
    page_pv_id: Optional[str] = Query(None)
):
    """
    获取霸王餐店铺清单
    1. 当传入关键词时：并发调用小蚕官方微服务实时搜索 (FusionService.SearchPromotions) 与美团赏金按比例返现 (SilkwormRcsService.MeituanShangjinGetPoiList)
       通过携带 page_pv_id 维持翻页会话，支持连续触底无缝流式翻页
    2. 当未传关键词时：并发拉取官方商圈推荐 Feed，支持「距离最近」(sort=1, 默认) 与「综合排序」(sort=0) 原生排序
    """
    city_code = _safe_val(city_code, 440303)
    longitude = _safe_val(longitude, "114.13166")
    latitude = _safe_val(latitude, "22.548361")
    account_key = _safe_val(account_key, None)
    keyword = _safe_val(keyword, None)
    platform = _safe_val(platform, "all")
    condition = _safe_val(condition, "all")
    rebate_type = _safe_val(rebate_type, "all")
    sort_by = _safe_val(sort_by, "distance")
    offset = int(_safe_val(offset, 0))
    limit = int(_safe_val(limit, 50))
    page_pv_id = _safe_val(page_pv_id, "")

    accounts = db.get_all_accounts()
    if not accounts and not account_key:
        return {
            "ok": True,
            "stores": [],
            "total": 0,
            "source": "empty",
            "message": "当前未登录或未托管小蚕账号，请先在【小蚕账号】中心添加账号"
        }

    target_acc = None
    if account_key:
        target_acc = db.get_account_by_key(account_key)
    if not target_acc and accounts:
        target_acc = accounts[0]

    if not target_acc:
        return {"ok": True, "stores": [], "total": 0, "source": "empty", "message": "当前账号已退出，请先登录账号"}

    token = target_acc.get("token")
    if not token:
        return {"ok": True, "stores": [], "total": 0, "source": "empty", "message": "当前账号凭据已失效或已退出"}

    silk_id = target_acc.get("silk_id")
    user_id = target_acc.get("user_id")

    if target_acc:
        if target_acc.get("longitude") and (not longitude or longitude in ("114.305393", "114.13166", "")):
            longitude = target_acc.get("longitude")
        if target_acc.get("latitude") and (not latitude or latitude in ("30.593099", "22.548361", "")):
            latitude = target_acc.get("latitude")
        if target_acc.get("city_code") and (not city_code or city_code in (420100, 440303)):
            city_code = target_acc.get("city_code")

    try:
        lat = float(latitude)
    except Exception:
        lat = 22.548361
    try:
        lon = float(longitude)
    except Exception:
        lon = 114.13166

    stores = []
    source = "feed"
    has_more = True
    next_offset = 0
    current_pv_id = page_pv_id or ""

    # 分支一：用户输入了关键词，采用混合搜索策略：
    # 1. 调用官方微服务实时搜索满减活动 (SilkwormFusion.FusionService.SearchPromotions)
    # 2. 调用美团赏金按比例返现检索 (SilkwormRcs.SilkwormRcsService.MeituanShangjinGetPoiList)，带 page_pv_id 维持翻页游标
    # 3. 首屏检索时额外并发拉取商圈 Feed，覆盖官方搜索中遗漏的「大牌券」商户
    if keyword and keyword.strip():
        kw = keyword.strip()
        source = "hybrid_search"
        try:
            logger.info(f"发起 hybrid_search: kw={kw}, city={city_code}, lon={lon}, lat={lat}, silk={silk_id}, user={user_id}, offset={offset}, page_pv_id={current_pv_id}")
            search_offsets = [offset, offset + 20] if offset == 0 else [offset]
            search_tasks = [
                client.search_stores(
                    keyword=kw,
                    city_code=city_code,
                    longitude=str(lon),
                    latitude=str(lat),
                    offset=off,
                    limit=20,
                    token=token,
                    silk_id=silk_id,
                    user_id=user_id
                )
                for off in search_offsets
            ]
            shangjin_tasks = [
                client.search_shangjin_stores(
                    keyword=kw,
                    latitude=lat,
                    longitude=lon,
                    token=token,
                    silk_id=silk_id,
                    user_id=user_id,
                    city_code=city_code,
                    sort_type=3,
                    page_pv_id=current_pv_id
                )
            ]
            feed_offsets = [0, 35] if offset == 0 else []
            feed_tasks = [
                client.get_store_list(
                    city_code=city_code,
                    longitude=str(lon),
                    latitude=str(lat),
                    offset=foff,
                    limit=35,
                    token=token,
                    silk_id=silk_id,
                    user_id=user_id
                )
                for foff in feed_offsets
            ]
            all_results = await asyncio.gather(*(search_tasks + shangjin_tasks + feed_tasks), return_exceptions=True)
            search_res = all_results[:len(search_tasks)]
            shangjin_res = all_results[len(search_tasks):len(search_tasks) + len(shangjin_tasks)]
            feed_res = all_results[len(search_tasks) + len(shangjin_tasks):]

            seen_pids = set()
            total_raw_count = 0
            has_more_shangjin = False
            has_more_search = False

            # 1. 优先加入官方美团赏金按比例返现活动商户 (SilkwormRcsService.MeituanShangjinGetPoiList)
            for sjr in shangjin_res:
                if isinstance(sjr, dict):
                    returned_pv = sjr.get("page_pv_id")
                    if returned_pv:
                        current_pv_id = returned_pv
                    pois = sjr.get("poi_list") or []
                    total_raw_count += len(pois)
                    if len(pois) >= 10:
                        has_more_shangjin = True
                    for poi in pois:
                        sj_items = _normalize_shangjin_item(poi, user_lat=lat, user_lon=lon)
                        for item in sj_items:
                            dedup_k = f"{item.get('platform')}_{item['store_id']}_{item['promotion_id']}"
                            if dedup_k not in seen_pids:
                                seen_pids.add(dedup_k)
                                stores.append(item)
                elif isinstance(sjr, Exception):
                    logger.warning(f"赏金按比例返现检索批次异常: {sjr}")

            # 2. 加入官方微服务搜索直接命中的商户活动 (支持美团/饿了么/京东各平台全档位方案展开)
            for off, sr in zip(search_offsets, search_res):
                if isinstance(sr, dict):
                    raw_proms = sr.get("promotions") or sr.get("promotion_list") or sr.get("feed_items") or []
                    total_raw_count += len(raw_proms)
                    sr_total = sr.get("total") or 0
                    if sr_total > (off + len(raw_proms)):
                        has_more_search = True
                    for p in raw_proms:
                        promos = _extract_all_promos_from_raw(p, is_search=True, only_meituan=False, user_lat=lat, user_lon=lon)
                        for item in promos:
                            dedup_k = f"{item.get('platform')}_{item['store_id']}_{item['promotion_id']}"
                            if dedup_k not in seen_pids:
                                seen_pids.add(dedup_k)
                                stores.append(item)
                elif isinstance(sr, Exception):
                    logger.warning(f"搜索并发批次异常: {sr}")

            # 3. 从商圈 Feed 匹配补全「按比例返」与大牌券特权商户 (仅首屏)
            if feed_res:
                kw_lower = kw.lower()
                for fr in feed_res:
                    if isinstance(fr, dict):
                        feed_items = fr.get("feed_items") or fr.get("items") or fr.get("promotion_list") or []
                        for p in feed_items:
                            promos = _extract_all_promos_from_raw(p, is_search=False, only_meituan=False, user_lat=lat, user_lon=lon)
                            for item in promos:
                                sname = item.get("name", "").lower()
                                rdesc = item.get("rebate_desc", "").lower()
                                rtype = item.get("rebate_type_text", "").lower()
                                cond = item.get("condition", "").lower()
                                
                                is_match = (
                                    kw_lower in sname or 
                                    kw_lower in rdesc or 
                                    kw_lower in rtype or 
                                    kw_lower in cond or
                                    ("比例" in kw_lower and item.get("rebate_type") == "percent") or
                                    ("满返" in kw_lower and item.get("rebate_type") == "fixed") or
                                    ("大牌" in kw_lower and item.get("need_brand_coupon")) or
                                    ("免评" in kw_lower and item.get("condition") == "无需评价")
                                )
                                if is_match:
                                    dedup_k = f"{item.get('platform')}_{item['store_id']}_{item['promotion_id']}"
                                    if dedup_k not in seen_pids:
                                        seen_pids.add(dedup_k)
                                        stores.append(item)
                                    else:
                                        for idx, existing_s in enumerate(stores):
                                            if f"{existing_s.get('platform')}_{existing_s['store_id']}_{existing_s['promotion_id']}" == dedup_k:
                                                stores[idx] = item
                                                break
                    elif isinstance(fr, Exception):
                        logger.warning(f"Feed检索批次异常: {fr}")

            has_more = has_more_shangjin or has_more_search or (total_raw_count >= 10)
            next_offset = offset + (20 * len(search_offsets) if search_offsets else 20)
            logger.info(f"混合实时搜索合并返回: keyword={kw}, offset={offset}, total={len(stores)}, has_more={has_more}, pv_id={current_pv_id}")
        except Exception as e:
            logger.error(f"小蚕官方实时搜索异常: {e}")
            return {"ok": False, "stores": [], "total": 0, "source": source, "has_more": False, "error": f"小蚕官方实时搜索接口异常: {e}"}

    # 分支二：常规商圈浏览，通过并发多批次抓取扩展至 100+ 家
    else:
        source = "feed"
        try:
            # 官方 GetFeedPromotions 排序参数：
            # sort = 1 为「距离最近」由近及远原生排序 (默认)
            # sort = 0 为「综合排序」官方原生推荐排序
            official_sort = 1 if sort_by == "distance" else 0

            # 每次拉取并发 3 批 (offset, offset+35, offset+70)，确保每次触底都能获得足够的新商户增量
            step = 35
            offsets = [offset, offset + step, offset + (step * 2)]
            tasks = [
                client.get_store_list(
                    city_code=city_code,
                    longitude=str(lon),
                    latitude=str(lat),
                    offset=off,
                    limit=step,
                    token=token,
                    silk_id=silk_id,
                    user_id=user_id,
                    sort=official_sort
                )
                for off in offsets
            ]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            seen_promotions = set()
            total_raw_count = 0
            for br in batch_results:
                if isinstance(br, dict):
                    feed_items = br.get("feed_items") or br.get("items") or br.get("promotion_list") or []
                    total_raw_count += len(feed_items)
                    for p in feed_items:
                        promos = _extract_all_promos_from_raw(p, is_search=False, only_meituan=False, user_lat=lat, user_lon=lon)
                        for item in promos:
                            dedup_key = f"{item.get('platform')}_{item['store_id']}_{item['promotion_id']}"
                            if dedup_key not in seen_promotions:
                                seen_promotions.add(dedup_key)
                                stores.append(item)
                elif isinstance(br, Exception):
                    logger.warning(f"并发拉取批次异常 (非致命): {br}")
            # 只要本次批次中官方返回了非空数据，说明后续仍有商户可供拉取
            has_more = total_raw_count > 0
            next_offset = offset + (step * len(offsets))
            logger.info(f"商圈Feed合并返回: offset={offset}, total={len(stores)}, has_more={has_more}")
        except Exception as e:
            logger.error(f"拉取小蚕官方店铺列表失败: {e}")
            return {"ok": False, "stores": [], "total": 0, "source": source, "has_more": False, "error": f"小蚕官方接口调用异常: {e}"}

    # 平台过滤 (支持美团/饿了么/京东全量平台)
    if platform and platform != "all":
        if platform in ("jingdong", "jd"):
            stores = [s for s in stores if s.get("platform") in ("jingdong", "jd") or s.get("store_platform") == 3]
        elif platform in ("eleme", "taobao"):
            stores = [s for s in stores if s.get("platform") in ("eleme", "taobao") or s.get("store_platform") == 2]
        elif platform == "meituan":
            stores = [s for s in stores if s.get("platform") == "meituan" or s.get("store_platform") == 1]
        else:
            stores = [s for s in stores if s.get("platform") == platform]

    # 评价条件过滤
    if condition and condition != "all":
        if condition == "no_review":
            stores = [s for s in stores if s.get("condition") == "无需评价"]
        elif condition == "good_review":
            stores = [s for s in stores if s.get("condition") != "无需评价"]

    # 返利模式过滤 (全部 / 实付满返 / 按比例返)
    if rebate_type and rebate_type != "all":
        if rebate_type in ("fixed", "amount"):
            stores = [s for s in stores if s.get("rebate_type") == "fixed"]
        elif rebate_type in ("percent", "ratio"):
            stores = [s for s in stores if s.get("rebate_type") == "percent"]

    # 排序处理 (注意：未知或全国通用距离按 9999999 排到最后，避免 0m 霸榜置顶)
    if sort_by == "distance":
        stores.sort(key=lambda x: (x.get("distance", 0) if x.get("distance", 0) > 0 else 9999999))
    elif sort_by == "rebate":
        stores.sort(key=lambda x: x["rebate_price"], reverse=True)
    elif sort_by == "rate":
        stores.sort(key=lambda x: x["rebate_rate"], reverse=True)
    elif sort_by == "left":
        stores.sort(key=lambda x: x["left_number"], reverse=True)

    # 4. 同名商家多任务合并 (同平台同分店深度聚合，跨平台严格独立拆分)
    # 按「同平台 + 同分店唯一标识」聚合，美团/饿了么/京东即使同店同名也作为独立商户卡片展示，档位绝不跨平台混淆
    merged_stores = []
    store_map = {}
    branch_to_key = {}
    id_to_key = {}

    for s in stores:
        sid = str(s.get("store_id") or "").strip()
        sname = str(s.get("name") or "").strip()
        
        # 严格平台代码规范化，杜绝不同平台混淆
        splat = str(s.get("platform") or "").lower().strip()
        store_plat = s.get("store_platform")
        if not splat or splat == "all":
            if store_plat == 2:
                splat = "eleme"
            elif store_plat == 3:
                splat = "jingdong"
            else:
                splat = "meituan"
        if splat in ("taobao", "ele"):
            splat = "eleme"
        elif splat in ("jd",):
            splat = "jingdong"

        plat_code = 3 if splat == "jingdong" else (2 if splat == "eleme" else 1)

        # 核心分店主键：以平台为命名空间隔离，杜绝同店名跨平台合并！
        branch_k = _get_store_branch_key(splat, sname)

        # 确定主键：有效 store_id 必须携带平台前缀，杜绝跨平台同 store_id 错乱合并！
        plat_sid = f"{splat}_{sid}" if sid and sid != "0" else None
        store_key = None
        if plat_sid and plat_sid in id_to_key:
            store_key = id_to_key[plat_sid]
        elif branch_k in branch_to_key:
            store_key = branch_to_key[branch_k]
        elif plat_sid:
            store_key = f"{splat}_id_{sid}"
        else:
            store_key = branch_k

        if branch_k not in branch_to_key:
            branch_to_key[branch_k] = store_key
        if plat_sid and plat_sid not in id_to_key:
            id_to_key[plat_sid] = store_key

        promo_item = {
            "promotion_id": s["promotion_id"],
            "order_money": s["order_money"],
            "rebate_price": s["rebate_price"],
            "rebate_rate": s["rebate_rate"],
            "rebate_desc": s.get("rebate_desc", f"满{s['order_money']}返{s['rebate_price']}元"),
            "rebate_type": s.get("rebate_type", "fixed"),
            "rebate_type_text": s.get("rebate_type_text", "实付满返"),
            "condition": s["condition"],
            "left_number": s["left_number"],
            "start_time": s["start_time"],
            "end_time": s["end_time"],
            "opening_hours": s.get("opening_hours", "00:00-23:59"),
            "delivery_time_tip": s.get("delivery_time_tip", ""),
            "if_can_advance_order": s.get("if_can_advance_order", False),
            "start_date_timestamp": s.get("start_date_timestamp"),
            "end_date_timestamp": s.get("end_date_timestamp"),
            "platform": splat,
            "store_platform": plat_code,
            "need_brand_coupon": s.get("need_brand_coupon", False),
            "brand_left_number": s.get("brand_left_number", 0),
            "is_vip_brand": s.get("is_vip_brand", False),
            "days_limit": s.get("days_limit", 0),
            "days_order_limit": s.get("days_order_limit", 1),
            "same_group_id": s.get("same_group_id", 0),
            "if_use_red_pack": s.get("if_use_red_pack", True),
            "address": s.get("address", "")
        }

        if store_key not in store_map:
            store_copy = dict(s)
            store_copy["platform"] = splat
            store_copy["store_platform"] = plat_code
            store_copy["promotions"] = [promo_item]
            store_copy["promotion_count"] = 1
            store_map[store_key] = store_copy
            merged_stores.append(store_copy)
        else:
            existing = store_map[store_key]
            existing_plat = existing.get("platform") or "meituan"

            # 严格防线：若平台不一致，绝对禁止合并到已有条目中，创建独立商户条目！
            if existing_plat != splat:
                isolated_key = f"{splat}_{store_key}"
                store_copy = dict(s)
                store_copy["platform"] = splat
                store_copy["store_platform"] = plat_code
                store_copy["promotions"] = [promo_item]
                store_copy["promotion_count"] = 1
                store_map[isolated_key] = store_copy
                merged_stores.append(store_copy)
                continue

            if not any(p["promotion_id"] == s["promotion_id"] for p in existing["promotions"]):
                existing["promotions"].append(promo_item)
                existing["promotion_count"] = len(existing["promotions"])
                # 若已有商户距离为 0 但当前活动解析出了有效距离，补充更新商户距离信息
                if s.get("distance", 0) > 0 and existing.get("distance", 0) == 0:
                    existing["distance"] = s["distance"]
                    existing["distance_text"] = s["distance_text"]
                # 择优展示外层：优先展示有名额的活动，其次展示返利金额更高的活动
                has_quota_now = s.get("left_number", 0) > 0
                has_quota_exist = existing.get("left_number", 0) > 0
                better_quota = (not has_quota_exist and has_quota_now)
                better_rebate = (has_quota_now == has_quota_exist and s.get("rebate_price", 0) > existing.get("rebate_price", 0))

                if better_quota or better_rebate:
                    existing["left_number"] = s["left_number"]
                    existing["promotion_id"] = s["promotion_id"]
                    existing["order_money"] = s["order_money"]
                    existing["rebate_price"] = s["rebate_price"]
                    existing["rebate_rate"] = s["rebate_rate"]
                    existing["rebate_desc"] = s.get("rebate_desc")
                    existing["rebate_type"] = s.get("rebate_type")
                    existing["rebate_type_text"] = s.get("rebate_type_text")
                    existing["condition"] = s["condition"]
                    existing["start_time"] = s.get("start_time", "00:00")
                    existing["end_time"] = s.get("end_time", "23:59")
                    existing["start_date_timestamp"] = s.get("start_date_timestamp")
                    existing["end_date_timestamp"] = s.get("end_date_timestamp")
                    existing["if_can_advance_order"] = s.get("if_can_advance_order", False)
                    existing["need_brand_coupon"] = s.get("need_brand_coupon", False)
                    existing["brand_left_number"] = s.get("brand_left_number", 0)
                    existing["is_vip_brand"] = s.get("is_vip_brand", False)
                    existing["days_limit"] = s.get("days_limit", 0)
                    existing["days_order_limit"] = s.get("days_order_limit", 1)
                    existing["same_group_id"] = s.get("same_group_id", 0)
                    existing["if_use_red_pack"] = s.get("if_use_red_pack", True)
                    if s.get("address") and not existing.get("address"):
                        existing["address"] = s["address"]
                elif existing.get("start_time") in ("00:00", "", None) and s.get("start_time") and s.get("start_time") != "00:00":
                    existing["start_time"] = s["start_time"]
                    existing["end_time"] = s["end_time"]

                # 无论是否为最佳方案，只要当前活动具备有效门牌地址，即刻为商户补齐地址
                if s.get("address") and not existing.get("address"):
                    existing["address"] = s["address"]

    # 再次保障每个店铺名下的档位只属于本平台，并拆分 fixed_plans 与 percent_plans
    for ms in merged_stores:
        ms_plat = ms.get("platform") or "meituan"
        ms["promotions"] = [p for p in ms.get("promotions", []) if (p.get("platform") or "meituan") == ms_plat]
        ms["promotion_count"] = len(ms["promotions"])
        f_plans = [p for p in ms.get("promotions", []) if p.get("rebate_type") == "fixed"]
        p_plans = [p for p in ms.get("promotions", []) if p.get("rebate_type") == "percent"]
        ms["fixed_plans"] = f_plans
        ms["percent_plans"] = p_plans
        if f_plans:
            ms["fixed_plan"] = f_plans[0]
            if ms.get("start_time") in ("00:00", "", None) and f_plans[0].get("start_time") and f_plans[0].get("start_time") != "00:00":
                ms["start_time"] = f_plans[0]["start_time"]
                ms["end_time"] = f_plans[0]["end_time"]
        if p_plans:
            ms["percent_plan"] = p_plans[0]

    return {
        "ok": True,
        "stores": merged_stores,
        "total": len(merged_stores),
        "raw_total": len(stores),
        "source": source,
        "has_more": has_more,
        "next_offset": next_offset,
        "page_pv_id": current_pv_id,
        "keyword": keyword.strip() if keyword else ""
    }


def _extract_all_promos_from_raw(
    p: Dict[str, Any],
    is_search: bool = False,
    only_meituan: bool = False,
    user_lat: Optional[float] = None,
    user_lon: Optional[float] = None
) -> List[Dict[str, Any]]:
    """
    从单条推荐或搜索结果记录中，完整提取出所有返利方案 (plan_activity_info_list)：
    严格执行规则：
    1. 若 only_meituan=True，严格限定平台必须为美团外卖（store_platform == 1 或 platform == 'meituan'），其他平台直接剔除。
    2. 对每一档计划精确识别是「实付满返」还是「按比例返」，并清理所有 Emoji 符号与特殊字符。
    """
    # 官方美团赏金按比例返现 POI (source == 5 或具备 wm_poi_id 无独立 promotion_id)
    if p.get("source") == 5 or (p.get("wm_poi_id") and not p.get("promotion_id")):
        return _normalize_shangjin_item(p, user_lat=user_lat, user_lon=user_lon)

    store_obj = p.get("store") or {}
    tp = p.get("tp_promotion") or {}
    platform_code = p.get("store_platform") or store_obj.get("store_platform") or tp.get("store_platform", 1)

    # 若限定美团，非美团直接跳过
    if only_meituan:
        if platform_code != 1 and p.get("platform") != "meituan":
            return []

    base_item = _normalize_search_item(p, user_lat=user_lat, user_lon=user_lon) if is_search else _normalize_feed_item(p, user_lat=user_lat, user_lon=user_lon)
    if only_meituan and base_item.get("platform") != "meituan" and base_item.get("store_platform") != 1:
        return []

    plans = p.get("plan_activity_info_list") or []

    if not plans:
        base_item["name"] = _clean_emoji(base_item.get("name", ""))
        base_item["rebate_desc"] = _clean_emoji(base_item.get("rebate_desc", ""))
        return [base_item]

    items = []
    for idx, plan in enumerate(plans, start=1):
        plan_ratio = plan.get("user_ratio") or plan.get("ratio") or 0
        try:
            p_r = float(plan_ratio)
            plan_pct = p_r if p_r <= 100 else (p_r / 100.0)
        except Exception:
            plan_pct = 0.0

        plan_cap = plan.get("user_max_commission") or plan.get("max_commission") or 0
        try:
            p_c = float(plan_cap)
            plan_cap_val = (p_c / 100.0) if p_c > 50 else p_c
        except Exception:
            plan_cap_val = 0.0

        plan_order = plan.get("order_money") or plan.get("min_price_tip") or 0
        try:
            p_om = float(plan_order) / 100.0 if float(plan_order) > 50 else float(plan_order)
        except Exception:
            p_om = 0.0

        plan_rebate = plan.get("user_rebate") or plan.get("discount_amount") or 0
        try:
            p_rb = float(plan_rebate) / 100.0 if float(plan_rebate) > 50 else float(plan_rebate)
        except Exception:
            p_rb = 0.0

        plan_left = plan.get("left_number")
        if plan_left is None:
            plan_left = plan.get("inventory")
        if plan_left is None:
            plan_left = plan.get("meituan_left_number")
        if plan_left is None:
            plan_left = (plan.get("tp_promotion") or {}).get("tp_left_number")
        if plan_left is None:
            plan_left = base_item.get("left_number")
        if plan_left is None:
            plan_left = 0

        plan_item = dict(base_item)
        plan_item["promotion_id"] = f"{base_item['promotion_id']}_p{idx}" if idx > 1 else base_item["promotion_id"]
        plan_item["left_number"] = int(plan_left)

        # 提取档位专属开抢时间与时段 (若档位有自身独立时段则覆盖，否则继承商户活动基准时段)
        plan_start_hour = plan.get("start_time_hour")
        plan_start_minute = plan.get("start_time_minute")
        plan_end_hour = plan.get("end_time_hour")
        plan_end_minute = plan.get("end_time_minute")
        plan_start_direct = plan.get("start_time")
        plan_end_direct = plan.get("end_time")
        plan_start_ts = plan.get("start_timestamp") or plan.get("start_date_timestamp")
        plan_end_ts = plan.get("end_timestamp") or plan.get("end_date_timestamp")

        if any(x is not None for x in [plan_start_hour, plan_start_minute, plan_start_direct, plan_start_ts]):
            plan_item["start_time"] = _format_time_pair(
                hour_val=plan_start_hour, 
                min_val=plan_start_minute, 
                direct_str=plan_start_direct, 
                ts_val=plan_start_ts, 
                default=base_item.get("start_time", "00:00")
            )
        if any(x is not None for x in [plan_end_hour, plan_end_minute, plan_end_direct, plan_end_ts]):
            plan_item["end_time"] = _format_time_pair(
                hour_val=plan_end_hour, 
                min_val=plan_end_minute, 
                direct_str=plan_end_direct, 
                ts_val=plan_end_ts, 
                default=base_item.get("end_time", "23:59")
            )
        if plan_start_ts:
            plan_item["start_date_timestamp"] = plan_start_ts
        if plan_end_ts:
            plan_item["end_date_timestamp"] = plan_end_ts
        if plan.get("if_can_advance_order") is not None:
            plan_item["if_can_advance_order"] = bool(plan.get("if_can_advance_order"))
        if plan.get("rebate_condition_str"):
            plan_item["condition"] = plan.get("rebate_condition_str")

        # 判断方案类型：按比例返 vs 实付满返
        is_pct = (plan_pct > 0) or (p_om == 0 and plan_cap_val > 0)

        if is_pct:
            plan_item["rebate_type"] = "percent"
            plan_item["rebate_type_text"] = "按比例返"
            cap_val = plan_cap_val if plan_cap_val > 0 else p_rb
            pct_fmt = int(plan_pct) if plan_pct.is_integer() else round(plan_pct, 1)
            cap_fmt = int(cap_val) if cap_val.is_integer() else round(cap_val, 1)
            if plan_pct > 0 and cap_val > 0:
                plan_item["rebate_desc"] = f"返{pct_fmt}% (最高¥{cap_fmt})"
            elif plan_pct > 0:
                plan_item["rebate_desc"] = f"返{plan_pct}%"
            else:
                plan_item["rebate_desc"] = f"按比例返 (最高¥{cap_fmt})"
            plan_item["order_money"] = 0.0
            plan_item["rebate_price"] = round(cap_val, 2)
            plan_item["rebate_rate"] = pct_fmt
        else:
            plan_item["rebate_type"] = "fixed"
            plan_item["rebate_type_text"] = "实付满返"
            om_fmt = int(p_om) if p_om.is_integer() else p_om
            rb_fmt = int(p_rb) if p_rb.is_integer() else p_rb
            plan_item["rebate_desc"] = f"满{om_fmt}返{rb_fmt}元" if (p_om > 0 or p_rb > 0) else "实付满返"
            plan_item["order_money"] = round(p_om, 2)
            plan_item["rebate_price"] = round(p_rb, 2)
            plan_item["rebate_rate"] = round(p_rb / p_om * 100, 1) if p_om > 0 else 0

        plan_item["name"] = _clean_emoji(plan_item.get("name", ""))
        plan_item["rebate_desc"] = _clean_emoji(plan_item.get("rebate_desc", ""))
        items.append(plan_item)

    return items


@router.get("/store/dual-rebate-scan")
async def scan_dual_rebate_stores(
    city_code: int = Query(440303),
    longitude: str = Query("114.13166"),
    latitude: str = Query("22.548361"),
    account_key: Optional[str] = Query(None),
    platform: Optional[str] = Query("meituan"),
    keyword: Optional[str] = Query(None),
    max_stores: int = Query(300)
):
    """
    美团外卖同店双返利高精度智能扫描：
    1. 严格限定美团外卖平台；
    2. 严格判定必须同时存在「实付满返」与「按比例返」两类方案；
    3. 清洗过滤所有 Emoji 表情与符号；
    4. 将同一店铺的名下双返利方案深度整合为一个统一模型，支持一键/分别抢单；
    5. 支持搜索关键词过滤与最大商户筛选数量配置（默认300家，支持100-800家）。
    """
    city_code = _safe_val(city_code, 440303)
    longitude = _safe_val(longitude, "114.13166")
    latitude = _safe_val(latitude, "22.548361")
    account_key = _safe_val(account_key, None)
    keyword = _safe_val(keyword, None)
    try:
        max_stores = int(_safe_val(max_stores, 300))
    except Exception:
        max_stores = 300
    # 强制限定为美团外卖
    platform = "meituan"

    # 0. 前置 WAF 拦截状态检查，避免在冷却期盲目发起大规模扫描
    if is_waf_blocked():
        remain_sec = get_waf_cooldown_seconds()
        return {
            "ok": False,
            "total_scanned_promotions": 0,
            "total_scanned_stores": 0,
            "dual_rebate_count": 0,
            "stores": [],
            "message": f"当前网络IP处于官方腾讯云WAF临时频次拦截保护冷却中（剩余约 {remain_sec} 秒），系统已自动拦截批量扫描以避免延长封锁。如需立即恢复，请切换手机热点或重启光猫更换IP。"
        }

    accounts = db.get_all_accounts()
    if not accounts and not account_key:
        return {
            "ok": False,
            "total_scanned_promotions": 0,
            "total_scanned_stores": 0,
            "dual_rebate_count": 0,
            "stores": [],
            "message": "请先绑定或选择小蚕账号"
        }

    target_acc = None
    if account_key:
        target_acc = db.get_account_by_key(account_key)
    if not target_acc and accounts:
        target_acc = accounts[0]

    if not target_acc:
        return {"ok": False, "stores": [], "total_scanned_promotions": 0, "total_scanned_stores": 0, "dual_rebate_count": 0, "message": "当前账号凭据无效"}

    token = target_acc.get("token")
    silk_id = target_acc.get("silk_id")
    user_id = target_acc.get("user_id")

    if target_acc:
        if target_acc.get("longitude") and (not longitude or longitude in ("114.305393", "114.13166", "")):
            longitude = target_acc.get("longitude")
        if target_acc.get("latitude") and (not latitude or latitude in ("30.593099", "22.548361", "")):
            latitude = target_acc.get("latitude")
        if target_acc.get("city_code") and (not city_code or city_code in (420100, 440303)):
            city_code = target_acc.get("city_code")

    try:
        lat = float(latitude)
    except Exception:
        lat = 22.548361
    try:
        lon = float(longitude)
    except Exception:
        lon = 114.13166

    logger.info(f"开始执行美团同店双返利/多活动高精度扫描: city={city_code}, lon={lon}, lat={lat}, 目标商户范围={max_stores}, 关键词={keyword}")

    seen_store_keys = set()
    store_groups: Dict[str, List[Dict[str, Any]]] = {}
    seen_promo_keys = set()
    all_raw_promotions = []

    # 1. 若用户指定了搜索关键词（如 曼玲粥、tea'stone、老碗会 等），并发调用满减搜索与官方按比例返现检索
    search_promos = []
    if keyword and keyword.strip():
        kw_clean = keyword.strip()
        try:
            s_tasks = [
                client.search_stores(
                    keyword=kw_clean,
                    city_code=city_code,
                    longitude=str(lon),
                    latitude=str(lat),
                    offset=s_off,
                    limit=20,
                    token=token,
                    silk_id=silk_id,
                    user_id=user_id
                )
                for s_off in [0, 20, 40]
            ]
            sj_task = client.search_shangjin_stores(
                keyword=kw_clean,
                latitude=lat,
                longitude=lon,
                token=token,
                silk_id=silk_id,
                user_id=user_id,
                city_code=city_code,
                sort_type=3,
                page_pv_id=""
            )
            s_results = await asyncio.gather(*(s_tasks + [sj_task]), return_exceptions=True)
            for sr in s_results[:len(s_tasks)]:
                if isinstance(sr, dict):
                    raw_proms = sr.get("promotions") or sr.get("promotion_list") or []
                    for rp in raw_proms:
                        extracted = _extract_all_promos_from_raw(rp, is_search=True, only_meituan=True, user_lat=lat, user_lon=lon)
                        search_promos.extend(extracted)
            sj_res = s_results[len(s_tasks)]
            if isinstance(sj_res, dict):
                pois = sj_res.get("poi_list") or []
                for poi in pois:
                    search_promos.extend(_normalize_shangjin_item(poi, user_lat=lat, user_lon=lon))
                # 若赏金商户返回满一页，继续拉取下一页以确保双返利覆盖完整
                pv_id = sj_res.get("page_pv_id")
                if pv_id and len(pois) >= 10:
                    try:
                        sj2 = await client.search_shangjin_stores(
                            keyword=kw_clean,
                            latitude=lat,
                            longitude=lon,
                            token=token,
                            silk_id=silk_id,
                            user_id=user_id,
                            city_code=city_code,
                            sort_type=3,
                            page_pv_id=pv_id
                        )
                        if isinstance(sj2, dict):
                            for poi2 in sj2.get("poi_list") or []:
                                search_promos.extend(_normalize_shangjin_item(poi2, user_lat=lat, user_lon=lon))
                    except Exception:
                        pass
            logger.info(f"双返利专属搜索关键词 '{kw_clean}' 精准命中 {len(search_promos)} 条活动 (含满减与按比例返)")
        except Exception as e:
            logger.warning(f"双返利专属搜索失败: {e}")

    for promo in search_promos:
        sname = (promo.get("name") or "").strip()
        if not sname:
            continue
        skey = _get_store_branch_key("meituan", sname)
        seen_store_keys.add(skey)
        if skey not in store_groups:
            store_groups[skey] = []
        pkey = f"{promo.get('platform')}_{promo.get('store_id')}_{promo.get('promotion_id')}"
        if pkey not in seen_promo_keys:
            seen_promo_keys.add(pkey)
            store_groups[skey].append(promo)
            all_raw_promotions.append(promo)

    # 2. 模拟客户端下拉触底流式分页获取附近店铺：
    # 并发拉取官方附近店铺 Feed 与美团赏金按比例返现池；
    # 按照距离由近到远由近及远流式拉取，并按商户数量设定范围 (target_limit) 智能停止。
    offset = 0
    page_size = 35
    target_limit = max_stores if (max_stores and max_stores > 0) else 300
    max_pages = max(80, int(target_limit // 10) + 15)
    
    round_idx = 0
    batch_size = 2  # 每次微批次并行拉取 2 页触底数据 (70 条)，降低并发以防止触发腾讯云 WAF 频次拦截
    shangjin_pv = ""
    should_stop = False

    while (round_idx * batch_size) < max_pages:
        if is_waf_blocked():
            logger.warning("附近店铺流式触底加载：检测到 WAF 风控熔断，立即终止扫描")
            should_stop = True
            break

        if target_limit > 0 and len(seen_store_keys) >= target_limit:
            logger.info(f"附近店铺流式触底加载：已获取商户数 {len(seen_store_keys)} 达到目标范围 {target_limit}，停止继续加载")
            break

        current_offsets = [offset + (i * page_size) for i in range(batch_size)]
        feed_tasks = [
            client.get_store_list(
                city_code=city_code,
                longitude=str(lon),
                latitude=str(lat),
                offset=off,
                limit=page_size,
                token=token,
                silk_id=silk_id,
                user_id=user_id,
                sort=1  # 严格按距离最近由近及远扫描
            )
            for off in current_offsets
        ]
        # 每轮并发拉取 1 批官方美团赏金按比例返现商户流
        sj_task = client.search_shangjin_stores(
            keyword="",
            latitude=lat,
            longitude=lon,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            city_code=city_code,
            sort_type=3,
            page_pv_id=shangjin_pv
        )

        all_batch = await asyncio.gather(*(feed_tasks + [sj_task]), return_exceptions=True)
        if is_waf_blocked():
            logger.warning("附近店铺触底批次拉取检测到 WAF 风控熔断，立即终止后续扫描")
            should_stop = True
            break
        batch_results = all_batch[:len(feed_tasks)]
        sj_res = all_batch[len(feed_tasks)]
        empty_in_batch = 0

        for r_idx, res in enumerate(batch_results):
            if isinstance(res, Exception) or not isinstance(res, dict):
                logger.warning(f"附近店铺触底批次 (offset={current_offsets[r_idx]}) 异常: {res}")
                empty_in_batch += 1
                continue

            feed_items = res.get("feed_items") or res.get("items") or res.get("promotion_list") or []
            if not feed_items:
                empty_in_batch += 1
                continue

            for item in feed_items:
                promos = _extract_all_promos_from_raw(item, is_search=False, only_meituan=True, user_lat=lat, user_lon=lon)
                if not promos:
                    continue

                for promo in promos:
                    sname = (promo.get("name") or "").strip()
                    if not sname:
                        continue
                    skey = _get_store_branch_key("meituan", sname)

                    seen_store_keys.add(skey)
                    if skey not in store_groups:
                        store_groups[skey] = []

                    pkey = f"{promo.get('platform')}_{promo.get('store_id')}_{promo.get('promotion_id')}"
                    if pkey not in seen_promo_keys:
                        seen_promo_keys.add(pkey)
                        store_groups[skey].append(promo)
                        all_raw_promotions.append(promo)

        if isinstance(sj_res, dict):
            shangjin_pv = sj_res.get("page_pv_id") or shangjin_pv
            pois = sj_res.get("poi_list") or []
            for poi in pois:
                promos = _normalize_shangjin_item(poi, user_lat=lat, user_lon=lon)
                for promo in promos:
                    sname = (promo.get("name") or "").strip()
                    if not sname:
                        continue
                    skey = _get_store_branch_key("meituan", sname)

                    seen_store_keys.add(skey)
                    if skey not in store_groups:
                        store_groups[skey] = []

                    pkey = f"{promo.get('platform')}_{promo.get('store_id')}_{promo.get('promotion_id')}"
                    if pkey not in seen_promo_keys:
                        seen_promo_keys.add(pkey)
                        store_groups[skey].append(promo)
                        all_raw_promotions.append(promo)
        else:
            logger.warning(f"赏金按比例返现接口异常 (round={round_idx}): {sj_res}")

        if empty_in_batch == len(batch_results):
            if round_idx == 0:
                # 第一轮若全空，可能是整点接口瞬时拥堵或网络延迟，等待 1.2 秒重试一次，防止开局误判为空
                logger.warning("附近店铺第 1 轮返回空或异常，等待 1.2 秒执行快速重试...")
                await asyncio.sleep(1.2)
                retry_feed_tasks = [
                    client.get_store_list(
                        city_code=city_code,
                        longitude=str(lon),
                        latitude=str(lat),
                        offset=off,
                        limit=page_size,
                        token=token,
                        silk_id=silk_id,
                        user_id=user_id,
                        sort=1
                    )
                    for off in current_offsets
                ]
                retry_results = await asyncio.gather(*retry_feed_tasks, return_exceptions=True)
                retry_empty = 0
                for r_idx, res in enumerate(retry_results):
                    if isinstance(res, Exception) or not isinstance(res, dict):
                        retry_empty += 1
                        continue
                    feed_items = res.get("feed_items") or res.get("items") or res.get("promotion_list") or []
                    if not feed_items:
                        retry_empty += 1
                        continue
                    for item in feed_items:
                        promos = _extract_all_promos_from_raw(item, is_search=False, only_meituan=True, user_lat=lat, user_lon=lon)
                        for promo in promos:
                            sname = (promo.get("name") or "").strip()
                            if not sname:
                                continue
                            skey = _get_store_branch_key("meituan", sname)
                            seen_store_keys.add(skey)
                            if skey not in store_groups:
                                store_groups[skey] = []
                            pkey = f"{promo.get('platform')}_{promo.get('store_id')}_{promo.get('promotion_id')}"
                            if pkey not in seen_promo_keys:
                                seen_promo_keys.add(pkey)
                                store_groups[skey].append(promo)
                                all_raw_promotions.append(promo)
                if retry_empty == len(retry_results):
                    should_stop = True
            else:
                should_stop = True

        logger.info(
            f"附近店铺触底加载第 {round_idx + 1} 轮: offset={offset}..{offset + (batch_size-1)*page_size}, "
            f"累计已扫描商户={len(seen_store_keys)} (设定范围={target_limit})"
        )

        if (target_limit > 0 and len(seen_store_keys) >= target_limit) or should_stop:
            break

        offset += batch_size * page_size
        round_idx += 1
        # 防 WAF 频次风控：微批次之间增加平滑随机延时 (350ms ~ 650ms)
        import random
        await asyncio.sleep(random.uniform(0.35, 0.65))

    # 赏金按比例返现池深度保障：
    # 若在流式循环中获取到的比例方案偏少 (< 20条) 或接口发生抖动，执行独立赏金池补全
    existing_percent_promos = [p for p in all_raw_promotions if p.get("rebate_type") == "percent"]
    if len(existing_percent_promos) < 20 and not is_waf_blocked():
        logger.info(f"赏金比例返现活动数量较少 ({len(existing_percent_promos)}条)，执行独立赏金池补偿拉取...")
        shangjin_extra_pv = ""
        for _ in range(4):
            if is_waf_blocked():
                break
            try:
                import random
                await asyncio.sleep(random.uniform(0.3, 0.5))
                sj_extra = await client.search_shangjin_stores(
                    keyword="",
                    latitude=lat,
                    longitude=lon,
                    token=token,
                    silk_id=silk_id,
                    user_id=user_id,
                    city_code=city_code,
                    sort_type=3,
                    page_pv_id=shangjin_extra_pv
                )
                if isinstance(sj_extra, dict):
                    shangjin_extra_pv = sj_extra.get("page_pv_id") or shangjin_extra_pv
                    pois = sj_extra.get("poi_list") or []
                    for poi in pois:
                        promos = _normalize_shangjin_item(poi, user_lat=lat, user_lon=lon)
                        for promo in promos:
                            sname = (promo.get("name") or "").strip()
                            if not sname:
                                continue
                            skey = _get_store_branch_key("meituan", sname)
                            seen_store_keys.add(skey)
                            if skey not in store_groups:
                                store_groups[skey] = []
                            pkey = f"{promo.get('platform')}_{promo.get('store_id')}_{promo.get('promotion_id')}"
                            if pkey not in seen_promo_keys:
                                seen_promo_keys.add(pkey)
                                store_groups[skey].append(promo)
                                all_raw_promotions.append(promo)
                    if not pois:
                        break
            except Exception as e_sj:
                logger.warning(f"独立赏金池补偿拉取异常: {e_sj}")
                break

    # 3. 停止加载后，严格按照用户的核心定义筛选美团同店双返利：
    # 核心规则：同一商户名下，必须同时存在「实付满返」与「按比例返」两类方案！
    dual_stores = []
    for gkey, plist in store_groups.items():
        fixed_plans = [
            p for p in plist 
            if p.get("rebate_type") == "fixed" and (p.get("rebate_price", 0) > 0 or p.get("order_money", 0) > 0)
        ]
        percent_plans = [
            p for p in plist 
            if p.get("rebate_type") == "percent" and (p.get("rebate_rate", 0) > 0 or p.get("rebate_price", 0) > 0)
        ]

        # 严格满足：必须同时拥有实付满返和按比例返！
        if not (fixed_plans and percent_plans):
            continue

        fixed_plans.sort(key=lambda x: (
            0 if x.get("left_number", 0) > 0 else 1,
            -x.get("rebate_price", 0),
            -x.get("left_number", 0)
        ))
        percent_plans.sort(key=lambda x: (
            0 if x.get("left_number", 0) > 0 else 1,
            -x.get("rebate_rate", 0),
            -x.get("rebate_price", 0),
            -x.get("left_number", 0)
        ))

        best_fixed = fixed_plans[0]
        best_percent = percent_plans[0]
        dual_tag = "实付满返 + 按比例返"
        dual_desc = f"{best_fixed.get('rebate_desc')} + {best_percent.get('rebate_desc')}"

        base = plist[0]
        # 提取同店多方案中最精确的非零正向距离
        best_dist = 0
        best_dist_text = "附近"
        for p in plist:
            p_dist = p.get("distance", 0) or 0
            if p_dist > 0:
                if best_dist == 0 or p_dist < best_dist:
                    best_dist = p_dist
                    best_dist_text = p.get("distance_text") or f"{p_dist}m"
        if best_dist == 0 and base.get("distance_text"):
            best_dist_text = base.get("distance_text")

        # 深度提取同店多方案中包含的有效真实门牌地址（满减活动由官方下发门牌地址，按比例赏金接口官方未下发）
        best_address = ""
        for p in plist:
            addr = (p.get("address") or "").strip()
            if addr:
                best_address = addr
                break
        if not best_address:
            for fp in fixed_plans:
                addr = (fp.get("address") or "").strip()
                if addr:
                    best_address = addr
                    break

        store_item = {
            "store_id": base.get("store_id") or str(uuid.uuid4()),
            "name": _clean_emoji(base.get("name", "美团同店双返利店铺")),
            "platform": "meituan",
            "store_platform": 1,
            "dual_tag": dual_tag,
            "icon": base.get("icon") or "",
            "distance": best_dist,
            "distance_text": best_dist_text,
            "condition": base.get("condition", "无需评价"),
            "start_time": best_fixed.get("start_time", "00:00"),
            "end_time": best_fixed.get("end_time", "23:59"),
            "opening_hours": base.get("opening_hours") or "00:00-23:59",
            "delivery_time_tip": base.get("delivery_time_tip") or "",
            "address": best_address or base.get("address", ""),
            
            "fixed_plan": best_fixed,
            "percent_plan": best_percent,
            "fixed_plans": fixed_plans,
            "percent_plans": percent_plans,
            "fixed_left": max(0, int(best_fixed.get("left_number") or 0)),
            "percent_left": max(0, int(best_percent.get("left_number") or 0)),

            # 兼容通用字段
            "order_money": best_fixed.get("order_money", 0),
            "rebate_price": best_fixed.get("rebate_price", 0),
            "rebate_rate": best_percent.get("rebate_rate", 0),
            "rebate_desc": dual_desc,
            "rebate_type": "dual",
            "rebate_type_text": "同店双返利",
            "left_number": max(best_fixed.get("left_number", 0), best_percent.get("left_number", 0)),
            "need_brand_coupon": any(p.get("need_brand_coupon") for p in plist),
            "brand_left_number": max(p.get("brand_left_number", 0) for p in plist),
            "is_vip_brand": any(p.get("is_vip_brand") for p in plist),
            "days_limit": plist[0].get("days_limit", 0),
            "days_order_limit": plist[0].get("days_order_limit", 1),
            "promotions": fixed_plans + percent_plans,
            "promotion_count": len(fixed_plans) + len(percent_plans),
            "is_dual_rebate": True
        }
        dual_stores.append(store_item)

    # 4. 排序与关键词处理 (优先展示关键词命中的商户，并按距离由近到远排列)
    if keyword and keyword.strip():
        kw_clean = keyword.strip().lower()
        dual_stores.sort(
            key=lambda x: (
                0 if (kw_clean in x.get("name", "").lower() or 
                      kw_clean in (x.get("fixed_plan", {}).get("rebate_desc", "")).lower() or 
                      kw_clean in (x.get("percent_plan", {}).get("rebate_desc", "")).lower() or
                      any(kw_clean in p.get("rebate_desc", "").lower() for p in x.get("promotions", [])))
                else 1, 
                x.get("distance", 0) if x.get("distance", 0) > 0 else 9999999
            )
        )
    else:
        dual_stores.sort(key=lambda x: (x.get("distance", 0) if x.get("distance", 0) > 0 else 9999999))

    total_found_count = len(dual_stores)
    logger.info(f"同店双返利/多活动筛选完成: 流式拉取商户={len(seen_store_keys)}, 活动档位={len(seen_promo_keys)}, 筛选出双方案商户={total_found_count}")

    return {
        "ok": True,
        "total_scanned_promotions": len(seen_promo_keys),
        "total_scanned_stores": len(seen_store_keys),
        "dual_rebate_count": total_found_count,
        "returned_count": total_found_count,
        "percent_promo_count": len([p for p in all_raw_promotions if p.get("rebate_type") == "percent"]),
        "fixed_promo_count": len([p for p in all_raw_promotions if p.get("rebate_type") == "fixed"]),
        "max_stores": max_stores,
        "stores": dual_stores,
        "message": f"扫描完成：已扫描附近 {len(seen_store_keys)} 家商户，成功筛选出 {total_found_count} 家同店双福利/多活动店铺"
    }


@router.post("/store/grab-now")
async def grab_store_now(data: Dict[str, Any] = Body(...)):
    """
    即时抢单接口
    当前时间已到且有名额时，直接执行抢单；若因库存瞬间被抢光导致失败，返回 can_monitor=True
    """
    account_key = data.get("account_key")
    if not account_key:
        raise HTTPException(status_code=400, detail="请先选择小蚕账号")
    account = db.get_account_by_key(account_key)
    if not account or not account.get("token"):
        raise HTTPException(status_code=400, detail="当前账号已退出或凭据失效")

    promotion_id = data.get("promotion_id")
    if not promotion_id:
        raise HTTPException(status_code=400, detail="缺少 promotion_id 参数")

    from ..core.appointment_worker import execute_grab_for_appointment
    apt_data = {
        "id": 0,
        "account_key": account_key,
        "store_id": str(data.get("store_id", "")),
        "store_name": data.get("store_name", "店铺活动"),
        "promotion_id": str(promotion_id),
        "platform": data.get("platform", "meituan"),
        "store_platform": int(data.get("store_platform") or (3 if data.get("platform") in ("jingdong", "jd") else (2 if data.get("platform") == "eleme" else 1))),
        "order_money": float(data.get("order_money", 0.0)),
        "rebate_price": float(data.get("rebate_price", 0.0)),
        "rebate_desc": data.get("rebate_desc", ""),
        "redpack_mode": int(data.get("redpack_mode", 0)),
        "redpack_id": data.get("redpack_id"),
        "redpack_name": data.get("redpack_name", "")
    }

    res = await execute_grab_for_appointment(apt_data, account, advance=bool(data.get("advance", False)), action_source="store_grab")
    if res["ok"]:
        return {
            "ok": True,
            "order_id": res.get("order_id"),
            "message": f"小蚕官方抢单成功！已为您锁定【{apt_data['store_name']}】名额 (订单ID: {res.get('order_id')})！"
        }
    else:
        err_code = res.get("code")
        err_msg = res.get("message") or "抢单未成功"
        # 仅当官方提示无名额/开仓已被抢光时才推荐开启名额监听；若为卡券不足(61)、未登录、风控等，不应弹出名额抢空模态框
        can_monitor = err_code in (40003, 40004, 40037, 40038, 40039, 40040) or "名额" in err_msg or "抢完" in err_msg or "已满" in err_msg
        return {
            "ok": False,
            "code": err_code,
            "can_monitor": can_monitor,
            "message": err_msg
        }


@router.post("/store/cancel-promotion-order")
async def cancel_promotion_order(data: Dict[str, Any] = Body(...)):
    """取消已抢到的霸王餐订单名额 (对齐小蚕官方 SilkwormService.CancelPromotionQuota) 并打入运行日志"""
    account_key = data.get("account_key") or data.get("account")
    order_id = data.get("promotion_order_id") or data.get("order_id")
    if not account_key:
        raise HTTPException(status_code=400, detail="请先选择账号")
    if not order_id:
        raise HTTPException(status_code=400, detail="缺少订单编号 promotion_order_id")
    account = db.get_account_by_key(account_key)
    if not account or not account.get("token"):
        raise HTTPException(status_code=400, detail="账号凭据失效或未登录")

    job_id = f"job_cancel_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    nickname = account.get("nickname") or account_key
    cancel_steps = []
    t_start = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    cancel_steps.append(f"[{t_start}] 启动霸王餐取消名额流程 - 账号: 【{nickname}】, 官方订单号: #{order_id}")

    try:
        t_req = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        cancel_steps.append(f"[{t_req}] 步骤 1/2: 调用官方 SilkwormService.CancelPromotionQuota RPC 接口")
        res = await client.cancel_promotion_quota(
            promotion_order_id=int(order_id),
            token=account["token"],
            silk_id=account.get("silk_id"),
            user_id=account.get("user_id"),
            city_code=account.get("city_code") or 440303
        )
        t_fin = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        cancel_steps.append(f"[{t_fin}] 步骤 2/2: 官方响应成功，订单名额已退还，本地及远程锁定解除")
        cancel_steps.append(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 流程结束 - 取消名额成功")
        try:
            db.add_job_log(job_id, account_key, "store_cancel", "success", "\n".join(cancel_steps))
        except Exception:
            pass
        return {"ok": True, "message": "订单名额已成功在官方后台取消", "raw": res}
    except XiaoCanRPCError as e:
        t_err = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        cancel_steps.append(f"[{t_err}] 官方接口返回失败: {e.msg} (错误码: {e.code})")
        try:
            db.add_job_log(job_id, account_key, "store_cancel", "error", "\n".join(cancel_steps))
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"官方取消失败 [{e.code}]: {e.msg}")
    except Exception as e:
        t_err = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        cancel_steps.append(f"[{t_err}] 取消接口网络或服务异常: {str(e)}")
        try:
            db.add_job_log(job_id, account_key, "store_cancel", "error", "\n".join(cancel_steps))
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"取消名额失败: {str(e)}")


@router.post("/store/signup-redpacks")
async def get_signup_redpacks(data: Dict[str, Any] = Body(...)):
    """获取指定账号当前可用红包清单 (对齐小蚕官方 RedPackService.GetUserMaxRedPack)"""
    account_key = data.get("account_key") or data.get("account")
    if not account_key:
        raise HTTPException(status_code=400, detail="请选择账号")
    account = db.get_account_by_key(account_key)
    if not account or not account.get("token"):
        raise HTTPException(status_code=400, detail="账号未登录")

    try:
        res = await client.get_user_max_redpack(
            token=account["token"],
            silk_id=account.get("silk_id"),
            user_id=account.get("user_id"),
            city_code=account.get("city_code") or 440303
        )
        packs = res.get("platform_red_packs") or []
        items = []
        for p in packs:
            raw_reward = float(p.get("reward_num", 0) or 0) / 100
            items.append({
                "id": p.get("user_red_pack_id"),
                "name": p.get("name") or f"立减红包 ¥{raw_reward:.2f}",
                "reward_num": p.get("reward_num", 0),
                "end_time": p.get("end_time")
            })
        return {"ok": True, "items": items, "max_redpack": res.get("rep_pack")}
    except Exception as e:
        logger.warning(f"获取报名红包异常: {e}")
        return {"ok": True, "items": [], "max_redpack": None}


@router.post("/store/appoint")
async def create_appointment(data: Dict[str, Any] = Body(...)):
    """提交店铺抢单预约或名额监听任务"""
    account_key = data.get("account_key")
    if not account_key or not db.get_account_by_key(account_key):
        raise HTTPException(status_code=400, detail="未登录有效小蚕账号，无法创建预约或监听任务")
    
    task_type = data.get("task_type") or ("monitor" if data.get("mode") == "monitor" else "countdown")
    data["task_type"] = task_type
    
    # 初始状态设为 running，真实追踪全流程生命周期
    task_name = "实时名额监听捡漏任务" if task_type == "monitor" else "倒计时预约抢单任务"
    log_tid = "store_monitor" if task_type == "monitor" else "store_appoint"
    job_id = f"job_create_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    account = db.get_account_by_key(account_key)
    nick = account.get("nickname") if account else account_key
    plat_str = "美团外卖" if data.get("platform") == "meituan" else ("饿了么" if data.get("platform") == "eleme" else "京东外卖")
    use_adv = bool(data.get("use_advance_card"))
    adv_note = "【使用超前券·提前30分钟开抢】" if use_adv else ""
    rp_mode = int(data.get("redpack_mode", 0))
    rp_note = f"指定红包 #{data.get('redpack_id')} ({data.get('redpack_name', '')})" if rp_mode == 1 else ("不使用红包" if rp_mode == 2 else "自动最优红包")
    s_log = (
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 初始化{task_name}成功 - 任务装载就绪\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 目标店铺: 【{data.get('store_name', '商户活动')}】 (平台: {plat_str}, 活动ID: {data.get('promotion_id')})\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 预约策略: 开抢/开始时间 {data.get('start_time', '即刻')} {adv_note}, 监听截止 {data.get('until_time', '活动结束')}, 提前量 {data.get('early_ms', 500)}ms\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 卡券与红包关联: {rp_note} | {'已绑定超前抢单券' if use_adv else '未启用超前券'}\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 执行账号: 【{nick}】，已正式装载入自适应调度队列，持续跟踪执行流水..."
    )
    log_id = 0
    try:
        log_id = db.add_job_log(job_id, account_key, log_tid, "running", s_log)
    except Exception:
        pass

    data["log_id"] = log_id
    aid = db.add_appointment(data)
    # 将包含真实 aid 的信息反写至日志中
    if log_id > 0:
        updated_s_log = s_log.replace("任务装载就绪", f"任务编号 #{aid}")
        db.update_job_log(log_id, status="running", output=updated_s_log)

    return {"ok": True, "appointment_id": aid, "message": f"{task_name}已创建并加入调度队列"}


@router.post("/store/stock-watch")
async def create_stock_watch(data: Dict[str, Any] = Body(...)):
    """创建名额监控捡漏任务 (对齐小蚕会员助手 /api/store/stock-watch)"""
    account_key = data.get("account") or data.get("account_key")
    if not account_key or not db.get_account_by_key(account_key):
        raise HTTPException(status_code=400, detail="未找到有效账号")

    timeout_sec = int(data.get("timeout_sec") or 3600)
    poll_sec = int(data.get("poll_sec") or 5)
    
    # 计算截止时间
    now = datetime.now()
    until_dt = now + timedelta(seconds=timeout_sec)
    until_str = until_dt.strftime("%H:%M")

    job_id = f"job_watch_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    account = db.get_account_by_key(account_key)
    nick = account.get("nickname") if account else account_key
    store_title = data.get("store_name") or data.get("label") or "商户活动"
    s_log = (
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 启动名额监控捡漏任务 - 调度引擎就绪\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 店铺【{store_title}】(活动ID: {data['promotion_id']})\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 监听参数: 轮询频率每 {poll_sec} 秒, 最长持续 {timeout_sec//60} 分钟 (截止 {until_str})\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 监听通道已就绪，检测到名额释放时将执行毫秒级抢单..."
    )
    log_id = 0
    try:
        log_id = db.add_job_log(job_id, account_key, "store_monitor", "running", s_log)
    except Exception:
        pass

    apt_data = {
        "account_key": account_key,
        "store_id": str(data.get("store_id") or ""),
        "store_name": store_title,
        "store_icon": data.get("store_icon") or data.get("icon") or "",
        "promotion_id": str(data["promotion_id"]),
        "task_type": "monitor",
        "start_time": now.strftime("%H:%M"),
        "until_time": until_str,
        "check_interval": poll_sec,
        "redpack_mode": 0 if data.get("redpack_mode") == "auto" else (2 if data.get("redpack_mode") == "none" else 1),
        "redpack_id": data.get("redpack_id"),
        "platform": "meituan" if int(data.get("store_platform") or 1) == 1 else "eleme",
        "log_id": log_id
    }
    aid = db.add_appointment(apt_data)
    if log_id > 0:
        updated_s_log = s_log.replace("调度引擎就绪", f"任务编号 #{aid}")
        db.update_job_log(log_id, status="running", output=updated_s_log)

    return {"ok": True, "appointment_id": aid, "message": f"已开始名额监控 · 最长 {timeout_sec//60} 分钟 · 每 {poll_sec} 秒"}


@router.post("/store/keyword-watch")
async def create_keyword_watch(data: Dict[str, Any] = Body(...)):
    """创建定时搜索捡漏任务 (对齐小蚕会员助手 /api/store/keyword-watch)"""
    account_key = data.get("account") or data.get("account_key")
    if not account_key or not db.get_account_by_key(account_key):
        raise HTTPException(status_code=400, detail="未找到有效账号")

    keyword = (data.get("keyword") or "").strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="缺少搜索关键词")

    start_at = data.get("start_at") or datetime.now().strftime("%H:%M")
    timeout_sec = int(data.get("timeout_sec") or 3600)
    poll_sec = max(10, int(data.get("poll_sec") or 30))
    platforms = data.get("platforms") or ["meituan"]
    plat = platforms[0] if platforms else "meituan"

    # 计算截止时间
    now = datetime.now()
    until_dt = now + timedelta(seconds=timeout_sec)
    until_str = until_dt.strftime("%H:%M")

    job_id = f"job_kw_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    account = db.get_account_by_key(account_key)
    nick = account.get("nickname") if account else account_key
    s_log = (
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 创建商户定时搜索捡漏任务 - 调度引擎就绪\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 搜索关键词: 「{keyword}」 (平台: {plat})\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 检索时段: 开始于 {start_at}, 截止至 {until_str}, 频率每 {poll_sec} 秒\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 账号【{nick}】自动搜索流水已启动，持续监听中..."
    )
    log_id = 0
    try:
        log_id = db.add_job_log(job_id, account_key, "store_keyword", "running", s_log)
    except Exception:
        pass

    apt_data = {
        "account_key": account_key,
        "store_id": "0",
        "store_name": keyword,
        "promotion_id": "0",
        "task_type": "keyword",
        "start_time": start_at,
        "until_time": until_str,
        "check_interval": poll_sec,
        "platform": plat,
        "redpack_mode": 0 if data.get("redpack_mode") == "auto" else (2 if data.get("redpack_mode") == "none" else 1),
        "redpack_id": data.get("redpack_id"),
        "rebate_desc": f"定时搜索【{keyword}】",
        "log_id": log_id
    }
    aid = db.add_appointment(apt_data)
    if log_id > 0:
        updated_s_log = s_log.replace("调度引擎就绪", f"任务编号 #{aid}")
        db.update_job_log(log_id, status="running", output=updated_s_log)

    return {"ok": True, "appointment_id": aid, "appointment": {"id": aid, "start_at": start_at}, "message": f"已添加定时搜索「{keyword}」-> {start_at}"}


@router.post("/store/name-monitor")
async def create_name_monitor(data: Dict[str, Any] = Body(...)):
    """
    创建店名抢单监听任务：
    支持多平台 (美团/饿了么/全部)、匹配模式 (包含/精确)、模式过滤 (全部/满返/按比例)、
    最低返利金额、最低返利比例、最高起送门槛、成功后自动停止等全方位配置。
    """
    account_key = data.get("account") or data.get("account_key")
    if not account_key or not db.get_account_by_key(account_key):
        raise HTTPException(status_code=400, detail="未找到有效账号")

    keyword = (data.get("keyword") or data.get("store_name") or "").strip()
    if not keyword:
        raise HTTPException(status_code=400, detail="缺少监听商户名称或关键词")

    match_mode = data.get("match_mode") or "contains"
    platform = data.get("platform") or "all"
    rebate_mode_filter = data.get("rebate_mode_filter") or "all"
    min_rebate_price = float(data.get("min_rebate_price") or 0.0)
    min_rebate_rate = float(data.get("min_rebate_rate") or 0.0)
    max_order_money = float(data.get("max_order_money") or 0.0)
    auto_stop_on_success = int(data.get("auto_stop_on_success") if data.get("auto_stop_on_success") is not None else 1)

    raw_timeout = data.get("timeout_sec")
    timeout_sec = int(raw_timeout) if raw_timeout is not None else 7200
    poll_sec = max(5, int(data.get("check_interval") or data.get("poll_sec") or 10))

    now = datetime.now()
    start_at = data.get("start_time") or now.strftime("%H:%M")
    if data.get("until_time"):
        until_str = data.get("until_time")
    elif timeout_sec > 0:
        until_dt = now + timedelta(seconds=timeout_sec)
        until_str = until_dt.strftime("%H:%M")
    else:
        until_str = "23:59"

    job_id = f"job_namemon_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    account = db.get_account_by_key(account_key)
    nick = account.get("nickname") if account else account_key

    plat_desc = "全平台 (美团/饿了么)" if platform == "all" else ("美团外卖" if platform == "meituan" else "饿了么")
    match_desc = "包含匹配" if match_mode == "contains" else "精确完全匹配"
    filter_desc = "全部方案" if rebate_mode_filter == "all" else ("仅实付满返" if rebate_mode_filter == "fixed" else "仅按比例返")
    req_parts = []
    if min_rebate_price > 0:
        req_parts.append(f"最低返利 ¥{min_rebate_price:.1f}")
    if min_rebate_rate > 0:
        req_parts.append(f"最低比例 {min_rebate_rate:.0f}%")
    if max_order_money > 0:
        req_parts.append(f"门槛≤¥{max_order_money:.1f}")
    req_str = "、".join(req_parts) if req_parts else "不设额外门槛"

    s_log = (
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 创建店名抢单监听任务 - 调度引擎装载就绪\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 目标商户关键词: 「{keyword}」 (模式: {match_desc} | 平台: {plat_desc})\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 方案筛选要求: {filter_desc} | 阈值规则: {req_str}\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 监听频率: 每 {poll_sec} 秒嗅探一次 | 时段: {start_at} 至 {until_str} | 抢成自停: {'开启' if auto_stop_on_success else '关闭'}\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 执行账号: 【{nick}】，高频嗅探与自动秒抢通道已就绪..."
    )
    log_id = 0
    try:
        log_id = db.add_job_log(job_id, account_key, "store_monitor", "running", s_log)
    except Exception:
        pass

    apt_data = {
        "account_key": account_key,
        "store_id": "0",
        "store_name": keyword,
        "store_icon": data.get("store_icon") or "",
        "promotion_id": "0",
        "task_type": "name_monitor",
        "start_time": start_at,
        "until_time": until_str,
        "check_interval": poll_sec,
        "platform": platform,
        "keyword": keyword,
        "match_mode": match_mode,
        "rebate_mode_filter": rebate_mode_filter,
        "min_rebate_price": min_rebate_price,
        "min_rebate_rate": min_rebate_rate,
        "max_order_money": max_order_money,
        "auto_stop_on_success": auto_stop_on_success,
        "redpack_mode": int(data.get("redpack_mode", 0)),
        "redpack_id": str(data.get("redpack_id") or ""),
        "redpack_name": str(data.get("redpack_name") or ""),
        "rebate_desc": f"店名监听【{keyword}】({plat_desc})",
        "log_id": log_id
    }
    aid = db.add_appointment(apt_data)
    if log_id > 0:
        updated_s_log = s_log.replace("调度引擎装载就绪", f"任务编号 #{aid}")
        db.update_job_log(log_id, status="running", output=updated_s_log)

    from ..core.appointment_worker import invalidate_worker_cache
    invalidate_worker_cache()

    return {
        "ok": True,
        "appointment_id": aid,
        "message": f"已启动「{keyword}」店名监听抢单任务 (编号 #{aid})"
    }


@router.get("/store/appointments")
async def list_appointments(account_key: Optional[str] = Query(None)):
    """获取预约队列列表"""
    items = db.get_appointments(account_key)
    return {"ok": True, "appointments": items, "total": len(items)}


@router.post("/store/appointments/{aid}/stop")
async def stop_appointment(aid: int):
    """主动停止指定名额监听或预约任务"""
    apt = db.get_appointment_by_id(aid)
    if not apt:
        raise HTTPException(status_code=404, detail="未找到对应的监听任务")
    db.update_appointment(aid, {
        "status": "cancelled",
        "outcome": f"用户已于 {time.strftime('%H:%M:%S')} 主动停止监听"
    })
    job_id = f"job_stop_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    acc_key = apt.get("account_key", "")
    s_log = (
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 用户主动停止监听任务 #{aid}\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 目标店铺: 【{apt.get('store_name', '商户活动')}】\n"
        f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 监听状态已转为 cancelled，定时轮询与突发抢单已解除"
    )
    linked_log_id = int(apt.get("log_id") or 0)
    if linked_log_id > 0:
        try:
            with db.get_conn() as conn:
                row = conn.execute("SELECT output FROM job_logs WHERE id = ?", (linked_log_id,)).fetchone()
                prev_output = row["output"] if row else ""
            new_output = (prev_output + "\n" + s_log).strip()
            db.update_job_log(linked_log_id, status="error", output=new_output)
        except Exception:
            db.add_job_log(job_id, acc_key, "store_monitor", "error", s_log)
    else:
        try:
            db.add_job_log(job_id, acc_key, "store_monitor", "error", s_log)
        except Exception:
            pass
    from ..core.appointment_worker import invalidate_worker_cache
    invalidate_worker_cache()
    return {"ok": True, "message": "已主动停止该名额监听任务"}


@router.delete("/store/appointments/{aid}")
async def cancel_appointment(aid: int):
    """取消店铺预约"""
    apt = db.get_appointment_by_id(aid)
    if apt:
        acc_key = apt.get("account_key", "")
        job_id = f"job_del_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        s_log = (
            f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 用户删除预约/监听任务 #{aid}\n"
            f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 目标商户: 【{apt.get('store_name', '商户活动')}】\n"
            f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 该任务已从系统调度队列永久移除"
        )
        linked_log_id = int(apt.get("log_id") or 0)
        if linked_log_id > 0:
            try:
                with db.get_conn() as conn:
                    row = conn.execute("SELECT output FROM job_logs WHERE id = ?", (linked_log_id,)).fetchone()
                    prev_output = row["output"] if row else ""
                new_output = (prev_output + "\n" + s_log).strip()
                db.update_job_log(linked_log_id, status="error", output=new_output)
            except Exception:
                db.add_job_log(job_id, acc_key, "store_appoint", "error", s_log)
        else:
            try:
                db.add_job_log(job_id, acc_key, "store_appoint", "error", s_log)
            except Exception:
                pass
    db.delete_appointment(aid)
    from ..core.appointment_worker import invalidate_worker_cache
    invalidate_worker_cache()
    return {"ok": True, "message": "已取消预约"}


# ---------------- 4.1 霸王餐订单管理接口 ---------------- #

@router.get("/orders")
async def list_orders(
    account_key: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    keyword: Optional[str] = Query(None),
    limit: int = Query(50),
    offset: int = Query(0)
):
    """获取真实霸王餐订单列表 (自动同步真实历史与进行中订单，100% 真实数据)"""
    account_key = _safe_val(account_key, None)
    status = _safe_val(status, None)
    platform = _safe_val(platform, None)
    keyword = _safe_val(keyword, None)
    limit = int(_safe_val(limit, 50))
    offset = int(_safe_val(offset, 0))

    accounts = db.get_all_accounts()
    if not accounts and not account_key:
        return {"ok": True, "orders": [], "total": 0, "message": "当前未登录或未托管小蚕账号"}

    acc = None
    if account_key:
        acc = db.get_account_by_key(account_key)
    if not acc and accounts:
        acc = accounts[0]
        account_key = acc.get("key")

    if not acc:
        return {"ok": True, "orders": [], "total": 0, "message": "当前账号已退出"}

    token = acc.get("token")
    if not token:
        return {"ok": True, "orders": [], "total": 0, "message": "当前账号凭据已失效或已退出"}

    city_code = acc.get("city_code", 440303)
    silk_id = acc.get("silk_id")
    user_id = acc.get("user_id")

    # 转换前端状态过滤参数为官方 RPC order_status
    # 官方真实状态集：0=待上传, 1=审核中, 2=已完成, 3=已驳回, 4=已取消, 99=全部
    query_st = 99
    if status == "pending":
        query_st = 0
    elif status == "auditing":
        query_st = 1
    elif status == "completed":
        query_st = 2
    elif status == "rejected":
        query_st = 3
    elif status == "cancelled":
        query_st = 4
    else:
        query_st = 99

    # 尝试从小蚕官方 RPC 同步最新真实订单
    try:
        rpc_res = await client.get_order_list(
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            order_status=query_st,
            offset=0,
            number=50,
            city_code=city_code
        )
        raw_orders = rpc_res.get("order_list") or rpc_res.get("orders") or []
        for o in raw_orders:
            order_sn = str(o.get("order_id_str") or o.get("promotion_order_id") or "")
            if not order_sn:
                continue
            promo = o.get("store_promotion") or {}
            store_obj = promo.get("store") or {}
            store_name = store_obj.get("name") or promo.get("store_name") or "小蚕霸王餐"
            store_icon = (
                store_obj.get("icon") or
                store_obj.get("logo") or
                store_obj.get("head_img") or
                promo.get("picture") or
                promo.get("store_icon") or
                promo.get("icon") or
                o.get("store_icon") or
                ""
            )
            store_id = str(store_obj.get("store_id") or "")
            if not store_icon and (store_id or store_name):
                try:
                    conn = db.get_db()
                    cur = conn.cursor()
                    r_icon = cur.execute(
                        "SELECT store_icon FROM orders WHERE (store_id = ? OR store_name = ?) AND store_icon != '' LIMIT 1",
                        (store_id, store_name)
                    ).fetchone()
                    if r_icon and r_icon[0]:
                        store_icon = r_icon[0]
                except Exception:
                    pass
            
            raw_om = float(o.get("store_platform_order_money", 0))
            order_money = round(raw_om / 100.0 if raw_om >= 100 else raw_om, 2)
            if order_money == 0 and o.get("platform_order_total_price"):
                try:
                    order_money = round(float(o.get("platform_order_total_price")), 2)
                except Exception:
                    pass

            raw_rebate = float(o.get("user_rebate", 0))
            rebate_money = round(raw_rebate / 100.0 if raw_rebate >= 100 else raw_rebate, 2)

            raw_orig = float(o.get("original_user_rebate", 0))
            orig_rebate = round(raw_orig / 100.0 if raw_orig >= 100 else raw_orig, 2)

            raw_redpack = float(o.get("redpack_reward_num", 0) or 0)
            redpack_reward = round(raw_redpack / 100.0 if raw_redpack >= 100 else raw_redpack, 2)

            sp = o.get("store_platform")
            if sp == 3:
                order_platform = "jingdong"
            elif sp == 2:
                order_platform = "eleme"
            else:
                order_platform = "meituan"

            # 官方状态枚举校准：0=待上传, 1=审核中, 2=已完成, 3=已驳回, 4=已取消, 99=全部
            raw_st = o.get("order_status", 0)
            platform_order_id = str(o.get("platform_order_id") or "").strip()

            if raw_st == 2:
                status_str = "completed"
            elif raw_st == 4:
                status_str = "cancelled"
            elif raw_st == 1:
                status_str = "auditing"
            elif raw_st == 3:
                status_str = "rejected"
            elif raw_st == 0:
                # 官方状态 0 统一为 pending (待上传大类，包含未绑单号与已绑单号待反馈)
                status_str = "pending"
            else:
                status_str = "completed" if raw_st in (5, 6) else "pending"

            cond_val = promo.get("rebate_condition")
            cond_raw = promo.get("rebate_condition_str") or ("无需评价" if cond_val == 99 else ("随心好评" if cond_val == 2 else "用餐反馈"))
            cond_str = cond_raw.replace("（需含字含图）", "").replace("(需含字含图)", "").strip() if cond_raw else "用餐反馈"
            if not cond_str or cond_str == "图文好评":
                cond_str = "用餐反馈"
            screens = o.get("platform_evaluation_screenshot") or []
            receipt_img = screens[0] if screens else ""
            ot = o.get("order_time")
            c_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ot)) if ot else time.strftime("%Y-%m-%d %H:%M:%S")
            tt = o.get("timeout_time")
            exp_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tt)) if tt else ""

            db.save_order({
                "account_key": account_key,
                "order_sn": order_sn,
                "promotion_order_id": int(o.get("promotion_order_id") or 0),
                "platform_order_id": platform_order_id,
                "store_id": store_id,
                "store_name": _clean_emoji(store_name),
                "store_icon": store_icon,
                "platform": order_platform,
                "order_money": order_money,
                "rebate_money": rebate_money,
                "original_user_rebate": orig_rebate,
                "redpack_reward_num": redpack_reward,
                "timeout_time": int(tt or 0),
                "status": status_str,
                "condition": cond_str,
                "receipt_img": receipt_img,
                "reject_reason": "",
                "expire_time": exp_at,
                "created_at": c_at,
                "order_time": int(ot or 0)
            })

        # 若本次查询为「待上传」(query_st == 0) 或全部，核验本地与官方待处理列表
        if query_st == 0 and account_key:
            official_pending_sns = {str(o.get("order_id_str") or o.get("promotion_order_id") or "") for o in raw_orders if o}
            local_pending = db.get_orders(account_key=account_key, status="pending", limit=100)
            now_str = time.strftime("%Y-%m-%d %H:%M:%S")
            now_ts = int(time.time())
            for lp in local_pending:
                lp_sn = str(lp.get("order_sn") or "")
                if lp_sn and lp_sn not in official_pending_sns:
                    lp_exp = lp.get("expire_time") or ""
                    lp_tt = int(lp.get("timeout_time") or 0)
                    if (lp_tt > 0 and lp_tt < now_ts) or (lp_exp and lp_exp < now_str):
                        db.update_order(lp["id"], {"status": "cancelled"})
    except Exception as e:
        logger.info(f"官方 RPC 同步订单异常 (将直接返回本地数据库记录): {e}")

    orders = db.get_orders(
        account_key=account_key,
        status=status,
        platform=platform,
        keyword=keyword,
        limit=limit,
        offset=offset
    )
    return {"ok": True, "orders": orders, "total": len(orders)}


@router.get("/orders/stats")
async def get_orders_stats(account_key: Optional[str] = Query(None)):
    """获取霸王餐订单返利与收益看板统计 (结合官方真实资产与本地订单进行综合核准)"""
    accounts = db.get_all_accounts()
    if not accounts and not account_key:
        return {
            "ok": True,
            "stats": {
                "total_orders": 0,
                "completed_orders": 0,
                "pending_orders": 0,
                "total_rebate": 0,
                "pending_rebate": 0,
                "total_spent": 0
            }
        }
    stats = db.get_order_stats(account_key=account_key)
    
    # 结合当前主控账号的官方原生资产进行综合校准
    acc = None
    if account_key:
        acc = db.get_account_by_key(account_key)
    if not acc and accounts:
        acc = accounts[0]
        
    if acc:
        official_completed = int(acc.get("completed_number") or 0)
        official_total_rebate = round(((acc.get("withdraw_total") or 0) + (acc.get("silk") or 0)) / 100.0, 2)
        if official_completed > stats["completed_orders"]:
            stats["completed_orders"] = official_completed
            stats["total_orders"] = max(stats["total_orders"], official_completed + stats["pending_orders"])
        if official_total_rebate > stats["total_rebate"]:
            stats["total_rebate"] = official_total_rebate
            
    return {"ok": True, "stats": stats}


@router.get("/dashboard/chart-data")
async def get_dashboard_chart_data_endpoint(account_key: Optional[str] = Query(None)):
    """获取仪表盘可视化图表数据 (近7天返利趋势、订单状态分布、平台分布)"""
    chart_data = db.get_dashboard_chart_data(account_key=account_key)
    return {"ok": True, "data": chart_data}


@router.get("/time")
async def get_beijing_time_endpoint(force: bool = False):
    """获取经由 ntp.aliyun.com 授时中心校准的高精度北京时间"""
    try:
        if force:
            await time_service.async_sync_ntp(force=True)
        return time_service.get_ntp_status()
    except Exception as e:
        logger.error(f"NTP授时异常: {e}")
        now_ts = time.time()
        bj_time_tuple = time.gmtime(now_ts + 8 * 3600)
        return {
            "ok": True,
            "timestamp": now_ts,
            "beijing_time": time.strftime("%Y-%m-%d %H:%M:%S", bj_time_tuple),
            "server": "ntp.aliyun.com",
            "synced": False,
            "offset": 0.0,
            "offset_ms": 0.0,
            "round_trip_ms": 0.0,
            "last_sync": now_ts
        }


@router.get("/scheduler/jobs")
async def get_live_scheduler_jobs():
    """获取当前调度中心实时注册的任务与下次触发时间"""
    from ..core.scheduler import scheduler
    jobs = []
    for j in scheduler.get_jobs():
        jobs.append({
            "id": j.id,
            "name": j.name,
            "next_run_time": str(j.next_run_time) if hasattr(j, "next_run_time") and j.next_run_time else None,
            "trigger": str(j.trigger)
        })
    return {"ok": True, "total": len(jobs), "jobs": jobs}


@router.post("/scheduler/reload")
async def reload_scheduler_jobs_endpoint():
    """动态重新装载调度中心任务"""
    from ..core.scheduler import reload_schedules
    reload_schedules()
    return {"ok": True, "message": "调度中心任务已重新装载"}


@router.get("/cache/stats")
async def cache_stats_endpoint():
    """查询 RPC 内存缓存状态"""
    from ..protocol.cache import cache_stats, cache_list
    return {"ok": True, "stats": cache_stats(), "entries": cache_list()[:20]}


@router.post("/cache/clear")
async def cache_clear_endpoint():
    """清空 RPC 内存缓存"""
    from ..protocol.cache import cache_clear_all, cache_stats
    before = cache_stats()["total_entries"]
    cache_clear_all()
    after = cache_stats()["total_entries"]
    return {"ok": True, "cleared": before, "remaining": after}


@router.post("/orders")
async def create_order_endpoint(data: Dict[str, Any] = Body(...)):
    """手动录入新订单"""
    if not data.get("store_icon"):
        sname = (data.get("store_name") or "").strip()
        if sname:
            try:
                conn = db.get_db()
                cur = conn.cursor()
                r_icon = cur.execute(
                    "SELECT store_icon FROM orders WHERE store_name = ? AND store_icon != '' LIMIT 1",
                    (sname,)
                ).fetchone()
                if r_icon and r_icon[0]:
                    data["store_icon"] = r_icon[0]
            except Exception:
                pass
    saved = db.save_order(data)
    return {"ok": True, "order": saved, "message": "订单已成功登记"}


@router.put("/orders/{order_id}")
async def update_order_endpoint(order_id: int, data: Dict[str, Any] = Body(...)):
    """更新霸王餐订单信息"""
    updated = db.update_order(order_id, data)
    if not updated:
        raise HTTPException(status_code=404, detail="订单不存在")
    return {"ok": True, "order": updated, "message": "订单信息已更新"}


@router.post("/orders/{order_id}/submit-platform-id")
async def submit_platform_order_id(order_id: int, data: Dict[str, Any] = Body(...)):
    """提交外卖单号进入小蚕平台审核返利流程 (100% 直连官方接口)"""
    platform_order_id = (data.get("platform_order_id") or "").strip()
    if not platform_order_id:
        raise HTTPException(status_code=400, detail="外卖平台订单号不能为空")
    
    target_order = db.get_order_by_id(order_id)
    if not target_order:
        raise HTTPException(status_code=404, detail="订单不存在")

    existing_id = (target_order.get("platform_order_id") or "").strip()
    if existing_id and existing_id != platform_order_id:
        raise HTTPException(status_code=400, detail="外卖平台订单号已绑定，平台禁止二次修改")

    promo_order_id = target_order.get("promotion_order_id") or 0
    if not promo_order_id:
        order_sn = target_order.get("order_sn", "")
        m = re.search(r'(\d{8,10})$', order_sn)
        if m:
            promo_order_id = int(m.group(1))

    # 尝试直连小蚕官方 RPC 提交外卖单号
    upstream_res = None
    account_key = target_order.get("account_key")
    acc = db.get_account_by_key(account_key) if account_key else None
    token = acc.get("token") if acc else None

    if promo_order_id and token:
        try:
            upstream_res = await client.invoke_rpc(
                server_name="SilkwormPromotion",
                method_name="PromotionService.SubmitPlatformOrderId",
                body={
                    "promotion_order_id": promo_order_id,
                    "platform_order_id": platform_order_id,
                    "order_type": 0,
                    "app_id": 20
                },
                city_code=acc.get("city_code", 420100),
                token=token
            )
            logger.info(f"小蚕官方提交外卖单号结果: {upstream_res}")
        except Exception as e:
            logger.warning(f"小蚕官方接口提交单号返回: {e}")

    has_receipt = bool(data.get("receipt_img"))
    new_status = "auditing" if has_receipt else "pending"
    updated = db.update_order(order_id, {
        "platform_order_id": platform_order_id,
        "status": new_status,
        "receipt_img": data.get("receipt_img", "")
    })
    msg = "外卖单号与评价凭证已提交审核！预计 2-24 小时内核销到账" if has_receipt else "外卖单号已成功绑定！待外卖送达后即可在小蚕APP提交评价反馈"
    return {
        "ok": True,
        "order": updated,
        "upstream_response": upstream_res,
        "message": msg
    }


@router.delete("/orders/{order_id}")
async def remove_order(order_id: int):
    """删除订单记录"""
    db.delete_order(order_id)
    return {"ok": True, "message": "订单记录已删除"}


@router.post("/orders/{order_id}/delay-voucher")
async def delay_order_voucher(order_id: int):
    """
    触发小蚕移动端原生「领红包延时2小时」特权 (延长活动订单凭证上传有效期 2 小时)
    对齐官方微服务: SilkwormMarketing.SilkwormMobileMarketingService.CompleteTaskEvent
    """
    target_order = db.get_order_by_id(order_id)
    if not target_order:
        raise HTTPException(status_code=404, detail="订单记录不存在")

    account_key = target_order.get("account_key")
    acc = db.get_account_by_key(account_key) if account_key else None
    if not acc or not acc.get("token"):
        raise HTTPException(status_code=400, detail="对应小蚕账号凭据已失效或未登录")

    promo_order_id = target_order.get("promotion_order_id") or 0
    if not promo_order_id:
        order_sn = target_order.get("order_sn", "")
        m = re.search(r'(\d{8,10})$', order_sn)
        if m:
            promo_order_id = int(m.group(1))

    if not promo_order_id:
        raise HTTPException(status_code=400, detail="该订单缺少官方活动订单号 promotion_order_id，无法申请延时")

    # 根据平台确定任务类型: 4 为美团 (user_receive_meituan_task), 3 为饿了么 (user_receive_elem_task)
    plat = (target_order.get("platform") or "meituan").lower()
    task_type = 3 if "ele" in plat else 4

    try:
        res = await client.complete_task_event(
            token=acc["token"],
            silk_id=acc.get("silk_id"),
            user_id=acc.get("user_id"),
            city_code=acc.get("city_code") or 440303,
            promotion_order_id=promo_order_id,
            task_type=task_type
        )
        is_delayed = bool(res.get("if_delay") or res.get("if_delay_success"))
        delay_time = res.get("delay_time", 7200)
        delay_hours = round(delay_time / 3600.0, 1) if delay_time else 2.0
        msg = f"官方领红包延时特权已成功激活！凭据上传有效期已顺延 {delay_hours} 小时" if (is_delayed or delay_time > 0) else "已成功向官方提交延时请求"
        return {
            "ok": True,
            "is_delayed": is_delayed,
            "delay_time": delay_time,
            "message": msg,
            "raw": res
        }
    except XiaoCanRPCError as e:
        logger.warning(f"领红包延时接口返回异常: {e}")
        raise HTTPException(status_code=400, detail=f"官方接口响应: {e.msg} (错误码: {e.code})")
    except Exception as e:
        logger.error(f"领红包延时请求失败: {e}")
        raise HTTPException(status_code=500, detail=f"领红包延时网络或服务异常: {str(e)}")


@router.post("/orders/{order_id}/use-free-card")
async def use_order_free_card(order_id: int, data: Dict[str, Any] = Body(default={})):
    """
    对指定订单核销使用每月霸王餐全额免单神券
    对齐官方微服务: SilkwormVip.VipRightsService.UseFreeOrderCard
    """
    target_order = db.get_order_by_id(order_id)
    if not target_order:
        raise HTTPException(status_code=404, detail="订单记录不存在")

    account_key = target_order.get("account_key")
    acc = db.get_account_by_key(account_key) if account_key else None
    if not acc or not acc.get("token"):
        raise HTTPException(status_code=400, detail="对应小蚕账号凭据已失效或未登录")

    platform_order_id = (data.get("platform_order_id") or target_order.get("platform_order_id") or "").strip()
    if not platform_order_id:
        raise HTTPException(status_code=400, detail="请先填写并绑定外卖平台订单号方可使用免单券")

    plat = (target_order.get("platform") or "meituan").lower()
    order_business = 2 if "ele" in plat else 1

    # 获取用户免单券 ID
    user_free_card_id = data.get("user_free_card_id")
    if not user_free_card_id:
        try:
            f_info_res = await client.get_user_free_order_info(
                token=acc["token"],
                silk_id=acc.get("silk_id"),
                user_id=acc.get("user_id"),
                city_code=acc.get("city_code") or 440303
            )
            info_data = f_info_res.get("info") or {}
            if not info_data.get("had"):
                raise HTTPException(status_code=400, detail="当前账号本月尚未抢得外卖全额免单神券，请先在每日 14:00 参与秒杀")
            if info_data.get("used"):
                raise HTTPException(status_code=400, detail="当前账号本月免单券已使用，每月限使用 1 次")
            user_free_card_id = info_data.get("user_free_card_id") or info_data.get("id")
        except HTTPException:
            raise
        except Exception as ex:
            logger.warning(f"自动查询免单券信息失败: {ex}")

    if not user_free_card_id:
        user_free_card_id = 0

    try:
        res = await client.use_free_order_card(
            order_no=str(platform_order_id),
            order_business=int(order_business),
            user_free_card_id=int(user_free_card_id),
            token=acc["token"],
            silk_id=acc.get("silk_id"),
            user_id=acc.get("user_id"),
            city_code=acc.get("city_code") or 440303
        )
        return {
            "ok": True,
            "message": "免单券核销使用成功！该笔订单已享全额免单返现",
            "raw": res
        }
    except XiaoCanRPCError as e:
        logger.warning(f"使用免单券官方接口返回异常: {e}")
        raise HTTPException(status_code=400, detail=f"官方接口响应: {e.msg} (错误码: {e.code})")
    except Exception as e:
        logger.error(f"使用免单券请求失败: {e}")
        raise HTTPException(status_code=500, detail=f"核销免单券网络或服务异常: {str(e)}")



# ---------------- 5. 运行日志与统计 ---------------- #

@router.get("/jobs")
async def list_jobs(account_key: Optional[str] = Query(None), limit: int = Query(50)):
    """获取执行历史日志"""
    logs = db.get_job_logs(account_key, limit)
    return {"ok": True, "jobs": logs}


@router.delete("/jobs")
async def clear_jobs(account_key: Optional[str] = Query(None)):
    """清空执行历史日志"""
    db.clear_job_logs(account_key)
    return {"ok": True, "message": "运行日志已清空"}


@router.get("/me")
async def get_my_info():
    """获取用户信息 (全免费：无限车位、无限积分、永久免激活)"""
    accounts = db.get_all_accounts()
    return {
        "ok": True,
        "user": {
            "nickname": "管理员 / 极客玩家",
            "email": "local@xiaocan.bot",
            "is_admin": True,
            "points": 999999,          # 零商业化，无限点数
            "slots_max": 999,           # 无限车位
            "slots_used": len(accounts),
            "expires_at": "2099-12-31", # 永久不过期
            "is_vip": True
        }
    }


# ---------------- 6. 系统与通知配置 ---------------- #

@router.get("/settings")
async def get_settings():
    """获取系统设置与各通知渠道当前状态"""
    clawbot_auth, clawbot_src = clawbot_client.load_auth()
    return {
        "ok": True,
        # 微信 ClawBot (腾讯 iLink 原生内置通道)
        "clawbot_enabled": db.get_setting("clawbot_enabled", True),
        "clawbot_auth_path": db.get_setting("clawbot_auth_path", ""),
        "clawbot_auth_json": db.get_setting("clawbot_auth_json", ""),
        "clawbot_status": {
            "ready": bool(clawbot_auth and clawbot_auth.get("token") and clawbot_auth.get("userId")),
            "source": clawbot_src,
            "user_id": clawbot_auth.get("userId", "") if clawbot_auth else "",
            "account_id": clawbot_auth.get("accountId", "") if clawbot_auth else "",
            "has_context_token": bool(clawbot_auth.get("contextToken")) if clawbot_auth else False,
            "saved_at": clawbot_auth.get("savedAt", "") if clawbot_auth else ""
        },
        # QQ 机器人 (OneBot V11 / mystool-bot)
        "qq_bot_enabled": db.get_setting("qq_bot_enabled", True),
        "qq_bot_api": db.get_setting("qq_bot_api", "http://127.0.0.1:8080"),
        "qq_bot_group_id": db.get_setting("qq_bot_group_id", "954658571"),
        "qq_bot_user_id": db.get_setting("qq_bot_user_id", "2371445972"),
        "qq_bot_target_type": db.get_setting("qq_bot_target_type", "group"),
        "qq_bot_token": db.get_setting("qq_bot_token", ""),
        # 企业微信群机器人
        "wecom_enabled": db.get_setting("wecom_enabled", False),
        "wecom_webhook": db.get_setting("wecom_webhook", ""),
        # iOS Bark
        "bark_enabled": db.get_setting("bark_enabled", False),
        "bark_url": db.get_setting("bark_url", ""),
        # Telegram Bot
        "telegram_enabled": db.get_setting("telegram_enabled", False),
        "tg_bot_token": db.get_setting("tg_bot_token", ""),
        "tg_chat_id": db.get_setting("tg_chat_id", ""),
        # 触发策略
        "notify_on_grab": db.get_setting("notify_on_grab", True),
        "notify_on_appoint": db.get_setting("notify_on_appoint", True),
        "notify_on_spike": db.get_setting("notify_on_spike", True),
        # 位置服务与天地图 Web API (tianditu.gov.cn)
        "tianditu_key": db.get_setting("tianditu_key", "109fd484f999e3c0472ab15fa38fe2ac")
    }


@router.post("/settings")
async def save_settings(data: Dict[str, Any] = Body(...)):
    """保存系统设置"""
    for k, v in data.items():
        if k != "clawbot_status":
            db.set_setting(k, v)
    return {"ok": True, "message": "设置已保存"}


@router.post("/settings/test-tianditu")
async def test_tianditu_endpoint(data: Dict[str, Any] = Body(...)):
    """测试天地图 Web API 服务 Token 连通性"""
    tianditu_key = str(data.get("tianditu_key", "")).strip()
    res = await tianditu_client.test_connection(tianditu_key)
    return res


@router.post("/settings/test-notify")
async def test_notify_endpoint(data: Dict[str, Any] = Body(...)):
    """测试单个通知渠道连通性"""
    channel = data.get("channel", "")
    config = data.get("config", {})
    if not channel:
        raise HTTPException(status_code=400, detail="通知渠道参数不能为空")
    res = await notifier.test_single_channel(channel, config)
    return res


@router.get("/settings/clawbot/status")
@router.get("/settings/clawbot-status")
async def clawbot_status_endpoint(custom_path: Optional[str] = None):
    """检测微信 ClawBot 凭证状态与会话活跃度"""
    auth, msg = clawbot_client.load_auth(custom_path)
    if not auth or not auth.get("token") or not auth.get("userId"):
        return {"ok": False, "ready": False, "message": msg}
    return {
        "ok": True,
        "ready": True,
        "message": msg,
        "user_id": auth.get("userId", ""),
        "account_id": auth.get("accountId", ""),
        "has_context_token": bool(auth.get("contextToken")),
        "saved_at": auth.get("savedAt", "")
    }


@router.post("/settings/clawbot/qr")
async def clawbot_get_qr_endpoint(data: Optional[Dict[str, Any]] = Body(default={})):
    """生成微信 ClawBot 登录二维码（包含内存渲染的 Base64 PNG 图片）"""
    local_token = data.get("local_token") if isinstance(data, dict) else None
    res = await clawbot_client.get_qr_code(local_token)
    return res


@router.get("/settings/clawbot/poll")
async def clawbot_poll_qr_endpoint(
    qrcode: str = Query(..., description="二维码 ticket 标识"),
    verify_code: Optional[str] = Query(None, description="手机微信提示的数字配对码")
):
    """长轮询检测微信 ClawBot 扫码与授权状态"""
    res = await clawbot_client.poll_qr_status(qrcode, verify_code)
    return res


@router.post("/settings/clawbot/check-activation")
async def clawbot_check_activation_endpoint():
    """长轮询检测并抓取用户发给 ClawBot 的消息以激活 context_token"""
    res = await clawbot_client.check_activation(timeout=15.0)
    return res


@router.post("/settings/clawbot/unbind")
async def clawbot_unbind_endpoint():
    """解绑微信 ClawBot 账号并清除本地凭据"""
    clawbot_client.clear_auth()
    return {"ok": True, "message": "微信 ClawBot 已成功解绑"}


# ---------------- 7. 设备定位与逆地理编码 ---------------- #

CITY_CODE_MAP = {
    "武汉": 420100, "武汉市": 420100,
    "孝感": 420900, "孝感市": 420900,
    "应城": 420981, "应城市": 420981,
    "襄阳": 420600, "襄阳市": 420600,
    "宜昌": 420500, "宜昌市": 420500,
    "黄冈": 421100, "黄冈市": 421100,
    "黄石": 420200, "黄石市": 420200,
    "荆州": 421000, "荆州市": 421000,
    "北京": 110100, "北京市": 110100,
    "上海": 310100, "上海市": 310100,
    "广州": 440100, "广州市": 440100,
    "深圳": 440300, "深圳市": 440300,
    "杭州": 330100, "杭州市": 330100,
    "成都": 510100, "成都市": 510100,
    "南京": 320100, "南京市": 320100,
    "苏州": 320500, "苏州市": 320500,
    "重庆": 500100, "重庆市": 500100,
    "长沙": 430100, "长沙市": 430100,
    "西安": 610100, "西安市": 610100,
    "郑州": 410100, "郑州市": 410100,
    "合肥": 340100, "合肥市": 340100,
    "天津": 120100, "天津市": 120100,
    "福州": 350100, "福州市": 350100,
    "厦门": 350200, "厦门市": 350200,
    "青岛": 370200, "青岛市": 370200,
    "济南": 370100, "济南市": 370100,
    "东莞": 441900, "东莞市": 441900,
    "佛山": 440600, "佛山市": 440600,
    "无锡": 320200, "无锡市": 320200,
    "宁波": 330200, "宁波市": 330200,
}

CITY_COORDS = {
    420100: ("武汉", "114.305393", "30.593099"),
    420900: ("孝感", "113.926655", "30.926423"),
    420981: ("应城", "113.572458", "30.928236"),
    440300: ("深圳", "114.057868", "22.543099"),
    440100: ("广州", "113.264385", "23.129112"),
    110100: ("北京", "116.407395", "39.904211"),
    310100: ("上海", "121.473701", "31.230416"),
    330100: ("杭州", "120.155070", "30.274085"),
    510100: ("成都", "104.066541", "30.572269"),
    320100: ("南京", "118.796877", "32.060255"),
    430100: ("长沙", "112.938814", "28.228209"),
    500100: ("重庆", "106.551556", "29.563009"),
    610100: ("西安", "108.939770", "34.341574"),
    320500: ("苏州", "120.585315", "31.298886"),
}


@router.get("/location/search")
async def search_location(keyword: str = Query("", description="搜索地址或地标关键词")):
    """根据输入的地址或商圈关键词，通过天地图 Web API 检索详细地址与经纬度坐标候选列表"""
    clean_kw = (keyword or "").strip()
    if not clean_kw:
        return {
            "ok": True,
            "keyword": "",
            "candidates": []
        }

    try:
        candidates = await tianditu_client.search_poi(clean_kw)
    except Exception as e:
        logger.warning(f"天地图地址多候选检索异常: {e}")
        candidates = []

    if candidates:
        for c in candidates:
            if not c.get("city_code"):
                c_name = c.get("city_name", "")
                c["city_code"] = CITY_CODE_MAP.get(c_name) or CITY_CODE_MAP.get(f"{c_name}市") or 420100

        top = candidates[0]
        return {
            "ok": True,
            "keyword": clean_kw,
            "candidates": candidates,
            "city_code": top.get("city_code", 420100),
            "city_name": top.get("city_name", ""),
            "district_name": top.get("district_name", ""),
            "town_name": top.get("town_name", ""),
            "short_name": top.get("short_name", clean_kw),
            "full_address": top.get("full_address", ""),
            "latitude": top.get("latitude", ""),
            "longitude": top.get("longitude", ""),
            "source": top.get("source", "tianditu")
        }

    # 兜底：尝试从已有预设城市快速匹配
    for code, (cname, clng, clat) in CITY_COORDS.items():
        if cname in clean_kw or clean_kw in cname:
            c_item = {
                "city_code": code,
                "city_name": cname,
                "district_name": "",
                "town_name": "",
                "poi": clean_kw,
                "short_name": f"{cname} · {clean_kw}",
                "full_address": f"{cname}市 {clean_kw}",
                "latitude": clat,
                "longitude": clng,
                "source": "preset"
            }
            return {
                "ok": True,
                "keyword": clean_kw,
                "candidates": [c_item],
                "city_code": code,
                "city_name": cname,
                "district_name": "",
                "short_name": f"{cname} · {clean_kw}",
                "full_address": f"{cname}市 {clean_kw}",
                "latitude": clat,
                "longitude": clng,
                "source": "preset"
            }

    # 未检索到结果时，正常返回空候选列表，绝不抛出 400 异常
    return {
        "ok": True,
        "keyword": clean_kw,
        "candidates": []
    }


@router.get("/location/resolve")
async def resolve_location(latitude: float = Query(...), longitude: float = Query(...)):
    """逆地理编码：优先使用天地图 Web API 查指定经纬度地址，未配置或异常时平滑降级"""
    # 1. 优先调用天地图
    try:
        tianditu_res = await tianditu_client.resolve_location(latitude, longitude)
        if tianditu_res and tianditu_res.get("city_name"):
            c_name = tianditu_res["city_name"]
            loc = tianditu_res.get("district", "")
            prov = tianditu_res.get("province", "")
            addr_str = tianditu_res.get("address_name", f"{c_name} · {loc}")
            c_code = tianditu_res.get("city_code") or CITY_CODE_MAP.get(c_name) or CITY_CODE_MAP.get(f"{c_name}市") or 420100
            return {
                "ok": True,
                "city_code": c_code,
                "city_name": c_name,
                "locality": loc,
                "province": prov,
                "address_name": addr_str,
                "latitude": f"{latitude:.6f}",
                "longitude": f"{longitude:.6f}",
                "source": "tianditu",
                "accuracy": "high"
            }
    except Exception as e:
        logger.warning(f"天地图逆地理编码异常，切换降级通道: {e}")

    # 2. 降级备用通道 (BigDataCloud)
    import urllib.request
    import json

    city_name = "武汉"
    locality = ""
    province = ""
    addr = "电脑本机高精定位点"

    try:
        url = f"https://api.bigdatacloud.net/data/reverse-geocode-client?latitude={latitude}&longitude={longitude}&localityLanguage=zh"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        res = urllib.request.urlopen(req, timeout=3.5)
        data = json.loads(res.read().decode("utf-8"))
        raw_city = data.get("city") or data.get("locality") or ""
        locality = data.get("locality") or ""
        province = data.get("principalSubdivision") or ""
        if raw_city:
            city_name = raw_city.replace("市", "")
        if locality and city_name:
            addr = f"{city_name} · {locality}"
        elif city_name:
            addr = f"{city_name} (本机定位)"
    except Exception as e:
        logger.warning(f"备用逆地理编码解析异常: {e}")

    # 匹配 city_code
    city_code = CITY_CODE_MAP.get(city_name) or CITY_CODE_MAP.get(f"{city_name}市") or 420100

    return {
        "ok": True,
        "city_code": city_code,
        "city_name": city_name,
        "locality": locality,
        "province": province,
        "address_name": addr,
        "latitude": f"{latitude:.6f}",
        "longitude": f"{longitude:.6f}",
        "source": "fallback",
        "accuracy": "high"
    }


@router.get("/location/ip")
async def get_ip_location():
    """网络 IP 粗略定位兜底：通过 IP 归属地获取城市并结合天地图逆地理补全坐标"""
    import urllib.request
    import json

    try:
        req = urllib.request.Request("https://whois.pconline.com.cn/ipJson.jsp?json=true", headers={"User-Agent": "Mozilla/5.0"})
        res = urllib.request.urlopen(req, timeout=3.0)
        raw = res.read().decode("gbk", errors="ignore")
        data = json.loads(raw)
        c_code_str = data.get("cityCode", "")
        raw_city = data.get("city", "").replace("市", "")
        addr_text = data.get("addr", "") or f"{raw_city} (网络IP定位)"

        code_int = int(c_code_str) if c_code_str and c_code_str.isdigit() else (CITY_CODE_MAP.get(raw_city, 420100))

        # 查找对应城市的预设经纬度
        c_tuple = CITY_COORDS.get(code_int)
        if not c_tuple:
            for code, (cname, clng, clat) in CITY_COORDS.items():
                if cname in raw_city or raw_city in cname:
                    code_int = code
                    c_tuple = (cname, clng, clat)
                    break

        if not c_tuple:
            c_tuple = ("武汉", "114.305393", "30.593099")

        return {
            "ok": True,
            "city_code": code_int,
            "city_name": c_tuple[0],
            "address_name": addr_text,
            "latitude": c_tuple[2],
            "longitude": c_tuple[1],
            "source": "ip",
            "is_ip": True
        }
    except Exception as e:
        logger.warning(f"备用 IP 定位异常: {e}")

    # 2. 降级备用通道 (PConline)
    import urllib.request
    import json

    try:
        req = urllib.request.Request("https://whois.pconline.com.cn/ipJson.jsp?json=true", headers={"User-Agent": "Mozilla/5.0"})
        res = urllib.request.urlopen(req, timeout=3.0)
        raw = res.read().decode("gbk", errors="ignore")
        data = json.loads(raw)
        c_code_str = data.get("cityCode", "")
        raw_city = data.get("city", "").replace("市", "")
        addr_text = data.get("addr", "") or f"{raw_city} (网络IP定位)"

        code_int = int(c_code_str) if c_code_str and c_code_str.isdigit() else (CITY_CODE_MAP.get(raw_city, 420100))

        # 查找对应城市的预设经纬度
        c_tuple = CITY_COORDS.get(code_int)
        if not c_tuple:
            # 模糊匹配
            for code, (cname, clng, clat) in CITY_COORDS.items():
                if cname in raw_city or raw_city in cname:
                    code_int = code
                    c_tuple = (cname, clng, clat)
                    break

        if not c_tuple:
            c_tuple = ("武汉", "114.305393", "30.593099")

        return {
            "ok": True,
            "city_code": code_int,
            "city_name": c_tuple[0],
            "address_name": addr_text,
            "latitude": c_tuple[2],
            "longitude": c_tuple[1],
            "source": "fallback",
            "is_ip": True
        }
    except Exception as e:
        logger.warning(f"备用 IP 定位异常: {e}")
        return {
            "ok": True,
            "city_code": 420100,
            "city_name": "武汉",
            "address_name": "武汉市 (默认)",
            "latitude": "30.593099",
            "longitude": "114.305393",
            "source": "default",
            "is_ip": True
        }


# ---------------- 消息中心 API ---------------- #

@router.get("/messages")
async def get_messages(
    account_key: Optional[str] = None,
    channel_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 20
):
    """获取小蚕官方消息中心频道与通知列表"""
    acc = None
    if account_key:
        acc = db.get_account_by_key(account_key)
    if not acc:
        accounts = db.get_all_accounts()
        if accounts:
            acc = accounts[0]
            
    if not acc:
        return {
            "ok": True,
            "channels": [],
            "messages": [],
            "unread_total": 0,
            "message": "未接入小蚕账号"
        }

    token = acc.get("token")
    silk_id = str(acc.get("silk_id") or "")
    user_id = str(acc.get("user_id") or "")
    city_code = acc.get("city_code") or 440303

    try:
        channels_res, msgs_res = await asyncio.gather(
            client.get_message_channels(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code),
            client.get_messages(token=token, silk_id=silk_id, user_id=user_id, channel_id=channel_id, page=page, page_size=page_size, city_code=city_code),
            return_exceptions=True
        )

        channels = []
        if isinstance(channels_res, dict) and channels_res.get("list_channel"):
            channels = channels_res["list_channel"]

        messages = []
        if isinstance(msgs_res, dict) and msgs_res.get("list"):
            messages = msgs_res["list"]

        unread_total = sum(c.get("unread", 0) for c in channels)

        return {
            "ok": True,
            "channels": channels,
            "messages": messages,
            "unread_total": unread_total
        }
    except Exception as e:
        logger.error(f"获取小蚕消息中心失败: {e}")
        return {
            "ok": False,
            "channels": [],
            "messages": [],
            "unread_total": 0,
            "error": str(e)
        }


class ReadAllMessagesRequest(BaseModel):
    account_key: str


@router.post("/messages/read-all")
async def read_all_messages(req: ReadAllMessagesRequest):
    """一键将当前账号所有消息标记为已读"""
    acc = db.get_account_by_key(req.account_key)
    if not acc:
        raise HTTPException(status_code=404, detail="账号不存在")

    token = acc.get("token")
    silk_id = str(acc.get("silk_id") or "")
    user_id = str(acc.get("user_id") or "")
    city_code = acc.get("city_code") or 440303

    try:
        await client.set_all_messages_read(
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            city_code=city_code
        )
        return {"ok": True, "message": "已全部标记为已读"}
    except Exception as e:
        logger.error(f"全部标记已读失败: {e}")
        return {"ok": False, "error": str(e)}


