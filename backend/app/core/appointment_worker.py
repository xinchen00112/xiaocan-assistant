"""
霸王餐高精度预约与名额监听引擎 (Appointment & Restock Worker)
功能包含：
1. 未到时间点：倒计时 31 分钟发送预热提醒，倒计时 1 分钟启动高频就绪监听，到点准时/提前量毫秒级抢单
2. 已到时间点且有名额：直接即时抢单
3. 已到时间点但无名额：启动名额监听任务，定期轮询库存，检测到补仓/放单毫秒级秒抢，超时自动停，支持随时手动停止
"""
import asyncio
import logging
import random
import time
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

from ..models import database as db
from ..protocol.client import XiaoCanClient, XiaoCanRPCError, is_waf_blocked, get_waf_cooldown_seconds
from .notifier import send_system_notification

logger = logging.getLogger("xiaocan.appointment_worker")

client = XiaoCanClient()
_worker_task: Optional[asyncio.Task] = None
_is_running = False

# 内存热缓存 (Hot Cache)：将 50 次/秒高频自旋的磁盘 I/O 降低至每秒最多 1 次
_cached_active_appointments: list = []
_last_fetch_active_ts: float = 0.0
_cached_accounts: Dict[str, Any] = {}

# 任务运行时内存日志追踪 (避免高频写库，同时保留完整执行流水)
_apt_poll_counters: Dict[int, int] = {}
_apt_runtime_logs: Dict[int, list] = {}

def _append_apt_log(aid: int, log_id: int, text: str, sync_db: bool = False, status: Optional[str] = None):
    """追加任务步骤日志并有策略地同步回 SQLite 数据库"""
    if aid not in _apt_runtime_logs:
        _apt_runtime_logs[aid] = []
    _apt_runtime_logs[aid].append(text)
    
    # 限制单任务内存保留最多 60 条关键日志，防止无限增长
    if len(_apt_runtime_logs[aid]) > 60:
        _apt_runtime_logs[aid] = _apt_runtime_logs[aid][-50:]

    if log_id > 0 and (sync_db or status is not None):
        try:
            with db.get_conn() as conn:
                row = conn.execute("SELECT output FROM job_logs WHERE id = ?", (log_id,)).fetchone()
                prev_text = row["output"] if row else ""
            
            # 合并已有历史与新日志 (若已有历史，追加增量部分)
            full_output = (prev_text + "\n" + "\n".join(_apt_runtime_logs[aid])).strip()
            # 过滤相邻完全重复的多余行
            cleaned_lines = []
            for line in full_output.split("\n"):
                if not cleaned_lines or cleaned_lines[-1] != line:
                    cleaned_lines.append(line)
            # 保留最近 100 行
            final_text = "\n".join(cleaned_lines[-100:])
            db.update_job_log(log_id, status=status, output=final_text)
            # 同步完成后清空增量缓冲区
            _apt_runtime_logs[aid].clear()
        except Exception as e:
            logger.debug(f"同步任务 #{aid} 步骤日志到数据库异常: {e}")

def invalidate_worker_cache():
    global _last_fetch_active_ts
    _last_fetch_active_ts = 0.0


def _parse_time_today(time_str: str) -> Optional[float]:
    """解析 HH:MM 为今日的时间戳，若格式为 YYYY-MM-DD HH:MM:SS 则直接解析"""
    if not time_str or not str(time_str).strip():
        return None
    time_str = str(time_str).strip()
    now = datetime.now()
    try:
        if "-" in time_str:
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            return dt.timestamp()
        if ":" in time_str:
            parts = time_str.split(":")
            h, m = int(parts[0]), int(parts[1])
            s = int(parts[2]) if len(parts) > 2 else 0
            dt = datetime(now.year, now.month, now.day, h, m, s)
            return dt.timestamp()
    except Exception as e:
        logger.warning(f"解析时间戳失败 {time_str}: {e}")
    return None


async def execute_grab_for_appointment(apt: Dict[str, Any], account: Dict[str, Any], advance: bool = False, action_source: str = "store_grab") -> Dict[str, Any]:
    """执行小蚕官方抢单接口，并全程记录高精步骤日志入系统运行流水"""
    aid = apt.get("id") or 0
    token = account.get("token")
    silk_id = account.get("silk_id")
    user_id = account.get("user_id")
    city_code = account.get("city_code") or 440303
    account_key = apt.get("account_key") or account.get("key", "")
    nickname = account.get("nickname") or account_key
    store_name = apt.get("store_name", "店铺活动")
    promotion_id = apt.get("promotion_id", "")
    platform_name = "美团外卖" if apt.get("platform") == "meituan" else ("饿了么" if apt.get("platform") == "eleme" else "京东外卖")
    platform_code = int(apt.get("store_platform") or (3 if apt.get("platform") in ("jingdong", "jd") else (1 if apt.get("platform") == "meituan" else 2)))

    task_id = action_source if action_source in ("store_grab", "store_appoint", "store_monitor", "store_search", "store_cancel") else "store_grab"
    job_id = f"job_store_{int(time.time())}_{uuid.uuid4().hex[:6]}"

    source_labels = {
        "store_grab": "即时抢单",
        "store_appoint": "定时预约",
        "store_monitor": "名额监听",
        "store_keyword": "店名嗅探",
        "store_search": "定向搜索",
        "store_cancel": "取消名额",
    }
    source_label = source_labels.get(action_source, "自动抢单")

    log_steps = []
    t_start = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    log_steps.append(f"[{t_start}] 启动抢单流水: 店铺【{store_name}】(活动ID: {promotion_id}, 平台: {platform_name}, 触发方式: {source_label})")

    try:
        lat = float(account.get("latitude") or 0.0)
    except Exception:
        lat = 0.0
    try:
        lon = float(account.get("longitude") or 0.0)
    except Exception:
        lon = 0.0

    t_step1 = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    log_steps.append(f"[{t_step1}] 步骤 1/4: 账号鉴权及定位核验 - 账号【{nickname}】，定位经纬度 ({lat:.5f}, {lon:.5f})，城市代码 {city_code}")
    # 红包使用策略解析与防失效自愈降级 (优先使用提前 25 秒预加载并锁定的红包，消除开抢时的网络延时)
    redpack_id = apt.get("cached_redpack_id")
    redpack_mode = apt.get("redpack_mode", 0)
    rp_desc = "未使用"
    if redpack_id is not None:
        rp_desc = f"预锁定红包 #{redpack_id}"
    elif redpack_mode == 0:
        # 自动使用最高额度红包 (对齐微信小程序/省心版)
        try:
            max_rp = await client.get_user_max_redpack(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code)
            rep_pack = max_rp.get("rep_pack") or {}
            if not rep_pack and max_rp.get("platform_red_packs"):
                rep_pack = max_rp["platform_red_packs"][0]
            if rep_pack and rep_pack.get("user_red_pack_id"):
                redpack_id = int(rep_pack["user_red_pack_id"])
                rp_val = float(rep_pack.get("reward_num", 0) or 0) / 100
                rp_desc = f"智能匹配最优红包 #{redpack_id} (金额: ¥{rp_val:.2f})"
            else:
                rp_desc = "未找到可用平台红包，以无红包模式发起"
        except Exception as rpe:
            rp_desc = f"自动查询红包跳过: {rpe}"
    elif redpack_mode == 1:
        specified_rp_id = None
        if apt.get("redpack_id"):
            try:
                specified_rp_id = int(apt["redpack_id"])
            except Exception:
                pass

        if specified_rp_id:
            # 核验指定红包是否仍有效且未在移动端核销
            is_valid_specified = False
            unused_list = []
            try:
                rp_res = await client.get_app_redpack_list(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code, page=1, page_size=50)
                unused_list = rp_res.get("unused_items") or []
                for item in unused_list:
                    if int(item.get("user_red_pack_id") or 0) == specified_rp_id:
                        is_valid_specified = True
                        redpack_id = specified_rp_id
                        rp_name = item.get("name") or apt.get("redpack_name") or ""
                        item_val = float(item.get("reward_num", 0) or 0) / 100
                        rp_desc = f"指定红包 #{redpack_id} ({rp_name}, 金额: ¥{item_val:.2f})"
                        break
            except Exception as e:
                logger.warning(f"核验指定红包有效性异常: {e}")
                redpack_id = specified_rp_id
                rp_desc = f"指定红包 #{redpack_id}"
                is_valid_specified = True

            if not is_valid_specified:
                # 触发防核销自愈降级替换：预选红包已在移动端被使用或过期
                logger.warning(f"预约任务 #{aid} 指定红包 #{specified_rp_id} 已在端外使用或失效，启动平滑自愈替换...")
                best_replacement = None
                if unused_list:
                    sorted_unused = sorted(unused_list, key=lambda x: float(x.get("reward_num") or 0), reverse=True)
                    best_replacement = sorted_unused[0]

                if best_replacement and best_replacement.get("user_red_pack_id"):
                    redpack_id = int(best_replacement["user_red_pack_id"])
                    best_val = float(best_replacement.get("reward_num", 0) or 0) / 100
                    rp_desc = f"预设红包 #{specified_rp_id} 已在移动端使用，已自愈平滑替换为当前最优红包 #{redpack_id} (金额: ¥{best_val:.2f})"
                else:
                    redpack_id = None
                    rp_desc = f"预设红包 #{specified_rp_id} 已在移动端使用，当前无其它可用红包，自愈降级为无红包模式"
        else:
            rp_desc = "未指定有效红包ID，以无红包模式发起"
    else:
        rp_desc = "用户设置不使用红包"

    t_step2 = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    log_steps.append(f"[{t_step2}] 步骤 2/4: 红包策略决策 - {rp_desc}")

    try:
        t_step3 = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_steps.append(f"[{t_step3}] 步骤 3/4: 发起官方抢单 RPC 握手 - promotion_id={promotion_id}, store_platform={platform_code}, advance={advance}")

        res = await client.grab_promotion_quota(
            promotion_id=apt["promotion_id"],
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            city_code=city_code,
            store_platform=platform_code,
            if_advance_order=advance,
            latitude=lat,
            longitude=lon,
            redpack_id=redpack_id
        )
        
        # 严格校验小蚕官方订单编号 (成功时必须返回 promotion_order_id > 0)
        order_id = res.get("promotion_order_id") or (res.get("order") or {}).get("promotion_order_id") or 0
        try:
            order_id = int(order_id)
        except Exception:
            order_id = 0

        linked_log_id = int(apt.get("log_id") or 0)

        if order_id <= 0:
            err_msg = (res.get("status") or {}).get("msg") or res.get("msg") or "未返回有效订单号，官方抢单未生效"
            t_step4 = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            log_steps.append(f"[{t_step4}] 步骤 4/4: 官方接口返回未成功 - 提示: {err_msg}")
            log_steps.append(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 流程结束 - 抢单未锁定名额")
            logger.warning(f"预约/监听任务 #{aid} 抢单未成功: {err_msg}")
            grab_text = "\n".join(log_steps)
            if linked_log_id > 0:
                _append_apt_log(aid, linked_log_id, grab_text, sync_db=True, status="error")
            else:
                try:
                    db.add_job_log(job_id, account_key, task_id, "error", grab_text)
                except Exception:
                    pass
            return {"ok": False, "code": -1, "message": err_msg}

        t_step4 = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_steps.append(f"[{t_step4}] 步骤 4/4: 抢单成功锁定 - 官方系统订单 #{order_id} 生成完毕！")
        success_msg = f"抢单成功锁定！官方系统订单 #{order_id} 生成完毕，请前往外卖平台下单"

        # 自动触发 App 原生「领红包延时 2 小时」任务特权 (延长活动订单凭证上传有效期 2 小时)
        try:
            delay_task_type = 4 if int(platform_code) == 1 else 3
            delay_res = await client.complete_task_event(
                token=token,
                silk_id=silk_id,
                user_id=user_id,
                city_code=city_code,
                promotion_order_id=order_id,
                task_type=delay_task_type
            )
            is_delayed = bool(delay_res.get("if_delay") or delay_res.get("if_delay_success") or delay_res.get("ifDelay") or delay_res.get("ifDelaySuccess"))
            d_time = delay_res.get("delay_time") or delay_res.get("delayTime") or 7200
            if is_delayed or d_time > 0:
                log_steps.append(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 附加权益: 成功激活官方【领红包延时2小时】特权，凭据上传有效期已自动延长 2 小时 ({d_time}秒)")
            else:
                log_steps.append(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 附加权益: 已提交领红包延时申请")
        except Exception as de:
            logger.info(f"领红包延时触发提示: {de}")
            log_steps.append(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 附加权益: 领红包延时提示: {de}")

        log_steps.append(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 流程完成 - 订单状态已置为待下单，已投递抢单成功系统通知")
        logger.info(f"预约/监听任务 #{aid} 抢单成功: {success_msg}")

        # 写入订单库
        try:
            db.create_order({
                "account_key": apt["account_key"],
                "store_id": apt["store_id"],
                "store_name": apt["store_name"],
                "store_icon": apt.get("store_icon") or "",
                "platform": apt.get("platform", "meituan"),
                "order_money": apt.get("order_money", 0.0),
                "rebate_money": apt.get("rebate_price", 0.0),
                "status": "pending",
                "condition": "无需评价",
                "promotion_order_id": order_id
            })
        except Exception as oe:
            logger.warning(f"写入订单记录失败: {oe}")

        # 发送系统通知
        await send_system_notification(
            title="小蚕霸王餐抢单成功！",
            content=f"账号【{account.get('nickname')}】已成功抢到【{apt['store_name']}】({apt.get('rebate_desc', '')})，订单号 #{order_id}，请及时在平台下单！",
            account_key=apt.get("account_key")
        )

        if aid > 0:
            db.update_appointment(aid, {
                "status": "success",
                "outcome": success_msg
            })
            invalidate_worker_cache()

        grab_text = "\n".join(log_steps)
        if linked_log_id > 0:
            _append_apt_log(aid, linked_log_id, grab_text, sync_db=True, status="success")
        else:
            try:
                db.add_job_log(job_id, account_key, task_id, "success", grab_text)
            except Exception:
                pass

        return {"ok": True, "order_id": order_id, "message": success_msg}
    except XiaoCanRPCError as e:
        t_err = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_steps.append(f"[{t_err}] 官方接口响应异常: {e.msg} (错误码: {e.code})")
        logger.warning(f"抢单接口响应: code={e.code}, msg={e.msg}")
        grab_text = "\n".join(log_steps)
        linked_log_id = int(apt.get("log_id") or 0)
        if linked_log_id > 0:
            _append_apt_log(aid, linked_log_id, grab_text, sync_db=True, status="error")
        else:
            try:
                db.add_job_log(job_id, account_key, task_id, "error", grab_text)
            except Exception:
                pass
        return {"ok": False, "code": e.code, "message": e.msg}
    except Exception as e:
        t_err = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_steps.append(f"[{t_err}] 抢单接口网络异常: {str(e)}")
        logger.error(f"抢单接口网络异常: {e}")
        grab_text = "\n".join(log_steps)
        linked_log_id = int(apt.get("log_id") or 0)
        if linked_log_id > 0:
            _append_apt_log(aid, linked_log_id, grab_text, sync_db=True, status="error")
        else:
            try:
                db.add_job_log(job_id, account_key, task_id, "error", grab_text)
            except Exception:
                pass
        return {"ok": False, "code": -1, "message": str(e)}


async def process_active_appointments() -> bool:
    """单轮调度处理所有活跃的预约和监听任务，返回是否有临近 35 秒内的秒杀任务"""
    global _cached_active_appointments, _last_fetch_active_ts, _cached_accounts
    from . import time_service
    now_ts = time_service.now_ts()

    # 高频自旋保护：每秒最多向磁盘发起 1 次活跃任务检索
    if now_ts - _last_fetch_active_ts >= 1.0 or not _cached_active_appointments:
        _cached_active_appointments = db.get_active_appointments()
        _last_fetch_active_ts = now_ts
        _cached_accounts.clear()

    active_list = _cached_active_appointments
    if not active_list:
        return False

    now_str = datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
    has_primed = False

    for apt in active_list:
        aid = apt["id"]
        status = apt.get("status")
        task_type = apt.get("task_type") or "countdown"
        acc_key = apt["account_key"]

        if acc_key not in _cached_accounts:
            _cached_accounts[acc_key] = db.get_account_by_key(acc_key)
        account = _cached_accounts[acc_key]

        if not account or not account.get("token"):
            db.update_appointment(aid, {
                "status": "failed",
                "outcome": "对应账号凭据失效或已被删除"
            })
            invalidate_worker_cache()
            continue

        # ---------------- 分支一：店名抢单监听与智能嗅探 (name_monitor / keyword) ---------------- #
        if task_type in ("name_monitor", "keyword", "keyword_monitor"):
            linked_log_id = int(apt.get("log_id") or 0)
            # 1. 检查是否达到开始时间
            start_ts = _parse_time_today(apt.get("start_time"))
            if start_ts and now_ts < start_ts:
                continue

            # 2. 检查是否超过截止时间
            until_ts = _parse_time_today(apt.get("until_time"))
            if until_ts and now_ts >= until_ts:
                outcome_msg = f"已达到设定的店名监听截止时间 {apt.get('until_time')}，自动结束"
                db.update_appointment(aid, {
                    "status": "expired",
                    "outcome": outcome_msg
                })
                invalidate_worker_cache()
                if linked_log_id > 0:
                    t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                    _append_apt_log(aid, linked_log_id, f"[{t_now}] 店名监听截止时间到达 ({apt.get('until_time')})，监听任务自动结束", sync_db=True, status="error")
                continue

            # 3. 检查频率 (默认 30 秒一次，最低 10 秒 — 避免触发小蚕 WAF 频控)
            interval = max(10, int(apt.get("check_interval") or 30))
            last_checked = _parse_time_today(apt.get("last_checked_at"))
            if last_checked and (now_ts - last_checked) < interval:
                continue

            # 如果当前网络处于 WAF 熔断冷却期，主动跳过嗅探，避免延长封锁
            if is_waf_blocked():
                remain = get_waf_cooldown_seconds()
                if (int(now_ts) % 30) < 2 and linked_log_id > 0:
                    t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                    _append_apt_log(aid, linked_log_id, f"[{t_now}] [风控熔断守护] 当前网络IP处于官方腾讯云WAF临时拦截冷却中 (剩余约 {remain} 秒)，已自动暂停嗅探以避免延长封锁。切换手机热点或重启光猫更换IP可立即恢复。", sync_db=True, status="running")
                continue

            apt["last_checked_at"] = now_str
            db.update_appointment(aid, {"last_checked_at": now_str})

            # 4. 执行双源嗅探与多档位优选
            _apt_poll_counters[aid] = _apt_poll_counters.get(aid, 0) + 1
            poll_cnt = _apt_poll_counters[aid]

            # 防风控随机微抖动
            await asyncio.sleep(random.uniform(0.1, 0.4))

            kw = (apt.get("keyword") or apt.get("store_name") or "").strip()
            match_mode = apt.get("match_mode") or "contains"
            target_plat = apt.get("platform") or "all"
            rebate_mode_filter = apt.get("rebate_mode_filter") or "all"
            min_rebate_price = float(apt.get("min_rebate_price") or 0.0)
            min_rebate_rate = float(apt.get("min_rebate_rate") or 0.0)
            max_order_money = float(apt.get("max_order_money") or 0.0)
            auto_stop = int(apt.get("auto_stop_on_success") if apt.get("auto_stop_on_success") is not None else 1)

            token = account.get("token")
            silk_id = account.get("silk_id")
            user_id = account.get("user_id")
            city_code = int(account.get("city_code") or 440300)
            lon = str(account.get("longitude") or "114.13166")
            lat = str(account.get("latitude") or "22.548361")

            try:
                from ..api.endpoints import _extract_all_promos_from_raw, _normalize_shangjin_item

                search_tasks = [
                    client.search_stores(
                        keyword=kw,
                        city_code=city_code,
                        longitude=lon,
                        latitude=lat,
                        offset=0,
                        limit=20,
                        token=token,
                        silk_id=silk_id,
                        user_id=user_id
                    )
                ]
                has_sj = False
                if target_plat in ("all", "meituan"):
                    has_sj = True
                    search_tasks.append(
                        client.search_shangjin_stores(
                            keyword=kw,
                            latitude=float(lat),
                            longitude=float(lon),
                            token=token,
                            silk_id=silk_id,
                            user_id=user_id,
                            city_code=city_code,
                            sort_type=3
                        )
                    )

                results = await asyncio.gather(*search_tasks, return_exceptions=True)
                search_res = results[0] if len(results) > 0 and not isinstance(results[0], Exception) else {}
                sj_res = results[1] if has_sj and len(results) > 1 and not isinstance(results[1], Exception) else {}

                candidates = []
                # 解析常规活动列表
                raw_proms = search_res.get("promotions") or search_res.get("promotion_list") or search_res.get("feed_items") or []
                for p in raw_proms:
                    extracted = _extract_all_promos_from_raw(p, is_search=True, user_lat=float(lat), user_lon=float(lon))
                    candidates.extend(extracted)

                # 解析美团赏金按比例活动
                if isinstance(sj_res, dict):
                    pois = sj_res.get("poi_list") or []
                    for poi in pois:
                        sj_items = _normalize_shangjin_item(poi, user_lat=float(lat), user_lon=float(lon))
                        candidates.extend(sj_items)

                # 规则过滤与匹配
                matched_candidates = []
                total_found_stores = set()
                kw_lower = kw.lower()

                for c in candidates:
                    s_name = (c.get("name") or "").strip()
                    if not s_name:
                        continue
                    total_found_stores.add(s_name)

                    # 1. 店名匹配规则
                    if match_mode == "exact":
                        if s_name.lower() != kw_lower:
                            continue
                    else:
                        if kw_lower not in s_name.lower():
                            continue

                    # 2. 平台过滤
                    c_plat = c.get("platform")
                    if target_plat != "all" and c_plat != target_plat:
                        continue

                    # 3. 返利模式过滤 (all / fixed / percent)
                    c_rtype = c.get("rebate_type")
                    if rebate_mode_filter != "all" and c_rtype != rebate_mode_filter:
                        continue

                    # 4. 最低返利金额
                    c_rebate_price = float(c.get("rebate_price") or 0.0)
                    if min_rebate_price > 0 and c_rebate_price < min_rebate_price:
                        continue

                    # 5. 最低返利比例
                    c_rebate_rate = float(c.get("rebate_rate") or 0.0)
                    if min_rebate_rate > 0 and c_rebate_rate < min_rebate_rate:
                        continue

                    # 6. 最高起送/门槛金额
                    c_order_money = float(c.get("order_money") or 0.0)
                    if max_order_money > 0 and c_order_money > max_order_money:
                        continue

                    # 7. 库存名额
                    c_left = int(c.get("left_number") or 0)
                    if c_left <= 0:
                        continue

                    matched_candidates.append(c)

                # 按最优返利比选 (优先返利金额最高，次选返利比例最高)
                matched_candidates.sort(
                    key=lambda x: (float(x.get("rebate_price") or 0.0), float(x.get("rebate_rate") or 0.0)),
                    reverse=True
                )

                t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                best_item = matched_candidates[0] if matched_candidates else None

                if best_item:
                    store_label = best_item.get("name", kw)
                    rebate_label = best_item.get("rebate_desc", "")
                    step_str = f"[{t_now}] 第 {poll_cnt} 次嗅探成功: 检索到「{store_label}」有可抢名额 ({rebate_label}, 剩余库存 {best_item.get('left_number')} 份)，立即触发闪电抢单！"
                    logger.info(f"店名监听 #{aid} 命中可用方案【{store_label}】({rebate_label})，立即触发抢单！")
                    if linked_log_id > 0:
                        _append_apt_log(aid, linked_log_id, step_str, sync_db=True, status="running")

                    grab_apt = dict(apt)
                    grab_apt["promotion_id"] = str(best_item.get("promotion_id", ""))
                    grab_apt["store_id"] = str(best_item.get("store_id", "0"))
                    grab_apt["store_name"] = store_label
                    grab_apt["store_icon"] = best_item.get("store_icon", "")
                    grab_apt["platform"] = best_item.get("platform", "meituan")
                    grab_apt["store_platform"] = int(best_item.get("store_platform") or (1 if best_item.get("platform") == "meituan" else 2))
                    grab_apt["order_money"] = float(best_item.get("order_money") or 0.0)
                    grab_apt["rebate_price"] = float(best_item.get("rebate_price") or 0.0)
                    grab_apt["rebate_desc"] = rebate_label
                    grab_apt["rebate_type"] = best_item.get("rebate_type", "fixed")

                    is_adv = False
                    if apt.get("use_advance_card"):
                        s_time = best_item.get("start_time") or apt.get("start_time")
                        s_ts = _parse_time_today(s_time)
                        if s_ts and now_ts < s_ts:
                            is_adv = True

                    res = await execute_grab_for_appointment(grab_apt, account, advance=is_adv, action_source="store_monitor")
                    t_grab = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                    if res.get("ok"):
                        succ_msg = f"抢单成功: 【{store_label}】{rebate_label}"
                        logger.info(f"店名监听 #{aid} {succ_msg}")
                        if auto_stop:
                            db.update_appointment(aid, {
                                "status": "completed",
                                "outcome": succ_msg,
                                "store_name": store_label,
                                "promotion_id": str(best_item.get("promotion_id", ""))
                            })
                            invalidate_worker_cache()
                            if linked_log_id > 0:
                                _append_apt_log(aid, linked_log_id, f"[{t_grab}] {succ_msg}，任务已达成并自动停止", sync_db=True, status="success")
                        else:
                            if linked_log_id > 0:
                                _append_apt_log(aid, linked_log_id, f"[{t_grab}] {succ_msg}，保持持续监听模式", sync_db=True, status="running")
                    else:
                        fail_msg = res.get("message") or "名额被抢占或网络响应延迟"
                        logger.warning(f"店名监听 #{aid} 抢单尝试未成功: {fail_msg}")
                        if linked_log_id > 0:
                            _append_apt_log(aid, linked_log_id, f"[{t_grab}] 抢单尝试返回: {fail_msg}，继续保持高频蹲守", sync_db=True, status="running")
                else:
                    # 未找到符合条件的方案
                    store_count = len(total_found_stores)
                    step_str = f"[{t_now}] 第 {poll_cnt} 次嗅探: 关键词「{kw}」，发现 {store_count} 家商户 (当前暂无满足条件的剩余名额)，持续保持监听..."
                    if linked_log_id > 0:
                        _append_apt_log(aid, linked_log_id, step_str, sync_db=(poll_cnt == 1 or poll_cnt % 3 == 0), status="running")

            except Exception as se:
                logger.warning(f"店名监听 #{aid} 执行异常: {se}")
                is_waf = is_waf_blocked() or (getattr(se, "code", None) == 403) or ("WAF" in str(se))
                if is_waf:
                    cooldown = get_waf_cooldown_seconds() or 180
                    future_time = datetime.fromtimestamp(now_ts + cooldown).strftime("%Y-%m-%d %H:%M:%S")
                    db.update_appointment(aid, {"last_checked_at": future_time})
                    if linked_log_id > 0:
                        t_err = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                        _append_apt_log(aid, linked_log_id, f"[{t_err}] [风控熔断] 当前网络IP触发官方腾讯云WAF临时频次拦截，系统已自动休眠熔断保护 (剩余冷却约 {cooldown} 秒)。建议切换手机热点或重启光猫更换IP立即恢复。", sync_db=True, status="running")
                else:
                    if linked_log_id > 0:
                        t_err = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                        _append_apt_log(aid, linked_log_id, f"[{t_err}] 嗅探执行异常: {se}，将在下次周期重试", sync_db=False, status="running")

        # ---------------- 分支二：单店名额监听捡漏任务 (task_type == "monitor" or status == "monitoring") ---------------- #
        elif task_type == "monitor" or status == "monitoring":
            linked_log_id = int(apt.get("log_id") or 0)
            # 1. 检查是否达到截止时间
            until_ts = _parse_time_today(apt.get("until_time"))
            if until_ts and now_ts >= until_ts:
                outcome_msg = f"已达到设定的监听截止时间 {apt.get('until_time')}，自动结束监听"
                db.update_appointment(aid, {
                    "status": "expired",
                    "outcome": outcome_msg
                })
                logger.info(f"监听任务 #{aid} 已超时结束 (until_time={apt.get('until_time')})")
                if linked_log_id > 0:
                    exp_line = f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] 监听截止时间已到达 ({apt.get('until_time')})，名额监听自动停止"
                    _append_apt_log(aid, linked_log_id, exp_line, sync_db=True, status="error")
                continue

            # 2. 控制检查频率 (默认 30 秒一次，最低 10 秒 — 避免触发小蚕 WAF 频控)
            interval = max(10, int(apt.get("check_interval") or 30))
            last_checked = _parse_time_today(apt.get("last_checked_at"))
            if last_checked and (now_ts - last_checked) < interval:
                continue

            # 如果当前网络处于 WAF 熔断冷却期，主动跳过库存查询，避免延长封锁
            if is_waf_blocked():
                remain = get_waf_cooldown_seconds()
                if (int(now_ts) % 30) < 2 and linked_log_id > 0:
                    t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                    _append_apt_log(aid, linked_log_id, f"[{t_now}] [风控熔断守护] 当前网络IP处于官方腾讯云WAF临时拦截冷却中 (剩余约 {remain} 秒)，已自动暂停库存轮询以避免延长封锁。", sync_db=True, status="running")
                continue

            # 更新最后检查时间戳
            apt["last_checked_at"] = now_str
            db.update_appointment(aid, {"last_checked_at": now_str})

            # 3. 查验最新实时库存
            _apt_poll_counters[aid] = _apt_poll_counters.get(aid, 0) + 1
            poll_cnt = _apt_poll_counters[aid]
            try:
                detail = await client.get_store_promotion_detail(
                    promotion_id=apt["promotion_id"],
                    token=account["token"],
                    silk_id=account.get("silk_id"),
                    user_id=account.get("user_id"),
                    city_code=account.get("city_code", 440303)
                )
                pdetail = detail.get("promotion_detail") or {}
                mt_left = pdetail.get("meituan_left_number", 0)
                ele_left = pdetail.get("eleme_left_number", 0)
                plat = apt.get("platform", "meituan")
                target_left = mt_left if plat == "meituan" else ele_left
                if target_left is None or target_left == 0:
                    target_left = pdetail.get("left_number", 0)

                t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                step_str = f"[{t_now}] 第 {poll_cnt} 次名额检测: 美团余量 {mt_left or 0} 份 / 饿了么余量 {ele_left or 0} 份，持续监听中..."
                # 每轮巡检追加，且首轮或每 3 次或有名额变动时向数据库落盘一次
                sync_needed = (poll_cnt == 1) or (poll_cnt % 3 == 0) or (target_left and int(target_left) > 0)
                if linked_log_id > 0:
                    _append_apt_log(aid, linked_log_id, step_str, sync_db=sync_needed, status="running")

                if target_left and int(target_left) > 0:
                    # 关键修复：智能识别超前抢单窗口期
                    # 若用户开启了超前抢单券，且当前时间仍在正点开抢之前，必须携带 advance=True，否则官方返回活动未开始 (错误码 4)
                    is_adv = False
                    if apt.get("use_advance_card"):
                        start_ts = _parse_time_today(apt.get("start_time"))
                        if start_ts and now_ts < start_ts:
                            is_adv = True

                    adv_tag = "【超前券·提前抢单】" if is_adv else ""
                    fire_str = f"[{t_now}] 捕获到目标名额放单 (剩余: {target_left} 份)！{adv_tag}立即触发闪电抢单..."
                    logger.info(f"监听任务 #{aid} 捕获到名额放单 (剩余: {target_left})！{adv_tag}立即触发闪电抢单...")
                    if linked_log_id > 0:
                        _append_apt_log(aid, linked_log_id, fire_str, sync_db=True, status="running")
                    res = await execute_grab_for_appointment(apt, account, advance=is_adv, action_source="store_monitor")
                    if not res["ok"]:
                        logger.warning(f"监听捕获后抢单失败: {res['message']}，继续保持监听")
            except Exception as e:
                logger.debug(f"轮询活动实时库存异常 (非致命): {e}")
                is_waf = is_waf_blocked() or (getattr(e, "code", None) == 403) or ("WAF" in str(e))
                if is_waf:
                    cooldown = get_waf_cooldown_seconds() or 180
                    future_time = datetime.fromtimestamp(now_ts + cooldown).strftime("%Y-%m-%d %H:%M:%S")
                    db.update_appointment(aid, {"last_checked_at": future_time})
                    if linked_log_id > 0:
                        t_err = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                        _append_apt_log(aid, linked_log_id, f"[{t_err}] [风控熔断] 当前网络IP触发官方腾讯云WAF临时频次拦截，系统已自动休眠熔断保护 (剩余冷却约 {cooldown} 秒)", sync_db=True, status="running")

        # ---------------- 分支三：倒计时预约任务 (status in 'scheduled', 'primed', 'pending') ---------------- #
        elif status in ("scheduled", "primed", "pending"):
            linked_log_id = int(apt.get("log_id") or 0)
            start_ts = _parse_time_today(apt.get("start_time"))
            if not start_ts:
                continue

            use_advance = bool(apt.get("use_advance_card"))
            # 开启超前抢单券时，开抢时刻提前 30 分钟 (1800 秒)
            effective_start_ts = (start_ts - 30 * 60) if use_advance else start_ts
            diff = effective_start_ts - now_ts
            early_sec = max(0.0, float(apt.get("early_ms") or 500) / 1000.0)

            # 标记是否有临近开抢任务 (若在 35 秒内，开启 20ms 自适应超高精度监测)
            if 0 < diff <= 35:
                has_primed = True

            eff_time_str = datetime.fromtimestamp(effective_start_ts).strftime("%H:%M")
            adv_prefix = "【超前抢单·提前30分钟】" if use_advance else ""

            # 1. 倒计时 31 分钟预热通知提醒
            if diff <= (31 * 60) and not apt.get("notified_31m"):
                db.update_appointment(aid, {"notified_31m": 1})
                logger.info(f"任务 #{aid} 触发倒计时 31 分钟预热通知 (超前券: {use_advance})")
                t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                if linked_log_id > 0:
                    _append_apt_log(aid, linked_log_id, f"[{t_now}] 倒计时 31 分钟到达，已发送开火预热通知与通道就绪检查 {adv_prefix}(开火目标: {eff_time_str})", sync_db=True, status="running")
                try:
                    await send_system_notification(
                        title="霸王餐开抢预热提醒",
                        content=f"您预约的【{apt['store_name']}】({apt.get('rebate_desc', '')}) 还有 31 分钟即将开抢 {adv_prefix}（执行时间: {eff_time_str}），抢单通道已就绪！",
                        account_key=apt.get("account_key")
                    )
                except Exception as ne:
                    logger.warning(f"发送系统预热通知失败: {ne}")

            # 2. 倒计时 25 秒：长连接热激活 + 提前解析并锁定最高可用红包 (消除开火网络耗时)
            if diff <= 25 and not apt.get("prewarmed"):
                apt["prewarmed"] = 1
                db.update_appointment(aid, {"status": "primed"})
                logger.info(f"预约任务 #{aid} 提前 25 秒唤醒预热，长连接热激活并提前解析锁定红包...")
                t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                if linked_log_id > 0:
                    _append_apt_log(aid, linked_log_id, f"[{t_now}] 提前 25 秒唤醒：长连接握手热激活，预检锁定红包资产，微秒自旋压枪...", sync_db=True, status="running")
                try:
                    # 提前探测一次连接与锁定最优红包
                    await client.get_user_info(token=account["token"], silk_id=account.get("silk_id"), user_id=account.get("user_id"), city_code=account.get("city_code", 440303))
                    if apt.get("redpack_mode", 0) == 0 and not apt.get("cached_redpack_id"):
                        max_rp = await client.get_user_max_redpack(token=account["token"], silk_id=account.get("silk_id"), user_id=account.get("user_id"), city_code=account.get("city_code", 440303))
                        rep_pack = max_rp.get("rep_pack") or {}
                        if not rep_pack and max_rp.get("platform_red_packs"):
                            rep_pack = max_rp["platform_red_packs"][0]
                        if rep_pack and rep_pack.get("user_red_pack_id"):
                            apt["cached_redpack_id"] = int(rep_pack["user_red_pack_id"])
                            logger.info(f"预约任务 #{aid} 已提前锁定最优红包 #{apt['cached_redpack_id']}")
                    elif apt.get("redpack_mode") == 1 and apt.get("redpack_id") and not apt.get("cached_redpack_id"):
                        spec_id = int(apt["redpack_id"])
                        rplist_res = await client.get_app_redpack_list(token=account["token"], silk_id=account.get("silk_id"), user_id=account.get("user_id"), city_code=account.get("city_code", 440303), page=1, page_size=50)
                        unused = rplist_res.get("unused_items") or []
                        matched = any(int(item.get("user_red_pack_id") or 0) == spec_id for item in unused)
                        if matched:
                            apt["cached_redpack_id"] = spec_id
                            logger.info(f"预约任务 #{aid} 已提前核验并锁定指定红包 #{spec_id}")
                        else:
                            if unused:
                                sorted_unused = sorted(unused, key=lambda x: float(x.get("reward_num") or 0), reverse=True)
                                apt["cached_redpack_id"] = int(sorted_unused[0]["user_red_pack_id"])
                                logger.info(f"预约任务 #{aid} 预设红包 #{spec_id} 已在端外失效，提前自愈替换为最优红包 #{apt['cached_redpack_id']}")
                except Exception as rpe:
                    logger.debug(f"预约任务提前解析红包异常: {rpe}")

            # 3. 准点 / 提前量到达 (diff <= early_sec)：突发毫秒抢单！
            if diff <= early_sec:
                adv_log = "【超前抢单提前30分钟】" if use_advance else ""
                logger.info(f"任务 #{aid} 抢单时刻到达 (提前量 {early_sec}s, {adv_log})，发起突发毫秒抢单！")
                t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                if linked_log_id > 0:
                    _append_apt_log(aid, linked_log_id, f"[{t_now}] 预定开抢时刻到达 (提前量 {early_sec*1000:.0f}ms, {adv_log})，全速发射抢单报文！", sync_db=True, status="running")
                grab_ok = False
                last_msg = "抢单未生效"
                for try_idx in range(1, 4):
                    ts_fmt = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                    res = await execute_grab_for_appointment(apt, account, advance=use_advance, action_source="store_appoint")
                    if res.get("ok"):
                        logger.info(f"[{ts_fmt}] 任务 #{aid} 第{try_idx}次抢单成功！")
                        grab_ok = True
                        break
                    else:
                        last_msg = res.get("message") or "名额已满或尚未开仓"
                        logger.warning(f"[{ts_fmt}] 任务 #{aid} 第{try_idx}次抢单提示: {last_msg}")
                        if res.get("code") in (40003, 40004, 40037, 40038, 40039, 40040):
                            break
                    await asyncio.sleep(0.06)

                if not grab_ok:
                    until_str = apt.get("until_time")
                    if until_str:
                        db.update_appointment(aid, {
                            "status": "monitoring",
                            "outcome": f"首轮抢单未锁定 ({last_msg})，已自动切换为持续捡漏监听模式（持续至 {until_str}）"
                        })
                        logger.info(f"任务 #{aid} 首轮抢单未成功，已自动转入持续监听捡漏至 {until_str}")
                        if linked_log_id > 0:
                            t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                            _append_apt_log(aid, linked_log_id, f"[{t_now}] 首轮抢单未锁定 ({last_msg})，已自适应无缝转入实时名额监听捡漏模式（截止 {until_str}）...", sync_db=True, status="running")
                    else:
                        db.update_appointment(aid, {
                            "status": "failed",
                            "outcome": f"抢单失败: {last_msg}"
                        })
                        if linked_log_id > 0:
                            t_now = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                            _append_apt_log(aid, linked_log_id, f"[{t_now}] 抢单流程结束: {last_msg}", sync_db=True, status="error")

    return has_primed


async def _appointment_loop():
    """后台常驻循环：常态 3.0s 低功耗，有临近秒杀时自适应 0.5s 高精度监测"""
    logger.info("霸王餐预约与名额监听引擎已启动")
    while _is_running:
        try:
            has_primed = await process_active_appointments()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"预约监听主循环发生未捕获异常: {e}")
            has_primed = False

        if has_primed:
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(3.0)
    logger.info("霸王餐预约与名额监听引擎已安全退出")


def start_appointment_worker():
    global _worker_task, _is_running
    if not _is_running:
        _is_running = True
        try:
            loop = asyncio.get_running_loop()
            _worker_task = loop.create_task(_appointment_loop())
        except RuntimeError:
            pass


def stop_appointment_worker():
    global _worker_task, _is_running
    _is_running = False
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
