"""
小蚕霸王餐异步 RPC 客户端
负责与 gw.xiaocantech.com/rpc 进行通信
"""
import asyncio
import logging
import time
import uuid
from typing import Any, Dict, Optional
from curl_cffi.requests import AsyncSession
from .signer import generate_headers
from .cache import cache_rpc  # 后端内存缓存装饰器，可通过 @cache_rpc(ttl=300) 启用

logger = logging.getLogger("xiaocan.protocol")

GW_BASE_URL = "https://gw.xiaocantech.com/rpc"

# ---------- 小蚕 WAF 全局限速 ----------
_rate_lock = asyncio.Lock()
_last_call = 0.0
_MIN_INTERVAL = 0.8       # 每次 RPC 调用间隔 800ms
_MAX_CONCURRENT = 2       # 全局最多 2 个并发
_sem = asyncio.Semaphore(_MAX_CONCURRENT)


async def _rate_wait():
    global _last_call
    async with _rate_lock:
        now = asyncio.get_event_loop().time()
        if now - _last_call < _MIN_INTERVAL:
            await asyncio.sleep(_MIN_INTERVAL - (now - _last_call))
        _last_call = asyncio.get_event_loop().time()


class XiaoCanRPCError(Exception):
    def __init__(self, code: int, msg: str, raw: Optional[Dict[str, Any]] = None):
        super().__init__(f"XiaoCan RPC Error [{code}]: {msg}")
        self.code = code
        self.msg = msg
        self.raw = raw or {}


# 官方腾讯云 WAF 熔断状态守护 (防止被封期间盲目重发延长黑名单时间)
_waf_blocked_until: float = 0.0


def is_waf_blocked() -> bool:
    import time
    return time.time() < _waf_blocked_until


def get_waf_cooldown_seconds() -> int:
    import time
    return max(0, int(_waf_blocked_until - time.time()))


def mark_waf_blocked(duration_seconds: int = 15):
    global _waf_blocked_until
    _waf_blocked_until = max(_waf_blocked_until, time.time() + duration_seconds)


def clear_waf_block():
    global _waf_blocked_until
    _waf_blocked_until = 0.0


class XiaoCanClient:
    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout
        self.session = AsyncSession(impersonate="chrome124", timeout=timeout)

    async def close(self):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def invoke_rpc(
        self,
        server_name: str,
        method_name: str,
        body: Optional[Dict[str, Any]] = None,
        city_code: int = 440303,
        token: Optional[str] = None,
        user_id: Optional[str] = None,
        silk_id: Optional[str] = None,
        platform: Optional[str] = "app"
    ) -> Dict[str, Any]:
        # 1. 前置熔断保护：若当前网络处于腾讯云 WAF 冷却期，阻止盲发以防重置封锁计时
        if is_waf_blocked():
            remain_sec = get_waf_cooldown_seconds()
            raise XiaoCanRPCError(
                code=403,
                msg=f"当前网络IP处于官方腾讯云WAF临时频次拦截保护冷却中（剩余约 {remain_sec} 秒），系统已自动熔断休眠以避免延长封禁。如需立即恢复，请切换手机热点、重启光猫更换IP或等待冷却。"
            )

        payload = dict(body) if body else {}
        plat = (platform or "app").lower()
        if plat == "mini":
            payload["app_id"] = 20
        else:
            payload["app_id"] = 10

        headers = generate_headers(
            server_name=server_name,
            method_name=method_name,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id,
            platform=plat
        )

        try:
            async with _sem:
                await _rate_wait()
                resp = await self.session.post(
                    GW_BASE_URL,
                    json=payload,
                    headers=headers,
                    impersonate="chrome124"
                )
            if resp.status_code == 403:
                is_waf = "WAF" in resp.text or "stgw" in resp.headers.get("server", "").lower()
                if is_waf:
                    mark_waf_blocked(duration_seconds=15)
                hint = "已被腾讯云 WAF 防火墙拦截 [403 Forbidden]，系统已进入 15 秒频次缓冲。通常原因是：开启了海外代理/VPN/TUN模式，或当前IP触发了频控防护。请关闭全局代理、切换手机热点或稍后重试" if is_waf else "请求被服务器拒绝 [403 Forbidden]"
                logger.error(f"RPC {server_name}.{method_name} 访问受阻: {hint}")
                raise XiaoCanRPCError(code=403, msg=hint, raw={"status_code": 403, "text": resp.text[:200]})

            if resp.status_code != 200:
                logger.error(f"RPC {server_name}.{method_name} HTTP {resp.status_code} 异常响应: {resp.text[:200]}")
                raise XiaoCanRPCError(code=resp.status_code, msg=f"HTTP {resp.status_code} 异常", raw={"status_code": resp.status_code, "text": resp.text[:200]})

            data = resp.json()
        except XiaoCanRPCError:
            raise
        except Exception as e:
            logger.error(f"RPC {server_name}.{method_name} network failure: {e}")
            raise

        # 1. 检查网关返回的顶层 error (例如 Go micro 500/400 异常)
        if "error" in data:
            err_raw = data.get("error")
            err_code = 500
            err_msg = str(err_raw)
            if isinstance(err_raw, str) and err_raw.startswith("{"):
                try:
                    import json
                    parsed_err = json.loads(err_raw)
                    err_code = parsed_err.get("code", 500)
                    err_msg = parsed_err.get("detail") or parsed_err.get("status") or str(err_raw)
                except Exception:
                    pass
            logger.error(f"RPC {server_name}.{method_name} gateway error: code={err_code}, msg={err_msg}")
            raise XiaoCanRPCError(code=err_code, msg=err_msg, raw=data)

        # 2. 检查业务层 status 结构
        status = data.get("status") or {}
        if isinstance(status, dict):
            code = status.get("code", 0)
            msg = status.get("msg", "ok")
        else:
            code = data.get("code", 500) if isinstance(data.get("code"), int) else 500
            msg = str(status or data.get("detail") or data.get("msg") or "RPC Error")

        if code != 0:
            if code in (3, 25, 40002, 40003, 40004, 40037, 40038, 40039, 40040):
                logger.info(f"RPC {server_name}.{method_name} notice: code={code}, msg={msg}")
            else:
                logger.warning(f"RPC {server_name}.{method_name} rejected: code={code}, msg={msg}")
            raise XiaoCanRPCError(code=code, msg=msg, raw=data)

        return data

    # ---------------- 核心业务 API 封装 ---------------- #

    @cache_rpc(ttl=120)
    async def get_store_list(
        self,
        city_code: int,
        longitude: str,
        latitude: str,
        offset: int = 0,
        limit: int = 20,
        token: Optional[str] = None,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        sort: int = 0,
        store_platform: int = 0,
        scene: int = 2,
        meal_period: int = 3
    ) -> Dict[str, Any]:
        """获取外卖霸王餐附近店铺清单 (优先调用官方核心推荐 SilkwormFusion.FusionService.GetFeedPromotions，回落 RecService.GetStorePromotionList)"""
        # 1. 优先调用官方主页核心推荐 (GetFeedPromotions)
        fusion_body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "lon": float(longitude),
            "lat": float(latitude),
            "number": int(limit),
            "offset": int(offset),
            "sort": int(sort) if sort else 0,
            "scene": 1,
            "is_show_mt": True,
            "page_pv_id": str(uuid.uuid4()),
            "app_id": 10
        }
        try:
            res = await self.invoke_rpc(
                server_name="SilkwormFusion",
                method_name="FusionService.GetFeedPromotions",
                body=fusion_body,
                city_code=city_code,
                token=token,
                user_id=user_id,
                silk_id=silk_id
            )
            if res.get("feed_items"):
                return res
        except Exception as e:
            logger.warning(f"GetFeedPromotions 调用异常，尝试切换至 RecService: {e}")

        # 2. 备选调用官方多平台聚合推荐 (GetStorePromotionList)
        rec_body = {
            "city_code": int(city_code),
            "silk_id": int(silk_id) if silk_id else 0,
            "longitude": float(longitude),
            "latitude": float(latitude),
            "number": int(limit),
            "offset": int(offset),
            "promotion_sort": 1 if sort == 1 else 4,
            "promotion_category": 0,
            "promotion_filter": 0,
            "store_category": 0,
            "store_platform": int(store_platform),
            "store_type": 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormRec",
            method_name="RecService.GetStorePromotionList",
            body=rec_body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def search_stores(
        self,
        keyword: str,
        city_code: int,
        longitude: str,
        latitude: str,
        offset: int = 0,
        limit: int = 20,
        token: Optional[str] = None,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """调用小蚕官方微服务实时搜索店铺 (SilkwormFusion.FusionService.SearchPromotions)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "lat": float(latitude),
            "lon": float(longitude),
            "keyword": keyword.strip(),
            "offset": int(offset),
            "number": min(20, max(1, int(limit))),
            "app_id": 10
        }
        res = await self.invoke_rpc(
            server_name="SilkwormFusion",
            method_name="FusionService.SearchPromotions",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )
        if "promotions" in res and "promotion_list" not in res:
            res["promotion_list"] = res["promotions"]
        return res

    async def get_explore_store_list(
        self,
        city_code: int,
        longitude: str,
        latitude: str,
        offset: int = 0,
        limit: int = 20,
        token: Optional[str] = None,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """获取到店/探店团购霸王餐清单 (ExploreMobile.ListGroupPromotion)"""
        page_num = (int(offset) // int(limit)) + 1 if limit else 1
        body = {
            "loc": {
                "lat": float(latitude),
                "lng": float(longitude)
            },
            "city_code": int(city_code),
            "silk_id": int(silk_id) if silk_id else 0,
            "dist_item": 30000,
            "page": {
                "page_num": page_num,
                "page_size": int(limit)
            },
            "filter": {
                "is_brand": False,
                "is_can_order": True,
                "is_discount_lt4": False,
                "is_done_bargain": False,
                "is_multi": False,
                "is_single": False,
                "original_price_get": 0,
                "store_first_category": 0
            },
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormExplore",
            method_name="ExploreMobile.ListGroupPromotion",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def get_order_list(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        order_status: int = 99,
        offset: int = 0,
        number: int = 20,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取用户霸王餐订单列表 (Silkworm.SilkwormService.GetPromotionOrderList)"""
        # 官方真实状态集：0=待上传, 1=审核中, 2=已完成, 3=已驳回, 4=已取消, 15=待处理角标, 99=全部
        if order_status in (0, 1, 2, 3, 4, 15, 99):
            body_st = int(order_status)
        else:
            body_st = 99

        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "offset": int(offset),
            "number": int(number),
            "order_status": body_st,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="Silkworm",
            method_name="SilkwormService.GetPromotionOrderList",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def search_shangjin_stores(
        self,
        keyword: str,
        latitude: float,
        longitude: float,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440304,
        sort_type: int = 3,
        page_pv_id: str = ""
    ) -> Dict[str, Any]:
        """获取美团按比例返现/赏金商户活动 (SilkwormRcs.SilkwormRcsService.MeituanShangjinGetPoiList)"""
        body = {
            "lat": float(latitude),
            "lng": float(longitude),
            "silk_id": int(silk_id) if silk_id else 0,
            "page_pv_id": page_pv_id or "",
            "sort_type": int(sort_type),
            "search_word": keyword.strip() if keyword else "",
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormRcs",
            method_name="SilkwormRcsService.MeituanShangjinGetPoiList",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    @cache_rpc(ttl=300)
    async def get_user_info(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        platform: Optional[str] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """获取小蚕官方真实个人资料 (Silkworm.SilkwormService.GetClientUserInfo)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "if_need_subscribe": True,
            "if_need_notify_status": True,
            "inviter_silk_id": 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="Silkworm",
            method_name="SilkwormService.GetClientUserInfo",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    # ---------------- 官方任务中心与元宝生态 (ActivityTask / Vip) ---------------- #

    @cache_rpc(ttl=300)
    async def get_user_task_v2(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取元宝总览与待领取状态 (ActivityTask.ActivityTaskMobileService.UserTaskV2)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="ActivityTask",
            method_name="ActivityTaskMobileService.UserTaskV2",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def get_unreceived_point_records(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        page: int = 1,
        page_size: int = 10,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取待领取元宝明细列表 (ActivityTask.ActivityTaskMobileService.GetUnReceivedPointRecords)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "page": int(page),
            "page_size": int(page_size),
            "status": 1,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="ActivityTask",
            method_name="ActivityTaskMobileService.GetUnReceivedPointRecords",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def get_wait_claimed_points(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取待领取的元宝点数统计 (ActivityTask.ActivityTaskMobileService.WaitClaimedPoints)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="ActivityTask",
            method_name="ActivityTaskMobileService.WaitClaimedPoints",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def get_user_sign_in_days(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取会员每日连续签到天数与今日签到状态 (SilkwormVip.VipRightsService.UserSignInDays)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.UserSignInDays",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def get_sign_in_node(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取签到里程碑节点奖励状态 (SilkwormVip.VipRightsService.SignInNode)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "need_status": True,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.SignInNode",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def sign_in_lottery(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """会员专属每日签到抽奖 (SilkwormVip.VipRightsService.SignInLottery)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.SignInLottery",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def check_activity_eligibility(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """检查用户活动与红包雨参与资格 (SilkwormShareSupport.SilkwormShareSupportService.CheckActivityEligibility)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "check_red_packet_guide": True,
            "check_subscribe": True,
            "check_supporter_experiment": True,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormShareSupport",
            method_name="SilkwormShareSupportService.CheckActivityEligibility",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def get_client_withdraw_list(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        offset: int = 0,
        number: int = 10,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取提现记录列表 (Silkworm.SilkwormService.GetClientWithdrawList)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "offset": int(offset),
            "number": int(number),
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="Silkworm",
            method_name="SilkwormService.GetClientWithdrawList",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    # ---------------- 消息中心 (SilkwormMessageCenter) ---------------- #

    async def get_message_channels(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取消息中心频道分类及未读数 (SilkwormMessageCenter.MessageCenterService.ListChannelsSimple)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "page": 1,
            "page_size": 20,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormMessageCenter",
            method_name="MessageCenterService.ListChannelsSimple",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def get_messages(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        channel_id: Optional[int] = None,
        page: int = 1,
        page_size: int = 20,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取消息列表 (SilkwormMessageCenter.MessageCenterService.ListMessages)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "page": int(page),
            "page_size": int(page_size),
            "app_id": 10
        }
        if channel_id is not None and channel_id > 0:
            body["channel_id"] = int(channel_id)
        else:
            body["query_all"] = True

        return await self.invoke_rpc(
            server_name="SilkwormMessageCenter",
            method_name="MessageCenterService.ListMessages",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    async def set_all_messages_read(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """将当前账号所有消息标记为已读 (SilkwormMessageCenter.MessageCenterService.SetMessageAllRead)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormMessageCenter",
            method_name="MessageCenterService.SetMessageAllRead",
            body=body,
            city_code=city_code,
            token=token,
            user_id=user_id,
            silk_id=silk_id
        )

    # ---------------- 官方微服务日常任务与营销活动 ---------------- #

    async def do_user_sign(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """元宝乐园每日签到 (SilkwormMobileMarketingService.UserSign)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormMarketing",
            method_name="SilkwormMobileMarketingService.UserSign",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def do_checkin(self, token: str, city_code: int = 440303, silk_id: Optional[str] = None, user_id: Optional[str] = None) -> Dict[str, Any]:
        """每日签到打卡 (兼容旧接口别名)"""
        return await self.do_user_sign(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code)

    async def complete_task_event(
        self,
        task_type: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """完成任务领奖励 (SilkwormMobileMarketingService.CompleteTaskEvent: 3=饿了么, 4=美团, 6=社群)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "task_type": int(task_type)
        }
        return await self.invoke_rpc(
            server_name="SilkwormMarketing",
            method_name="SilkwormMobileMarketingService.CompleteTaskEvent",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def incr_lottery_number(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """增加每日抽奖机会 (SilkwormMobileMarketingService.UserIncrLotteryNumber)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormMarketing",
            method_name="SilkwormMobileMarketingService.UserIncrLotteryNumber",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def get_vip_up_gift(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取/激活会员升级及每日专属成长礼包 (SilkwormMobileMarketingService.GetUserUpVipGift)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormMarketing",
            method_name="SilkwormMobileMarketingService.GetUserUpVipGift",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def list_vip_gifts(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取会员专属权益礼包列表 (SilkwormMobileMarketingService.ListVipGifts)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormMarketing",
            method_name="SilkwormMobileMarketingService.ListVipGifts",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def list_vip_gift_details(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取各VIP等级特权卡券明细 (SilkwormMobileMarketingService.ListVipGiftDetails)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormMarketing",
            method_name="SilkwormMobileMarketingService.ListVipGiftDetails",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def get_vip_task(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取会员成长任务状态 (SilkwormVipMobile.GetVipTask)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="SilkwormVipMobile.GetVipTask",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def get_vip_prizes(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        platform: str = "app"
    ) -> Dict[str, Any]:
        """获取会员专属每日膨胀红包池与成长值 (SilkwormVipMobile.VipPrizes)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="SilkwormVipMobile.VipPrizes",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform=platform
        )

    async def get_vip_rebate_info(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取会员专属大牌秒杀与返利库存 (SilkwormVipMobile.GetVipRebateInfo)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="SilkwormVipMobile.GetVipRebateInfo",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    # ---------------- 官方转盘与幸运抽奖 ---------------- #

    async def get_lottery_info(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取今日幸运转盘配置与剩余抽奖机会 (SilkwormLotteryMobile.LotteryInfo)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.LotteryInfo",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def add_lottery_times(
        self,
        lottery_type: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """领取免费任务赠送的开红包机会 (SilkwormLotteryMobile.AddLotteryTimes: 1=签到, 2=分享, 4=沾一沾, 8=饿了么, 9=美团, 10=到店, 11=福利)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "type": int(lottery_type)
        }
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.AddLotteryTimes",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def do_lottery_spin(
        self,
        token: str,
        prize_type: int = 1,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """执行幸运转盘抽奖/免费开红包 (SilkwormLotteryMobile.Lottery)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "prize_type": int(prize_type)
        }
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.Lottery",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def receive_extra_lottery(
        self,
        step: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """领取阶梯累计开红包奖励 (SilkwormLotteryMobile.ReceiveExtraLottery: step 1=第一阶段/3次, step 2=第二阶段/9次)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "step": int(step)
        }
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.ReceiveExtraLottery",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def claim_ad_lottery_chance(
        self,
        bus_type: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """激励视频/商城广告浏览完成获取额外抽奖 (SilkwormLotteryMobile.OnAdViewed: bus_type 2=视频广告, 4=抖音商城)"""
        import hmac
        import hashlib
        import base64
        import string
        import random
        import time

        nonce = "".join(random.choice(string.ascii_lowercase) for _ in range(6))
        ts = int(time.time())
        sid = int(silk_id) if silk_id else 0
        raw_str = f"silk_id={sid}&timestamp={ts}&nonce={nonce}&bus_type={bus_type}"
        key = b"lcjkbqadfrzsewxy"
        sign = base64.b64encode(hmac.new(key, raw_str.encode("utf-8"), hashlib.sha256).digest()).decode("utf-8")

        body = {
            "silk_id": sid,
            "timestamp": ts,
            "nonce": nonce,
            "bus_type": int(bus_type),
            "sign": sign
        }
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.OnAdViewed",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def lottery_send_redpack(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """免费开红包页面自动赠送红包机制 (PartnerPromotionService.LotterySendRedPack)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="lottery",
            method_name="PartnerPromotionService.LotterySendRedPack",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def get_lottery_progress(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取阶梯累计抽奖进度 (SilkwormLotteryMobile.GetLotteryProgress)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.GetLotteryProgress",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    # ---------------- 官方整点红包雨与权益秒杀 ---------------- #

    async def get_redpack_rain_event(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取当前或即将开始的红包雨场次信息 (SilkwormLotteryMobile.GetRedPackRainEvent)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "city_code": int(city_code)
        }
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.GetRedPackRainEvent",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def join_redpack_rain(
        self,
        token: str,
        event_id: Optional[int] = None,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """报名/接入整点红包雨场次 (SilkwormLotteryMobile.JoinRedPackRainEvent)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "city_code": int(city_code)
        }
        if event_id:
            body["event_id"] = int(event_id)
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.JoinRedPackRainEvent",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def grab_redpack_rain(
        self,
        token: str,
        event_id: Optional[int] = None,
        click_num: int = 15,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """在红包雨进行时抢抓红包额度 (SilkwormLotteryMobile.RedPackRainGrabNum)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "click_num": int(click_num)
        }
        if event_id:
            body["event_id"] = int(event_id)
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.RedPackRainGrabNum",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def list_user_redpack(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """查询用户领取的红包雨红包记录 (SilkwormLotteryMobile.ListUserRedPack)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormLottery",
            method_name="SilkwormLotteryMobile.ListUserRedPack",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )


    # ---------------- 官方独立 App 原生 VIP 特权与秒杀微服务 (VipRightsService) ---------------- #

    # 1. 每月保底大牌券通道 (每月固定保底张数，如 SVIP6 每月 8 张保底，严禁在每日日常抢券定时任务中调用)
    async def get_brand_card_info(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        card_type: int = 1
    ) -> Dict[str, Any]:
        """查询每月保底 SVIP 大牌神券总额度与剩余保底配额 (SilkwormVip.VipRightsService.BrandCardInfo)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "type": int(card_type)
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.BrandCardInfo",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def grab_brand_card(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        card_type: int = 1
    ) -> Dict[str, Any]:
        """手动领取每月保底 SVIP 大牌神券 (SilkwormVip.VipRightsService.GrabBrandCard - 注意：此接口直接扣减当月保底张数，日常定时任务绝不可调用！)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "type": int(card_type)
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.GrabBrandCard",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    # 2. 每日 09:30 抢大牌券专属通道 (官方额外放量大牌券，独立库存，不扣减每月保底张数)
    async def get_extra_brand_card_pool(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """查询每日 09:30 SVIP 额外大牌放量券配置与实时库存 (SilkwormVip.VipRightsService.ExtraBrandCardPool)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.ExtraBrandCardPool",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def grab_extra_brand_card(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        card_type: int = 99
    ) -> Dict[str, Any]:
        """秒杀抢占每日 09:30 SVIP 额外大牌放量券 (SilkwormVip.VipRightsService.GrabExtraBrandCard - 默认 card_type=99)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "type": int(card_type)
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.GrabExtraBrandCard",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    # 3. 影音会员周卡 (腾讯视频/网易云等) 特权通道
    async def get_user_tencent_vip_info(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """查询每月影音周卡(网易云音乐/腾讯视频等)用户状态与余量 (SilkwormVip.VipRightsService.UserTencentVipInfo)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.UserTencentVipInfo",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def get_user_tencent_vip_list(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """查询历史影音会员周卡领取记录与CDK券码明细 (SilkwormVip.VipRightsService.UserTencentVipList)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "page": int(page),
            "page_size": int(page_size)
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.UserTencentVipList",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def grab_tencent_vip_quota(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """秒杀抢占影音会员周卡(网易云音乐/腾讯视频等)配额 (SilkwormVip.VipRightsService.GrabTencentVipQuota)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.GrabTencentVipQuota",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def get_user_free_order_info(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """查询每月外卖免单券持有与核销状态 (SilkwormVip.VipRightsService.UserFreeOrderInfo)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.UserFreeOrderInfo",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def grab_free_order_quota(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """秒杀抢占 14:00 每月外卖霸王餐免单神券配额 (SilkwormVip.VipRightsService.GrabFreeOrderQuota)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.GrabFreeOrderQuota",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def use_free_order_card(
        self,
        order_no: str,
        order_business: int,
        user_free_card_id: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """对指定外卖订单核销使用免单券 (SilkwormVip.VipRightsService.UseFreeOrderCard)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "order_no": str(order_no),
            "order_business": int(order_business),
            "user_free_card_id": int(user_free_card_id)
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.UseFreeOrderCard",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def get_plus_vip_rebate_card_infos(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """查询 SVIP 专属返利券档位列表 (SilkwormVip.VipRightsService.PlusVipRebateCardInfos)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.PlusVipRebateCardInfos",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def grab_rebate_card_quota(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """秒杀抢占今日 09:00 SVIP 专属大额返利券 (SilkwormVip.VipRightsService.GrabRebateCardQuota)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.GrabRebateCardQuota",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def get_user_sign_in_days(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """查询会员打卡签到天数与今日签到状态 (SilkwormVip.VipRightsService.UserSignInDays)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="VipRightsService.UserSignInDays",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def list_flash_sale_exchanges(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        new_level: int = 5,
        is_plus: bool = True
    ) -> Dict[str, Any]:
        """查询元宝商城限量秒杀兑换商品列表 (SilkwormCommunity.SilkwormMobileCommunityService.ListFlashSaleExchanges)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "new_level": int(new_level),
            "is_plus": bool(is_plus)
        }
        return await self.invoke_rpc(
            server_name="SilkwormCommunity",
            method_name="SilkwormMobileCommunityService.ListFlashSaleExchanges",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def today_exchange(
        self,
        exchange_id: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """秒杀兑换今日元宝商品 (SilkwormCommunity.SilkwormMobileCommunityService.TodayExchange)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "exchange_id": int(exchange_id)
        }
        return await self.invoke_rpc(
            server_name="SilkwormCommunity",
            method_name="SilkwormMobileCommunityService.TodayExchange",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform="app"
        )

    async def complete_task_event(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        promotion_order_id: Optional[int] = None,
        task_type: int = 4,
        platform: str = "app"
    ) -> Dict[str, Any]:
        """完成领红包延时或营销任务 (SilkwormMarketing.SilkwormMobileMarketingService.CompleteTaskEvent)
        task_type: 4 (美团订单延时 2 小时 - user_receive_meituan_task), 3 (饿了么订单延时 2 小时 - user_receive_elem_task)
        """
        body: Dict[str, Any] = {
            "silk_id": int(silk_id) if silk_id else 0,
            "task_type": int(task_type)
        }
        if promotion_order_id is not None:
            body["promotion_order_id"] = int(promotion_order_id)

        return await self.invoke_rpc(
            server_name="SilkwormMarketing",
            method_name="SilkwormMobileMarketingService.CompleteTaskEvent",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id,
            platform=platform
        )

    async def vip_prizes_lottery(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        vip_level: int = 0,
        platform: str = "app"
    ) -> Dict[str, Any]:
        """执行每日 09:30 SVIP 额外大牌券秒杀 (严禁调用 GrabBrandCard 消耗每月保底)"""
        try:
            return await self.grab_extra_brand_card(token=token, silk_id=silk_id, user_id=user_id, city_code=city_code, card_type=99)
        except Exception:
            body: Dict[str, Any] = {
                "silk_id": int(silk_id) if silk_id else 0
            }
            if vip_level:
                body["level"] = int(vip_level)
            return await self.invoke_rpc(
                server_name="SilkwormVip",
                method_name="SilkwormVipMobile.VipPrizesLottery",
                body=body,
                city_code=city_code,
                token=token,
                silk_id=silk_id,
                user_id=user_id,
                platform=platform
            )

    async def get_vip_rebate_info(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """查询每日 09:00 SVIP 专属返利券库存与进度 (SilkwormVipMobile.GetVipRebateInfo)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="SilkwormVip",
            method_name="SilkwormVipMobile.GetVipRebateInfo",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def exchange_goods(
        self,
        goods_id: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """元宝商城限量秒杀兑换商品 (SilkwormMobileCommunityService.ExchangeGoods)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "goods_id": int(goods_id),
            "city_code": int(city_code)
        }
        return await self.invoke_rpc(
            server_name="SilkwormCommunity",
            method_name="SilkwormMobileCommunityService.ExchangeGoods",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def grab_promotion_quota(
        self,
        promotion_id: Any,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        store_platform: int = 1,
        if_advance_order: bool = False,
        latitude: float = 0.0,
        longitude: float = 0.0,
        redpack_id: Optional[Any] = None,
        vip_extra_silk_card_id: Optional[Any] = None,
        vip_promotion_card_id: Optional[Any] = None
    ) -> Dict[str, Any]:
        """抢占霸王餐秒杀名额配额 (双模智能路由: 官方直连微服务 Silkworm.SilkwormService.GrabPromotionQuota / 聚合微服务 SilkwormFusion.FusionService.FusionGrabPromotionQuota)"""
        try:
            pid_int = int(promotion_id)
        except Exception:
            pid_int = 0

        # 对齐移动 App 原生协议参数 (PromotionDetailViewModel$order$1)
        body: Dict[str, Any] = {
            "silk_id": int(silk_id) if silk_id else 0,
            "promotion_id": pid_int,
            "store_platform": int(store_platform or 1),
            "if_advance_order": bool(if_advance_order),
            "latitude": float(latitude or 0.0),
            "longitude": float(longitude or 0.0),
            "city_code": int(city_code or 440303)
        }
        if redpack_id is not None:
            try:
                r_int = int(redpack_id)
                if r_int > 0:
                    body["redpack_id"] = r_int
            except (ValueError, TypeError):
                pass
        if vip_promotion_card_id is not None:
            try:
                vp_int = int(vip_promotion_card_id)
                if vp_int > 0:
                    body["vip_promotion_card_id"] = vp_int
            except (ValueError, TypeError):
                pass
        if vip_extra_silk_card_id is not None:
            try:
                v_int = int(vip_extra_silk_card_id)
                if v_int > 0:
                    body["vip_extra_silk_card_id"] = v_int
            except (ValueError, TypeError):
                pass

        # 智能判定活动微服务路由：
        # 1. 19位雪花算法ID (pid_int >= 10_000_000_000): 聚合微服务 SilkwormFusion.FusionService.FusionGrabPromotionQuota
        # 2. 8~9位标准活动ID (pid_int < 10_000_000_000): 官方直连微服务 Silkworm.SilkwormService.GrabPromotionQuota
        is_fusion_pid = pid_int >= 10_000_000_000

        primary_server = "SilkwormFusion" if is_fusion_pid else "Silkworm"
        primary_method = "FusionService.FusionGrabPromotionQuota" if is_fusion_pid else "SilkwormService.GrabPromotionQuota"
        fallback_server = "Silkworm" if is_fusion_pid else "SilkwormFusion"
        fallback_method = "SilkwormService.GrabPromotionQuota" if is_fusion_pid else "FusionService.FusionGrabPromotionQuota"

        try:
            return await self.invoke_rpc(
                server_name=primary_server,
                method_name=primary_method,
                body=body,
                city_code=city_code,
                token=token,
                silk_id=silk_id,
                user_id=user_id,
                platform="app"
            )
        except XiaoCanRPCError as rpc_err:
            # 当返回错误码为 2 (参数错误) 或 3 (活动已过期 / 服务端不认该ID)，自动尝试备用微服务进行自愈
            if rpc_err.code in (2, 3):
                logger.info(
                    f"主路由 {primary_server}.{primary_method} 返回兼容性响应码 {rpc_err.code} ({rpc_err.msg})，启动自愈备用路由 {fallback_server}.{fallback_method}..."
                )
                return await self.invoke_rpc(
                    server_name=fallback_server,
                    method_name=fallback_method,
                    body=body,
                    city_code=city_code,
                    token=token,
                    silk_id=silk_id,
                    user_id=user_id,
                    platform="app"
                )
            raise rpc_err

    @cache_rpc(ttl=180)
    async def get_user_max_redpack(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取用户当前可用红包清单与最大红包 (RedPackService.GetUserMaxRedPack)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="RedPackService",
            method_name="RedPackService.GetUserMaxRedPack",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def cancel_promotion_quota(
        self,
        promotion_order_id: Any,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """取消已抢到的霸王餐订单名额 (SilkwormService.CancelPromotionQuota)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "promotion_order_id": int(promotion_order_id)
        }
        return await self.invoke_rpc(
            server_name="Silkworm",
            method_name="SilkwormService.CancelPromotionQuota",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def get_store_promotion_detail(
        self,
        promotion_id: Any,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """查询店铺霸王餐活动实时库存与详情 (SilkwormService.GetStorePromotionDetail)"""
        try:
            pid_int = int(promotion_id)
        except Exception:
            pid_int = 0

        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "promotion_id": pid_int
        }
        return await self.invoke_rpc(
            server_name="Silkworm",
            method_name="SilkwormService.GetStorePromotionDetail",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def appoint_store(
        self,
        token: str,
        promotion_id: str,
        redpack_mode: int = 0,
        redpack_id: Optional[str] = None,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """提交店铺霸王餐预约抢单 (PromotionService.AppointOrder)"""
        body = {
            "promotion_id": str(promotion_id),
            "redpack_mode": int(redpack_mode),
            "redpack_id": redpack_id or "",
            "app_id": 10,
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="Promotion",
            method_name="PromotionService.AppointOrder",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    # ---------------- 官方钱包提现与卡券资产 ---------------- #

    async def client_withdraw(
        self,
        money_cents: int,
        token: str,
        withdraw_type: int = 1,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """钱包余额申请提现到微信或支付宝 (SilkwormService.ClientWithdraw)
        withdraw_type: 0 微信钱包, 1 支付宝
        """
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "money": int(money_cents),
            "type": int(withdraw_type)
        }
        return await self.invoke_rpc(
            server_name="Silkworm",
            method_name="SilkwormService.ClientWithdraw",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    # ---------------- 天天赚元宝与气泡元宝收取 ---------------- #

    async def get_daily_tasks(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取天天赚元宝中心任务列表与完成状态 (ActivityTaskMobileService.GetDailyTask)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "user_id": int(user_id) if user_id else 0
        }
        return await self.invoke_rpc(
            server_name="ActivityTask",
            method_name="ActivityTaskMobileService.GetDailyTask",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def complete_activity_task(
        self,
        task_id: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """完成天天赚元宝中心指定任务 (如任务57抖音电商浏览30s) (ActivityTaskMobileService.CompleteTask)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "user_id": int(user_id) if user_id else 0,
            "task_id": int(task_id)
        }
        return await self.invoke_rpc(
            server_name="ActivityTask",
            method_name="ActivityTaskMobileService.CompleteTask",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def wait_claimed_points(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取待收取的未收元宝与气泡任务明细 (ActivityTaskMobileService.WaitClaimedPoints)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0
        }
        return await self.invoke_rpc(
            server_name="ActivityTask",
            method_name="ActivityTaskMobileService.WaitClaimedPoints",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def collect_points(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """一键收取账户所有待领气泡元宝与达标任务元宝 (ActivityTask.ActivityTaskMobileService.CollectPoints)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="ActivityTask",
            method_name="ActivityTaskMobileService.CollectPoints",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def user_claim_points(
        self,
        task_id: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """收取指定的已完成任务元宝或气泡元宝 (ActivityTaskMobileService.UserClaimPoints)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "user_id": int(user_id) if user_id else 0,
            "task_id": int(task_id)
        }
        return await self.invoke_rpc(
            server_name="ActivityTask",
            method_name="ActivityTaskMobileService.UserClaimPoints",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    async def draw_page_gift(
        self,
        page_gift_type: int,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """领取页面礼包 (SilkwormMobileMarketingService.DrawPageGift)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "page_gift_type": int(page_gift_type)
        }
        return await self.invoke_rpc(
            server_name="SilkwormMarketing",
            method_name="SilkwormMobileMarketingService.DrawPageGift",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    @cache_rpc(ttl=300)
    async def get_user_card_number(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取用户特权卡券数量统计 (SilkwormCardService.GetUserCardNumber)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "card_number_mode": 1,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="SilkwormCard",
            method_name="SilkwormCardService.GetUserCardNumber",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    @cache_rpc(ttl=300)
    async def get_user_card_list(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        status: int = 0,
        offset: int = 0,
        number: int = 30
    ) -> Dict[str, Any]:
        """获取用户专属卡券与特权券列表 (SilkwormCardService.GetUserCardList, status: 0未使用, 1已使用, 2已过期)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "status": int(status),
            "offset": int(offset),
            "number": int(number),
            "app_id": 10
        }
        try:
            return await self.invoke_rpc(
                server_name="SilkwormCard",
                method_name="SilkwormCardService.GetUserCardList",
                body=body,
                city_code=city_code,
                token=token,
                silk_id=silk_id,
                user_id=user_id
            )
        except Exception as e:
            logger.warning(f"GetUserCardList failed: {e}")
            return {"status": {"code": 0}, "list": []}

    @cache_rpc(ttl=300)
    async def get_user_redpack_num(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303
    ) -> Dict[str, Any]:
        """获取用户可用红包总数 (RedPackService.GetUserRedPackNum)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="RedPackService",
            method_name="RedPackService.GetUserRedPackNum",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )

    @cache_rpc(ttl=180)
    async def get_app_redpack_list(
        self,
        token: str,
        silk_id: Optional[str] = None,
        user_id: Optional[str] = None,
        city_code: int = 440303,
        page: int = 1,
        page_size: int = 30
    ) -> Dict[str, Any]:
        """获取用户霸王餐红包列表 (RedPackService.GetAppRedPackList)"""
        body = {
            "silk_id": int(silk_id) if silk_id else 0,
            "page": int(page),
            "page_size": int(page_size),
            "app_id": 10
        }
        return await self.invoke_rpc(
            server_name="RedPackService",
            method_name="RedPackService.GetAppRedPackList",
            body=body,
            city_code=city_code,
            token=token,
            silk_id=silk_id,
            user_id=user_id
        )


