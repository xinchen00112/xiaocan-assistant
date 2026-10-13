"""
小蚕霸王餐协议特征签名计算 (League of Legends 特征头)
逆向提取自小蚕小程序与网关通讯 RPC 协议
"""
import hashlib
import time
import uuid
from typing import Optional, Dict


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def generate_nami() -> str:
    """生成合规的 36 位客户端会话 UUID"""
    return str(uuid.uuid4())


def get_ashe(time_millis: int, server_name: str, method_name: str, nami: str) -> str:
    """
    计算 X-Ashe 签名：
    1. 计算 md5((serverName.methodName).toLowerCase())
    2. 将第一步的散列值与时间戳及 nami 拼接后再计算 md5
    """
    x = md5_hex(f"{server_name}.{method_name}".lower())
    return md5_hex(f"{x}{time_millis}{nami}")


def generate_headers(
    server_name: str,
    method_name: str,
    city_code: int = 440303,
    token: Optional[str] = None,
    user_id: Optional[str] = None,
    silk_id: Optional[str] = None,
    nami: Optional[str] = None,
    time_millis: Optional[int] = None,
    platform: str = "app",
    device_id: Optional[str] = None,
) -> Dict[str, str]:
    """
    生成小蚕霸王餐网关调用所需的全部请求头 (英雄联盟特征头规范)
    全局默认采用官方独立 App 原生客户端协议规范，彻底杜绝 50010 限制
    """
    if time_millis is None:
        time_millis = int(time.time() * 1000)
    if nami is None:
        nami = generate_nami()

    ashe = get_ashe(time_millis, server_name, method_name, nami)
    is_mini = str(platform).lower() == "mini"

    headers = {
        "x-City": str(city_code),
        "X-Garen": str(time_millis),
        "X-Nami": nami,
        "x-Annie": "XC",
        "servername": server_name,
        "methodname": method_name,
        "X-Ashe": ashe,
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }

    if is_mini:
        # 微信小程序环境特征 (仅在显式指定 mini 时生效)
        headers["X-Platform"] = "mini"
        headers["appid"] = "20"
        headers["X-App-Sr"] = "20"
        headers["version"] = "3.20.6.67"
        headers["X-Version"] = "3.20.6.67"
        headers["X-Model"] = "microsoft microsoft"
        headers["xweb_xhr"] = "1"
        headers["Sec-Fetch-Site"] = "cross-site"
        headers["Sec-Fetch-Mode"] = "cors"
        headers["Sec-Fetch-Dest"] = "empty"
        headers["Referer"] = "https://servicewechat.com/wx52ae177248081591/798/page-frame.html"
        headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/132.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) "
            "NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x63090a13) "
            "UnifiedPCWindowsWechat(0xf254173b) XWEB/19027"
        )
    else:
        # 全局默认采用移动独立 App 原生协议特征 (彻底突破大牌券、影音周卡等 50010 限制)
        headers["X-Platform"] = "Android"
        headers["appid"] = "10"
        headers["X-App-Sr"] = "10"
        headers["version"] = "3.21.1"
        headers["X-Version"] = "3.21.1"
        headers["x-channel"] = "xiaocan"
        headers["X-CityCode"] = str(city_code)
        headers["X-Model"] = "Android Phone"
        headers["User-Agent"] = "XiaoCan/3.21.1 (Linux; U; Android 14; zh-cn; Mobile) okhttp/4.12.0"
        if device_id:
            headers["device_id"] = device_id
            headers["X-Device-Id"] = device_id

    if user_id:
        headers["x-Vayne"] = str(user_id)
    if silk_id:
        headers["x-Teemo"] = str(silk_id)

    if token:
        clean_token = token.replace("Bearer ", "").strip()
        headers["Authorization"] = f"Bearer {clean_token}"
        headers["token"] = clean_token
        headers["x-Sivir"] = clean_token

        # 若未显式传入 user_id，自动尝试从 JWT 中解码
        if not user_id and clean_token.count(".") == 2:
            try:
                import json
                import base64
                parts = clean_token.split(".")
                padded = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
                payload = json.loads(base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8"))
                uid = payload.get("UserId") or payload.get("user_id")
                if uid:
                    headers["x-Vayne"] = str(uid)
            except Exception:
                pass

    return headers
