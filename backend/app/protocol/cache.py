"""
后端内存缓存层 (Memory Cache)
用于对 XiaoCanClient 的 RPC 调用做时间窗口缓存，避免高频请求触发 WAF。

用法（在 client.py 里的方法上加装饰器即可）：

    from .cache import cache_rpc

    class XiaoCanClient:
        @cache_rpc(ttl=300)  # 缓存 5 分钟
        async def get_user_card_number(self, ...):
            ...

    # 需要强制刷新时：
    cache_invalidate("get_user_card_number", key="xxx")  # 清某个
    cache_clear_all()                                     # 全清
"""
import asyncio
import hashlib
import json
import time
import logging
from typing import Any, Callable, Optional, Dict, Tuple

logger = logging.getLogger("xiaocan.cache")

# 全局缓存: { "func_name:hash(args)": (timestamp, result) }
_cache: Dict[str, Tuple[float, Any]] = {}
_MAX_CACHE_SIZE = 500  # 最多缓存 500 条，防止内存泄漏


def _make_key(func_name: str, *args, **kwargs) -> str:
    """根据函数名 + 参数生成稳定 hash key，跳过 args[0] (self) 使多实例共享缓存"""
    real_args = args[1:] if args else ()
    raw = f"{func_name}:{json.dumps((real_args, sorted(kwargs.items())), default=str, sort_keys=True)}"
    return hashlib.md5(raw.encode()).hexdigest()


def _is_cacheable(result: Any) -> bool:
    """判断结果是否值得缓存（避免缓存 WAF 熔断后的兜底空结果）
    
    规则：
      - None / False / 空字符串 → 不缓存
      - 空 dict / 空 list → 不缓存
      - dict 去掉通用响应壳（status/code 等）后，剩余业务字段全是空 → 不缓存
      - 其他 → 缓存
    """
    if result is None or result is False or result == "":
        return False
    if isinstance(result, (list, dict)) and len(result) == 0:
        return False
    if isinstance(result, dict):
        # 剥离通用 RPC 响应壳字段，只看业务数据
        business = {k: v for k, v in result.items() if k not in ("status", "code", "msg")}
        if not business:
            return False
        # 剩余业务字段的 list 都为空、且 str 都为空、且没有有意义的 scalar
        for v in business.values():
            if isinstance(v, list) and len(v) > 0:
                return True
            if isinstance(v, str) and len(v) > 0:
                return True
            if isinstance(v, (int, float, bool)):
                return True  # 哪怕 0 也是合法业务值（如 can_use_number=0）
            if isinstance(v, dict) and len(v) > 0:
                return True
        # 所有业务字段都是空 list / 空 str / 空 dict
        return False
    return True


def cache_rpc(ttl: int = 300):
    """
    RPC 调用缓存装饰器（异步函数专用）
    
    Args:
        ttl: 缓存存活秒数，默认 300 秒
    """
    def decorator(func: Callable) -> Callable:
        cache_name = func.__name__

        async def wrapper(*args, **kwargs):
            key = _make_key(cache_name, *args, **kwargs)
            now = time.time()

            # 命中缓存
            if key in _cache:
                ts, result = _cache[key]
                if now - ts < ttl:
                    logger.info(f"[Cache HIT] {cache_name} key={key[:12]} age={int(now-ts)}s ttl={ttl}s")
                    return result
                # 过期了
                del _cache[key]
                logger.info(f"[Cache EXPIRED] {cache_name} key={key[:12]}")

            # 缓存满了，清理最旧的一半
            if len(_cache) >= _MAX_CACHE_SIZE:
                items = sorted(_cache.items(), key=lambda x: x[1][0])
                for k, _ in items[:_MAX_CACHE_SIZE // 2]:
                    del _cache[k]

            # 实际调用
            result = await func(*args, **kwargs)

            # 空结果不缓存（WAF 熔断兜底返回 {} / {"list": []} 等）
            if not _is_cacheable(result):
                logger.info(f"[Cache SKIP] {cache_name} key={key[:12]} reason=empty_result")
                return result

            _cache[key] = (now, result)
            logger.info(f"[Cache SET] {cache_name} key={key[:12]} ttl={ttl}s total={len(_cache)}")
            return result

        wrapper.__name__ = cache_name
        wrapper.__qualname__ = cache_name
        return wrapper

    return decorator


def cache_invalidate(func_name: str, *args, **kwargs) -> bool:
    """手动清除某个函数的缓存"""
    key = _make_key(func_name, *args, **kwargs)
    if key in _cache:
        del _cache[key]
        return True
    return False


def cache_clear_all() -> None:
    """清掉全部缓存"""
    n = len(_cache)
    _cache.clear()
    logger.info(f"[Cache CLEAR] 清除 {n} 条")


def cache_stats() -> Dict[str, int]:
    """返回缓存统计"""
    return {
        "total_entries": len(_cache),
        "max_size": _MAX_CACHE_SIZE,
    }


def cache_list() -> list:
    """列出所有缓存项（调试用）"""
    now = time.time()
    result = []
    for key, (ts, val) in _cache.items():
        ttl_left = max(0, 300 - int(now - ts))
        result.append({"key": key[:12], "age_sec": int(now - ts), "ttl_left": ttl_left})
    return sorted(result, key=lambda x: -x["age_sec"])
