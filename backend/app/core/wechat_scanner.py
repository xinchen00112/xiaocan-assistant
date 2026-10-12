"""
电脑版微信小程序无感内存捕获核心
原理：直接读取当前运行的 WeChatAppEx.exe (微信内置小程序渲染引擎) 进程内存中的小蚕官方鉴权凭证
优势：
1. 0 配置：不修改 Windows 网络代理，不需要任何端口转发
2. 0 风险：不安装任何 CA 根证书，不产生断网问题
3. 毫秒级捕获：用户只需在电脑微信中点开一次「小蚕霸王餐」小程序，瞬间提取 Token 与 Silk ID

注意：本模块仅在 Windows 上可用，Linux/macOS 提供空实现。
"""
import sys
import logging
from typing import Optional, Dict, Any, List, Tuple

logger = logging.getLogger("xiaocan.scanner")

_IS_WINDOWS = sys.platform.startswith("win")

if _IS_WINDOWS:
    import ctypes
    import re
    import json
    import base64
    import time
    import threading
    from ctypes import wintypes

    PROCESS_QUERY_INFORMATION = 0x0400
    PROCESS_VM_READ = 0x0010
    MEM_COMMIT = 0x1000
    TH32CS_SNAPPROCESS = 0x00000002

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    class PROCESSENTRY32(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_void_p),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", ctypes.c_char * 260),
        ]

    class MEMORY_BASIC_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BaseAddress", ctypes.c_void_p),
            ("AllocationBase", ctypes.c_void_p),
            ("AllocationProtect", wintypes.DWORD),
            ("RegionSize", ctypes.c_size_t),
            ("State", wintypes.DWORD),
            ("Protect", wintypes.DWORD),
            ("Type", wintypes.DWORD),
        ]

    JWT_PATTERN = re.compile(rb"eyJ[A-Za-z0-9_-]{10,}\.ey[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}")
    SILK_PATTERN = re.compile(rb'["\']silk_id["\']\s*:\s*(\d{7,11})')
    NICK_PATTERN = re.compile(rb'["\']nickname["\']\s*:\s*["\']([^"\']{1,20})["\']')


    def get_wechat_applet_pids() -> List[Tuple[int, str]]:
        """获取所有正在运行的微信小程序运行引擎进程 (WeChatAppEx.exe)"""
        pids = []
        h_snap = kernel32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
        if h_snap == -1:
            return pids

        pe = PROCESSENTRY32()
        pe.dwSize = ctypes.sizeof(PROCESSENTRY32)
        if kernel32.Process32First(h_snap, ctypes.byref(pe)):
            while True:
                exe = pe.szExeFile.decode("gbk", errors="ignore").lower()
                if "wechatappex" in exe or "wmpf" in exe:
                    pids.append((pe.th32ProcessID, exe))
                if not kernel32.Process32Next(h_snap, ctypes.byref(pe)):
                    break
        kernel32.CloseHandle(h_snap)
        return pids


    def scan_wechat_credentials() -> Optional[Dict[str, Any]]:
        """
        极速扫描微信进程内存，捕获小蚕真实鉴权凭证
        返回: dict(token=..., user_id=..., silk_id=..., nickname=..., expires_at=...) 或 None
        """
        pids = get_wechat_applet_pids()
        if not pids:
            logger.info("未检测到运行中的微信小程序进程 (WeChatAppEx.exe)")
            return None

        best_token = None
        best_user_id = None
        best_silk_id = None
        best_nickname = None
        best_exp = None

        mbi_size = ctypes.sizeof(MEMORY_BASIC_INFORMATION)

        for pid, exe in pids:
            h_proc = kernel32.OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, pid)
            if not h_proc:
                continue

            addr = 0
            mbi = MEMORY_BASIC_INFORMATION()

            try:
                while kernel32.VirtualQueryEx(h_proc, ctypes.c_void_p(addr), ctypes.byref(mbi), mbi_size) == mbi_size:
                    # 只扫描已提交且可读写的常规内存块
                    if mbi.State == MEM_COMMIT and mbi.Protect in (0x02, 0x04, 0x20, 0x40):
                        size = mbi.RegionSize
                        if 4096 <= size <= 16 * 1024 * 1024:
                            buf = ctypes.create_string_buffer(size)
                            bytes_read = ctypes.c_size_t()
                            if kernel32.ReadProcessMemory(h_proc, ctypes.c_void_p(addr), buf, size, ctypes.byref(bytes_read)):
                                chunk = buf.raw[: bytes_read.value]

                                if b"eyJVc2VySWQi" in chunk or b"eyJhbGciOi" in chunk:
                                    matches = JWT_PATTERN.findall(chunk)
                                    for m in matches:
                                        try:
                                            t = m.decode("latin1")
                                            parts = t.split(".")
                                            if len(parts) == 3:
                                                padded = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
                                                payload = json.loads(
                                                    base64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")
                                                )
                                                if "UserId" in payload:
                                                    best_token = t
                                                    best_user_id = str(payload["UserId"])
                                                    exp_ts = payload.get("exp")
                                                    if exp_ts:
                                                        best_exp = time.strftime(
                                                            "%Y-%m-%d %H:%M:%S", time.localtime(exp_ts)
                                                        )
                                        except Exception:
                                            pass

                                if not best_silk_id and b"silk_id" in chunk:
                                    m_silk = SILK_PATTERN.search(chunk)
                                    if m_silk:
                                        best_silk_id = m_silk.group(1).decode("latin1")

                                if not best_nickname and b"nickname" in chunk:
                                    m_nick = NICK_PATTERN.search(chunk)
                                    if m_nick:
                                        try:
                                            nick_str = m_nick.group(1).decode("utf-8", errors="ignore").strip()
                                            if nick_str and not nick_str.isdigit() and nick_str.lower() not in ("xcmap", "null", "undefined", "none", "default"):
                                                best_nickname = nick_str
                                        except Exception:
                                            pass

                    addr += mbi.RegionSize
                    if addr >= 0x7FFFFFFFFFFF:
                        break
            finally:
                kernel32.CloseHandle(h_proc)

            if best_token and best_silk_id:
                break

        if best_token:
            return {
                "token": best_token,
                "user_id": best_user_id or "",
                "silk_id": best_silk_id or "",
                "nickname": best_nickname or "小蚕微信用户",
                "expires_at": best_exp or "长期有效",
                "city_code": 420100,
                "city_name": "武汉",
            }

        return None


    class WeChatAutoListener:
        """后台自动感知监听器 (仅 Windows)"""

        def __init__(self):
            self._running = False
            self._thread: Optional[threading.Thread] = None
            self._last_captured_account: Optional[Dict[str, Any]] = None
            self._captured_event = threading.Event()
            self._lock = threading.Lock()
            self._start_time = 0
            self._timeout_seconds = 120

        @property
        def is_running(self) -> bool:
            return self._running

        def get_status(self) -> Dict[str, Any]:
            with self._lock:
                elapsed = int(time.time() - self._start_time) if self._running else 0
                remain = max(0, self._timeout_seconds - elapsed) if self._running else 0
                return {
                    "is_running": self._running,
                    "status": "captured" if self._last_captured_account else ("listening" if self._running else "idle"),
                    "account": self._last_captured_account,
                    "elapsed": elapsed,
                    "remaining": remain,
                }

        def start(self, timeout: int = 120, on_captured_callback=None):
            with self._lock:
                if self._running:
                    return
                self._running = True
                self._timeout_seconds = timeout
                self._start_time = time.time()
                self._last_captured_account = None
                self._captured_event.clear()

            def _worker():
                logger.info("微信小程序无感感知监听已启动...")
                while self._running:
                    if time.time() - self._start_time > self._timeout_seconds:
                        logger.info("微信小程序监听已超时退出")
                        with self._lock:
                            self._running = False
                        break

                    try:
                        res = scan_wechat_credentials()
                        if res and res.get("token"):
                            logger.info(f"内存扫描成功捕获小蚕凭据: UserID={res.get('user_id')}, SilkID={res.get('silk_id')}")
                            if on_captured_callback:
                                try:
                                    saved_acc = on_captured_callback(res)
                                    if saved_acc:
                                        res.update(saved_acc)
                                except Exception as e:
                                    logger.error(f"回调保存账号失败: {e}")

                            with self._lock:
                                self._last_captured_account = res
                                self._running = False
                                self._captured_event.set()
                            break
                    except Exception as e:
                        logger.error(f"监听扫描发生异常: {e}")

                    time.sleep(1.5)

            self._thread = threading.Thread(target=_worker, daemon=True)
            self._thread.start()

        def stop(self):
            with self._lock:
                self._running = False
                self._captured_event.set()
            logger.info("微信小程序感知监听已主动停止")


    wechat_listener = WeChatAutoListener()

else:
    # ---------- Linux/macOS 空实现 ----------
    def get_wechat_applet_pids() -> List[Tuple[int, str]]:
        return []

    def scan_wechat_credentials() -> Optional[Dict[str, Any]]:
        return None

    class WeChatAutoListener:
        def __init__(self):
            self._running = False

        @property
        def is_running(self) -> bool:
            return False

        def get_status(self) -> Dict[str, Any]:
            return {"is_running": False, "status": "unsupported", "account": None, "elapsed": 0, "remaining": 0}

        def start(self, timeout: int = 120, on_captured_callback=None):
            logger.info("微信小程序内存扫描仅支持 Windows，当前为 Linux/macOS 平台，已跳过")

        def stop(self):
            pass

    wechat_listener = WeChatAutoListener()
