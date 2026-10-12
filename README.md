# 小蚕小帮手 (XiaoCan Assistant)

> 基于 FastAPI + 逆向 RPC 协议的自动化挂机 / 秒杀 / 抢单小帮手

## 功能特性

- 🚀 店铺抢单、预约、库存监控
- 📦 双前端：完整管理后台 (`/`) + 精简用户前端 (`/slim/`)
- 🔐 小蚕 `x-Sivir` token 登录 + JWT 用户体系
- 🔄 **RPC 内存缓存层**（多实例共享 + 空结果过滤）
- 🛡️ 腾讯云 WAF 熔断保护（15s 冷却）
- 📋 账户、卡券、红包、订单全链路管理

## 系统要求

- Ubuntu 20.04+ / Debian 11+ / CentOS 7+
- Python 3.10+
- 服务器有公网 IP（可访问 `gw.xiaocantech.com`）

## 快速安装

### 方式一：一键脚本（推荐）

```bash
# 1. 下载最新 Release 压缩包
wget https://github.com/<YOUR_USERNAME>/xiaocan-assistant/releases/latest/download/xiaocan-assistant-full.tar.gz

# 2. 解压
cd /opt
rm -rf xiaocan
mkdir xiaocan && cd xiaocan
tar -xzf /path/to/xiaocan-assistant-full.tar.gz

# 3. 一键安装（自动创建 venv、装依赖、配 systemd、启动）
bash install.sh
```

安装脚本会自动完成：

| 步骤 | 内容 |
|------|------|
| 1 | 检查/安装 Python 3.11 |
| 2 | 创建 `/opt/xiaocan` 目录，复制 backend / frontend/dist / slim-frontend-dist |
| 3 | 创建 venv + `pip install -r requirements.txt` |
| 4 | 注册 systemd 服务 `xiaocan.service`（开机自启） |
| 5 | `systemctl restart xiaocan` 启动服务 |

### 方式二：手动部署

```bash
# 创建目录
sudo mkdir -p /opt/xiaocan/backend /opt/xiaocan/frontend /opt/xiaocan/slim-frontend
cd /opt/xiaocan

# 上传 backend/ 源码、frontend/dist/、slim-frontend-dist/、install.sh

# Python 环境
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# systemd 服务
sudo tee /etc/systemd/system/xiaocan.service <<EOF
[Unit]
Description=XiaoCan Assistant
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/xiaocan/backend
ExecStart=/opt/xiaocan/backend/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8690
Restart=on-failure
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable xiaocan
sudo systemctl restart xiaocan
```

## 访问地址

| 服务 | 地址 |
|------|------|
| 完整管理后台 | `http://<服务器IP>:8690/` |
| 精简用户前端 | `http://<服务器IP>:8690/slim/` |
| API 根路径 | `http://<服务器IP>:8690/api/` |

默认账号：`admin / admin123`

## 常用命令

```bash
systemctl status xiaocan     # 查看状态
systemctl restart xiaocan    # 重启
systemctl stop xiaocan       # 停止
journalctl -u xiaocan -f     # 实时日志
journalctl -u xiaocan -n 50  # 最近 50 行日志

# 缓存管理（安全，不调小蚕 API）
curl -s http://127.0.0.1:8690/api/cache/stats   # 查询缓存状态
curl -s -X POST http://127.0.0.1:8690/api/cache/clear  # 清空缓存
```

## RPC 内存缓存层

后端内置全局内存缓存装饰器 `@cache_rpc(ttl=300)`，覆盖 8 个高频 RPC 方法：

| 方法 | TTL | 说明 |
|------|-----|------|
| `get_store_list` | 120s | 店铺列表 |
| `get_user_info` | 300s | 用户详情 |
| `get_user_task_v2` | 300s | 元宝任务 |
| `get_user_max_redpack` | 180s | 最大红包 |
| `get_user_card_number` | 300s | 卡券数量 |
| `get_user_card_list` | 300s | 卡券列表 |
| `get_user_redpack_num` | 300s | 红包数量 |
| `get_app_redpack_list` | 180s | 红包列表 |

### 缓存特性

- ✅ **多实例共享**：跳过 `self` 参数生成 key，`endpoints.py` / `scheduler.py` / `appointment_worker.py` 各自的独立 `XiaoCanClient` 实例共享同一份缓存
- ✅ **空结果过滤**：WAF 熔断兜底返回的 `{"list": []}` 等空结构被 `_is_cacheable()` 拦截不缓存
- ✅ **过期自动重调**：TTL 到期后自动失效，下次调用实时请求
- ✅ **熔断期不重复调用**：`XiaoCanRPCError` 异常不进入缓存，熔断期内调用 raise 直接返回

### 日志关键字

```
[Cache SET]      首次调用，写入缓存
[Cache HIT]      命中缓存，秒回
[Cache SKIP]     空结果被过滤，不缓存
[Cache EXPIRED]  TTL 过期，重新请求
[Cache CLEAR]    /api/cache/clear 手动清空
```

## 目录结构

```
xiaocan-assistant/
├── install.sh                    # 一键安装脚本
├── backend/
│   ├── main.py                   # FastAPI 入口
│   ├── requirements.txt          # Python 依赖
│   ├── data/.gitkeep             # SQLite 数据库占位目录
│   └── app/
│       ├── api/                  # 路由层
│       ├── core/                 # 调度、预约、NTP、证书等
│       ├── models/               # SQLite 数据模型
│       ├── certs/                # HTTPS 自签证书
│       └── protocol/             # RPC 协议层
│           ├── cache.py          # ⭐ 内存缓存层
│           ├── client.py         # XiaoCanClient
│           └── signer.py         # X-Ashe 签名
├── frontend/dist/                # 完整管理后台（构建产物）
└── slim-frontend-dist/           # 精简用户前端（构建产物）
```

## 注意事项

1. **Python venv 不要打包**：Windows 上创建的 venv 在 Linux 上不可用，让 `install.sh` 自动创建
2. **不要开启多 worker**：`systemd` 里 ExecStart 只有 `uvicorn main:app --host 0.0.0.0 --port 8690`，单进程即可（缓存基于进程内存，多 worker 不共享）
3. **腾讯云 WAF**：如果部署在腾讯云机房，确保后端 `XiaoCanClient` 使用 `curl_cffi` 绕过 TLS 指纹检测（默认已启用）
4. **调用频率限制**：全局最多 2 并发 / 每次 RPC 间隔 ≥ 800ms，避免触发频控被封

## License

Free for personal use.
