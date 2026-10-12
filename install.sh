#!/bin/bash
set -e

# ============================================================
# 小蚕小帮手 一键安装脚本
# 用法: bash install.sh
# 支持: Ubuntu 20.04+, Debian 11+, CentOS 7+ / Rocky 8+
# 默认端口: 8690
# ============================================================

INSTALL_DIR="${INSTALL_DIR:-/opt/xiaocan}"
SERVICE_NAME="xiaocan"
PYTHON_BIN=""

echo "=========================================="
echo "  小蚕小帮手 一键安装"
echo "=========================================="

# ---------- 1. 检查并安装 Python 3.10+ ----------
echo ""
echo "[1/5] 检查 Python 环境..."

if command -v python3 &>/dev/null; then
    PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    PY_MAJOR=$(echo $PY_VER | cut -d. -f1)
    PY_MINOR=$(echo $PY_VER | cut -d. -f2)
    if [ "$PY_MAJOR" -ge 3 ] && [ "$PY_MINOR" -ge 10 ]; then
        PYTHON_BIN="python3"
        echo "  ✓ python3 $PY_VER 已安装"
    fi
fi

if [ -z "$PYTHON_BIN" ]; then
    echo "  正在安装 Python 3.11..."
    if command -v apt-get &>/dev/null; then
        apt-get update -qq
        apt-get install -y -qq python3.11 python3.11-venv python3.11-dev 2>/dev/null || \
        { apt-get install -y -qq software-properties-common; add-apt-repository -y ppa:deadsnakes/ppa; apt-get update -qq; apt-get install -y -qq python3.11 python3.11-venv python3.11-dev; }
        PYTHON_BIN="python3.11"
    elif command -v yum &>/dev/null; then
        yum install -y python3.11 python3.11-devel 2>/dev/null || \
        { yum install -y python3 python3-devel; }
        PYTHON_BIN="python3.11"
    else
        echo "  ✗ 不支持的发行版，请手动安装 Python 3.10+"
        exit 1
    fi
fi

# ---------- 2. 创建安装目录 ----------
echo ""
echo "[2/5] 准备安装目录: $INSTALL_DIR"
mkdir -p "$INSTALL_DIR"

# 脚本所在目录 = 解压后的包根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 复制后端
if [ -d "$SCRIPT_DIR/backend" ]; then
    rm -rf "$INSTALL_DIR/backend"
    cp -r "$SCRIPT_DIR/backend" "$INSTALL_DIR/backend"
    # 清理
    rm -rf "$INSTALL_DIR/backend/venv" "$INSTALL_DIR/backend/__pycache__"
    echo "  ✓ backend 已复制"
else
    echo "  ✗ 找不到 backend/ 目录"
    exit 1
fi

# 复制前端 build 产物
if [ -d "$SCRIPT_DIR/frontend/dist" ]; then
    rm -rf "$INSTALL_DIR/frontend/dist"
    mkdir -p "$INSTALL_DIR/frontend"
    cp -r "$SCRIPT_DIR/frontend/dist" "$INSTALL_DIR/frontend/dist"
    echo "  ✓ frontend/dist 已复制"
else
    echo "  ✗ 找不到 frontend/dist — 请先在本机执行 npm run build"
    exit 1
fi

if [ -d "$SCRIPT_DIR/slim-frontend-dist" ]; then
    rm -rf "$INSTALL_DIR/slim-frontend-dist"
    cp -r "$SCRIPT_DIR/slim-frontend-dist" "$INSTALL_DIR/slim-frontend-dist"
    echo "  ✓ slim-frontend-dist 已复制"
else
    echo "  ✗ 找不到 slim-frontend-dist — 请先在本机执行 npm run build"
    exit 1
fi

# ---------- 3. 创建 venv 并安装依赖 ----------
echo ""
echo "[3/5] 创建 Python 虚拟环境..."
cd "$INSTALL_DIR/backend"
$PYTHON_BIN -m venv venv
source venv/bin/activate
pip install --upgrade pip -q
echo "  正在安装依赖（首次可能较慢）..."
pip install -r requirements.txt -q
echo "  ✓ Python 依赖安装完成"

# ---------- 4. 创建 systemd service ----------
echo ""
echo "[4/5] 配置 systemd 服务..."

cat > /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=XiaoCan Assistant (小蚕小帮手)
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=${INSTALL_DIR}/backend
ExecStart=${INSTALL_DIR}/backend/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8690
Restart=on-failure
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable ${SERVICE_NAME}
echo "  ✓ systemd 服务已配置 (开机自启)"

# ---------- 5. 启动服务 ----------
echo ""
echo "[5/5] 启动服务..."
systemctl restart ${SERVICE_NAME}
sleep 2

if systemctl is-active --quiet ${SERVICE_NAME}; then
    echo ""
    echo "=========================================="
    echo "  ✓ 安装完成！"
    echo "=========================================="
    echo ""
    echo "  原管理后台:  http://$(hostname -I | awk '{print $1}'):8690/"
    echo "  精简前端:    http://$(hostname -I | awk '{print $1}'):8690/slim/"
    echo "  默认账号:    admin / admin123"
    echo ""
    echo "  常用命令:"
    echo "    systemctl status ${SERVICE_NAME}    # 查看状态"
    echo "    systemctl restart ${SERVICE_NAME}   # 重启"
    echo "    journalctl -u ${SERVICE_NAME} -f    # 查看日志"
    echo ""
else
    echo "  ✗ 服务启动失败，查看日志:"
    journalctl -u ${SERVICE_NAME} -n 20 --no-pager
    exit 1
fi
