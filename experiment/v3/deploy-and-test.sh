#!/bin/bash
# deploy-and-test.sh -- 将 Agent-Fabric v3 同步到 devbox-boe 并运行验收测试
# 用法: ./experiment/v3/deploy-and-test.sh
set -euo pipefail

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
DEVBOX_HOST="devbox-boe"
DEVBOX_USER="wangchaoyu.v2"
DEVBOX_ADDR="10.251.254.247"
REMOTE_DIR="/data00/agentfabric/af-v3"

# SSH 选项: Kerberos GSSAPI 认证, 禁用密码提示
SSH_OPTS="-o GSSAPIAuthentication=yes -o PasswordAuthentication=no -o StrictHostKeyChecking=accept-new"

# 本地项目根目录 (脚本所在位置向上两级)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# 日志文件
LOG_FILE="${SCRIPT_DIR}/devbox-test.log"

# 结果目录
LOCAL_RESULTS="${SCRIPT_DIR}/results"
REMOTE_RESULTS="${REMOTE_DIR}/experiment/v3/results"

# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------
BLUE='\033[1;34m'
GREEN='\033[1;32m'
RED='\033[1;31m'
NC='\033[0m'

section() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
}

ok() {
    echo -e "${GREEN}[OK]${NC} $1"
}

fail() {
    echo -e "${RED}[FAIL]${NC} $1" >&2
}

ssh_cmd() {
    # shellcheck disable=SC2086
    ssh ${SSH_OPTS} "${DEVBOX_USER}@${DEVBOX_ADDR}" "export PATH=/usr/local/go/bin:\$PATH; $@"
}

cleanup() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        fail "脚本异常退出 (exit code: ${exit_code}), 完整日志见: ${LOG_FILE}"
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 0. 准备
# ---------------------------------------------------------------------------
section "0. 准备工作"

echo "项目根目录: ${PROJECT_ROOT}"
echo "远端目标:   ${DEVBOX_USER}@${DEVBOX_ADDR}:${REMOTE_DIR}"
echo "日志文件:   ${LOG_FILE}"

# 确保日志文件所在目录存在
mkdir -p "$(dirname "${LOG_FILE}")"

# 将后续所有 stdout/stderr 同时输出到终端和日志
exec > >(tee -a "${LOG_FILE}") 2>&1

echo "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "---"

# ---------------------------------------------------------------------------
# 1. 同步代码到 devbox-boe
# ---------------------------------------------------------------------------
section "1. 同步代码到 devbox-boe (rsync)"

# 先确保远端目录存在
ssh_cmd "mkdir -p ${REMOTE_DIR}"

rsync -azh --delete \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.pytest_cache' \
    --exclude='experiment/v3/results' \
    --exclude='experiment/v4/results' \
    --exclude='experiment/results' \
    --exclude='*.egg-info' \
    --exclude='.trae' \
    --exclude='.vscode' \
    --exclude='devbox-test.log' \
    -e "ssh ${SSH_OPTS}" \
    "${PROJECT_ROOT}/" \
    "${DEVBOX_USER}@${DEVBOX_ADDR}:${REMOTE_DIR}/"

ok "代码同步完成"

# ---------------------------------------------------------------------------
# 2a. 检查 Go 版本
# ---------------------------------------------------------------------------
section "2a. 检查 Go 版本 (需要 >= 1.22)"

GO_VERSION=$(ssh_cmd "cd ${REMOTE_DIR} && go version" 2>&1) || {
    fail "无法获取 Go 版本, 请确认 devbox 上已安装 Go"
    exit 10
}
echo "远端 Go 版本: ${GO_VERSION}"

# 提取主版本号和次版本号
GO_MINOR=$(echo "${GO_VERSION}" | grep -oE 'go[0-9]+\.[0-9]+' | head -1 | sed 's/go//' | cut -d. -f2)
GO_MAJOR=$(echo "${GO_VERSION}" | grep -oE 'go[0-9]+\.[0-9]+' | head -1 | sed 's/go//' | cut -d. -f1)

if [ -z "${GO_MINOR}" ] || [ -z "${GO_MAJOR}" ]; then
    fail "无法解析 Go 版本号"
    exit 11
fi

if [ "${GO_MAJOR}" -lt 1 ] || { [ "${GO_MAJOR}" -eq 1 ] && [ "${GO_MINOR}" -lt 22 ]; }; then
    fail "Go 版本过低: 需要 >= 1.22, 当前为 ${GO_MAJOR}.${GO_MINOR}"
    exit 12
fi

ok "Go 版本满足要求"

# ---------------------------------------------------------------------------
# 2b. 编译项目
# ---------------------------------------------------------------------------
section "2b. 编译项目 (go build)"

ssh_cmd "cd ${REMOTE_DIR} && go build ./..." || {
    fail "go build 失败"
    exit 20
}

ok "项目编译成功"

# ---------------------------------------------------------------------------
# 2c. 运行单元测试
# ---------------------------------------------------------------------------
section "2c. 运行单元测试 (wal / runtime / observe / protect)"

ssh_cmd "cd ${REMOTE_DIR} && go test -v -count=1 ./wal/... ./runtime/... ./observe/... ./protect/..." || {
    fail "单元测试失败"
    exit 30
}

ok "单元测试全部通过"

# ---------------------------------------------------------------------------
# 2d. 运行 v3 验收测试
# ---------------------------------------------------------------------------
section "2d. 运行 v3 验收测试 (experiment/v3)"

ssh_cmd "cd ${REMOTE_DIR} && go test -v -count=1 -timeout 10m ./experiment/v3/" || {
    fail "验收测试失败"
    exit 40
}

ok "验收测试全部通过"

# ---------------------------------------------------------------------------
# 2e. 收集远端结果
# ---------------------------------------------------------------------------
section "2e. 列出远端测试结果"

ssh_cmd "ls -lh ${REMOTE_RESULTS}/ 2>/dev/null" || {
    echo "(远端结果目录为空或不存在)"
}

# ---------------------------------------------------------------------------
# 3. 下载结果到本地
# ---------------------------------------------------------------------------
section "3. 下载结果到本地"

mkdir -p "${LOCAL_RESULTS}"

rsync -azh \
    -e "ssh ${SSH_OPTS}" \
    "${DEVBOX_USER}@${DEVBOX_ADDR}:${REMOTE_RESULTS}/" \
    "${LOCAL_RESULTS}/" || {
    fail "下载结果失败 (可能远端无结果文件)"
    exit 50
}

ok "结果已下载到: ${LOCAL_RESULTS}/"
echo ""
echo "本地结果文件:"
ls -lh "${LOCAL_RESULTS}/"

# ---------------------------------------------------------------------------
# 完成
# ---------------------------------------------------------------------------
section "全部完成"

echo "结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "完整日志: ${LOG_FILE}"
echo "测试结果: ${LOCAL_RESULTS}/"
echo ""
ok "Agent-Fabric v3 验收测试流程执行成功"
