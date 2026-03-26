#!/bin/bash
# 流量监控系统 - 统一启动脚本

# 项目根目录
PROJECT_DIR="/root/traffic-monitor"
SERVER_DIR="$PROJECT_DIR/server"
MONITOR_DIR="$PROJECT_DIR/monitor"
LOG_DIR="$PROJECT_DIR/logs"
DATA_DIR="$PROJECT_DIR/data"
PID_DIR="$PROJECT_DIR/logs"

# 可执行文件
HTTP_SERVER="$SERVER_DIR/http_server.py"
MONITOR_SCRIPT="$MONITOR_DIR/traffic_monitor.sh"
INIT_DB="$SERVER_DIR/init_db.py"

# 默认参数
NAMESPACES="default"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# 显示帮助信息
show_help() {
    echo "流量监控系统启动脚本"
    echo
    echo "用法: $0 [选项] {start|stop|status|restart}"
    echo
    echo "选项:"
    echo "  --namespaces <ns1,ns2,...>  指定要监控的命名空间（逗号分隔）"
    echo "                             默认: default（当前默认命名空间）"
    echo "                             示例: default,serverSpace,vpnSpace"
    echo
    echo "子命令:"
    echo "  start    启动所有服务"
    echo "  stop     停止所有服务"
    echo "  status   查看服务状态"
    echo "  restart  重启所有服务"
    echo "  -h       显示帮助信息"
    echo
    echo "示例:"
    echo "  $0 start                                    # 仅监控默认命名空间"
    echo "  $0 --namespaces default,serverSpace start   # 监控多个命名空间"
    echo "  $0 stop                                     # 停止服务"
    echo "  $0 status                                   # 查看状态"
}

# 解析命令行参数
parse_args() {
    COMMAND=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --namespaces)
                NAMESPACES="$2"
                shift 2
                ;;
            --namespaces=*)
                NAMESPACES="${1#*=}"
                shift
                ;;
            start|stop|status|restart)
                COMMAND="$1"
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                echo "未知参数: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# 创建必要的目录
create_dirs() {
    mkdir -p "$LOG_DIR"
    mkdir -p "$DATA_DIR"
    mkdir -p "$PID_DIR"
}

# 初始化数据库
init_database() {
    echo -e "${BLUE}初始化数据库...${NC}"
    python3 "$INIT_DB" > /dev/null 2>&1
    echo -e "${GREEN}✓ 数据库初始化完成${NC}"
}

# 启动服务
start_services() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}启动流量监控系统${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo -e "${CYAN}监控命名空间: ${NAMESPACES}${NC}"
    echo

    # 检查监控服务是否已在运行
    if [ -f "$PID_DIR/monitor.pid" ]; then
        local pid
        pid=$(cat "$PID_DIR/monitor.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${YELLOW}监控服务已在运行 (PID: $pid)，请先执行 stop${NC}"
            return 1
        fi
    fi

    # 检查 HTTP 服务是否已在运行
    if [ -f "$PID_DIR/http_server.pid" ]; then
        local pid
        pid=$(cat "$PID_DIR/http_server.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${YELLOW}HTTP 服务器已在运行 (PID: $pid)，请先执行 stop${NC}"
            return 1
        fi
    fi

    create_dirs
    init_database

    # 启动流量监控服务（附带命名空间参数）
    echo -e "${BLUE}启动流量监控服务...${NC}"
    chmod +x "$MONITOR_SCRIPT"
    nohup "$MONITOR_SCRIPT" --namespaces "$NAMESPACES" \
        > "$LOG_DIR/monitor.log" 2>&1 &
    local monitor_pid=$!
    echo "$monitor_pid" > "$PID_DIR/monitor.pid"
    echo -e "${GREEN}✓ 流量监控服务已启动 (PID: $monitor_pid)${NC}"

    # 等待监控服务初始化
    sleep 1

    # 启动 HTTP 服务器
    echo -e "${BLUE}启动 HTTP 服务器...${NC}"
    nohup python3 "$HTTP_SERVER" \
        > "$LOG_DIR/http_server.log" 2>&1 &
    local http_pid=$!
    echo "$http_pid" > "$PID_DIR/http_server.pid"
    echo -e "${GREEN}✓ HTTP 服务器已启动 (PID: $http_pid)${NC}"

    echo
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}所有服务已成功启动！${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo
    echo -e "${CYAN}后端 API 地址: http://<服务器IP>:8080${NC}"
    echo -e "${CYAN}API 端点:${NC}"
    echo -e "${CYAN}  GET /api/namespaces              - 命名空间列表${NC}"
    echo -e "${CYAN}  GET /api/current?namespace=<ns>  - 当前数据${NC}"
    echo -e "${CYAN}  GET /api/history?namespace=<ns>  - 历史数据${NC}"
    echo -e "${CYAN}  GET /api/stream?namespace=<ns>   - SSE 实时流${NC}"
    echo
    echo -e "${CYAN}前端页面: 用浏览器打开 web/index.html，输入服务器地址连接${NC}"
    echo
    echo -e "${CYAN}日志文件:${NC}"
    echo -e "${CYAN}  - $LOG_DIR/monitor.log${NC}"
    echo -e "${CYAN}  - $LOG_DIR/http_server.log${NC}"
    echo
    echo -e "${YELLOW}使用 '$0 stop' 停止服务${NC}"
    echo -e "${YELLOW}使用 '$0 status' 查看状态${NC}"
}

# 停止服务
stop_services() {
    echo -e "${BLUE}停止流量监控服务...${NC}"

    # 停止监控服务
    if [ -f "$PID_DIR/monitor.pid" ]; then
        local pid
        pid=$(cat "$PID_DIR/monitor.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            # 发送 SIGTERM，等待子进程优雅退出
            kill "$pid" 2>/dev/null
            sleep 1
            # 若仍存活则强制终止
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null
            fi
            echo -e "${GREEN}✓ 流量监控服务已停止 (PID: $pid)${NC}"
        else
            echo -e "${YELLOW}流量监控服务进程不存在 (PID: $pid)${NC}"
        fi
        rm -f "$PID_DIR/monitor.pid"
    else
        echo -e "${YELLOW}流量监控服务未运行${NC}"
    fi

    # 停止 HTTP 服务
    if [ -f "$PID_DIR/http_server.pid" ]; then
        local pid
        pid=$(cat "$PID_DIR/http_server.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            kill "$pid" 2>/dev/null
            sleep 1
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null
            fi
            echo -e "${GREEN}✓ HTTP 服务器已停止 (PID: $pid)${NC}"
        else
            echo -e "${YELLOW}HTTP 服务器进程不存在 (PID: $pid)${NC}"
        fi
        rm -f "$PID_DIR/http_server.pid"
    else
        echo -e "${YELLOW}HTTP 服务器未运行${NC}"
    fi

    echo -e "${GREEN}所有服务已停止${NC}"
}

# 查看状态
check_status() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}流量监控系统状态${NC}"
    echo -e "${BLUE}========================================${NC}"

    # 检查监控服务
    echo -e "${CYAN}流量监控服务:${NC}"
    if [ -f "$PID_DIR/monitor.pid" ]; then
        local pid
        pid=$(cat "$PID_DIR/monitor.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "  状态: ${GREEN}运行中${NC}"
            echo -e "  PID:  $pid"
            echo -e "  CPU:  $(ps -p "$pid" -o %cpu --no-headers 2>/dev/null || echo 'N/A')%"
            echo -e "  内存: $(ps -p "$pid" -o %mem --no-headers 2>/dev/null || echo 'N/A')%"
        else
            echo -e "  状态: ${RED}已停止（PID 文件残留）${NC}"
        fi
    else
        echo -e "  状态: ${RED}未运行${NC}"
    fi

    echo

    # 检查 HTTP 服务
    echo -e "${CYAN}HTTP 服务器:${NC}"
    if [ -f "$PID_DIR/http_server.pid" ]; then
        local pid
        pid=$(cat "$PID_DIR/http_server.pid")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "  状态: ${GREEN}运行中${NC}"
            echo -e "  PID:  $pid"
            echo -e "  端口: 8080"
            echo -e "  CPU:  $(ps -p "$pid" -o %cpu --no-headers 2>/dev/null || echo 'N/A')%"
            echo -e "  内存: $(ps -p "$pid" -o %mem --no-headers 2>/dev/null || echo 'N/A')%"
        else
            echo -e "  状态: ${RED}已停止（PID 文件残留）${NC}"
        fi
    else
        echo -e "  状态: ${RED}未运行${NC}"
    fi

    echo

    # 检查数据库
    echo -e "${CYAN}数据库:${NC}"
    if [ -f "$DATA_DIR/traffic_monitor.db" ]; then
        echo -e "  状态: ${GREEN}存在${NC}"
        echo -e "  大小: $(ls -lh "$DATA_DIR/traffic_monitor.db" | awk '{print $5}')"
        # 显示各命名空间记录数
        local ns_counts
        ns_counts=$(python3 - "$DATA_DIR/traffic_monitor.db" <<'PY' 2>/dev/null
import sqlite3, sys
db = sys.argv[1]
conn = sqlite3.connect(db)
rows = conn.execute(
    "SELECT namespace, COUNT(*) FROM traffic_history GROUP BY namespace ORDER BY namespace"
).fetchall()
conn.close()
for ns, cnt in rows:
    print("    %-20s %d 条" % (ns, cnt))
PY
)
        if [ -n "$ns_counts" ]; then
            echo -e "  命名空间记录数:"
            echo "$ns_counts"
        fi
    else
        echo -e "  状态: ${YELLOW}不存在${NC}"
    fi

    echo -e "${BLUE}========================================${NC}"
}

# 主程序
parse_args "$@"

case "$COMMAND" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    status)
        check_status
        ;;
    restart)
        stop_services
        sleep 2
        start_services
        ;;
    "")
        echo "错误: 未指定子命令"
        echo
        show_help
        exit 1
        ;;
    *)
        echo "错误: 未知子命令 '$COMMAND'"
        echo
        show_help
        exit 1
        ;;
esac
