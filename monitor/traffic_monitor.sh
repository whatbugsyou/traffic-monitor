#!/bin/bash
# 流量监控 - 支持多命名空间并发采集
REFRESH_INTERVAL=1
DATA_FILE="/root/traffic-monitor/data/traffic_data.json"
DB_PATH="/root/traffic-monitor/data/traffic_monitor.db"
NAMESPACES="default"

# 解析命令行参数
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
        *)
            shift
            ;;
    esac
done

# 在指定命名空间中执行命令，default 命名空间直接执行
ns_exec() {
    local ns="$1"
    shift
    if [ "$ns" = "default" ]; then
        "$@"
    else
        ip netns exec "$ns" "$@"
    fi
}

# 获取指定命名空间的所有接口原始字节
get_interfaces() {
    local ns="$1"
    local results=""

    # 获取该命名空间下的网络接口列表
    local devlist
    if [ "$ns" = "default" ]; then
        devlist=$(ls /sys/class/net/)
    else
        devlist=$(ip netns exec "$ns" ip -o link show 2>/dev/null \
            | awk -F': ' '{print $2}' | awk '{print $1}')
    fi

    for dev in $devlist; do
        case "$dev" in lo|*.*|*@*) continue ;; esac

        local rx tx
        if [ "$ns" = "default" ]; then
            rx=$(cat "/sys/class/net/$dev/statistics/rx_bytes" 2>/dev/null || echo 0)
            tx=$(cat "/sys/class/net/$dev/statistics/tx_bytes" 2>/dev/null || echo 0)
        else
            rx=$(ip netns exec "$ns" cat "/sys/class/net/$dev/statistics/rx_bytes" 2>/dev/null || echo 0)
            tx=$(ip netns exec "$ns" cat "/sys/class/net/$dev/statistics/tx_bytes" 2>/dev/null || echo 0)
        fi

        local entry="{\"name\":\"$dev\",\"rx_bytes\":$rx,\"tx_bytes\":$tx}"
        if [ -z "$results" ]; then
            results="$entry"
        else
            results="$results,$entry"
        fi
    done

    echo "$results"
}

# 获取指定命名空间的 ppp0 原始丢包数据
get_ppp0() {
    local ns="$1"

    # 检查 ppp0 是否存在于该命名空间
    if [ "$ns" = "default" ]; then
        [ -d "/sys/class/net/ppp0" ] || { echo '{"available":false}'; return; }
    else
        ip netns exec "$ns" test -d "/sys/class/net/ppp0" 2>/dev/null \
            || { echo '{"available":false}'; return; }
    fi

    local info
    info=$(ns_exec "$ns" ifconfig ppp0 2>/dev/null)

    local rx_pkt rx_err rx_drop rx_over rx_frame
    local tx_pkt tx_err tx_drop tx_over tx_carrier

    rx_pkt=$(echo "$info"  | awk '/RX packets/{print $3}')
    rx_err=$(echo "$info"  | awk '/RX errors/{print $3}')
    rx_drop=$(echo "$info" | awk '/RX errors/{print $5}')
    rx_over=$(echo "$info" | awk '/RX errors/{print $7}')
    rx_frame=$(echo "$info"| awk '/RX errors/{print $9}')
    tx_pkt=$(echo "$info"  | awk '/TX packets/{print $3}')
    tx_err=$(echo "$info"  | awk '/TX errors/{print $3}')
    tx_drop=$(echo "$info" | awk '/TX errors/{print $5}')
    tx_over=$(echo "$info" | awk '/TX errors/{print $7}')
    tx_carrier=$(echo "$info" | awk '/TX errors/{print $9}')

    cat <<PPPEOF
{"available":true,"rx_packets":${rx_pkt:-0},"rx_errors":${rx_err:-0},"rx_dropped":${rx_drop:-0},"rx_overruns":${rx_over:-0},"rx_frame":${rx_frame:-0},"tx_packets":${tx_pkt:-0},"tx_errors":${tx_err:-0},"tx_dropped":${tx_drop:-0},"tx_overruns":${tx_over:-0},"tx_carrier":${tx_carrier:-0}}
PPPEOF
}

# 写入 SQLite 历史（带 namespace 字段）
add_history() {
    local data="$1"
    python3 - "$data" "$DB_PATH" <<'PY' 2>/dev/null
import sqlite3, json, sys
data, db_path = sys.argv[1], sys.argv[2]
try:
    parsed = json.loads(data)
    namespace   = parsed['namespace']
    timestamp   = parsed['timestamp']
    timestamp_ms = parsed['timestamp_ms']
    conn = sqlite3.connect(db_path, timeout=10)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO traffic_history (namespace, timestamp, timestamp_ms, data) VALUES (?, ?, ?, ?)",
        (namespace, timestamp, timestamp_ms, data)
    )
    conn.commit()
    conn.close()
except: pass
PY
}

# 单个命名空间的采集循环（在子进程中运行）
ns_collect_loop() {
    local ns="$1"

    while true; do
        local ts ts_ms interfaces ppp0 data

        ts=$(date '+%Y-%m-%d %H:%M:%S')
        ts_ms=$(( $(date +%s) * 1000 ))
        interfaces=$(get_interfaces "$ns")
        ppp0=$(get_ppp0 "$ns")

        data="{\"namespace\":\"$ns\",\"timestamp\":\"$ts\",\"timestamp_ms\":$ts_ms,\"interfaces\":[$interfaces],\"ppp0\":$ppp0}"

        # 写入最新数据文件（以命名空间为后缀区分）
        if [ "$ns" = "default" ]; then
            echo "$data" > "$DATA_FILE"
        else
            echo "$data" > "${DATA_FILE%.json}_${ns}.json"
        fi

        add_history "$data"

        sleep "$REFRESH_INTERVAL"
    done
}

# ---- 主进程 ----
echo "流量监控启动，间隔${REFRESH_INTERVAL}秒，命名空间:[${NAMESPACES}]，PID:$$"
echo $$ > /tmp/traffic_monitor.pid

# 将逗号分隔的命名空间转换为数组
IFS=',' read -ra NS_LIST <<< "$NAMESPACES"

CHILD_PIDS=()

# 为每个命名空间启动独立子进程
for ns in "${NS_LIST[@]}"; do
    ns="${ns// /}"   # 去除空格
    [ -z "$ns" ] && continue
    echo "  启动命名空间采集子进程: $ns"
    ns_collect_loop "$ns" &
    CHILD_PIDS+=($!)
done

# 捕获退出信号，清理子进程和 PID 文件
cleanup() {
    echo "停止流量监控，清理子进程..."
    for pid in "${CHILD_PIDS[@]}"; do
        kill "$pid" 2>/dev/null
    done
    rm -f /tmp/traffic_monitor.pid
    exit 0
}

trap cleanup SIGINT SIGTERM

# 等待所有子进程（主进程保持运行）
wait
