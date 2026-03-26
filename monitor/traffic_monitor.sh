#!/bin/bash
# 流量监控 - 自动采集所有命名空间
# 通过读取 /var/run/netns/ 目录获取命名空间列表
# 通过读取 /sys/class/net/ 文件系统获取网络接口数据

REFRESH_INTERVAL=1
DATA_DIR="/root/traffic-monitor/data"
DATA_FILE="$DATA_DIR/traffic_data.json"
DB_PATH="$DATA_DIR/traffic_monitor.db"
NETNS_DIR="/var/run/netns"

# 获取所有命名空间列表（包含 default）
get_all_namespaces() {
    local namespaces="default"

    # 读取 /var/run/netns/ 目录获取所有非默认命名空间
    if [ -d "$NETNS_DIR" ]; then
        for ns_file in "$NETNS_DIR"/*; do
            [ -f "$ns_file" ] || continue
            local ns_name
            ns_name=$(basename "$ns_file")
            [ -n "$ns_name" ] && namespaces="$namespaces,$ns_name"
        done
    fi

    echo "$namespaces"
}

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

# 获取指定命名空间的网络接口统计（通过读取文件系统）
# 对于非 default 命名空间，需要进入 /proc/<pid>/ns/net 查看其网络接口
get_interfaces() {
    local ns="$1"
    local results=""

    if [ "$ns" = "default" ]; then
        # 默认命名空间：直接读取 /sys/class/net/
        for dev_path in /sys/class/net/*; do
            [ -d "$dev_path" ] || continue
            local dev
            dev=$(basename "$dev_path")

            # 排除 lo、VLAN 子接口、veth 别名
            case "$dev" in lo|*.*|*@*) continue ;; esac

            local rx tx
            rx=$(cat "$dev_path/statistics/rx_bytes" 2>/dev/null) || rx=0
            tx=$(cat "$dev_path/statistics/tx_bytes" 2>/dev/null) || tx=0

            [ -z "$results" ] && results="{\"name\":\"$dev\",\"rx_bytes\":$rx,\"tx_bytes\":$tx}" \
                || results="$results,{\"name\":\"$dev\",\"rx_bytes\":$rx,\"tx_bytes\":$tx}"
        done
    else
        # 非默认命名空间：通过 ip netns exec 读取 /sys/class/net/
        # 注意：在命名空间内，/sys/class/net/ 会显示该命名空间的接口
        local devlist
        devlist=$(ip netns exec "$ns" ls /sys/class/net/ 2>/dev/null) || return

        for dev in $devlist; do
            # 排除 lo、VLAN 子接口、veth 别名
            case "$dev" in lo|*.*|*@*) continue ;; esac

            local rx tx
            # 在命名空间内读取 /sys/class/net/<dev>/statistics/
            rx=$(ip netns exec "$ns" cat "/sys/class/net/$dev/statistics/rx_bytes" 2>/dev/null) || rx=0
            tx=$(ip netns exec "$ns" cat "/sys/class/net/$dev/statistics/tx_bytes" 2>/dev/null) || tx=0

            [ -z "$results" ] && results="{\"name\":\"$dev\",\"rx_bytes\":$rx,\"tx_bytes\":$tx}" \
                || results="$results,{\"name\":\"$dev\",\"rx_bytes\":$rx,\"tx_bytes\":$tx}"
        done
    fi

    echo "$results"
}

# 获取指定命名空间的 ppp0 丢包数据
# 优先读取 /sys/class/net/ppp0/statistics/，如果不存在则用 ifconfig
get_ppp0() {
    local ns="$1"

    # 检查 ppp0 是否存在
    local ppp0_exists=0
    if [ "$ns" = "default" ]; then
        [ -d "/sys/class/net/ppp0" ] && ppp0_exists=1
    else
        ip netns exec "$ns" test -d /sys/class/net/ppp0 2>/dev/null && ppp0_exists=1
    fi

    if [ "$ppp0_exists" -eq 0 ]; then
        echo '{"available":false}'
        return
    fi

    # 尝试从 /sys/class/net/ppp0/statistics/ 读取（更高效）
    # 但 ppp 接口的丢包信息通常需要从 /proc/net/dev 或 ifconfig 获取
    # 这里保持使用 ifconfig，因为 /sys 下没有完整的丢包统计
    local info
    info=$(ns_exec "$ns" ifconfig ppp0 2>/dev/null)

    if [ -z "$info" ]; then
        echo '{"available":false}'
        return
    fi

    # 解析 ifconfig 输出
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
    namespace = parsed['namespace']
    timestamp = parsed['timestamp']
    timestamp_ms = parsed['timestamp_ms']
    conn = sqlite3.connect(db_path, timeout=10)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO traffic_history (namespace, timestamp, timestamp_ms, data) VALUES (?, ?, ?, ?)",
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
    local ns_data_file

    if [ "$ns" = "default" ]; then
        ns_data_file="$DATA_FILE"
    else
        ns_data_file="$DATA_DIR/traffic_data_${ns}.json"
    fi

    while true; do
        local ts ts_ms interfaces ppp0 data

        ts=$(date '+%Y-%m-%d %H:%M:%S')
        ts_ms=$(( $(date +%s) * 1000 ))
        interfaces=$(get_interfaces "$ns")
        ppp0=$(get_ppp0 "$ns")

        data="{\"namespace\":\"$ns\",\"timestamp\":\"$ts\",\"timestamp_ms\":$ts_ms,\"interfaces\":[$interfaces],\"ppp0\":$ppp0}"

        # 写入最新数据文件
        echo "$data" > "$ns_data_file"

        # 写入数据库
        add_history "$data"

        sleep "$REFRESH_INTERVAL"
    done
}

# ---- 主进程 ----
# 自动获取所有命名空间
NAMESPACES=$(get_all_namespaces)

echo "=========================================="
echo "流量监控启动"
echo "=========================================="
echo "采样间隔: ${REFRESH_INTERVAL}秒"
echo "命名空间: [${NAMESPACES}]"
echo "PID: $$"
echo "=========================================="

echo $$ > /tmp/traffic_monitor.pid

# 确保数据目录存在
mkdir -p "$DATA_DIR"

# 将逗号分隔的命名空间转换为数组
IFS=',' read -ra NS_LIST <<< "$NAMESPACES"

CHILD_PIDS=()

# 为每个命名空间启动独立子进程
for ns in "${NS_LIST[@]}"; do
    ns="${ns// /}"   # 去除空格
    [ -z "$ns" ] && continue
    echo "  启动命名空间采集: $ns"
    ns_collect_loop "$ns" &
    CHILD_PIDS+=($!)
done

echo "=========================================="
echo "所有命名空间采集子进程已启动"
echo "子进程 PIDs: ${CHILD_PIDS[*]}"
echo "=========================================="

# 捕获退出信号，清理子进程和 PID 文件
cleanup() {
    echo ""
    echo "停止流量监控，清理子进程..."
    for pid in "${CHILD_PIDS[@]}"; do
        kill "$pid" 2>/dev/null
    done
    # 等待子进程退出
    sleep 1
    for pid in "${CHILD_PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null
    done
    rm -f /tmp/traffic_monitor.pid
    echo "流量监控已停止"
    exit 0
}

trap cleanup SIGINT SIGTERM

# 等待所有子进程（主进程保持运行）
wait
