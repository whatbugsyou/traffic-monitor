#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简易HTTP服务器
用于提供流量监控Web界面和数据API
兼容 Python 3.6
支持分层存储：按时间范围自动选择合适的数据粒度
"""

import http.server
import json
import os
import socketserver
import sqlite3
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

# 配置
PORT = 8080
DATA_DIR = "/root/traffic-monitor/data"
DB_PATH = "/root/traffic-monitor/data/traffic_monitor.db"
WEB_ROOT = "/root/traffic-monitor/web"

# 聚合间隔配置
INTERVAL_10S = 10  # 10秒
INTERVAL_1M = 60  # 1分钟


def get_data_file(namespace):
    """返回对应命名空间的数据文件路径"""
    if namespace == "default":
        return os.path.join(DATA_DIR, "traffic_data.json")
    return os.path.join(DATA_DIR, "traffic_data_%s.json" % namespace)


def parse_query_params(query_string):
    """解析 URL 查询参数，返回 dict"""
    params = {}
    if not query_string:
        return params
    for param in query_string.split("&"):
        if "=" in param:
            key, value = param.split("=", 1)
            params[key] = value
        elif param:
            params[param] = ""
    return params


def get_history_by_duration(namespace, duration_minutes):
    """
    根据时间范围获取历史数据，自动选择合适的数据表

    Args:
        namespace: 命名空间
        duration_minutes: 时间范围（分钟）

    Returns:
        list: 数据列表
    """
    if not os.path.exists(DB_PATH):
        return []

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    now_ms = int(time.time() * 1000)
    since_ms = now_ms - (duration_minutes * 60 * 1000)

    try:
        if duration_minutes <= 5:
            # 5分钟内：使用原始数据（1秒/点）
            cursor.execute(
                "SELECT data FROM traffic_history"
                " WHERE namespace = ? AND timestamp_ms > ?"
                " ORDER BY timestamp_ms ASC",
                (namespace, since_ms),
            )
            rows = cursor.fetchall()
            conn.close()

            result = []
            prev_data = None

            for row in rows:
                try:
                    data = json.loads(row[0])
                    data["resolution"] = "1s"

                    # 计算速度（从上一次数据）
                    if prev_data is not None:
                        interval_ms = data.get("timestamp_ms", 0) - prev_data.get(
                            "timestamp_ms", 0
                        )
                        if interval_ms > 0:
                            for iface in data.get("interfaces", []):
                                iface_name = iface.get("name")
                                # 查找上一次的同名接口
                                prev_iface = None
                                for pi in prev_data.get("interfaces", []):
                                    if pi.get("name") == iface_name:
                                        prev_iface = pi
                                        break

                                if prev_iface:
                                    # 计算速度（字节/秒）
                                    rx_speed = max(
                                        0,
                                        int(
                                            (
                                                iface.get("rx_bytes", 0)
                                                - prev_iface.get("rx_bytes", 0)
                                            )
                                            * 1000
                                            / interval_ms
                                        ),
                                    )
                                    tx_speed = max(
                                        0,
                                        int(
                                            (
                                                iface.get("tx_bytes", 0)
                                                - prev_iface.get("tx_bytes", 0)
                                            )
                                            * 1000
                                            / interval_ms
                                        ),
                                    )
                                    iface["rx_speed"] = rx_speed
                                    iface["tx_speed"] = tx_speed

                    # 计算丢包增量（从上一次数据）
                    if prev_data is not None:
                        if data.get("ppp0", {}).get("available") and prev_data.get(
                            "ppp0", {}
                        ).get("available"):
                            rx_dropped_inc = max(
                                0,
                                data["ppp0"].get("rx_dropped", 0)
                                - prev_data["ppp0"].get("rx_dropped", 0),
                            )
                            tx_dropped_inc = max(
                                0,
                                data["ppp0"].get("tx_dropped", 0)
                                - prev_data["ppp0"].get("tx_dropped", 0),
                            )
                            data["ppp0"]["rx_dropped_inc"] = rx_dropped_inc
                            data["ppp0"]["tx_dropped_inc"] = tx_dropped_inc

                    result.append(data)
                    prev_data = data

                except (json.JSONDecodeError, KeyError):
                    continue

            return result

        elif duration_minutes <= 60:
            # 1小时内：使用10秒聚合数据
            cursor.execute(
                "SELECT namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,"
                "       rx_dropped_sum, tx_dropped_sum, sample_count"
                " FROM traffic_history_10s"
                " WHERE namespace = ? AND timestamp_ms > ?"
                " ORDER BY timestamp_ms ASC",
                (namespace, since_ms),
            )
            rows = cursor.fetchall()
            conn.close()

            result = []
            for ns, ts, ts_ms, rx_speed, tx_speed, rx_drop, tx_drop, sample_cnt in rows:
                result.append(
                    {
                        "namespace": ns,
                        "timestamp": ts,
                        "timestamp_ms": ts_ms,
                        "interfaces": [],  # 聚合数据不包含接口详情
                        "rx_speed_avg": rx_speed,
                        "tx_speed_avg": tx_speed,
                        "rx_dropped_sum": rx_drop,
                        "tx_dropped_sum": tx_drop,
                        "resolution": "10s",
                    }
                )
            return result

        else:
            # 超过1小时：使用1分钟聚合数据
            cursor.execute(
                "SELECT namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,"
                "       rx_dropped_sum, tx_dropped_sum, sample_count"
                " FROM traffic_history_1m"
                " WHERE namespace = ? AND timestamp_ms > ?"
                " ORDER BY timestamp_ms ASC",
                (namespace, since_ms),
            )
            rows = cursor.fetchall()
            conn.close()

            result = []
            for ns, ts, ts_ms, rx_speed, tx_speed, rx_drop, tx_drop, sample_cnt in rows:
                result.append(
                    {
                        "namespace": ns,
                        "timestamp": ts,
                        "timestamp_ms": ts_ms,
                        "interfaces": [],
                        "rx_speed_avg": rx_speed,
                        "tx_speed_avg": tx_speed,
                        "rx_dropped_sum": rx_drop,
                        "tx_dropped_sum": tx_drop,
                        "resolution": "1m",
                    }
                )
            return result

    except sqlite3.OperationalError:
        conn.close()
        return []


def aggregate_data_for_namespace(namespace):
    """
    为指定命名空间聚合数据
    从原始数据生成10秒和1分钟聚合数据
    """
    if not os.path.exists(DB_PATH):
        return

    conn = sqlite3.connect(DB_PATH, timeout=10)
    cursor = conn.cursor()

    now_ms = int(time.time() * 1000)

    # ═══════════════════════════════════════════
    # 生成10秒聚合数据
    # ═══════════════════════════════════════════
    one_hour_ago_ms = now_ms - (60 * 60 * 1000)

    cursor.execute(
        "SELECT data, timestamp_ms FROM traffic_history"
        " WHERE namespace = ? AND timestamp_ms > ?"
        " ORDER BY timestamp_ms ASC",
        (namespace, one_hour_ago_ms),
    )
    rows = cursor.fetchall()

    if rows:
        # 按10秒分组
        groups_10s = {}
        prev_data = None

        for data_json, ts_ms in rows:
            try:
                data = json.loads(data_json)
                group_key = (ts_ms // (INTERVAL_10S * 1000)) * (INTERVAL_10S * 1000)

                if group_key not in groups_10s:
                    groups_10s[group_key] = {
                        "rx_speeds": [],
                        "tx_speeds": [],
                        "rx_dropped_list": [],
                        "tx_dropped_list": [],
                        "timestamp": None,
                        "prev_data": None,
                    }

                groups_10s[group_key]["timestamp"] = data.get("timestamp")

                # 计算速度（需要上一条数据）
                if prev_data is not None:
                    interval_ms = ts_ms - prev_data.get("timestamp_ms", ts_ms)
                    if interval_ms > 0:
                        for iface in data.get("interfaces", []):
                            iface_name = iface.get("name")
                            prev_iface = None
                            for pi in prev_data.get("interfaces", []):
                                if pi.get("name") == iface_name:
                                    prev_iface = pi
                                    break

                            if prev_iface:
                                rx_speed = (
                                    (
                                        iface.get("rx_bytes", 0)
                                        - prev_iface.get("rx_bytes", 0)
                                    )
                                    * 1000
                                    / interval_ms
                                )
                                tx_speed = (
                                    (
                                        iface.get("tx_bytes", 0)
                                        - prev_iface.get("tx_bytes", 0)
                                    )
                                    * 1000
                                    / interval_ms
                                )
                                groups_10s[group_key]["rx_speeds"].append(rx_speed)
                                groups_10s[group_key]["tx_speeds"].append(tx_speed)

                if data.get("ppp0", {}).get("available"):
                    groups_10s[group_key]["rx_dropped_list"].append(
                        data["ppp0"].get("rx_dropped", 0)
                    )
                    groups_10s[group_key]["tx_dropped_list"].append(
                        data["ppp0"].get("tx_dropped", 0)
                    )

                prev_data = data

            except (json.JSONDecodeError, KeyError):
                continue

        # 写入10秒聚合表
        for group_key, group_data in groups_10s.items():
            if group_data["rx_speeds"]:
                cursor.execute(
                    "INSERT OR REPLACE INTO traffic_history_10s"
                    " (namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,"
                    "  rx_dropped_sum, tx_dropped_sum, sample_count)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        namespace,
                        group_data["timestamp"],
                        group_key,
                        sum(group_data["rx_speeds"]) / len(group_data["rx_speeds"])
                        if group_data["rx_speeds"]
                        else 0,
                        sum(group_data["tx_speeds"]) / len(group_data["tx_speeds"])
                        if group_data["tx_speeds"]
                        else 0,
                        max(group_data["rx_dropped_list"])
                        if group_data["rx_dropped_list"]
                        else 0,
                        max(group_data["tx_dropped_list"])
                        if group_data["tx_dropped_list"]
                        else 0,
                        len(group_data["rx_speeds"]),
                    ),
                )

    # ═══════════════════════════════════════════
    # 生成1分钟聚合数据（从10秒聚合表）
    # ═══════════════════════════════════════════
    three_hours_ago_ms = now_ms - (3 * 60 * 60 * 1000)

    cursor.execute(
        "SELECT rx_speed_avg, tx_speed_avg, rx_dropped_sum, tx_dropped_sum,"
        "       sample_count, timestamp, timestamp_ms"
        " FROM traffic_history_10s"
        " WHERE namespace = ? AND timestamp_ms > ?"
        " ORDER BY timestamp_ms ASC",
        (namespace, three_hours_ago_ms),
    )
    rows_10s = cursor.fetchall()

    if rows_10s:
        groups_1m = {}

        for rx_avg, tx_avg, rx_drop, tx_drop, sample_cnt, ts, ts_ms in rows_10s:
            group_key = (ts_ms // (INTERVAL_1M * 1000)) * (INTERVAL_1M * 1000)

            if group_key not in groups_1m:
                groups_1m[group_key] = {
                    "rx_speeds": [],
                    "tx_speeds": [],
                    "rx_dropped": [],
                    "tx_dropped": [],
                    "timestamps": [],
                }

            groups_1m[group_key]["rx_speeds"].append(rx_avg)
            groups_1m[group_key]["tx_speeds"].append(tx_avg)
            groups_1m[group_key]["rx_dropped"].append(rx_drop)
            groups_1m[group_key]["tx_dropped"].append(tx_drop)
            groups_1m[group_key]["timestamps"].append(ts)

        for group_key, group_data in groups_1m.items():
            if group_data["rx_speeds"]:
                cursor.execute(
                    "INSERT OR REPLACE INTO traffic_history_1m"
                    " (namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,"
                    "  rx_dropped_sum, tx_dropped_sum, sample_count)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        namespace,
                        group_data["timestamps"][-1]
                        if group_data["timestamps"]
                        else None,
                        group_key,
                        sum(group_data["rx_speeds"]) / len(group_data["rx_speeds"]),
                        sum(group_data["tx_speeds"]) / len(group_data["tx_speeds"]),
                        max(group_data["rx_dropped"])
                        if group_data["rx_dropped"]
                        else 0,
                        max(group_data["tx_dropped"])
                        if group_data["tx_dropped"]
                        else 0,
                        len(group_data["rx_speeds"]),
                    ),
                )

    conn.commit()
    conn.close()


def aggregate_all_namespaces():
    """聚合所有命名空间的数据"""
    if not os.path.exists(DB_PATH):
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT DISTINCT namespace FROM traffic_history")
        namespaces = [row[0] for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        conn.close()
        return

    conn.close()

    for ns in namespaces:
        try:
            aggregate_data_for_namespace(ns)
        except Exception as e:
            print("[聚合] 命名空间 %s 聚合失败: %s" % (ns, str(e)))


class AggregationThread(threading.Thread):
    """后台聚合线程"""

    def __init__(self, interval=10):
        super().__init__(daemon=True)
        self.interval = interval
        self.running = True

    def run(self):
        print("[聚合线程] 启动，间隔 %d 秒" % self.interval)
        while self.running:
            try:
                aggregate_all_namespaces()
            except Exception as e:
                print("[聚合线程] 错误: %s" % str(e))
            time.sleep(self.interval)

    def stop(self):
        self.running = False


class TrafficMonitorHandler(http.server.SimpleHTTPRequestHandler):
    """自定义请求处理器 - 兼容 Python 3.6"""

    def do_GET(self):
        """处理GET请求"""
        parsed_path = urlparse(self.path)
        path = parsed_path.path

        if path == "/api/namespaces":
            self.handle_namespaces()
        elif path == "/api/current":
            self.handle_current_data()
        elif path == "/api/history":
            self.handle_history_data()
        elif path == "/api/stream":
            self.handle_sse_stream()
        elif path == "/" or path == "/index.html":
            self.handle_index()
        else:
            self.handle_static_file(path)

    def handle_namespaces(self):
        """返回数据库中所有已存在的命名空间列表"""
        try:
            if not os.path.exists(DB_PATH):
                self.send_json_response([])
                return

            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            # 从原始数据表获取命名空间
            try:
                cursor.execute(
                    "SELECT DISTINCT namespace FROM traffic_history ORDER BY namespace ASC"
                )
                rows = cursor.fetchall()
            except sqlite3.OperationalError:
                rows = []

            conn.close()

            namespaces = [row[0] for row in rows]
            self.send_json_response(namespaces)
        except Exception as e:
            self.send_json_response({"error": str(e)}, 500)

    def handle_current_data(self):
        """返回当前流量数据，支持 ?namespace= 参数"""
        try:
            parsed_path = urlparse(self.path)
            params = parse_query_params(parsed_path.query)
            namespace = params.get("namespace", "default")

            data_file = get_data_file(namespace)
            if os.path.exists(data_file):
                with open(data_file, "r") as f:
                    data = json.load(f)
                self.send_json_response(data)
            else:
                self.send_json_response({"error": "No data available yet"}, 404)
        except Exception as e:
            self.send_json_response({"error": str(e)}, 500)

    def handle_history_data(self):
        """
        返回历史流量数据

        支持参数:
        - namespace: 命名空间（默认 default）
        - duration: 时间范围（分钟），5/10/15/30/60/120/180
        - since: 时间戳（毫秒），向后兼容
        """
        try:
            parsed_path = urlparse(self.path)
            params = parse_query_params(parsed_path.query)
            namespace = params.get("namespace", "default")

            # 优先使用 duration 参数
            if "duration" in params:
                duration_str = params.get("duration", "5")
                duration_minutes = int(duration_str) if duration_str.isdigit() else 5
                # 限制范围
                duration_minutes = max(5, min(180, duration_minutes))

                data = get_history_by_duration(namespace, duration_minutes)
                self.send_json_response(
                    {
                        "namespace": namespace,
                        "duration_minutes": duration_minutes,
                        "count": len(data),
                        "data": data,
                    }
                )
                return

            # 向后兼容：使用 since 参数
            since = params.get("since")
            since_ms = int(since) if since and since.isdigit() else None

            if not os.path.exists(DB_PATH):
                self.send_json_response([])
                return

            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            if since_ms:
                cursor.execute(
                    "SELECT data FROM traffic_history"
                    " WHERE namespace = ? AND timestamp_ms > ?"
                    " ORDER BY timestamp_ms ASC",
                    (namespace, since_ms),
                )
            else:
                # 默认返回最近5分钟
                since_ms = int(time.time() * 1000) - (5 * 60 * 1000)
                cursor.execute(
                    "SELECT data FROM traffic_history"
                    " WHERE namespace = ? AND timestamp_ms > ?"
                    " ORDER BY timestamp_ms ASC",
                    (namespace, since_ms),
                )

            rows = cursor.fetchall()
            conn.close()

            data = [json.loads(row[0]) for row in rows]
            self.send_json_response(data)
        except Exception as e:
            self.send_json_response({"error": str(e)}, 500)

    def handle_sse_stream(self):
        """
        处理SSE流请求
        支持 ?namespace= 和 ?duration= 参数
        根据 duration 选择不同粒度的数据表进行推送
        """
        try:
            parsed_path = urlparse(self.path)
            params = parse_query_params(parsed_path.query)
            namespace = params.get("namespace", "default")
            duration_str = params.get("duration", "5")
            duration_minutes = int(duration_str) if duration_str.isdigit() else 5
            duration_minutes = max(5, min(180, duration_minutes))

            # 根据 duration 决定查询哪个表和推送间隔
            if duration_minutes <= 5:
                table = "traffic_history"
                push_interval = 1  # 1秒
                resolution = "1s"
            elif duration_minutes <= 60:
                table = "traffic_history_10s"
                push_interval = 10  # 10秒
                resolution = "10s"
            else:
                table = "traffic_history_1m"
                push_interval = 60  # 60秒
                resolution = "1m"

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            last_timestamp = 0
            print(
                "[SSE] 客户端连接建立，namespace=%s，duration=%d分钟，resolution=%s"
                % (namespace, duration_minutes, resolution)
            )

            while True:
                try:
                    if os.path.exists(DB_PATH):
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()

                        if table == "traffic_history":
                            # 原始数据表
                            cursor.execute(
                                "SELECT data FROM %s"
                                " WHERE namespace = ? AND timestamp_ms > ?"
                                " ORDER BY timestamp_ms ASC" % table,
                                (namespace, last_timestamp),
                            )
                            rows = cursor.fetchall()
                            conn.close()

                            if rows:
                                new_data = [json.loads(row[0]) for row in rows]
                                last_timestamp = new_data[-1]["timestamp_ms"]
                                # 添加分辨率标记
                                for d in new_data:
                                    d["resolution"] = "1s"
                            else:
                                new_data = []
                        else:
                            # 聚合数据表
                            cursor.execute(
                                "SELECT namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,"
                                "       rx_dropped_sum, tx_dropped_sum"
                                " FROM %s"
                                " WHERE namespace = ? AND timestamp_ms > ?"
                                " ORDER BY timestamp_ms ASC" % table,
                                (namespace, last_timestamp),
                            )
                            rows = cursor.fetchall()
                            conn.close()

                            if rows:
                                new_data = []
                                for (
                                    ns,
                                    ts,
                                    ts_ms,
                                    rx_speed,
                                    tx_speed,
                                    rx_drop,
                                    tx_drop,
                                ) in rows:
                                    new_data.append(
                                        {
                                            "namespace": ns,
                                            "timestamp": ts,
                                            "timestamp_ms": ts_ms,
                                            "interfaces": [],
                                            "rx_speed_avg": rx_speed,
                                            "tx_speed_avg": tx_speed,
                                            "rx_dropped_sum": rx_drop,
                                            "tx_dropped_sum": tx_drop,
                                            "resolution": resolution,
                                        }
                                    )
                                last_timestamp = new_data[-1]["timestamp_ms"]
                            else:
                                new_data = []

                        if new_data:
                            message = json.dumps(
                                {
                                    "type": "incremental",
                                    "data": new_data,
                                    "resolution": resolution,
                                    "timestamp": time.time(),
                                },
                                ensure_ascii=False,
                            )

                            self.wfile.write(("data: %s\n\n" % message).encode("utf-8"))
                            self.wfile.flush()

                    time.sleep(push_interval)

                except (BrokenPipeError, ConnectionResetError):
                    print("[SSE] 客户端断开连接，namespace=%s" % namespace)
                    break
                except Exception as e:
                    print("[SSE] 推送异常 (namespace=%s): %s" % (namespace, str(e)))
                    time.sleep(push_interval)

        except Exception as e:
            print("[SSE] SSE流处理异常: %s" % str(e))

    def handle_index(self):
        """处理首页请求"""
        index_file = Path(WEB_ROOT) / "index.html"
        if index_file.exists():
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            with open(str(index_file), "rb") as f:
                self.wfile.write(f.read())
        else:
            self.send_error(404, "Index file not found")

    def handle_static_file(self, path):
        """处理静态文件请求"""
        if path.startswith("/"):
            path = path[1:]

        file_path = Path(WEB_ROOT) / path

        try:
            file_path = file_path.resolve()
            web_root = Path(WEB_ROOT).resolve()
            if not str(file_path).startswith(str(web_root)):
                self.send_error(403, "Forbidden")
                return
        except Exception:
            self.send_error(400, "Bad Request")
            return

        if not file_path.exists() or not file_path.is_file():
            self.send_error(404, "File not found")
            return

        mime_type = self.guess_type(str(file_path))

        try:
            self.send_response(200)
            self.send_header("Content-type", mime_type)
            self.end_headers()
            with open(str(file_path), "rb") as f:
                self.wfile.write(f.read())
        except Exception:
            self.send_error(500, "Internal Server Error")

    def send_json_response(self, data, status=200):
        """发送JSON响应，附带CORS头"""
        self.send_response(status)
        self.send_header("Content-type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def do_OPTIONS(self):
        """处理CORS预检请求"""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def log_message(self, format, *args):
        """自定义日志格式"""
        print(
            "[%s] %s - %s"
            % (self.log_date_time_string(), self.address_string(), format % args)
        )


# 全局聚合线程
aggregation_thread = None


def run_server():
    """启动HTTP服务器"""
    global aggregation_thread

    Path(WEB_ROOT).mkdir(parents=True, exist_ok=True)

    # 启动后台聚合线程
    aggregation_thread = AggregationThread(interval=10)
    aggregation_thread.start()

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("", PORT), TrafficMonitorHandler) as httpd:
        print("HTTP服务器启动在端口 %d" % PORT)
        print("Web根目录: %s" % WEB_ROOT)
        print("访问地址: http://localhost:%d" % PORT)
        print("")
        print("API端点:")
        print("  GET /api/namespaces                  - 命名空间列表")
        print("  GET /api/current?namespace=<ns>      - 当前数据")
        print(
            "  GET /api/history?namespace=<ns>&duration=<分钟> - 历史数据（按时间范围）"
        )
        print("  GET /api/stream?namespace=<ns>       - SSE实时流")
        print("")
        print("时间范围选项 (duration 参数):")
        print("  5 分钟   → 原始数据 (1秒/点, ~300条)")
        print("  10-60分钟 → 10秒聚合 (~360条)")
        print("  1-3小时   → 1分钟聚合 (~180条)")
        print("")
        print("按 Ctrl+C 停止服务器")

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n正在停止服务器...")
            if aggregation_thread:
                aggregation_thread.stop()
            print("服务器已停止")


if __name__ == "__main__":
    run_server()
