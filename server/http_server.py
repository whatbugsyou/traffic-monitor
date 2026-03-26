#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简易HTTP服务器
用于提供流量监控Web界面和数据API
兼容 Python 3.6
"""

import http.server
import json
import os
import socketserver
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlparse

# 配置
PORT = 8080
DATA_DIR = "/root/traffic-monitor/data"
DB_PATH = "/root/traffic-monitor/data/traffic_monitor.db"
WEB_ROOT = "/root/traffic-monitor/web"


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
            cursor.execute(
                "SELECT DISTINCT namespace FROM traffic_history ORDER BY namespace ASC"
            )
            rows = cursor.fetchall()
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
        """返回历史流量数据，支持 ?namespace= 和 ?since= 参数"""
        try:
            parsed_path = urlparse(self.path)
            params = parse_query_params(parsed_path.query)
            namespace = params.get("namespace", "default")
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
                cursor.execute(
                    "SELECT data FROM traffic_history"
                    " WHERE namespace = ?"
                    " ORDER BY timestamp_ms ASC",
                    (namespace,),
                )

            rows = cursor.fetchall()
            conn.close()

            data = [json.loads(row[0]) for row in rows]
            self.send_json_response(data)
        except Exception as e:
            self.send_json_response({"error": str(e)}, 500)

    def handle_sse_stream(self):
        """处理SSE流请求，支持 ?namespace= 参数"""
        try:
            parsed_path = urlparse(self.path)
            params = parse_query_params(parsed_path.query)
            namespace = params.get("namespace", "default")

            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            last_timestamp = 0
            print("[SSE] 客户端连接建立，namespace=%s，开始推送数据..." % namespace)

            while True:
                try:
                    if os.path.exists(DB_PATH):
                        conn = sqlite3.connect(DB_PATH)
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT data FROM traffic_history"
                            " WHERE namespace = ? AND timestamp_ms > ?"
                            " ORDER BY timestamp_ms ASC",
                            (namespace, last_timestamp),
                        )
                        rows = cursor.fetchall()
                        conn.close()

                        if rows:
                            new_data = [json.loads(row[0]) for row in rows]
                            last_timestamp = new_data[-1]["timestamp_ms"]

                            message = json.dumps(
                                {
                                    "type": "incremental",
                                    "data": new_data,
                                    "timestamp": time.time(),
                                },
                                ensure_ascii=False,
                            )

                            self.wfile.write(("data: %s\n\n" % message).encode("utf-8"))
                            self.wfile.flush()

                    time.sleep(1)

                except (BrokenPipeError, ConnectionResetError):
                    print("[SSE] 客户端断开连接，namespace=%s" % namespace)
                    break
                except Exception as e:
                    print("[SSE] 推送异常 (namespace=%s): %s" % (namespace, str(e)))
                    time.sleep(1)

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


def run_server():
    """启动HTTP服务器"""
    Path(WEB_ROOT).mkdir(parents=True, exist_ok=True)

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("", PORT), TrafficMonitorHandler) as httpd:
        print("HTTP服务器启动在端口 %d" % PORT)
        print("Web根目录: %s" % WEB_ROOT)
        print("访问地址: http://localhost:%d" % PORT)
        print("API端点:")
        print("  - http://localhost:%d/api/namespaces        (命名空间列表)" % PORT)
        print(
            "  - http://localhost:%d/api/current           (当前流量数据，支持 ?namespace=)"
            % PORT
        )
        print(
            "  - http://localhost:%d/api/history           (历史流量数据，支持 ?namespace= ?since=)"
            % PORT
        )
        print(
            "  - http://localhost:%d/api/stream            (SSE实时数据流，支持 ?namespace=)"
            % PORT
        )
        print("\n按 Ctrl+C 停止服务器")

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n服务器已停止")


if __name__ == "__main__":
    run_server()
