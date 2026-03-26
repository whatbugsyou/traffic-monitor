# 流量监控系统

实时网络流量监控与可视化系统，支持多命名空间、前后端分离部署。

## 功能特性

- ✅ 实时流量监控（每秒采样）
- ✅ 多 network namespace 并发采集
- ✅ SQLite 数据库存储（按命名空间自动清理旧数据）
- ✅ SSE 实时推送（服务器主动推送，支持 `?namespace=` 过滤）
- ✅ 前后端分离：后端暴露 REST API，前端为独立静态页面
- ✅ 前端支持输入服务器地址连接，动态切换命名空间
- ✅ ECharts 可视化图表（各网卡流量折线图）
- ✅ PPP0 接口丢包统计
- ✅ 增量数据传输（减少网络流量）
- ✅ 自动重连机制
- ✅ Python 3.6 兼容

## 目录结构

```
traffic-monitor/
├── bin/
│   └── start.sh               # 统一启动脚本
├── server/
│   ├── http_server.py         # HTTP 服务器（REST API + SSE）
│   └── init_db.py             # 数据库初始化 / 迁移
├── monitor/
│   └── traffic_monitor.sh     # 流量采集脚本（支持多命名空间）
├── web/
│   └── index.html             # 独立前端页面（可本地打开）
├── logs/
│   ├── monitor.log
│   └── http_server.log
├── data/
│   ├── traffic_monitor.db     # SQLite 数据库
│   ├── traffic_data.json      # default 命名空间当前数据
│   └── traffic_data_<ns>.json # 其他命名空间当前数据
└── README.md
```

## 架构说明

```
服务器端                          客户端（任意浏览器）
─────────────────────────         ──────────────────────────
traffic_monitor.sh                web/index.html（本地文件）
  ├─ default namespace               ├─ 输入服务器地址
  ├─ serverSpace namespace    ──▶    ├─ 获取命名空间列表
  └─ vpnSpace namespace              ├─ 选择命名空间
        │                            └─ SSE 实时接收数据
        ▼
  traffic_monitor.db
        │
        ▼
  http_server.py :8080
    /api/namespaces
    /api/current?namespace=
    /api/history?namespace=
    /api/stream?namespace=
```

## 快速开始

### 服务器端部署

**1. 仅监控默认命名空间**

```bash
/root/traffic-monitor/bin/start.sh start
```

**2. 监控多个命名空间**

```bash
/root/traffic-monitor/bin/start.sh --namespaces default,serverSpace,vpnSpace start
```

**3. 停止服务**

```bash
/root/traffic-monitor/bin/start.sh stop
```

**4. 查看状态**

```bash
/root/traffic-monitor/bin/start.sh status
```

**5. 重启服务**

```bash
/root/traffic-monitor/bin/start.sh stop && /root/traffic-monitor/bin/start.sh --namespaces default,serverSpace start
```

### 客户端（前端）使用

前端 `web/index.html` 是一个独立的静态页面，**无需部署到服务器**，可以直接用浏览器打开本地文件，或放到任意静态托管服务。

1. 用浏览器打开 `web/index.html`
2. 在顶部输入框填写服务器地址，例如 `http://182.204.177.75:8080`
3. 点击「连接」按钮
4. 从下拉框中选择命名空间（自动从服务器获取）
5. 实时查看流量图表

> 服务器地址会自动保存到 `localStorage`，刷新页面后无需重新输入。

## API 文档

后端服务运行在服务器的 **8080** 端口，所有接口均支持跨域（CORS）。

### 获取命名空间列表

```http
GET /api/namespaces
```

**响应示例**：
```json
["default", "serverSpace", "vpnSpace"]
```

### 获取当前流量数据

```http
GET /api/current
GET /api/current?namespace=serverSpace
```

**响应示例**：
```json
{
  "namespace": "serverSpace",
  "timestamp": "2024-03-12 22:35:15",
  "timestamp_ms": 1710263715000,
  "interfaces": [
    {"name": "ppp0", "rx_bytes": 1234567890, "tx_bytes": 987654321}
  ],
  "ppp0": {
    "available": true,
    "rx_packets": 12345,
    "rx_dropped": 10
  }
}
```

### 获取历史流量数据

```http
GET /api/history
GET /api/history?namespace=serverSpace
GET /api/history?namespace=serverSpace&since=1710263715000
```

**参数**：
- `namespace`（可选，默认 `default`）：指定命名空间
- `since`（可选）：时间戳（毫秒），只返回该时间之后的数据

### SSE 实时推送

```http
GET /api/stream
GET /api/stream?namespace=serverSpace
```

**JavaScript 示例**：

```javascript
const es = new EventSource('http://182.204.177.75:8080/api/stream?namespace=serverSpace');
es.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  // msg.type === 'incremental'
  // msg.data === [ { namespace, timestamp, timestamp_ms, interfaces, ppp0 }, ... ]
};
```

**推送格式**：
```
data: {"type":"incremental","data":[...]}
```

## 命令行参数说明

### traffic_monitor.sh

```
用法: traffic_monitor.sh [--namespaces <ns1,ns2,...>]

选项:
  --namespaces   指定要采集的命名空间，逗号分隔（默认: default）
                 "default" 表示当前默认命名空间（不执行 ip netns exec）
                 其他命名空间通过 ip netns exec <ns> 执行

示例:
  ./traffic_monitor.sh
  ./traffic_monitor.sh --namespaces default,serverSpace,vpnSpace
```

### start.sh

```
用法: start.sh [--namespaces <ns1,ns2,...>] {start|stop|status|restart}

选项:
  --namespaces <ns1,ns2,...>   指定要监控的命名空间（逗号分隔）

示例:
  ./start.sh start
  ./start.sh --namespaces default,serverSpace start
  ./start.sh stop
  ./start.sh status
```

## 数据库说明

使用 SQLite，表结构如下：

```sql
CREATE TABLE traffic_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace    TEXT    NOT NULL DEFAULT 'default',
    timestamp    TEXT    NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    data         TEXT    NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(namespace, timestamp_ms)
);
```

- 每个命名空间各自保留最近 **300 条**记录（约 5 分钟），超出自动清理最旧记录
- 首次运行会自动初始化；已有旧版数据库（无 `namespace` 列）会自动迁移，原数据归入 `default` 命名空间

## 技术栈

| 层级 | 技术 |
|------|------|
| 采集 | Bash + `/sys/class/net` + `ifconfig` |
| 后端 | Python 3.6 标准库（`http.server`、`sqlite3`、`socketserver`） |
| 数据库 | SQLite |
| 推送协议 | SSE（Server-Sent Events） |
| 前端 | 原生 HTML/CSS/JavaScript |
| 图表 | ECharts 5.4.3（BootCDN） |

## 故障排查

### 服务无法启动

```bash
# 检查端口占用
netstat -tuln | grep 8080

# 查看日志
tail -f /root/traffic-monitor/logs/http_server.log
tail -f /root/traffic-monitor/logs/monitor.log
```

### 前端无法连接

- 确认服务器防火墙已开放 8080 端口
- 确认输入的服务器地址包含协议头，例如 `http://` 而非只填 IP
- 打开浏览器开发者工具查看 Console / Network 报错

### 命名空间下没有数据

```bash
# 确认命名空间存在
ip netns list

# 手动测试采集
ip netns exec serverSpace ip link show

# 查看数据库各命名空间记录数
/root/traffic-monitor/bin/start.sh status
```

### 数据库迁移（旧版升级）

旧版数据库（无 `namespace` 列）在执行任意 `start.sh` 命令时会自动迁移，无需手动操作。如需手动触发：

```bash
python3 /root/traffic-monitor/server/init_db.py
```

## 版本历史

- **v3.0**：前后端分离重构，多命名空间支持，数据库增加 namespace 字段
- **v2.0**：迁移到 SSE，项目结构重组
- **v1.5**：迁移到 SQLite 数据库
- **v1.0**：初始版本，HTTP 轮询 + JSON 存储

## 许可证

MIT License