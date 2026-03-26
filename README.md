# 流量监控系统

实时网络流量监控与可视化系统，支持多命名空间、前后端分离部署。

## 功能特性

- ✅ 实时流量监控（每秒采样）
- ✅ **自动获取所有命名空间**（读取 `/var/run/netns/` 目录）
- ✅ 多 network namespace 并发采集
- ✅ **数据保留 3 小时**（每个命名空间 10800 条记录）
- ✅ SQLite 数据库存储（按命名空间自动清理旧数据）
- ✅ SSE 实时推送（服务器主动推送，支持 `?namespace=` 过滤）
- ✅ 前后端分离：后端暴露 REST API，前端为独立静态页面
- ✅ 前端支持输入服务器地址连接，动态切换命名空间
- ✅ ECharts 可视化图表（支持大数据量降采样渲染）
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
│   └── traffic_monitor.sh     # 流量采集脚本（自动获取所有命名空间）
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
  │ 自动读取 /var/run/netns/         ├─ 输入服务器地址
  ├─ default namespace               ├─ 获取命名空间列表
  ├─ serverSpace namespace    ──▶    ├─ 选择命名空间
  └─ vpnSpace namespace              └─ SSE 实时接收数据
        │
        ▼
  traffic_monitor.db（保留3小时）
        │
        ▼
  http_server.py :8080
    /api/namespaces
    /api/current?namespace=
    /api/history?namespace=
    /api/stream?namespace=
```

## 数据量预估

| 参数 | 数值 |
|------|------|
| 命名空间数 | 300（预估） |
| 采样间隔 | 1 秒 |
| 数据保留时间 | 3 小时 = 10800 秒 |
| 每条记录大小 | ~400 字节（JSON） |

**总数据量**：
- 记录数：300 × 10800 = **3,240,000 条**
- 数据库大小：3,240,000 × 400 ≈ **1.3 GB**

**单个命名空间首次加载**：
- 记录数：10800 条
- 数据量：10800 × 400 ≈ **4.3 MB**

> 注：前端通过 SSE 增量传输，首次加载后仅传输新数据，大幅减少网络流量。

## 快速开始

### 服务器端部署

```bash
# 启动服务（自动监控所有命名空间）
/root/traffic-monitor/bin/start.sh start

# 停止服务
/root/traffic-monitor/bin/start.sh stop

# 查看状态
/root/traffic-monitor/bin/start.sh status

# 重启服务
/root/traffic-monitor/bin/start.sh stop && /root/traffic-monitor/bin/start.sh start
```

### 客户端（前端）使用

前端 `web/index.html` 是一个独立的静态页面，**无需部署到服务器**，可以直接用浏览器打开本地文件，或放到任意静态托管服务。

1. 用浏览器打开 `web/index.html`
2. 在顶部输入框填写服务器地址，例如 `http://182.204.177.75:8080`
3. 点击「连接」按钮
4. 从下拉框中选择命名空间（自动从服务器获取）
5. 实时查看流量图表

> 服务器地址会自动保存到 `localStorage`，刷新页面后无需重新输入。

## 命名空间自动发现

监控脚本启动时会自动获取所有网络命名空间：

1. **默认命名空间**：始终监控，标记为 `default`
2. **其他命名空间**：通过读取 `/var/run/netns/` 目录获取

```
/var/run/netns/
├── serverSpace    → 自动发现并监控
├── vpnSpace       → 自动发现并监控
└── clientSpace    → 自动发现并监控
```

> 无需手动指定命名空间列表，新增命名空间后重启服务即可自动纳入监控。

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

- 每个命名空间各自保留最近 **10800 条**记录（约 3 小时），超出自动清理最旧记录
- 首次运行会自动初始化；已有旧版数据库（无 `namespace` 列）会自动迁移，原数据归入 `default` 命名空间

### 手动清理旧数据

```bash
# 清理超过 3 小时的数据（默认）
python3 /root/traffic-monitor/server/init_db.py cleanup

# 清理超过指定小时数的数据
python3 /root/traffic-monitor/server/init_db.py cleanup 6
```

## 技术栈

| 层级 | 技术 |
|------|------|
| 采集 | Bash + `/sys/class/net` + `/var/run/netns/` |
| 后端 | Python 3.6 标准库（`http.server`、`sqlite3`、`socketserver`） |
| 数据库 | SQLite |
| 推送协议 | SSE（Server-Sent Events） |
| 前端 | 原生 HTML/CSS/JavaScript |
| 图表 | ECharts 5.4.3（支持大数据量降采样） |

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

# 确认 /var/run/netns/ 目录
ls -la /var/run/netns/

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

## 性能优化

### 前端大数据量渲染

前端图表支持以下优化：

1. **降采样渲染**：ECharts 配置 `sampling: 'lttb'`，使用 Largest-Triangle-Three-Buckets 算法
2. **大数据模式**：配置 `large: true`，启用 ECharts 大数据优化
3. **X 轴标签间隔**：每 10 分钟显示一个时间标签，避免重叠

### 后端数据清理

数据库触发器自动清理超过 10800 条的旧数据，避免数据库无限增长。

## 版本历史

- **v3.1**：自动获取所有命名空间，数据保留时间改为 3 小时，前端支持大数据量渲染
- **v3.0**：前后端分离重构，多命名空间支持，数据库增加 namespace 字段
- **v2.0**：迁移到 SSE，项目结构重组
- **v1.5**：迁移到 SQLite 数据库
- **v1.0**：初始版本，HTTP 轮询 + JSON 存储

## 许可证

MIT License