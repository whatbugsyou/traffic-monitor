# 流量监控系统

实时网络流量监控与可视化系统，支持多命名空间、前后端分离部署、分层存储。

## 功能特性

- ✅ 实时流量监控（每秒采样）
- ✅ **自动获取所有命名空间**（读取 `/var/run/netns/` 目录）
- ✅ **分层存储**：根据时间范围自动选择数据粒度
  - 5分钟内：原始数据（1秒/点）
  - 1小时内：10秒聚合数据
  - 3小时内：1分钟聚合数据
- ✅ **前端时间范围选择**：5分钟 ~ 3小时可切换
- ✅ SSE 实时推送（仅5分钟范围）
- ✅ 前后端分离：后端暴露 REST API，前端为独立静态页面
- ✅ ECharts 可视化图表（支持大数据量降采样渲染）
- ✅ PPP0 接口丢包统计
- ✅ Python 3.6 兼容

## 目录结构

```
traffic-monitor/
├── bin/
│   └── start.sh               # 统一启动脚本
├── server/
│   ├── http_server.py         # HTTP 服务器（REST API + SSE + 后台聚合）
│   └── init_db.py             # 数据库初始化 / 迁移 / 聚合
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
服务器端                              客户端（任意浏览器）
─────────────────────────────         ──────────────────────────────
traffic_monitor.sh                    web/index.html（本地文件）
  │ 自动读取 /var/run/netns/           │
  ├─ default namespace                 ├─ 输入服务器地址
  ├─ serverSpace namespace             ├─ 选择时间范围（5分钟~3小时）
  └─ vpnSpace namespace         ──▶    ├─ 选择命名空间
        │                              └─ 实时/历史数据展示
        ▼
  ┌─────────────────────────────────────┐
  │ traffic_monitor.db (SQLite)         │
  │ ├─ traffic_history     (原始 5分钟)  │
  │ ├─ traffic_history_10s (聚合 1小时)  │
  │ └─ traffic_history_1m  (聚合 3小时)  │
  └─────────────────────────────────────┘
        │
        ▼
  http_server.py :8080
    /api/namespaces
    /api/current?namespace=
    /api/history?namespace=&duration=
    /api/stream?namespace=
```

## 分层存储策略

| 时间范围 | 数据表 | 数据粒度 | 数据量 | 用途 |
|----------|--------|----------|--------|------|
| 0 ~ 5 分钟 | traffic_history | 1秒/点 | ~300 条 | 实时监控详细数据 |
| 5 分钟 ~ 1 小时 | traffic_history_10s | 10秒/点 | ~360 条 | 近期趋势分析 |
| 1 小时 ~ 3 小时 | traffic_history_1m | 1分钟/点 | ~180 条 | 历史趋势概览 |

**优势**：
- 减少数据传输量：单个命名空间最多 ~840 条数据
- 优化前端渲染：ECharts 不需要渲染过多数据点
- 降低存储压力：数据库自动清理旧数据

## 数据量预估

| 参数 | 数值 |
|------|------|
| 命名空间数 | 300（预估） |
| 采样间隔 | 1 秒 |
| 数据保留时间 | 3 小时 |

**数据库大小**：
- 原始数据：300 × 300 = 90,000 条
- 10秒聚合：300 × 360 = 108,000 条
- 1分钟聚合：300 × 180 = 54,000 条
- 总计：~252,000 条，约 **100 MB**

**单个命名空间首次加载**：
- 5分钟：~300 条（~120 KB）
- 1小时：~360 条（~144 KB）
- 3小时：~180 条（~72 KB）

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

前端 `web/index.html` 是一个独立的静态页面，**无需部署到服务器**，可以直接用浏览器打开本地文件。

1. 用浏览器打开 `web/index.html`
2. 在顶部输入框填写服务器地址，例如 `http://192.168.1.1:8080`
3. 点击「连接」按钮
4. 选择时间范围（5分钟 ~ 3小时）
5. 从下拉框中选择命名空间（自动从服务器获取）
6. 实时查看流量图表

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
GET /api/current?namespace=<ns>
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

### 获取历史流量数据（按时间范围）

```http
GET /api/history?namespace=<ns>&duration=<分钟>
```

**参数**：
- `namespace`（可选，默认 `default`）：指定命名空间
- `duration`（可选，默认 5）：时间范围（分钟），可选值：5/10/15/30/60/120/180

**响应示例**：
```json
{
  "namespace": "serverSpace",
  "duration_minutes": 30,
  "count": 180,
  "data": [
    {
      "namespace": "serverSpace",
      "timestamp": "2024-03-12 22:35:15",
      "timestamp_ms": 1710263715000,
      "rx_speed_avg": 12345.67,
      "tx_speed_avg": 9876.54,
      "rx_dropped_sum": 10,
      "tx_dropped_sum": 5,
      "resolution": "10s"
    }
  ]
}
```

### SSE 实时推送

```http
GET /api/stream?namespace=<ns>
```

**JavaScript 示例**：

```javascript
const es = new EventSource('http://192.168.1.1:8080/api/stream?namespace=serverSpace');
es.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  // msg.type === 'incremental'
  // msg.data === [ { namespace, timestamp, ... }, ... ]
};
```

## 数据库说明

使用 SQLite，包含三张表：

### 1. 原始数据表（traffic_history）

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

- 每个命名空间保留最近 **300 条**记录（约 5 分钟）
- 自动清理超出记录

### 2. 10秒聚合表（traffic_history_10s）

```sql
CREATE TABLE traffic_history_10s (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace       TEXT    NOT NULL DEFAULT 'default',
    timestamp       TEXT    NOT NULL,
    timestamp_ms    INTEGER NOT NULL,
    rx_speed_avg    REAL    NOT NULL DEFAULT 0,
    tx_speed_avg    REAL    NOT NULL DEFAULT 0,
    rx_dropped_sum  INTEGER NOT NULL DEFAULT 0,
    tx_dropped_sum  INTEGER NOT NULL DEFAULT 0,
    sample_count    INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(namespace, timestamp_ms)
);
```

- 每个命名空间保留最近 **360 条**记录（约 1 小时）

### 3. 1分钟聚合表（traffic_history_1m）

结构与 10秒聚合表相同，每个命名空间保留最近 **180 条**记录（约 3 小时）。

### 手动维护命令

```bash
# 查看数据库状态
python3 /root/traffic-monitor/server/init_db.py status

# 手动触发聚合
python3 /root/traffic-monitor/server/init_db.py aggregate

# 清理超过指定小时的数据
python3 /root/traffic-monitor/server/init_db.py cleanup 3
```

## 技术栈

| 层级 | 技术 |
|------|------|
| 采集 | Bash + `/sys/class/net` + `/var/run/netns/` |
| 后端 | Python 3.6 标准库（`http.server`、`sqlite3`、`socketserver`） |
| 数据库 | SQLite（分层存储） |
| 推送协议 | SSE（Server-Sent Events） |
| 前端 | 原生 HTML/CSS/JavaScript |
| 图表 | ECharts 5.4.3（支持降采样） |

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
- 确认输入的服务器地址包含协议头，例如 `http://`
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

旧版数据库在执行 `start.sh start` 时会自动迁移，无需手动操作。

## 性能优化

### 分层存储

- 原始数据仅保留 5 分钟，减少存储压力
- 聚合数据按需生成，降低查询开销
- 前端按时间范围获取合适粒度的数据

### 前端渲染

- ECharts 配置 `sampling: 'lttb'` 降采样算法
- 配置 `large: true` 启用大数据优化
- X 轴标签按间隔显示，避免重叠

### 后端聚合

- 后台线程每 10 秒执行一次聚合
- 从原始数据生成 10 秒和 1 分钟聚合数据
- 数据库触发器自动清理旧数据

## 版本历史

- **v3.2**：分层存储优化，前端时间范围选择，数据按粒度返回
- **v3.1**：自动获取所有命名空间，数据保留时间改为 3 小时
- **v3.0**：前后端分离重构，多命名空间支持
- **v2.0**：迁移到 SSE，项目结构重组
- **v1.0**：初始版本

## 许可证

MIT License