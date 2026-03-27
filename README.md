# Traffic Monitor

高性能网络流量监控系统，使用 Rust 编写。实时监控网络命名空间，自动发现、分层存储、RESTful API。

## 快速开始

```bash
# 构建
cargo build --release

# 运行
cargo run --release

# 测试
curl http://localhost:8080/api/namespaces
```

## 核心功能

- **实时监控**: 每秒采样，自动发现所有网络命名空间
- **分层存储**: 1秒/10秒/1分钟三级聚合
- **REST API**: 完整的 HTTP 接口
- **SSE 推送**: 实时数据流
- **Web 界面**: 内置可视化仪表板

## API 端点

| 端点 | 说明 |
|------|------|
| `GET /api/namespaces` | 命名空间列表 |
| `GET /api/current?namespace=<ns>` | 当前数据 |
| `GET /api/history?namespace=<ns>&duration=<min>` | 历史数据 |
| `GET /api/stream?namespace=<ns>` | SSE 实时流 |

## 使用

### 开发

```bash
cargo run              # 调试模式
cargo run --release    # 发布模式
cargo test             # 运行测试
cargo fmt              # 格式化代码
cargo clippy           # 代码检查
```

### 生产部署

#### 方式一：动态链接（需要较新的系统）

```bash
# 构建发布版本
cargo build --release

# 安装为系统服务
sudo make install
sudo systemctl start traffic-monitor
```

#### 方式二：静态链接（推荐，兼容旧系统）

对于 CentOS 7.6 等使用旧版 GLIBC 的系统，需要使用 musl 静态编译：

```bash
# 安装 musl 工具链（Debian/Ubuntu）
apt-get install musl-tools musl-dev

# 构建 musl 静态链接版本
./build.sh musl

# 生成的二进制文件位于：
# target/x86_64-unknown-linux-musl/release/traffic-monitor
```

**部署到目标服务器：**

```bash
# 复制二进制文件和资源目录
scp target/x86_64-unknown-linux-musl/release/traffic-monitor user@server:/path/
scp -r data web user@server:/path/

# 在目标服务器运行
chmod +x traffic-monitor && ./traffic-monitor
```

#### 构建方式对比

| 构建方式 | 命令 | 输出路径 | 兼容性 |
|---------|------|---------|--------|
| 动态链接 | `cargo build --release` | `target/release/` | 需要 GLIBC 2.29+ |
| 静态链接 | `./build.sh musl` | `target/x86_64-unknown-linux-musl/release/` | 兼容所有 x86_64 Linux |

> **提示**: 如果遇到 `GLIBC_2.29 not found` 等错误，请使用 musl 静态编译。

### 服务管理

```bash
./run.sh start    # 启动服务
./run.sh stop     # 停止服务
./run.sh status   # 查看状态
```

## 配置

编辑 `src/main.rs` 修改配置：

```rust
ServerConfig {
    host: "0.0.0.0",
    port: 8080,
    aggregation_interval_secs: 10,
}

CollectorConfig {
    interval_secs: 1,  // 采集频率
}

DatabaseConfig {
    retention_raw_minutes: 5,    // 原始数据保留
    retention_10s_hours: 1,      // 10秒聚合保留
    retention_1m_hours: 3,       // 1分钟聚合保留
}
```

## 项目结构

```
traffic-monitor/
├── src/              # 源代码
├── web/              # Web 界面
├── data/             # 数据库
├── logs/             # 日志
├── Cargo.toml        # 依赖配置
└── Makefile          # 系统任务
```

## 性能

- **内存**: 50-100 MB (300 命名空间)
- **延迟**: 5-50ms
- **并发**: 10,000+ 连接
- **吞吐**: 10,000+ req/s

## 数据存储

| 时间范围 | 分辨率 | 保留时间 | 数据点 |
|---------|--------|---------|--------|
| 0-5分钟 | 1秒 | 5分钟 | ~300 |
| 5-60分钟 | 10秒 | 1小时 | ~360 |
| 1-3小时 | 1分钟 | 3小时 | ~180 |

## 故障排查

```bash
# 检查端口
netstat -tuln | grep 8080

# 查看日志
tail -f logs/traffic_monitor.log

# 检查命名空间
ls -la /var/run/netns/

# 数据库统计
make db-stats
```

## 许可证

MIT License