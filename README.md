# Traffic Monitor

使用 Rust 编写的网络流量监控服务，提供实时采集、历史查询、SSE 推送和 Web 界面。

## 快速开始

### 构建

```bash
cargo build
```

发布构建：

```bash
cargo build --release
```

### 运行

开发模式运行：

```bash
cargo run
```

发布模式运行：

```bash
cargo run --release
```

服务默认监听 `8080` 端口。

## 测试与检查

运行测试：

```bash
cargo test
```

格式化代码：

```bash
cargo fmt
```

静态检查：

```bash
cargo clippy
```

生成文档：

```bash
cargo doc --open
```

## 核心功能

- **实时监控**：按固定间隔采样网络流量数据
- **历史查询**：支持按时间范围查询历史数据
- **SSE 推送**：提供实时数据流
- **Web 界面**：内置前端页面用于可视化展示

## API 端点

| 端点 | 说明 |
|------|------|
| `GET /api/namespaces` | 获取命名空间列表 |
| `GET /api/current?namespace=<ns>` | 获取当前流量数据 |
| `GET /api/history?namespace=<ns>&duration=<min>` | 获取历史数据 |
| `GET /api/stream?namespace=<ns>` | 获取 SSE 实时流 |

## 项目结构

```text
traffic-monitor/
├── src/          # Rust 源代码
├── web/          # Web 界面资源
├── data/         # 数据目录
├── Cargo.toml    # Rust 项目配置
└── README.md
```

## 配置

当前配置主要定义在源码中，启动前如需调整监听地址、端口、采样间隔或数据保留策略，请根据项目中的配置结构进行修改后重新编译运行。

## 故障排查

检查服务是否启动：

```bash
cargo run
```

检查端口监听：

```bash
netstat -tuln | grep 8080
```

如果接口可访问，可以先验证：

```bash
curl http://localhost:8080/api/namespaces
```

## 许可证

MIT License