mod collector;
mod database;
mod models;
mod server;

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::collector::TrafficCollector;
use crate::database::Database;
use crate::models::{CollectorConfig, DatabaseConfig, ServerConfig};
use crate::server::HttpServerWrapper;

#[actix_rt::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("========================================");
    log::info!("Traffic Monitor Server (Rust)");
    log::info!("========================================");

    // 加载配置
    let db_config = DatabaseConfig {
        db_path: "data/traffic_monitor.db".to_string(),
        retention_raw_minutes: 5,
        retention_10s_hours: 1,
        retention_1m_hours: 3,
        retention_1h_days: 7,
        retention_1d_days: 30,
    };

    let collector_config = CollectorConfig { interval_secs: 1 };

    let server_config = ServerConfig {
        host: "0.0.0.0".to_string(),
        port: 8080,
        web_root: "web".to_string(),
    };

    log::info!("Configuration loaded:");
    log::info!("  Database: {}", db_config.db_path);
    log::info!("  Server: {}:{}", server_config.host, server_config.port);
    log::info!("  Collection interval: {}s", collector_config.interval_secs);

    // 创建数据库
    log::info!("Initializing database...");
    let db = Arc::new(Database::new(db_config).context("Failed to initialize database")?);
    log::info!("Database initialized successfully");

    // 创建采集器
    log::info!("Starting traffic collector...");
    let collector = Arc::new(
        TrafficCollector::new(Arc::clone(&db), collector_config)
            .context("Failed to create traffic collector")?,
    );

    // 启动采集器
    collector
        .start()
        .await
        .context("Failed to start traffic collector")?;
    log::info!("Traffic collector started");

    // 创建 HTTP 服务器
    log::info!("Starting HTTP server...");
    let http_server =
        HttpServerWrapper::new(server_config, Arc::clone(&db), Arc::clone(&collector));

    log::info!("========================================");
    log::info!("All services started successfully!");
    log::info!("========================================");
    log::info!("Backend API: http://localhost:8080");
    log::info!("Web interface: Open web/index.html in browser");
    log::info!("");
    log::info!("API Endpoints:");
    log::info!("  GET /api/namespaces              - List namespaces");
    log::info!("  GET /api/current?namespace=<ns>  - Current data");
    log::info!("  GET /api/history?namespace=<ns>&duration=<min> - Historical data");
    log::info!("  GET /api/stream?namespace=<ns>   - SSE real-time stream");
    log::info!("");
    log::info!("Press Ctrl+C to stop the server");

    // 启动 HTTP 服务器（使用 actix 的信号处理）
    if let Err(e) = http_server.start().await {
        log::error!("HTTP server error: {}", e);
    }

    // 服务器关闭后停止采集器
    log::info!("Stopping traffic collector...");
    if let Err(e) = collector.stop() {
        log::error!("Failed to stop collector: {}", e);
    } else {
        log::info!("Traffic collector stopped");
    }

    log::info!("All services stopped successfully");
    log::info!("Goodbye!");

    Ok(())
}
