mod collector;
mod database;
mod models;
mod netlink_client;
mod netns;
mod server;

use anyhow::{Context, Result};
use std::sync::Arc;

use crate::collector::TrafficCollector;
use crate::database::Database;
use crate::models::{CollectorConfig, DatabaseConfig, ServerConfig};
use crate::server::HttpServerWrapper;

#[actix_rt::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("========================================");
    log::info!("Traffic Monitor Server (Rust)");
    log::info!("========================================");

    let db_config = DatabaseConfig::default();
    let collector_config = CollectorConfig::default();
    let server_config = ServerConfig::default();

    log::info!("Configuration loaded:");
    log::info!("  Database: {}", db_config.db_path);
    log::info!("  Server: {}:{}", server_config.host, server_config.port);
    log::info!("  Collection interval: {}s", collector_config.interval_secs);

    log::info!("Initializing database...");
    let db = Arc::new(Database::new(db_config).context("Failed to initialize database")?);
    log::info!("Database initialized successfully");

    log::info!("Starting traffic collector...");
    let collector = Arc::new(
        TrafficCollector::new(Arc::clone(&db), collector_config)
            .context("Failed to create traffic collector")?,
    );

    // 启动采集器（所有后台任务在 tokio runtime 中并行运行）
    collector.start().await;
    log::info!("Traffic collector started");

    log::info!("Starting HTTP server...");
    let http_server =
        HttpServerWrapper::new(server_config, Arc::clone(&db), Arc::clone(&collector));

    log::info!("========================================");
    log::info!("All services started successfully!");
    log::info!("========================================");
    log::info!("Backend API: http://localhost:18080");
    log::info!("Web interface: Open web/index.html in browser");
    log::info!("");
    log::info!("API Endpoints:");
    log::info!("  GET /api/namespaces              - List namespaces");
    log::info!("  GET /api/current?namespace=<ns>  - Current data");
    log::info!("  GET /api/history?namespace=<ns>&duration=<min> - Historical data");
    log::info!("  GET /api/stream?namespace=<ns>   - SSE real-time stream");
    log::info!("");
    log::info!("Press Ctrl+C to stop the server");

    if let Err(e) = http_server.start().await {
        log::error!("HTTP server error: {}", e);
    }

    // HTTP 服务器停止后，优雅关闭 collector（等待存储任务写入剩余数据）
    log::info!("Stopping traffic collector...");
    if let Err(e) = collector.shutdown().await {
        log::error!("Failed to shutdown collector: {}", e);
    } else {
        log::info!("Traffic collector stopped gracefully");
    }

    log::info!("All services stopped successfully");
    log::info!("Goodbye!");

    Ok(())
}
