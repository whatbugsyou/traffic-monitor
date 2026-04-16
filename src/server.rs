use actix_cors::Cors;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::collector::TrafficCollector;
use crate::database::Database;
use crate::models::*;

/// HTTP 服务器
pub struct HttpServerWrapper {
    config: ServerConfig,
    db: Arc<Database>,
    collector: Arc<TrafficCollector>,
}

impl HttpServerWrapper {
    /// 创建新的 HTTP 服务器
    pub fn new(config: ServerConfig, db: Arc<Database>, collector: Arc<TrafficCollector>) -> Self {
        HttpServerWrapper {
            config,
            db,
            collector,
        }
    }

    /// 启动 HTTP 服务器
    pub async fn start(&self) -> Result<()> {
        let db = Arc::clone(&self.db);
        let collector = Arc::clone(&self.collector);
        let web_root = self.config.web_root.clone();
        let host = self.config.host.clone();
        let port = self.config.port;

        log::info!("Starting HTTP server on {}:{}", host, port);
        log::info!("Web root: {}", web_root);
        log::info!("API endpoints:");
        log::info!("  GET /api/namespaces                      - List namespaces");
        log::info!("  GET /api/current?namespace=<ns>          - Current data");
        log::info!(
            "  GET /api/history?namespace=<ns>&duration=<min>&resolution=<res> - Historical data"
        );
        log::info!("  GET /api/stream?namespace=<ns>&duration=<min>&resolution=<res> - SSE real-time stream");

        let server = HttpServer::new(move || {
            let cors = Cors::permissive();

            App::new()
                .wrap(cors)
                .app_data(web::Data::new(Arc::clone(&db)))
                .app_data(web::Data::new(Arc::clone(&collector)))
                .route("/", web::get().to(index_handler))
                .route("/api/namespaces", web::get().to(get_namespaces))
                .route("/api/current", web::get().to(get_current_data))
                .route("/api/history", web::get().to(get_history_data))
                .route("/api/stream", web::get().to(sse_stream))
                .service(actix_files::Files::new("/", &web_root).index_file("index.html"))
        })
        .bind(format!("{}:{}", host, port))
        .context("Failed to bind HTTP server")?
        .workers(2)
        .run();

        // 获取服务器句柄用于优雅关闭
        let server_handle = server.handle();

        // 在单独的任务中等待 Ctrl+C 信号
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                log::info!("Received Ctrl+C signal, initiating graceful shutdown...");
                server_handle.stop(true).await;
            }
        });

        // 等待服务器关闭
        server.await.context("Failed to run HTTP server")?;

        Ok(())
    }
}

/// 首页处理器
async fn index_handler() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../web/index.html"))
}

/// 获取命名空间列表
async fn get_namespaces(collector: web::Data<Arc<TrafficCollector>>) -> impl Responder {
    let namespaces = collector.get_namespaces().await;
    let response = NamespacesResponse { namespaces };
    HttpResponse::Ok().json(response)
}

/// 获取当前数据
async fn get_current_data(
    db: web::Data<Arc<Database>>,
    query: web::Query<CurrentQuery>,
) -> impl Responder {
    let namespace = query.namespace.as_deref().unwrap_or("default");

    match db.get_current_data(namespace) {
        Ok(Some(data)) => HttpResponse::Ok().json(data),
        Ok(None) => {
            HttpResponse::NotFound().body(format!("No data found for namespace: {}", namespace))
        }
        Err(e) => {
            log::error!("Failed to get current data for {}: {}", namespace, e);
            HttpResponse::InternalServerError().body(format!("Failed to get current data: {}", e))
        }
    }
}

/// 获取历史数据
async fn get_history_data(
    db: web::Data<Arc<Database>>,
    query: web::Query<HistoryQuery>,
) -> impl Responder {
    let namespace = query.namespace.as_deref().unwrap_or("default");
    let duration_minutes = query.duration.unwrap_or(5);

    match db.get_history_by_duration(namespace, duration_minutes) {
        Ok(mut data) => {
            // 计算速度
            calculate_speeds_for_list(&mut data);

            let response = HistoryResponse {
                namespace: namespace.to_string(),
                duration_minutes,
                count: data.len(),
                data,
            };
            HttpResponse::Ok().json(response)
        }
        Err(e) => {
            log::error!("Failed to get history data for {}: {}", namespace, e);
            HttpResponse::InternalServerError().body(format!("Failed to get history data: {}", e))
        }
    }
}

/// SSE 实时流
async fn sse_stream(
    db: web::Data<Arc<Database>>,
    collector: web::Data<Arc<TrafficCollector>>,
    query: web::Query<StreamQuery>,
) -> impl Responder {
    let namespace = query.namespace.as_deref().unwrap_or("default").to_string();
    let duration_minutes = query.duration.unwrap_or(5);

    // 根据时间跨度推导数据分辨率
    let resolution = Resolution::from_duration_minutes(duration_minutes);

    log::info!(
        "SSE stream connected for namespace: {}, duration: {}min, resolution: {:?}",
        namespace,
        duration_minutes,
        resolution
    );

    // 精准订阅指定命名空间和分辨率的数据流
    let receiver = collector.subscribe(&namespace, resolution).await;

    // 如果命名空间不存在，返回错误
    let mut receiver = match receiver {
        Some(rx) => rx,
        None => {
            log::warn!("Namespace not found for SSE stream: {}", namespace);
            return HttpResponse::NotFound().body(format!("Namespace not found: {}", namespace));
        }
    };

    // 从数据库获取最近一条数据作为 prev_data 的初始值
    let initial_data = db.get_current_data(&namespace).ok().flatten();
    if initial_data.is_some() {
        log::debug!(
            "SSE stream initialized with previous data for namespace: {}",
            namespace
        );
    }

    // 创建 SSE 流（精准订阅，计算速度后发送）
    let stream = async_stream::stream! {
        log::info!("SSE stream started for namespace: {}", namespace);
        // 保存上一次的数据用于计算速度（初始化为数据库中的最新数据）
        let mut prev_data = initial_data;

        loop {
            match receiver.recv().await {
                Ok(mut data) => {
                    // 计算速度
                    calculate_interface_speeds(&mut data, prev_data.as_ref());

                    // 发送带速度的数据
                    let message = SseMessage {
                        message_type: "incremental".to_string(),
                        data: vec![data.clone()],
                    };

                    // 更新 prev_data（保存当前数据用于下次计算）
                    prev_data = Some(data);

                    if let Ok(json) = serde_json::to_string(&message) {
                        yield Ok(web::Bytes::from(format!("data: {}\n\n", json))) as Result<web::Bytes, std::convert::Infallible>;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    log::info!("SSE stream closed for namespace: {}", namespace);
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    log::warn!("SSE stream lagged for namespace: {}, continuing", namespace);
                    // 继续接收，忽略滞后消息
                    continue;
                }
            }
        }
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/event-stream"))
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("Connection", "keep-alive"))
        .streaming(stream)
}

/// 当前数据查询参数
#[derive(Debug, Deserialize)]
struct CurrentQuery {
    namespace: Option<String>,
}

/// 历史数据查询参数
#[derive(Debug, Deserialize)]
struct HistoryQuery {
    namespace: Option<String>,
    /// 时间跨度（分钟）
    duration: Option<u32>,
}

/// SSE 流查询参数
#[derive(Debug, Deserialize)]
struct StreamQuery {
    namespace: Option<String>,
    /// 时间跨度（分钟），后端根据时间跨度决定推送数据的分辨率
    duration: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;

    use crate::collector::TrafficCollector;

    #[actix_rt::test]
    async fn test_get_namespaces() {
        let config = DatabaseConfig::default();
        let db = Arc::new(Database::new(config).unwrap());

        let collector =
            Arc::new(TrafficCollector::new(Arc::clone(&db), CollectorConfig::default()).unwrap());

        let app = App::new()
            .app_data(web::Data::new(db))
            .app_data(web::Data::new(collector))
            .route("/api/namespaces", web::get().to(get_namespaces));

        let mut app = test::init_service(app).await;
        let req = test::TestRequest::get().uri("/api/namespaces").to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }
}
