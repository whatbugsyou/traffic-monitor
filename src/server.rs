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
        log::info!("  GET /api/namespaces              - List namespaces");
        log::info!("  GET /api/current?namespace=<ns>  - Current data");
        log::info!("  GET /api/history?namespace=<ns>&duration=<min> - Historical data");
        log::info!("  GET /api/stream?namespace=<ns>   - SSE real-time stream");

        let server = HttpServer::new(move || {
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
                .max_age(3600);

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
        .workers(2);

        // 使用 actix 内置的优雅关闭
        server.run().await.context("Failed to run HTTP server")?;

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
        Ok(data) => {
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
    collector: web::Data<Arc<TrafficCollector>>,
    query: web::Query<StreamQuery>,
) -> impl Responder {
    let namespace = query.namespace.as_deref().unwrap_or("default").to_string();
    log::info!("SSE stream connected for namespace: {}", namespace);

    // 订阅数据流
    let mut receiver = collector.subscribe();

    // 创建 SSE 流
    let stream = async_stream::stream! {
        log::info!("SSE stream started for namespace: {}", namespace);
        loop {
            match receiver.recv().await {
                Ok(data) => {
                    log::info!("Received data for namespace: {}, requested: {}", data.namespace, namespace);
                    // 过滤指定命名空间的数据
                    if data.namespace == namespace {
                        log::info!("Namespace matched, sending SSE data");
                        let message = SseMessage {
                            message_type: "incremental".to_string(),
                            data: vec![data],
                        };

                        if let Ok(json) = serde_json::to_string(&message) {
                            yield Ok(web::Bytes::from(format!("data: {}\n\n", json))) as Result<web::Bytes, std::convert::Infallible>;
                        }
                    } else {
                        log::debug!("Namespace mismatch, skipping data");
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    log::info!("SSE stream closed");
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    log::warn!("SSE stream lagged, continuing");
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
    duration: Option<u32>,
}

/// SSE 流查询参数
#[derive(Debug, Deserialize)]
struct StreamQuery {
    namespace: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;

    #[actix_rt::test]
    async fn test_get_namespaces() {
        let config = DatabaseConfig::default();
        let db = Arc::new(Database::new(config).unwrap());

        let app = App::new()
            .app_data(web::Data::new(db))
            .route("/api/namespaces", web::get().to(get_namespaces));

        let mut app = test::init_service(app).await;
        let req = test::TestRequest::get().uri("/api/namespaces").to_request();

        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }
}
