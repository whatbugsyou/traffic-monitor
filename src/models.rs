use serde::{Deserialize, Serialize};

/// 网络接口统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterfaceStats {
    pub name: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    /// 接收速度 (bytes/s)，查询时计算
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rx_speed: Option<u64>,
    /// 发送速度 (bytes/s)，查询时计算
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_speed: Option<u64>,
}

/// 流量数据（原始快照）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficData {
    pub namespace: String,
    pub timestamp: String,
    pub timestamp_ms: i64,
    pub interfaces: Vec<InterfaceStats>,
    /// 数据分辨率：1s, 10s, 1m
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
}

/// 历史数据响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub namespace: String,
    pub duration_minutes: u32,
    pub count: usize,
    pub data: Vec<TrafficData>,
}

/// 命名空间列表响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespacesResponse {
    pub namespaces: Vec<String>,
}

/// SSE 事件消息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SseMessage {
    #[serde(rename = "type")]
    pub message_type: String,
    pub data: Vec<TrafficData>,
}

/// 数据库配置
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub db_path: String,
    pub retention_raw_minutes: u32,
    pub retention_10s_hours: u32,
    pub retention_1m_hours: u32,
    pub retention_1h_days: u32,
    pub retention_1d_days: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            db_path: "data/traffic_monitor.db".to_string(),
            retention_raw_minutes: 5,
            retention_10s_hours: 1,
            retention_1m_hours: 3,
            retention_1h_days: 7,
            retention_1d_days: 30,
        }
    }
}

/// 采集器配置
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    pub interval_secs: u64,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        CollectorConfig { interval_secs: 1 }
    }
}

/// 服务器配置
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub web_root: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: "0.0.0.0".to_string(),
            port: 18080,
            web_root: "web".to_string(),
        }
    }
}
