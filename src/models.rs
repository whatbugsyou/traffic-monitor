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

/// 数据分辨率
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Resolution {
    /// 实时数据（1秒粒度）
    Realtime,
    /// 10秒聚合
    TenSeconds,
    /// 1分钟聚合
    OneMinute,
    /// 1小时聚合
    OneHour,
}

impl Resolution {
    /// 从时间跨度（分钟）推导合适的分辨率
    pub fn from_duration_minutes(duration_minutes: u32) -> Self {
        match duration_minutes {
            0..=5 => Resolution::Realtime,      // ≤ 5分钟：实时数据
            6..=60 => Resolution::TenSeconds,   // 5-60分钟：10秒聚合
            61..=1440 => Resolution::OneMinute, // 1-24小时：1分钟聚合
            _ => Resolution::OneHour,           // > 24小时：1小时聚合
        }
    }

    /// 获取分辨率的字符串表示
    pub fn as_str(&self) -> &'static str {
        match self {
            Resolution::Realtime => "1s",
            Resolution::TenSeconds => "10s",
            Resolution::OneMinute => "1m",
            Resolution::OneHour => "1h",
        }
    }
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
