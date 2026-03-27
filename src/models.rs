use serde::{Deserialize, Serialize};

/// 网络接口统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterfaceStats {
    pub name: String,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    /// 接收丢包数（累计值）
    pub rx_dropped: u64,
    /// 发送丢包数（累计值）
    pub tx_dropped: u64,
    /// 接收速度 (bytes/s)，查询时计算
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rx_speed: Option<u64>,
    /// 发送速度 (bytes/s)，查询时计算
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_speed: Option<u64>,
    /// 接收丢包速度 (包/s)，查询时计算
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rx_dropped_speed: Option<u64>,
    /// 发送丢包速度 (包/s)，查询时计算
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_dropped_speed: Option<u64>,
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

/// 计算接口速度（基于前后数据的字节差和时间差）
pub fn calculate_interface_speeds(current: &mut TrafficData, prev: Option<&TrafficData>) {
    if let Some(prev) = prev {
        let interval_ms = current.timestamp_ms - prev.timestamp_ms;
        if interval_ms > 0 {
            for iface in &mut current.interfaces {
                if let Some(prev_iface) = prev.interfaces.iter().find(|i| i.name == iface.name) {
                    // 计算流量速度
                    let rx_speed = iface
                        .rx_bytes
                        .saturating_sub(prev_iface.rx_bytes)
                        .saturating_mul(1000)
                        / interval_ms as u64;
                    let tx_speed = iface
                        .tx_bytes
                        .saturating_sub(prev_iface.tx_bytes)
                        .saturating_mul(1000)
                        / interval_ms as u64;

                    iface.rx_speed = Some(rx_speed);
                    iface.tx_speed = Some(tx_speed);

                    // 计算丢包速度（每秒丢包数）
                    let rx_dropped_speed = iface
                        .rx_dropped
                        .saturating_sub(prev_iface.rx_dropped)
                        .saturating_mul(1000)
                        / interval_ms as u64;
                    let tx_dropped_speed = iface
                        .tx_dropped
                        .saturating_sub(prev_iface.tx_dropped)
                        .saturating_mul(1000)
                        / interval_ms as u64;

                    iface.rx_dropped_speed = Some(rx_dropped_speed);
                    iface.tx_dropped_speed = Some(tx_dropped_speed);
                }
            }
        }
    }
}

/// 计算数据列表的速度（遍历列表，依次计算每条数据的速度）
pub fn calculate_speeds_for_list(data_list: &mut [TrafficData]) {
    for i in 0..data_list.len() {
        if i > 0 {
            // 获取前后数据的引用（需要 unsafe 因为 Rust 不允许同时可变和不可变引用）
            // 使用 split_at_mut 来安全地分割切片
            let (left, right) = data_list.split_at_mut(i);
            let prev = &left[i - 1];
            let current = &mut right[0];
            calculate_interface_speeds(current, Some(prev));
        }
    }
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
