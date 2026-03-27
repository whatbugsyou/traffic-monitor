use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::time::interval;

use crate::database::Database;
use crate::models::*;

const INTERVAL_10S: i64 = 10_000; // 10秒（毫秒）
const INTERVAL_1M: i64 = 60_000; // 1分钟（毫秒）
const INTERVAL_1H: i64 = 3_600_000; // 1小时（毫秒）
const INTERVAL_1D: i64 = 86_400_000; // 1天（毫秒）

/// 流量数据采集器
pub struct TrafficCollector {
    db: Arc<Database>,
    config: CollectorConfig,
    namespaces: Arc<RwLock<Vec<String>>>,
    /// 每个命名空间独立的广播通道，实现精准订阅
    data_senders: Arc<RwLock<HashMap<String, broadcast::Sender<TrafficData>>>>,
    shutdown: broadcast::Sender<()>,
}

impl TrafficCollector {
    /// 创建新的采集器
    pub fn new(db: Arc<Database>, config: CollectorConfig) -> Result<Self> {
        let (shutdown, _) = broadcast::channel(1);

        let namespaces = Arc::new(RwLock::new(Vec::new()));
        let data_senders = Arc::new(RwLock::new(HashMap::new()));

        Ok(TrafficCollector {
            db,
            config,
            namespaces,
            data_senders,
            shutdown,
        })
    }

    /// 启动采集器
    pub async fn start(&self) -> Result<()> {
        // 发现所有命名空间
        self.discover_namespaces().await?;

        // 为每个命名空间创建广播通道
        {
            let namespaces = self.namespaces.read().await;
            let mut senders = self.data_senders.write().await;
            for namespace in namespaces.iter() {
                if !senders.contains_key(namespace) {
                    let (sender, _) = broadcast::channel(100);
                    senders.insert(namespace.clone(), sender);
                    log::info!("Created broadcast channel for namespace: {}", namespace);
                }
            }
        }

        // 启动命名空间监控任务
        let namespaces = self.namespaces.read().await.clone();
        for namespace in namespaces {
            self.start_namespace_collector(namespace).await?;
        }

        // 启动命名空间发现任务（定期检查新命名空间）
        self.start_namespace_discovery().await?;

        Ok(())
    }

    /// 发现所有命名空间
    async fn discover_namespaces(&self) -> Result<()> {
        let mut namespaces = vec!["default".to_string()];

        // 读取 /var/run/netns/ 目录
        let netns_dir = Path::new("/var/run/netns");
        if netns_dir.exists() {
            let entries =
                fs::read_dir(netns_dir).context("Failed to read /var/run/netns directory")?;

            for entry in entries.flatten() {
                if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                    if let Some(name) = entry.file_name().to_str() {
                        namespaces.push(name.to_string());
                    }
                }
            }
        }

        namespaces.sort();
        namespaces.dedup();

        let mut ns_guard = self.namespaces.write().await;
        *ns_guard = namespaces;

        log::info!("Discovered {} namespaces", ns_guard.len());
        Ok(())
    }

    /// 启动命名空间发现任务
    async fn start_namespace_discovery(&self) -> Result<()> {
        let namespaces = Arc::clone(&self.namespaces);
        let data_senders = Arc::clone(&self.data_senders);
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut check_interval = interval(Duration::from_secs(60));

            loop {
                tokio::select! {
                    _ = check_interval.tick() => {
                        // 重新发现命名空间
                        let mut new_namespaces = vec!["default".to_string()];

                        if Path::new("/var/run/netns").exists() {
                            if let Ok(entries) = fs::read_dir("/var/run/netns") {
                                for entry in entries.flatten() {
                                    if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                                        if let Some(name) = entry.file_name().to_str() {
                                            new_namespaces.push(name.to_string());
                                        }
                                    }
                                }
                            }
                        }

                        new_namespaces.sort();
                        new_namespaces.dedup();

                        let mut ns_guard = namespaces.write().await;
                        if *ns_guard != new_namespaces {
                            log::info!("Namespaces changed: {:?} -> {:?}", *ns_guard, new_namespaces);

                            // 为新命名空间创建广播通道
                            let mut senders = data_senders.write().await;
                            for namespace in &new_namespaces {
                                if !senders.contains_key(namespace) {
                                    let (sender, _) = broadcast::channel(100);
                                    senders.insert(namespace.clone(), sender);
                                    log::info!("Created broadcast channel for new namespace: {}", namespace);
                                }
                            }

                            *ns_guard = new_namespaces;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Namespace discovery task stopped");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// 启动单个命名空间的采集任务
    async fn start_namespace_collector(&self, namespace: String) -> Result<()> {
        let db = Arc::clone(&self.db);
        let data_senders = Arc::clone(&self.data_senders);
        let mut shutdown_rx = self.shutdown.subscribe();
        let config = self.config.clone();
        let namespace_clone = namespace.clone();

        tokio::spawn(async move {
            let mut collect_interval = interval(Duration::from_secs(config.interval_secs));
            let mut last_10s_ts: Option<i64> = None;
            let mut last_1m_ts: Option<i64> = None;
            let mut last_1h_ts: Option<i64> = None;
            let mut last_1d_ts: Option<i64> = None;

            log::info!("Started collector for namespace: {}", namespace_clone);

            loop {
                tokio::select! {
                    _ = collect_interval.tick() => {
                        match collect_namespace_data(&namespace_clone).await {
                            Ok(data) => {
                                let timestamp_ms = data.timestamp_ms;

                                // 存储原始数据
                                if let Err(e) = db.insert_traffic_data(&data) {
                                    log::error!("Failed to store data for {}: {}", namespace_clone, e);
                                }

                                // 检查是否到达10秒聚合点
                                if should_aggregate(timestamp_ms, last_10s_ts, INTERVAL_10S) {
                                    if let Err(e) = db.insert_10s_aggregated(&data) {
                                        log::error!("Failed to store 10s aggregated data for {}: {}", namespace_clone, e);
                                    }
                                    last_10s_ts = Some(align_timestamp(timestamp_ms, INTERVAL_10S));
                                    log::debug!("Saved 10s aggregated data for {} at {}", namespace_clone, data.timestamp);
                                }

                                // 检查是否到达1分钟聚合点
                                if should_aggregate(timestamp_ms, last_1m_ts, INTERVAL_1M) {
                                    if let Err(e) = db.insert_1m_aggregated(&data) {
                                        log::error!("Failed to store 1m aggregated data for {}: {}", namespace_clone, e);
                                    }
                                    last_1m_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1M));
                                    log::debug!("Saved 1m aggregated data for {} at {}", namespace_clone, data.timestamp);
                                }

                                // 检查是否到达1小时聚合点
                                if should_aggregate(timestamp_ms, last_1h_ts, INTERVAL_1H) {
                                    if let Err(e) = db.insert_1h_aggregated(&data) {
                                        log::error!("Failed to store 1h aggregated data for {}: {}", namespace_clone, e);
                                    }
                                    last_1h_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1H));
                                    log::debug!("Saved 1h aggregated data for {} at {}", namespace_clone, data.timestamp);
                                }

                                // 检查是否到达1天聚合点
                                if should_aggregate(timestamp_ms, last_1d_ts, INTERVAL_1D) {
                                    if let Err(e) = db.insert_1d_aggregated(&data) {
                                        log::error!("Failed to store 1d aggregated data for {}: {}", namespace_clone, e);
                                    }
                                    last_1d_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1D));
                                    log::debug!("Saved 1d aggregated data for {} at {}", namespace_clone, data.timestamp);
                                }

                                // 向该命名空间的订阅者广播数据
                                let senders = data_senders.read().await;
                                if let Some(sender) = senders.get(&namespace_clone) {
                                    match sender.send(data.clone()) {
                                        Ok(subscriber_count) => {
                                            if subscriber_count > 0 {
                                                log::debug!("Broadcasting data for namespace {} to {} subscribers", namespace_clone, subscriber_count);
                                            }
                                        }
                                        Err(e) => {
                                            log::debug!("Failed to broadcast data for namespace {}: {}", namespace_clone, e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("Failed to collect data for {}: {}", namespace_clone, e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Collector stopped for namespace: {}", namespace_clone);
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// 订阅特定命名空间的数据（精准订阅）
    pub async fn subscribe(&self, namespace: &str) -> Option<broadcast::Receiver<TrafficData>> {
        let senders = self.data_senders.read().await;
        if let Some(sender) = senders.get(namespace) {
            log::info!("New subscriber connected for namespace: {}", namespace);
            Some(sender.subscribe())
        } else {
            log::warn!("Namespace not found for subscription: {}", namespace);
            None
        }
    }

    /// 获取所有已发现的命名空间
    pub async fn get_namespaces(&self) -> Vec<String> {
        self.namespaces.read().await.clone()
    }

    /// 停止采集器
    pub fn stop(&self) -> Result<()> {
        self.shutdown
            .send(())
            .context("Failed to send shutdown signal")?;
        Ok(())
    }
}

/// 判断是否应该进行聚合
fn should_aggregate(current_ts: i64, last_ts: Option<i64>, interval_ms: i64) -> bool {
    let current_aligned = align_timestamp(current_ts, interval_ms);

    match last_ts {
        None => true,
        Some(last) => current_aligned > last,
    }
}

/// 将时间戳对齐到指定的聚合间隔
fn align_timestamp(timestamp_ms: i64, interval_ms: i64) -> i64 {
    (timestamp_ms / interval_ms) * interval_ms
}

/// 采集指定命名空间的数据
async fn collect_namespace_data(namespace: &str) -> Result<TrafficData> {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let timestamp_ms = Utc::now().timestamp_millis();

    // 采集接口数据
    let interfaces = collect_interfaces(namespace).await?;

    let data = TrafficData {
        namespace: namespace.to_string(),
        timestamp,
        timestamp_ms,
        interfaces,
        resolution: Some("1s".to_string()),
    };

    Ok(data)
}

/// 采集网络接口数据
async fn collect_interfaces(namespace: &str) -> Result<Vec<InterfaceStats>> {
    let mut interfaces = Vec::new();

    if namespace == "default" {
        // 默认命名空间：直接读取 /sys/class/net/
        collect_interfaces_from_sysfs("/sys/class/net", &mut interfaces)?;
    } else {
        // 其他命名空间：使用 setns 系统调用切换命名空间
        collect_interfaces_via_setns(namespace, &mut interfaces)?;
    }

    Ok(interfaces)
}

/// 从 sysfs 读取接口数据
fn collect_interfaces_from_sysfs(
    base_path: &str,
    interfaces: &mut Vec<InterfaceStats>,
) -> Result<()> {
    let net_dir = Path::new(base_path);

    if !net_dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(net_dir).context("Failed to read /sys/class/net")? {
        let entry = entry?;
        let dev_path = entry.path();

        if !dev_path.is_dir() {
            continue;
        }

        let dev_name = entry.file_name().to_string_lossy().to_string();

        // 排除 lo、VLAN 子接口、veth 别名
        if dev_name == "lo" || dev_name.contains('.') || dev_name.contains('@') {
            continue;
        }

        let stats_path = dev_path.join("statistics");
        if !stats_path.exists() {
            continue;
        }

        let rx_bytes = read_sysfs_value(&stats_path.join("rx_bytes"))?;
        let tx_bytes = read_sysfs_value(&stats_path.join("tx_bytes"))?;

        interfaces.push(InterfaceStats {
            name: dev_name,
            rx_bytes,
            tx_bytes,
            rx_speed: None,
            tx_speed: None,
        });
    }

    Ok(())
}

/// 采集非默认命名空间的网络接口数据（使用 setns 系统调用，无需 fork）
fn collect_interfaces_via_setns(
    namespace: &str,
    interfaces: &mut Vec<InterfaceStats>,
) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    /// 命名空间 guard，drop 时自动恢复原命名空间
    struct NamespaceGuard {
        original_ns_fd: i32,
    }

    impl Drop for NamespaceGuard {
        fn drop(&mut self) {
            // 恢复原命名空间，忽略错误
            unsafe {
                libc::setns(self.original_ns_fd, libc::CLONE_NEWNET);
            }
        }
    }

    // 打开当前命名空间的 fd（用于恢复）
    let original_ns =
        fs::File::open("/proc/self/ns/net").context("Failed to open current namespace")?;
    let original_ns_fd = original_ns.as_raw_fd();

    // 打开目标命名空间
    let target_path = format!("/var/run/netns/{}", namespace);
    let target_ns = fs::File::open(&target_path)
        .with_context(|| format!("Failed to open namespace: {}", target_path))?;

    // 切换到目标命名空间
    unsafe {
        let ret = libc::setns(target_ns.as_raw_fd(), libc::CLONE_NEWNET);
        if ret != 0 {
            return Err(anyhow::anyhow!(
                "Failed to setns to {}: {}",
                namespace,
                std::io::Error::last_os_error()
            ));
        }
    }

    // 创建 guard，确保函数退出时恢复原命名空间
    let _guard = NamespaceGuard { original_ns_fd };

    // 现在已经在目标命名空间中，直接读取 /sys/class/net/
    collect_interfaces_from_sysfs("/sys/class/net", interfaces)
}

/// 读取 sysfs 值
fn read_sysfs_value(path: &Path) -> Result<u64> {
    let value = fs::read_to_string(path)
        .context("Failed to read sysfs value")?
        .trim()
        .parse()
        .context("Failed to parse sysfs value")?;

    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_timestamp() {
        // 测试10秒对齐
        assert_eq!(align_timestamp(12345, INTERVAL_10S), 10000);
        assert_eq!(align_timestamp(19999, INTERVAL_10S), 10000);
        assert_eq!(align_timestamp(20000, INTERVAL_10S), 20000);

        // 测试1分钟对齐
        assert_eq!(align_timestamp(12345, INTERVAL_1M), 0);
        assert_eq!(align_timestamp(60000, INTERVAL_1M), 60000);
        assert_eq!(align_timestamp(119999, INTERVAL_1M), 60000);
        assert_eq!(align_timestamp(120000, INTERVAL_1M), 120000);
    }

    #[test]
    fn test_should_aggregate() {
        // 第一次应该聚合
        assert!(should_aggregate(10000, None, INTERVAL_10S));

        // 同一个时间窗口内不应该再次聚合
        assert!(!should_aggregate(15000, Some(10000), INTERVAL_10S));

        // 进入下一个时间窗口应该聚合
        assert!(should_aggregate(20000, Some(10000), INTERVAL_10S));
        assert!(should_aggregate(25000, Some(10000), INTERVAL_10S));
    }
}
