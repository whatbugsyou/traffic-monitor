use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Semaphore, broadcast, RwLock};
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};

use crate::database::Database;
use crate::models::*;

const INTERVAL_10S: i64 = 10_000; // 10秒（毫秒）
const INTERVAL_1M: i64 = 60_000; // 1分钟（毫秒）
const INTERVAL_1H: i64 = 3_600_000; // 1小时（毫秒）
const INTERVAL_1D: i64 = 86_400_000; // 1天（毫秒）

/// 每个命名空间的多粒度广播通道
struct ResolutionChannels {
    /// 实时数据通道（1秒粒度）
    realtime: broadcast::Sender<TrafficData>,
    /// 10秒聚合通道
    agg_10s: broadcast::Sender<TrafficData>,
    /// 1分钟聚合通道
    agg_1m: broadcast::Sender<TrafficData>,
    /// 1小时聚合通道
    agg_1h: broadcast::Sender<TrafficData>,
}

impl ResolutionChannels {
    fn new() -> Self {
        let (realtime, _) = broadcast::channel(100);
        let (agg_10s, _) = broadcast::channel(100);
        let (agg_1m, _) = broadcast::channel(100);
        let (agg_1h, _) = broadcast::channel(100);

        ResolutionChannels {
            realtime,
            agg_10s,
            agg_1m,
            agg_1h,
        }
    }

    /// 获取指定分辨率的发送器
    fn get_sender(&self, resolution: Resolution) -> &broadcast::Sender<TrafficData> {
        match resolution {
            Resolution::Realtime => &self.realtime,
            Resolution::TenSeconds => &self.agg_10s,
            Resolution::OneMinute => &self.agg_1m,
            Resolution::OneHour => &self.agg_1h,
        }
    }

    /// 订阅指定分辨率的数据
    fn subscribe(&self, resolution: Resolution) -> broadcast::Receiver<TrafficData> {
        self.get_sender(resolution).subscribe()
    }
}

/// 流量数据采集器
pub struct TrafficCollector {
    db: Arc<Database>,
    config: CollectorConfig,
    namespaces: Arc<RwLock<Vec<String>>>,
    /// 每个命名空间的多粒度广播通道
    channels: Arc<RwLock<HashMap<String, ResolutionChannels>>>,
    aggregation_state: Arc<RwLock<HashMap<String, NamespaceAggregationState>>>,
    collection_in_progress: Arc<AtomicBool>,
    shutdown: broadcast::Sender<()>,
}

impl TrafficCollector {
    /// 创建新的采集器
    pub fn new(db: Arc<Database>, config: CollectorConfig) -> Result<Self> {
        let (shutdown, _) = broadcast::channel(1);
        let namespaces = Arc::new(RwLock::new(Vec::new()));
        let channels = Arc::new(RwLock::new(HashMap::new()));
        let aggregation_state = Arc::new(RwLock::new(HashMap::new()));
        let collection_in_progress = Arc::new(AtomicBool::new(false));

        Ok(TrafficCollector {
            db,
            config,
            namespaces,
            channels,
            aggregation_state,
            collection_in_progress,
            shutdown,
        })
    }

    /// 启动采集器
    pub async fn start(&self) -> Result<()> {
        // 发现所有命名空间
        self.discover_namespaces().await?;

        // 为每个命名空间创建多粒度广播通道和聚合状态
        self.ensure_namespace_runtime_state().await;

        // 启动统一采集调度任务
        self.start_collection_scheduler().await?;

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
        let channels = Arc::clone(&self.channels);
        let aggregation_state = Arc::clone(&self.aggregation_state);
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut check_interval = interval(Duration::from_secs(60));
            check_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

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

                            let mut ch = channels.write().await;
                            let mut agg = aggregation_state.write().await;
                            for namespace in &new_namespaces {
                                if !ch.contains_key(namespace) {
                                    ch.insert(namespace.clone(), ResolutionChannels::new());
                                    log::info!("Created channels for new namespace: {}", namespace);
                                }
                                agg.entry(namespace.clone()).or_insert_with(NamespaceAggregationState::default);
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

    /// 启动统一采集调度任务
    async fn start_collection_scheduler(&self) -> Result<()> {
        let db = Arc::clone(&self.db);
        let channels = Arc::clone(&self.channels);
        let namespaces = Arc::clone(&self.namespaces);
        let aggregation_state = Arc::clone(&self.aggregation_state);
        let collection_in_progress = Arc::clone(&self.collection_in_progress);
        let mut shutdown_rx = self.shutdown.subscribe();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut collect_interval = interval(Duration::from_secs(config.interval_secs));
            collect_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            log::info!("Started collection scheduler");

            loop {
                tokio::select! {
                    _ = collect_interval.tick() => {
                        if collection_in_progress.swap(true, Ordering::AcqRel) {
                            log::warn!("Skipping collection tick because previous round is still running");
                            continue;
                        }

                        let namespaces_snapshot = namespaces.read().await.clone();

                        log::debug!(
                            "Starting collection round for {} namespaces",
                            namespaces_snapshot.len()
                        );

                        let results = collect_namespace_round(&namespaces_snapshot).await;

                        for (namespace, result) in results {
                            match result {
                                Ok(mut data) => {
                                    let timestamp_ms = data.timestamp_ms;

                                    if let Err(e) = db.insert_traffic_data(&data) {
                                        log::error!("Failed to store data for {}: {}", namespace, e);
                                    }

                                    let mut state_guard = aggregation_state.write().await;
                                    let state = state_guard
                                        .entry(namespace.clone())
                                        .or_insert_with(NamespaceAggregationState::default);

                                    let ch = channels.read().await;
                                    if let Some(namespace_channels) = ch.get(&namespace) {
                                        data.resolution = Some("1s".to_string());
                                        let _ = namespace_channels.realtime.send(data.clone());

                                        if should_aggregate(timestamp_ms, state.last_10s_ts, INTERVAL_10S) {
                                            if let Err(e) = db.insert_10s_aggregated(&data) {
                                                log::error!(
                                                    "Failed to store 10s aggregated data for {}: {}",
                                                    namespace,
                                                    e
                                                );
                                            }
                                            state.last_10s_ts = Some(align_timestamp(timestamp_ms, INTERVAL_10S));
                                            data.resolution = Some("10s".to_string());
                                            let _ = namespace_channels.agg_10s.send(data.clone());
                                        }

                                        if should_aggregate(timestamp_ms, state.last_1m_ts, INTERVAL_1M) {
                                            if let Err(e) = db.insert_1m_aggregated(&data) {
                                                log::error!(
                                                    "Failed to store 1m aggregated data for {}: {}",
                                                    namespace,
                                                    e
                                                );
                                            }
                                            state.last_1m_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1M));
                                            data.resolution = Some("1m".to_string());
                                            let _ = namespace_channels.agg_1m.send(data.clone());
                                        }

                                        if should_aggregate(timestamp_ms, state.last_1h_ts, INTERVAL_1H) {
                                            if let Err(e) = db.insert_1h_aggregated(&data) {
                                                log::error!(
                                                    "Failed to store 1h aggregated data for {}: {}",
                                                    namespace,
                                                    e
                                                );
                                            }
                                            state.last_1h_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1H));
                                            data.resolution = Some("1h".to_string());
                                            let _ = namespace_channels.agg_1h.send(data.clone());
                                        }

                                        if should_aggregate(timestamp_ms, state.last_1d_ts, INTERVAL_1D) {
                                            if let Err(e) = db.insert_1d_aggregated(&data) {
                                                log::error!(
                                                    "Failed to store 1d aggregated data for {}: {}",
                                                    namespace,
                                                    e
                                                );
                                            }
                                            state.last_1d_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1D));
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::error!("Failed to collect data for {}: {}", namespace, e);
                                }
                            }
                        }

                        collection_in_progress.store(false, Ordering::Release);
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Collection scheduler stopped");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    async fn ensure_namespace_runtime_state(&self) {
        let namespaces = self.namespaces.read().await.clone();
        let mut channels = self.channels.write().await;
        let mut aggregation_state = self.aggregation_state.write().await;

        for namespace in namespaces {
            if !channels.contains_key(&namespace) {
                channels.insert(namespace.clone(), ResolutionChannels::new());
                log::info!("Created multi-resolution channels for namespace: {}", namespace);
            }
            aggregation_state
                .entry(namespace)
                .or_insert_with(NamespaceAggregationState::default);
        }
    }

    /// 订阅特定命名空间和分辨率的数据（精准订阅）
    pub async fn subscribe(
        &self,
        namespace: &str,
        resolution: Resolution,
    ) -> Option<broadcast::Receiver<TrafficData>> {
        let channels = self.channels.read().await;
        if let Some(ch) = channels.get(namespace) {
            log::info!(
                "New subscriber connected for namespace: {}, resolution: {:?}",
                namespace,
                resolution
            );
            Some(ch.subscribe(resolution))
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

#[derive(Debug, Default, Clone)]
struct NamespaceAggregationState {
    last_10s_ts: Option<i64>,
    last_1m_ts: Option<i64>,
    last_1h_ts: Option<i64>,
    last_1d_ts: Option<i64>,
}

async fn collect_namespace_round(namespaces: &[String]) -> Vec<(String, Result<TrafficData>)> {
    if namespaces.is_empty() {
        return Vec::new();
    }

    let max_concurrency = namespaces.len().min(8).max(1);
    let semaphore = Arc::new(Semaphore::new(max_concurrency));
    let mut join_set = JoinSet::new();

    for namespace in namespaces.iter().cloned() {
        let semaphore = Arc::clone(&semaphore);
        join_set.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .context("Failed to acquire namespace collection permit")?;
            let result = collect_namespace_data(&namespace).await;
            Ok::<_, anyhow::Error>((namespace, result))
        });
    }

    let mut results = Vec::with_capacity(namespaces.len());

    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok((namespace, result))) => results.push((namespace, result)),
            Ok(Err(err)) => {
                results.push((
                    "unknown".to_string(),
                    Err(err.context("Namespace collection task failed before execution")),
                ));
            }
            Err(err) => {
                results.push((
                    "unknown".to_string(),
                    Err(anyhow::anyhow!("Namespace collection task join failed: {}", err)),
                ));
            }
        }
    }

    results.sort_by(|a, b| a.0.cmp(&b.0));
    results
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
        resolution: None,
    };

    Ok(data)
}

/// 采集网络接口数据
async fn collect_interfaces(namespace: &str) -> Result<Vec<InterfaceStats>> {
    collect_interfaces_via_setns(namespace)
}

fn collect_interfaces_via_setns(namespace: &str) -> Result<Vec<InterfaceStats>> {
    use std::io::Read;
    use std::os::fd::AsRawFd;

    let namespace = namespace.to_string();

    log::debug!("Starting namespace collection for {}", namespace);

    let original_ns = fs::File::open("/proc/self/ns/net")
        .context("Failed to open current namespace")?;
    let original_ns_fd = original_ns.as_raw_fd();

    let target_ns = if namespace == "default" {
        None
    } else {
        let target_path = format!("/var/run/netns/{}", namespace);
        let target_ns = fs::File::open(&target_path)
            .with_context(|| format!("Failed to open namespace: {}", target_path))?;

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

        Some(target_ns)
    };

    let result = (|| -> Result<Vec<InterfaceStats>> {
        let mut net_dev_content = String::new();
        fs::File::open("/proc/net/dev")
            .context("Failed to open /proc/net/dev")?
            .read_to_string(&mut net_dev_content)
            .context("Failed to read /proc/net/dev")?;

        let mut interfaces = Vec::new();

        for line in net_dev_content.lines().skip(2) {
            let Some((dev_name_raw, stats_raw)) = line.split_once(':') else {
                continue;
            };

            let dev_name = dev_name_raw.trim().to_string();

            if dev_name == "lo" || dev_name.contains('.') || dev_name.contains('@') {
                continue;
            }

            let fields: Vec<&str> = stats_raw.split_whitespace().collect();
            if fields.len() < 16 {
                continue;
            }

            let rx_bytes = fields[0]
                .parse::<u64>()
                .with_context(|| format!("Failed to parse rx_bytes for interface {}", dev_name))?;
            let rx_dropped = fields[3]
                .parse::<u64>()
                .with_context(|| format!("Failed to parse rx_dropped for interface {}", dev_name))?;
            let tx_bytes = fields[8]
                .parse::<u64>()
                .with_context(|| format!("Failed to parse tx_bytes for interface {}", dev_name))?;
            let tx_dropped = fields[11]
                .parse::<u64>()
                .with_context(|| format!("Failed to parse tx_dropped for interface {}", dev_name))?;

            interfaces.push(InterfaceStats {
                name: dev_name,
                rx_bytes,
                tx_bytes,
                rx_dropped,
                tx_dropped,
                rx_speed: None,
                tx_speed: None,
                rx_dropped_speed: None,
                tx_dropped_speed: None,
            });
        }

        Ok(interfaces)
    })();

    if target_ns.is_some() {
        unsafe {
            let ret = libc::setns(original_ns_fd, libc::CLONE_NEWNET);
            if ret != 0 {
                return Err(anyhow::anyhow!(
                    "Failed to switch back to original namespace from {}: {}",
                    namespace,
                    std::io::Error::last_os_error()
                ));
            }
        }
    }

    let interfaces = result?;
    log::debug!(
        "Completed collection for namespace: {}, total {} interfaces",
        namespace,
        interfaces.len()
    );
    Ok(interfaces)
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
