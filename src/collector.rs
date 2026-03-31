use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};

use crate::database::Database;
use crate::models::*;

const INTERVAL_10S: i64 = 10_000; // 10秒（毫秒）
const INTERVAL_1M: i64 = 60_000; // 1分钟（毫秒）
const INTERVAL_1H: i64 = 3_600_000; // 1小时（毫秒）
const INTERVAL_1D: i64 = 86_400_000; // 1天（毫秒）
const MAX_NAMESPACE_CONCURRENCY: usize = 20;


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
        self.discover_namespaces().await?;
        self.ensure_namespace_runtime_state().await;
        self.start_collection_scheduler().await?;
        self.start_namespace_discovery().await?;
        Ok(())
    }

    /// 发现所有命名空间
    async fn discover_namespaces(&self) -> Result<()> {
        let namespaces = scan_namespaces()?;

        let mut ns_guard = self.namespaces.write().await;
        if *ns_guard != namespaces {
            log::info!("Namespaces changed: {:?} -> {:?}", *ns_guard, namespaces);
        }
        *ns_guard = namespaces;

        log::info!("Discovered {} namespaces", ns_guard.len());
        drop(ns_guard);

        self.ensure_namespace_runtime_state().await;
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
                        match scan_namespaces() {
                            Ok(new_namespaces) => {
                                let mut ns_guard = namespaces.write().await;
                                if *ns_guard != new_namespaces {
                                    log::info!("Namespaces changed: {:?} -> {:?}", *ns_guard, new_namespaces);

                                    let active_namespaces: HashSet<&str> =
                                        new_namespaces.iter().map(String::as_str).collect();

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
                            Err(e) => {
                                log::error!("Failed to refresh namespaces: {}", e);
                            }
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

                        let _collection_guard = CollectionProgressGuard::new(Arc::clone(&collection_in_progress));
                        let round_started_at = Instant::now();
                        let namespaces_snapshot = namespaces.read().await.clone();

                        let results = collect_namespace_round(&namespaces_snapshot).await;

                        let mut raw_data: Vec<TrafficData> = Vec::new();
                        let mut data_10s: Vec<TrafficData> = Vec::new();
                        let mut data_1m: Vec<TrafficData> = Vec::new();
                        let mut data_1h: Vec<TrafficData> = Vec::new();
                        let mut data_1d: Vec<TrafficData> = Vec::new();
                        let mut namespace_count = 0usize;
                        let mut failures = 0usize;

                        for (namespace, result) in results {
                            match result {
                                Ok(mut data) => {
                                    namespace_count += 1;
                                    let timestamp_ms = data.timestamp_ms;

                                    let mut state_guard = aggregation_state.write().await;
                                    let state = state_guard
                                        .entry(namespace.clone())
                                        .or_insert_with(NamespaceAggregationState::default);

                                    let channels_guard = channels.read().await;
                                    if let Some(namespace_channels) = channels_guard.get(&namespace) {
                                        data.resolution = Some(Resolution::Realtime.as_str().to_string());
                                        let _ = namespace_channels.realtime.send(data.clone());

                                        if should_aggregate(timestamp_ms, state.last_10s_ts, INTERVAL_10S) {
                                            state.last_10s_ts = Some(align_timestamp(timestamp_ms, INTERVAL_10S));
                                            data.resolution = Some(Resolution::TenSeconds.as_str().to_string());
                                            data_10s.push(data.clone());
                                            let _ = namespace_channels.agg_10s.send(data.clone());
                                        }

                                        if should_aggregate(timestamp_ms, state.last_1m_ts, INTERVAL_1M) {
                                            state.last_1m_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1M));
                                            data.resolution = Some(Resolution::OneMinute.as_str().to_string());
                                            data_1m.push(data.clone());
                                            let _ = namespace_channels.agg_1m.send(data.clone());
                                        }

                                        if should_aggregate(timestamp_ms, state.last_1h_ts, INTERVAL_1H) {
                                            state.last_1h_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1H));
                                            data.resolution = Some(Resolution::OneHour.as_str().to_string());
                                            data_1h.push(data.clone());
                                            let _ = namespace_channels.agg_1h.send(data.clone());
                                        }

                                        if should_aggregate(timestamp_ms, state.last_1d_ts, INTERVAL_1D) {
                                            state.last_1d_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1D));
                                            data_1d.push(data.clone());
                                        }

                                        raw_data.push(data);
                                    }
                                }
                                Err(e) => {
                                    failures += 1;
                                    log::error!("Failed to collect data for {}: {}", namespace, e);
                                }
                            }
                        }

                        if let Err(e) = db.insert_round_batch(
                            &raw_data,
                            &data_10s,
                            &data_1m,
                            &data_1h,
                            &data_1d,
                        ) {
                            log::error!("Failed to store round batch data: {}", e);
                        }

                        log::info!(
                            "Collection round finished in {} ms for {} namespaces ({} failures)",
                            round_started_at.elapsed().as_millis(),
                            namespace_count,
                            failures
                        );
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

        for namespace in namespaces.iter() {
            if !channels.contains_key(namespace) {
                channels.insert(namespace.clone(), ResolutionChannels::new());
                log::info!(
                    "Created multi-resolution channels for namespace: {}",
                    namespace
                );
            }
            aggregation_state
                .entry(namespace.clone())
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

struct CollectionProgressGuard {
    flag: Arc<AtomicBool>,
}

impl CollectionProgressGuard {
    fn new(flag: Arc<AtomicBool>) -> Self {
        Self { flag }
    }
}

impl Drop for CollectionProgressGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

fn scan_namespaces() -> Result<Vec<String>> {
    let mut namespaces = vec!["default".to_string()];

    let netns_dir = Path::new("/var/run/netns");
    if netns_dir.exists() {
        let entries = fs::read_dir(netns_dir).context("Failed to read /var/run/netns directory")?;

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
    Ok(namespaces)
}

async fn collect_namespace_round(
    namespaces: &[String],
) -> Vec<(String, Result<TrafficData>)> {
    let mut results = Vec::with_capacity(namespaces.len());
    let semaphore = Arc::new(Semaphore::new(MAX_NAMESPACE_CONCURRENCY));
    let mut join_set = JoinSet::new();
    let collect_started_at = Instant::now();

    for namespace in namespaces {
        let namespace = namespace.clone();

        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("collection semaphore closed unexpectedly");

        join_set.spawn_blocking(move || {
            let _permit = permit;
            let result = collect_namespace_data_blocking(&namespace);
            (namespace, result)
        });
    }

    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok((namespace, result)) => results.push((namespace, result)),
            Err(e) => {
                log::error!("Collection task failed: {}", e);
            }
        }
    }

    log::debug!(
        "Collection timing [round] collect_namespace_round finished in {} ms for {} namespaces",
        collect_started_at.elapsed().as_millis(),
        namespaces.len()
    );

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

fn collect_namespace_data_blocking(namespace: &str) -> Result<TrafficData> {
    let timestamp = Utc::now();
    let namespace_name = namespace.to_string();

    let interfaces = collect_interfaces_via_ip_netns_exec(namespace)?;

    Ok(TrafficData {
        namespace: namespace_name,
        timestamp: timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
        timestamp_ms: timestamp.timestamp_millis(),
        interfaces,
        resolution: None,
    })
}

fn collect_interfaces_via_ip_netns_exec(namespace: &str) -> Result<Vec<InterfaceStats>> {
    let output = if namespace == "default" {
        Command::new("cat")
            .arg("/proc/net/dev")
            .output()
            .context("Failed to execute cat /proc/net/dev")?
    } else {
        Command::new("ip")
            .args(["netns", "exec", namespace, "cat", "/proc/net/dev"])
            .output()
            .with_context(|| format!("Failed to execute ip netns exec {} cat /proc/net/dev", namespace))?
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let message = if stderr.is_empty() {
            format!("Command exited with status {}", output.status)
        } else {
            stderr
        };

        return Err(anyhow!(
            "Failed to collect network data for namespace {}: {}",
            namespace,
            message
        ));
    }

    let content = String::from_utf8(output.stdout)
        .with_context(|| format!("Failed to decode /proc/net/dev output for namespace {}", namespace))?;

    parse_interfaces_from_proc_net_dev(&content)
}

fn parse_interfaces_from_proc_net_dev(content: &str) -> Result<Vec<InterfaceStats>> {
    let mut interfaces = Vec::new();

    for line in content.lines().skip(2) {
        let Some((dev_name_raw, stats_raw)) = line.split_once(':') else {
            continue;
        };

        let dev_name = dev_name_raw.trim().to_string();

        if dev_name == "lo" {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_timestamp() {
        assert_eq!(align_timestamp(12345, INTERVAL_10S), 10000);
        assert_eq!(align_timestamp(19999, INTERVAL_10S), 10000);
        assert_eq!(align_timestamp(20000, INTERVAL_10S), 20000);

        assert_eq!(align_timestamp(12345, INTERVAL_1M), 0);
        assert_eq!(align_timestamp(60000, INTERVAL_1M), 60000);
        assert_eq!(align_timestamp(119999, INTERVAL_1M), 60000);
        assert_eq!(align_timestamp(120000, INTERVAL_1M), 120000);
    }

    #[test]
    fn test_should_aggregate() {
        assert!(should_aggregate(10000, None, INTERVAL_10S));
        assert!(!should_aggregate(15000, Some(10000), INTERVAL_10S));
        assert!(should_aggregate(20000, Some(10000), INTERVAL_10S));
        assert!(should_aggregate(25000, Some(10000), INTERVAL_10S));
    }

    #[test]
    fn test_parse_interfaces_from_proc_net_dev() {
        let content = r#"
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 100 1 0 2 0 0 0 0 200 3 0 4 0 0 0 0
  eth0: 1000 10 0 1 0 0 0 0 2000 20 0 2 0 0 0 0
 eth0.1: 500 5 0 0 0 0 0 0 600 6 0 0 0 0 0 0
veth1234@if5: 700 7 0 0 0 0 0 0 800 8 0 0 0 0 0 0
"#;

        let interfaces = parse_interfaces_from_proc_net_dev(content).unwrap();

        assert_eq!(interfaces.len(), 3);
        assert_eq!(interfaces[0].name, "eth0");
        assert_eq!(interfaces[0].rx_bytes, 1000);
        assert_eq!(interfaces[0].tx_bytes, 2000);
        assert_eq!(interfaces[0].rx_dropped, 1);
        assert_eq!(interfaces[0].tx_dropped, 2);

        assert_eq!(interfaces[1].name, "eth0.1");
        assert_eq!(interfaces[2].name, "veth1234@if5");
    }

}
