use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::time::{interval, MissedTickBehavior};

use crate::database::Database;
use crate::models::*;

const INTERVAL_10S: i64 = 10_000; // 10秒（毫秒）
const INTERVAL_1M: i64 = 60_000; // 1分钟（毫秒）
const INTERVAL_1H: i64 = 3_600_000; // 1小时（毫秒）
const INTERVAL_1D: i64 = 86_400_000; // 1天（毫秒）

static COLLECTOR_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
static COLLECTION_ROUND_ID: AtomicU64 = AtomicU64::new(1);

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
            let scheduler_instance_id = COLLECTOR_INSTANCE_ID.fetch_add(1, Ordering::Relaxed);
            let mut collect_interval = interval(Duration::from_secs(config.interval_secs));
            collect_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            log::info!(
                "Started collection scheduler, collector_instance_id: {}",
                scheduler_instance_id
            );

            loop {
                tokio::select! {
                    _ = collect_interval.tick() => {
                        if collection_in_progress.swap(true, Ordering::AcqRel) {
                            log::warn!(
                                "Skipping collection tick because previous round is still running, collector_instance_id: {}",
                                scheduler_instance_id
                            );
                            continue;
                        }

                        let round_id = COLLECTION_ROUND_ID.fetch_add(1, Ordering::Relaxed);
                        let namespaces_snapshot = namespaces.read().await.clone();

                        log::info!(
                            "Starting collection round: {}, collector_instance_id: {}, namespace_count: {}",
                            round_id,
                            scheduler_instance_id,
                            namespaces_snapshot.len()
                        );

                        let results = collect_namespace_round(&namespaces_snapshot).await;

                        for (namespace, result) in results {
                            match result {
                                Ok(mut data) => {
                                    let timestamp_ms = data.timestamp_ms;
                                    log::info!(
                                        "Collector sample ready for namespace: {}, collector_instance_id: {}, round_id: {}, timestamp_ms: {}",
                                        namespace,
                                        scheduler_instance_id,
                                        round_id,
                                        timestamp_ms
                                    );

                                    if let Err(e) = db.insert_traffic_data(&data) {
                                        log::error!(
                                            "Failed to store data for {} (collector_instance_id: {}, round_id: {}): {}",
                                            namespace,
                                            scheduler_instance_id,
                                            round_id,
                                            e
                                        );
                                    }

                                    let mut state_guard = aggregation_state.write().await;
                                    let state = state_guard
                                        .entry(namespace.clone())
                                        .or_insert_with(NamespaceAggregationState::default);

                                    let ch = channels.read().await;
                                    if let Some(namespace_channels) = ch.get(&namespace) {
                                        data.resolution = Some("1s".to_string());
                                        log::info!(
                                            "Sending realtime data for namespace: {}, collector_instance_id: {}, round_id: {}, timestamp_ms: {}",
                                            namespace,
                                            scheduler_instance_id,
                                            round_id,
                                            timestamp_ms
                                        );
                                        let _ = namespace_channels.realtime.send(data.clone());

                                        if should_aggregate(timestamp_ms, state.last_10s_ts, INTERVAL_10S) {
                                            if let Err(e) = db.insert_10s_aggregated(&data) {
                                                log::error!(
                                                    "Failed to store 10s aggregated data for {} (collector_instance_id: {}, round_id: {}): {}",
                                                    namespace,
                                                    scheduler_instance_id,
                                                    round_id,
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
                                                    "Failed to store 1m aggregated data for {} (collector_instance_id: {}, round_id: {}): {}",
                                                    namespace,
                                                    scheduler_instance_id,
                                                    round_id,
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
                                                    "Failed to store 1h aggregated data for {} (collector_instance_id: {}, round_id: {}): {}",
                                                    namespace,
                                                    scheduler_instance_id,
                                                    round_id,
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
                                                    "Failed to store 1d aggregated data for {} (collector_instance_id: {}, round_id: {}): {}",
                                                    namespace,
                                                    scheduler_instance_id,
                                                    round_id,
                                                    e
                                                );
                                            }
                                            state.last_1d_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1D));
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to collect data for {} (collector_instance_id: {}, round_id: {}): {}",
                                        namespace,
                                        scheduler_instance_id,
                                        round_id,
                                        e
                                    );
                                }
                            }
                        }

                        collection_in_progress.store(false, Ordering::Release);
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!(
                            "Collection scheduler stopped, collector_instance_id: {}",
                            scheduler_instance_id
                        );
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
    match namespaces {
        [] => Vec::new(),
        [ns1] => {
            let (r1,) = tokio::join!(collect_namespace_data(ns1));
            vec![(ns1.clone(), r1)]
        }
        [ns1, ns2] => {
            let (r1, r2) = tokio::join!(
                collect_namespace_data(ns1),
                collect_namespace_data(ns2)
            );
            vec![(ns1.clone(), r1), (ns2.clone(), r2)]
        }
        [ns1, ns2, ns3] => {
            let (r1, r2, r3) = tokio::join!(
                collect_namespace_data(ns1),
                collect_namespace_data(ns2),
                collect_namespace_data(ns3)
            );
            vec![(ns1.clone(), r1), (ns2.clone(), r2), (ns3.clone(), r3)]
        }
        [ns1, ns2, ns3, ns4] => {
            let (r1, r2, r3, r4) = tokio::join!(
                collect_namespace_data(ns1),
                collect_namespace_data(ns2),
                collect_namespace_data(ns3),
                collect_namespace_data(ns4)
            );
            vec![
                (ns1.clone(), r1),
                (ns2.clone(), r2),
                (ns3.clone(), r3),
                (ns4.clone(), r4),
            ]
        }
        _ => {
            let mut results = Vec::with_capacity(namespaces.len());

            for chunk in namespaces.chunks(4) {
                match chunk {
                    [ns1, ns2, ns3, ns4] => {
                        let (r1, r2, r3, r4) = tokio::join!(
                            collect_namespace_data(ns1),
                            collect_namespace_data(ns2),
                            collect_namespace_data(ns3),
                            collect_namespace_data(ns4)
                        );
                        results.push((ns1.clone(), r1));
                        results.push((ns2.clone(), r2));
                        results.push((ns3.clone(), r3));
                        results.push((ns4.clone(), r4));
                    }
                    [ns1, ns2, ns3] => {
                        let (r1, r2, r3) = tokio::join!(
                            collect_namespace_data(ns1),
                            collect_namespace_data(ns2),
                            collect_namespace_data(ns3)
                        );
                        results.push((ns1.clone(), r1));
                        results.push((ns2.clone(), r2));
                        results.push((ns3.clone(), r3));
                    }
                    [ns1, ns2] => {
                        let (r1, r2) = tokio::join!(
                            collect_namespace_data(ns1),
                            collect_namespace_data(ns2)
                        );
                        results.push((ns1.clone(), r1));
                        results.push((ns2.clone(), r2));
                    }
                    [ns1] => {
                        let (r1,) = tokio::join!(collect_namespace_data(ns1));
                        results.push((ns1.clone(), r1));
                    }
                    [] => {}
                    _ => unreachable!(),
                }
            }

            results
        }
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
        resolution: None,
    };

    Ok(data)
}

/// 采集网络接口数据
async fn collect_interfaces(namespace: &str) -> Result<Vec<InterfaceStats>> {
    collect_interfaces_via_setns(namespace).await
}



/// 采集网络接口数据（默认命名空间和其他命名空间统一走 setns 流程）
async fn collect_interfaces_via_setns(namespace: &str) -> Result<Vec<InterfaceStats>> {
    use std::fs;
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
    use tokio::io::AsyncReadExt;

    let namespace = namespace.to_string();

    // ===== 第一步：同步操作（在当前线程中完成）=====
    log::debug!("Starting sync phase for namespace: {}", namespace);

    // 1. 打开原命名空间 fd（用于恢复）
    let original_ns = fs::File::open("/proc/self/ns/net")
        .context("Failed to open current namespace")?;
    let original_ns_fd = original_ns.as_raw_fd();
    log::debug!("Opened original namespace fd: {}", original_ns_fd);

    // 2. 如果不是 default，则打开并切换到目标命名空间
    let target_ns = if namespace == "default" {
        None
    } else {
        let target_path = format!("/var/run/netns/{}", namespace);
        let target_ns = fs::File::open(&target_path)
            .with_context(|| format!("Failed to open namespace: {}", target_path))?;
        log::debug!("Opened target namespace: {}", target_path);

        log::debug!("Switching to namespace: {}", namespace);
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
        log::debug!("Successfully switched to namespace: {}", namespace);
        Some(target_ns)
    };

    // 3. 在当前命名空间视图中收集所有需要读取的文件 fd
    let mut file_fds: Vec<(String, OwnedFd, OwnedFd, OwnedFd, OwnedFd)> = Vec::new(); // (name, rx_bytes_fd, tx_bytes_fd, rx_dropped_fd, tx_dropped_fd)

    let net_dir = Path::new("/sys/class/net");
    if net_dir.exists() {
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

            // 打开所有需要的文件，并持有 fd 所有权，避免离开作用域后被提前关闭
            let rx_bytes_fd: OwnedFd = match fs::File::open(stats_path.join("rx_bytes")) {
                Ok(f) => f.into(),
                Err(_) => continue,
            };
            let tx_bytes_fd: OwnedFd = match fs::File::open(stats_path.join("tx_bytes")) {
                Ok(f) => f.into(),
                Err(_) => continue,
            };
            let rx_dropped_fd: OwnedFd = match fs::File::open(stats_path.join("rx_dropped")) {
                Ok(f) => f.into(),
                Err(_) => continue,
            };
            let tx_dropped_fd: OwnedFd = match fs::File::open(stats_path.join("tx_dropped")) {
                Ok(f) => f.into(),
                Err(_) => continue,
            };

            log::debug!(
                "Opened interface stats fds for {} in namespace {}: rx_bytes={}, tx_bytes={}, rx_dropped={}, tx_dropped={}",
                dev_name,
                namespace,
                rx_bytes_fd.as_raw_fd(),
                tx_bytes_fd.as_raw_fd(),
                rx_dropped_fd.as_raw_fd(),
                tx_dropped_fd.as_raw_fd()
            );

            file_fds.push((dev_name, rx_bytes_fd, tx_bytes_fd, rx_dropped_fd, tx_dropped_fd));
        }
    }

    // 4. 如果发生过切换，则切换回原命名空间
    if target_ns.is_some() {
        log::debug!(
            "Switching back to original namespace, collected {} interfaces from {}",
            file_fds.len(),
            namespace
        );
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
        log::debug!("Switched back to original namespace");
    }

    // ===== 第二步：异步并发读取（可以在任意线程执行）=====
    log::debug!(
        "Starting async read phase for {} interfaces in namespace: {}",
        file_fds.len(),
        namespace
    );

    let mut interfaces = Vec::new();

    for (dev_name, rx_bytes_fd, tx_bytes_fd, rx_dropped_fd, tx_dropped_fd) in file_fds {
        let mut rx_bytes_file = unsafe { tokio::fs::File::from_raw_fd(rx_bytes_fd.into_raw_fd()) };
        let mut tx_bytes_file = unsafe { tokio::fs::File::from_raw_fd(tx_bytes_fd.into_raw_fd()) };
        let mut rx_dropped_file = unsafe { tokio::fs::File::from_raw_fd(rx_dropped_fd.into_raw_fd()) };
        let mut tx_dropped_file = unsafe { tokio::fs::File::from_raw_fd(tx_dropped_fd.into_raw_fd()) };

        log::debug!("Concurrently reading 4 files for interface: {}", dev_name);
        let (rx_bytes_res, tx_bytes_res, rx_dropped_res, tx_dropped_res) = tokio::join!(
            async {
                let mut s = String::new();
                rx_bytes_file.read_to_string(&mut s).await?;
                Ok::<_, std::io::Error>(s.trim().parse::<u64>())
            },
            async {
                let mut s = String::new();
                tx_bytes_file.read_to_string(&mut s).await?;
                Ok::<_, std::io::Error>(s.trim().parse::<u64>())
            },
            async {
                let mut s = String::new();
                rx_dropped_file.read_to_string(&mut s).await?;
                Ok::<_, std::io::Error>(s.trim().parse::<u64>())
            },
            async {
                let mut s = String::new();
                tx_dropped_file.read_to_string(&mut s).await?;
                Ok::<_, std::io::Error>(s.trim().parse::<u64>())
            }
        );

        let rx_bytes = rx_bytes_res
            .context("Failed to read rx_bytes file")?
            .context("Failed to parse rx_bytes value")?;
        let tx_bytes = tx_bytes_res
            .context("Failed to read tx_bytes file")?
            .context("Failed to parse tx_bytes value")?;
        let rx_dropped = rx_dropped_res
            .context("Failed to read rx_dropped file")?
            .context("Failed to parse rx_dropped value")?;
        let tx_dropped = tx_dropped_res
            .context("Failed to read tx_dropped file")?
            .context("Failed to parse tx_dropped value")?;

        log::debug!(
            "Successfully collected stats for interface: {} in namespace {} (rx={}, tx={})",
            dev_name,
            namespace,
            rx_bytes,
            tx_bytes
        );

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
