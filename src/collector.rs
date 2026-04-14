//! 流量采集器核心模块
//!
//! 本模块负责从 Linux 网络命名空间中采集网络流量数据，
//! 采用职责分离架构：采集层、处理层、存储层、广播层独立运行。
//!
//! 数据流：
//! Scheduler ─► Collector ─► mpsc ─► Processor ─► mpsc ─► Storage
//!                                      │
//!                                      ▼
//!                               Broadcaster ─► 前端

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};

use crate::database::Database;
use crate::models::{CollectorConfig, RawTrafficData, Resolution, TrafficData};
use crate::netlink_client::NamespaceNetlinkClient;

// ============================================================================
// 常量定义
// ============================================================================

/// 10秒聚合间隔（单位：毫秒）
const INTERVAL_10S: i64 = 10_000;
/// 1分钟聚合间隔（单位：毫秒）
const INTERVAL_1M: i64 = 60_000;
/// 1小时聚合间隔（单位：毫秒）
const INTERVAL_1H: i64 = 3_600_000;
/// 1天聚合间隔（单位：毫秒）
const INTERVAL_1D: i64 = 86_400_000;
/// 命名空间并发采集上限
const MAX_NAMESPACE_CONCURRENCY: usize = 20;
/// 采集结果 channel 缓冲区大小
const COLLECTION_CHANNEL_SIZE: usize = 100;
/// 存储请求 channel 缓冲区大小
const STORAGE_CHANNEL_SIZE: usize = 100;
/// 广播通道缓冲区大小
const BROADCAST_CHANNEL_SIZE: usize = 100;

// ============================================================================
// 数据结构定义
// ============================================================================

/// 采集结果消息（采集层 → 处理层）
struct CollectionMessage {
    namespace: String,
    result: Result<RawTrafficData>,
}

/// 存储请求消息（处理层 → 存储层）
struct StorageMessage {
    data: RawTrafficData,
    resolutions: Vec<Resolution>,
    /// 1天聚合数据（单独处理，不需要广播）
    data_1d: Option<RawTrafficData>,
}

/// 广播请求消息（处理层 → 广播层）
struct BroadcastMessage {
    namespace: String,
    data: TrafficData,
    resolution: Resolution,
}

/// 管理不同分辨率的广播通道
#[derive(Clone)]
struct ResolutionChannels {
    realtime: broadcast::Sender<TrafficData>,
    agg_10s: broadcast::Sender<TrafficData>,
    agg_1m: broadcast::Sender<TrafficData>,
    agg_1h: broadcast::Sender<TrafficData>,
}

impl ResolutionChannels {
    fn new() -> Self {
        let (realtime, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
        let (agg_10s, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
        let (agg_1m, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
        let (agg_1h, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);

        ResolutionChannels {
            realtime,
            agg_10s,
            agg_1m,
            agg_1h,
        }
    }

    fn get_sender(&self, resolution: Resolution) -> &broadcast::Sender<TrafficData> {
        match resolution {
            Resolution::Realtime => &self.realtime,
            Resolution::TenSeconds => &self.agg_10s,
            Resolution::OneMinute => &self.agg_1m,
            Resolution::OneHour => &self.agg_1h,
        }
    }

    fn subscribe(&self, resolution: Resolution) -> broadcast::Receiver<TrafficData> {
        self.get_sender(resolution).subscribe()
    }
}

/// 单个命名空间的聚合状态
#[derive(Debug, Default, Clone)]
struct NamespaceAggregationState {
    last_10s_ts: Option<i64>,
    last_1m_ts: Option<i64>,
    last_1h_ts: Option<i64>,
    last_1d_ts: Option<i64>,
}

/// 单个命名空间的运行时状态
struct NamespaceRuntimeState {
    /// 广播通道
    channels: ResolutionChannels,
    /// 聚合状态
    aggregation_state: NamespaceAggregationState,
    /// netlink 客户端
    client: Option<Arc<NamespaceNetlinkClient>>,
}

impl NamespaceRuntimeState {
    fn new() -> Self {
        Self {
            channels: ResolutionChannels::new(),
            aggregation_state: NamespaceAggregationState::default(),
            client: None,
        }
    }
}

/// 采集进度守卫（RAII模式）
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

// ============================================================================
// 主采集器
// ============================================================================

/// 主采集器结构体
///
/// 协调各层工作：
/// - 调度层：定时触发采集
/// - 采集层：并发采集原始数据
/// - 处理层：聚合计算和分发
/// - 存储层：异步写入数据库
/// - 广播层：发送数据到前端
pub struct TrafficCollector {
    db: Arc<Database>,
    config: CollectorConfig,
    /// 命名空间运行时状态（独立锁）
    namespace_states: Arc<RwLock<HashMap<String, NamespaceRuntimeState>>>,
    /// 采集任务进行中标记
    collection_in_progress: Arc<AtomicBool>,
    /// 关闭信号发送器
    shutdown: broadcast::Sender<()>,
}

impl TrafficCollector {
    pub fn new(db: Arc<Database>, config: CollectorConfig) -> Result<Self> {
        let (shutdown, _) = broadcast::channel(1);
        let mut states = HashMap::new();
        states.insert("default".to_string(), NamespaceRuntimeState::new());
        let namespace_states = Arc::new(RwLock::new(states));
        let collection_in_progress = Arc::new(AtomicBool::new(false));

        Ok(TrafficCollector {
            db,
            config,
            namespace_states,
            collection_in_progress,
            shutdown,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.start_collection_scheduler().await
    }

    /// 启动采集调度器
    ///
    /// 创建独立的处理任务和存储任务，通过 channel 解耦各层
    async fn start_collection_scheduler(&self) -> Result<()> {
        // 创建 channels
        let (collection_tx, collection_rx) = mpsc::channel(COLLECTION_CHANNEL_SIZE);
        let (storage_tx, storage_rx) = mpsc::channel(STORAGE_CHANNEL_SIZE);
        let (broadcast_tx, broadcast_rx) = mpsc::channel(BROADCAST_CHANNEL_SIZE);

        // 启动命名空间同步任务（独立运行，不阻塞采集）
        let sync_task = self.spawn_namespace_sync_task();

        // 启动处理任务
        let processor_task = self.spawn_processor_task(collection_rx, storage_tx, broadcast_tx);

        // 启动存储任务
        let storage_task = self.spawn_storage_task(storage_rx);

        // 启动广播任务
        let broadcaster_task = self.spawn_broadcaster_task(broadcast_rx);

        // 启动调度任务
        let scheduler_task = self.spawn_scheduler_task(collection_tx);

        // 等待关闭信号
        let mut shutdown_rx = self.shutdown.subscribe();
        shutdown_rx.recv().await.ok();

        // 关闭所有任务
        sync_task.abort();
        scheduler_task.abort();
        processor_task.abort();
        storage_task.abort();
        broadcaster_task.abort();

        // 关闭所有客户端
        shutdown_all_clients(&self.namespace_states).await;

        Ok(())
    }

    /// 启动命名空间同步任务（独立运行，不阻塞采集）
    fn spawn_namespace_sync_task(&self) -> tokio::task::JoinHandle<()> {
        let namespace_states = Arc::clone(&self.namespace_states);
        let mut shutdown_rx = self.shutdown.subscribe();
        /// 同步间隔（秒）
        const SYNC_INTERVAL_SECS: u64 = 5;

        tokio::spawn(async move {
            let mut sync_interval = interval(Duration::from_secs(SYNC_INTERVAL_SECS));
            sync_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            log::info!("Started namespace sync task");

            loop {
                tokio::select! {
                    _ = sync_interval.tick() => {
                        let sync_started_at = Instant::now();

                        // 扫描命名空间
                        let desired_namespaces: Vec<String> = match scan_namespaces() {
                            Ok(namespaces) => namespaces,
                            Err(error) => {
                                log::error!("Failed to scan namespaces: {}", error);
                                continue;
                            }
                        };

                        // 同步命名空间状态
                        synchronize_namespace_runtime_state(&namespace_states, &desired_namespaces).await;

                        log::debug!(
                            "Namespace sync finished in {} ms",
                            sync_started_at.elapsed().as_millis()
                        );
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Namespace sync task stopped");
                        break;
                    }
                }
            }
        })
    }

    /// 启动调度任务
    fn spawn_scheduler_task(
        &self,
        collection_tx: mpsc::Sender<CollectionMessage>,
    ) -> tokio::task::JoinHandle<()> {
        let namespace_states = Arc::clone(&self.namespace_states);
        let collection_in_progress = Arc::clone(&self.collection_in_progress);
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown.subscribe();

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

                        let _guard = CollectionProgressGuard::new(Arc::clone(&collection_in_progress));
                        let round_started_at = Instant::now();

                        // 执行采集轮次（只获取快照，不等待同步）
                        Self::run_collection_round(
                            &namespace_states,
                            &collection_tx,
                        ).await;

                        log::debug!(
                            "Collection round scheduling finished in {} ms",
                            round_started_at.elapsed().as_millis()
                        );
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Collection scheduler stopped");
                        break;
                    }
                }
            }
        })
    }

    /// 执行一轮采集（只获取快照，不等待同步）
    async fn run_collection_round(
        namespace_states: &Arc<RwLock<HashMap<String, NamespaceRuntimeState>>>,
        collection_tx: &mpsc::Sender<CollectionMessage>,
    ) {
        // 获取客户端快照（一次读锁，快速完成）
        let clients_snapshot: Vec<(String, Option<Arc<NamespaceNetlinkClient>>)> = {
            let states_guard = namespace_states.read().await;
            states_guard
                .iter()
                .map(|(namespace, state)| (namespace.clone(), state.client.clone()))
                .collect()
        };

        // 并发采集
        let semaphore = Arc::new(Semaphore::new(MAX_NAMESPACE_CONCURRENCY));
        let mut join_set = JoinSet::new();

        for (namespace, client) in clients_snapshot {
            let semaphore = Arc::clone(&semaphore);
            let collection_tx = collection_tx.clone();

            join_set.spawn(async move {
                let _permit = semaphore
                    .acquire_owned()
                    .await
                    .expect("collection semaphore closed unexpectedly");

                let result = match client {
                    Some(client) => collect_namespace_data(&namespace, &client).await,
                    None => Err(anyhow!("no netlink client for namespace {}", namespace)),
                };

                // 发送采集结果到处理层
                if collection_tx
                    .send(CollectionMessage { namespace, result })
                    .await
                    .is_err()
                {
                    log::warn!("Collection channel closed, dropping result");
                }
            });
        }

        // 等待所有采集任务完成
        while let Some(result) = join_set.join_next().await {
            if let Err(error) = result {
                log::error!("Collection task panicked: {}", error);
            }
        }
    }

    /// 启动处理任务
    fn spawn_processor_task(
        &self,
        collection_rx: mpsc::Receiver<CollectionMessage>,
        storage_tx: mpsc::Sender<StorageMessage>,
        broadcast_tx: mpsc::Sender<BroadcastMessage>,
    ) -> tokio::task::JoinHandle<()> {
        let namespace_states = Arc::clone(&self.namespace_states);
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            log::info!("Started data processor");

            let mut collection_rx = collection_rx;
            let mut success_count = 0usize;
            let mut failure_count = 0usize;

            loop {
                tokio::select! {
                    // 接收采集结果，立即处理并发送
                    Some(message) = collection_rx.recv() => {
                        Self::process_collection_message(
                            &namespace_states,
                            message,
                            &storage_tx,
                            &broadcast_tx,
                            &mut success_count,
                            &mut failure_count,
                        ).await;
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!(
                            "Data processor stopped (total: {} success, {} failed)",
                            success_count,
                            failure_count
                        );
                        break;
                    }
                }
            }
        })
    }

    /// 处理单个采集消息，立即发送到存储层
    async fn process_collection_message(
        namespace_states: &Arc<RwLock<HashMap<String, NamespaceRuntimeState>>>,
        message: CollectionMessage,
        storage_tx: &mpsc::Sender<StorageMessage>,
        broadcast_tx: &mpsc::Sender<BroadcastMessage>,
        success_count: &mut usize,
        failure_count: &mut usize,
    ) {
        match message.result {
            Ok(data) => {
                *success_count += 1;
                let timestamp_ms = data.timestamp_ms;

                // 获取命名空间状态（独立锁）
                let (channels, agg_state) = {
                    let states_guard = namespace_states.read().await;
                    if let Some(state) = states_guard.get(&message.namespace) {
                        (state.channels.clone(), state.aggregation_state.clone())
                    } else {
                        log::warn!("Namespace {} not found in states", message.namespace);
                        return;
                    }
                };

                // 发送实时广播（在锁外）
                let realtime_traffic = TrafficData::from(data.clone());
                let _ = channels.realtime.send(realtime_traffic.clone());

                // 发送广播消息
                let _ = broadcast_tx
                    .send(BroadcastMessage {
                        namespace: message.namespace.clone(),
                        data: realtime_traffic,
                        resolution: Resolution::Realtime,
                    })
                    .await;

                // 计算聚合（无锁操作）
                let mut new_agg_state = agg_state.clone();
                let mut resolutions_to_store = vec![Resolution::Realtime];

                if should_aggregate(timestamp_ms, agg_state.last_10s_ts, INTERVAL_10S) {
                    new_agg_state.last_10s_ts = Some(align_timestamp(timestamp_ms, INTERVAL_10S));
                    resolutions_to_store.push(Resolution::TenSeconds);
                    let _ = channels.agg_10s.send(TrafficData::from(data.clone()));
                }

                if should_aggregate(timestamp_ms, agg_state.last_1m_ts, INTERVAL_1M) {
                    new_agg_state.last_1m_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1M));
                    resolutions_to_store.push(Resolution::OneMinute);
                    let _ = channels.agg_1m.send(TrafficData::from(data.clone()));
                }

                if should_aggregate(timestamp_ms, agg_state.last_1h_ts, INTERVAL_1H) {
                    new_agg_state.last_1h_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1H));
                    resolutions_to_store.push(Resolution::OneHour);
                    let _ = channels.agg_1h.send(TrafficData::from(data.clone()));
                }

                // 1天聚合数据单独处理（不需要广播）
                let data_1d_to_store =
                    if should_aggregate(timestamp_ms, agg_state.last_1d_ts, INTERVAL_1D) {
                        new_agg_state.last_1d_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1D));
                        Some(data.clone())
                    } else {
                        None
                    };

                // 更新聚合状态
                {
                    let mut states_guard = namespace_states.write().await;
                    if let Some(state) = states_guard.get_mut(&message.namespace) {
                        state.aggregation_state = new_agg_state;
                    }
                }

                // 立即发送到存储层
                if storage_tx
                    .send(StorageMessage {
                        data,
                        resolutions: resolutions_to_store,
                        data_1d: data_1d_to_store,
                    })
                    .await
                    .is_err()
                {
                    log::warn!("Storage channel closed");
                }
            }
            Err(error) => {
                *failure_count += 1;
                log::error!(
                    "Failed to collect data for {}: {}",
                    message.namespace,
                    error
                );
            }
        }
    }

    /// 启动存储任务（接收单个数据，批量写入）
    fn spawn_storage_task(
        &self,
        storage_rx: mpsc::Receiver<StorageMessage>,
    ) -> tokio::task::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            log::info!("Started data storage");

            let mut storage_rx = storage_rx;
            let mut realtime_buffer = Vec::new();
            let mut data_10s_buffer = Vec::new();
            let mut data_1m_buffer = Vec::new();
            let mut data_1h_buffer = Vec::new();
            let mut data_1d_buffer = Vec::new();
            const FLUSH_INTERVAL_MS: u64 = 100;

            loop {
                tokio::select! {
                    // 接收单个数据，缓冲
                    Some(message) = storage_rx.recv() => {
                        for resolution in &message.resolutions {
                            let mut data = message.data.clone();
                            data.resolution = Some(resolution.as_str().to_string());

                            match resolution {
                                Resolution::Realtime => realtime_buffer.push(data),
                                Resolution::TenSeconds => data_10s_buffer.push(data),
                                Resolution::OneMinute => data_1m_buffer.push(data),
                                Resolution::OneHour => data_1h_buffer.push(data),
                            }
                        }
                        // 1天数据单独处理
                        if let Some(data) = message.data_1d {
                            data_1d_buffer.push(data);
                        }
                    }
                    // 定期批量写入
                    _ = tokio::time::sleep(Duration::from_millis(FLUSH_INTERVAL_MS)) => {
                        if realtime_buffer.is_empty() {
                            continue;
                        }

                        let write_started_at = Instant::now();
                        let count = realtime_buffer.len();

                        if let Err(error) = db.insert_round_batch(
                            &realtime_buffer,
                            &data_10s_buffer,
                            &data_1m_buffer,
                            &data_1h_buffer,
                            &data_1d_buffer,
                        ) {
                            log::error!("Failed to store batch data: {}", error);
                        } else {
                            log::debug!(
                                "Stored {} records in {} ms",
                                count,
                                write_started_at.elapsed().as_millis()
                            );
                        }

                        // 清空缓冲区
                        realtime_buffer.clear();
                        data_10s_buffer.clear();
                        data_1m_buffer.clear();
                        data_1h_buffer.clear();
                        data_1d_buffer.clear();
                    }
                    _ = shutdown_rx.recv() => {
                        // 关闭前写入剩余数据
                        if !realtime_buffer.is_empty() {
                            if let Err(error) = db.insert_round_batch(
                                &realtime_buffer,
                                &data_10s_buffer,
                                &data_1m_buffer,
                                &data_1h_buffer,
                                &data_1d_buffer,
                            ) {
                                log::error!("Failed to store remaining data: {}", error);
                            }
                        }
                        log::info!("Data storage stopped");
                        break;
                    }
                }
            }
        })
    }

    /// 启动广播任务
    fn spawn_broadcaster_task(
        &self,
        broadcast_rx: mpsc::Receiver<BroadcastMessage>,
    ) -> tokio::task::JoinHandle<()> {
        let _namespace_states = Arc::clone(&self.namespace_states);
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            log::info!("Started broadcaster");

            let mut broadcast_rx = broadcast_rx;

            loop {
                tokio::select! {
                    Some(message) = broadcast_rx.recv() => {
                        // 广播消息已在处理层发送，这里可以做额外的处理
                        // 如日志记录、状态更新等
                        log::trace!(
                            "Broadcasted {} data for namespace {}",
                            message.resolution.as_str(),
                            message.namespace
                        );
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Broadcaster stopped");
                        break;
                    }
                }
            }
        })
    }

    /// 订阅指定命名空间和分辨率的数据
    pub async fn subscribe(
        &self,
        namespace: &str,
        resolution: Resolution,
    ) -> Option<broadcast::Receiver<TrafficData>> {
        let states_guard = self.namespace_states.read().await;
        states_guard
            .get(namespace)
            .map(|state| state.channels.subscribe(resolution))
    }

    /// 获取所有命名空间列表
    pub async fn get_namespaces(&self) -> Vec<String> {
        self.namespace_states.read().await.keys().cloned().collect()
    }

    /// 停止采集器
    pub async fn stop(&self) -> Result<()> {
        let _ = self.shutdown.send(());
        Ok(())
    }
}

// ============================================================================
// 辅助函数
// ============================================================================

/// 扫描文件系统中的网络命名空间
fn scan_namespaces() -> Result<Vec<String>> {
    let mut namespaces = vec!["default".to_string()];

    let netns_dir = Path::new("/var/run/netns");
    if netns_dir.exists() {
        let entries = fs::read_dir(netns_dir).context("Failed to read /var/run/netns")?;
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if !name.is_empty() {
                    namespaces.push(name.to_string());
                }
            }
        }
    }

    namespaces.sort();
    namespaces.dedup();
    Ok(namespaces)
}

/// 同步命名空间运行时状态
async fn synchronize_namespace_runtime_state(
    states: &Arc<RwLock<HashMap<String, NamespaceRuntimeState>>>,
    desired_namespaces: &[String],
) {
    let mut states_guard = states.write().await;

    // 移除不存在的命名空间
    let to_remove: Vec<String> = states_guard
        .keys()
        .filter(|k| !desired_namespaces.contains(k))
        .cloned()
        .collect();

    for namespace in to_remove {
        let namespace_log = namespace.clone();
        if let Some(state) = states_guard.remove(&namespace) {
            if let Some(client) = state.client {
                tokio::spawn(async move {
                    if let Err(e) = client.shutdown().await {
                        log::error!("Failed to shutdown client for {}: {}", namespace, e);
                    }
                });
            }
            log::info!("Removed namespace: {}", namespace_log);
        }
    }

    // 添加新命名空间
    for namespace in desired_namespaces {
        states_guard
            .entry(namespace.clone())
            .or_insert_with(NamespaceRuntimeState::new);
    }

    // 检查需要创建/重建的客户端
    let to_create: Vec<String> = states_guard
        .iter()
        .filter(|(_, state)| {
            state.client.is_none() || state.client.as_ref().map_or(true, |c| c.is_closed())
        })
        .map(|(ns, _)| ns.clone())
        .collect();

    // 释放锁后创建客户端
    drop(states_guard);

    for namespace in to_create {
        match NamespaceNetlinkClient::new(namespace.clone()).await {
            Ok(client) => {
                let mut states_guard = states.write().await;
                if let Some(state) = states_guard.get_mut(&namespace) {
                    state.client = Some(Arc::new(client));
                }
                log::info!("Created netlink client for namespace: {}", namespace);
            }
            Err(error) => {
                log::error!("Failed to create client for {}: {}", namespace, error);
            }
        }
    }
}

/// 关闭所有客户端
async fn shutdown_all_clients(states: &Arc<RwLock<HashMap<String, NamespaceRuntimeState>>>) {
    let clients: Vec<(String, Arc<NamespaceNetlinkClient>)> = {
        let mut states_guard = states.write().await;
        states_guard
            .iter_mut()
            .filter_map(|(ns, state)| state.client.take().map(|c| (ns.clone(), c)))
            .collect()
    };

    for (namespace, client) in clients {
        if let Err(error) = client.shutdown().await {
            log::error!("Failed to shutdown client for {}: {}", namespace, error);
        } else {
            log::info!("Shutdown client for namespace: {}", namespace);
        }
    }
}

/// 采集单个命名空间的数据
async fn collect_namespace_data(
    namespace: &str,
    client: &NamespaceNetlinkClient,
) -> Result<RawTrafficData> {
    let timestamp = Utc::now();
    let interfaces = client.collect_interfaces().await.with_context(|| {
        format!(
            "failed to collect interface stats for namespace {}",
            namespace
        )
    })?;

    Ok(RawTrafficData {
        namespace: namespace.to_string(),
        timestamp: timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
        timestamp_ms: timestamp.timestamp_millis(),
        interfaces,
        resolution: None,
    })
}

/// 判断是否需要聚合
fn should_aggregate(current_ts: i64, last_ts: Option<i64>, interval_ms: i64) -> bool {
    let current_aligned = align_timestamp(current_ts, interval_ms);
    match last_ts {
        None => true,
        Some(last) => current_aligned > last,
    }
}

/// 对齐时间戳到聚合间隔
fn align_timestamp(timestamp_ms: i64, interval_ms: i64) -> i64 {
    (timestamp_ms / interval_ms) * interval_ms
}

// ============================================================================
// 测试
// ============================================================================

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
}
