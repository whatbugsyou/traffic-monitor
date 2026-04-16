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

use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

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

/// 采集结果 channel 缓冲区大小
const COLLECTION_CHANNEL_SIZE: usize = 200;
/// 存储请求 channel 缓冲区大小
const STORAGE_CHANNEL_SIZE: usize = 200;
/// 广播通道缓冲区大小
const BROADCAST_CHANNEL_SIZE: usize = 200;

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
    #[allow(dead_code)]
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
pub(crate) struct NamespaceRuntimeState {
    /// 命名空间名称
    pub(crate) namespace: String,
    /// 广播通道
    channels: ResolutionChannels,
    /// 聚合状态
    aggregation_state: NamespaceAggregationState,
    /// netlink 客户端
    pub(crate) client: Option<Arc<NamespaceNetlinkClient>>,
}

impl NamespaceRuntimeState {
    /// 创建完整的状态（包含客户端）
    async fn new(namespace: &str) -> Result<Self> {
        let client = NamespaceNetlinkClient::new(namespace.to_string())
            .await
            .with_context(|| format!("Failed to create client for namespace {}", namespace))?;

        Ok(Self {
            namespace: namespace.to_string(),
            channels: ResolutionChannels::new(),
            aggregation_state: NamespaceAggregationState::default(),
            client: Some(Arc::new(client)),
        })
    }

    /// 重建客户端（用于客户端失效后恢复）
    async fn rebuild_client(&mut self) -> Result<()> {
        let client = NamespaceNetlinkClient::new(self.namespace.clone())
            .await
            .with_context(|| {
                format!("Failed to rebuild client for namespace {}", self.namespace)
            })?;

        self.client = Some(Arc::new(client));
        Ok(())
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
    /// 命名空间运行时状态（使用 DashMap 实现细粒度锁）
    namespace_states: Arc<DashMap<String, NamespaceRuntimeState>>,
    /// 采集任务进行中标记
    collection_in_progress: Arc<AtomicBool>,
    /// 任务取消令牌（Tokio 官方方案）
    cancel_token: CancellationToken,
    /// 后台任务集合（自动管理 JoinHandle）
    tasks: Mutex<JoinSet<()>>,
}

impl Drop for TrafficCollector {
    fn drop(&mut self) {
        // 确保取消令牌被触发，防止任务泄漏
        self.cancel_token.cancel();
        log::info!("TrafficCollector dropping, cancellation token triggered");
    }
}

impl TrafficCollector {
    pub fn new(db: Arc<Database>, config: CollectorConfig) -> Result<Self> {
        let namespace_states = Arc::new(DashMap::new());
        let collection_in_progress = Arc::new(AtomicBool::new(false));
        let cancel_token = CancellationToken::new();
        let tasks = Mutex::new(JoinSet::new());

        Ok(TrafficCollector {
            db,
            config,
            namespace_states,
            collection_in_progress,
            cancel_token,
            tasks,
        })
    }

    /// 启动采集器
    ///
    /// 创建独立的处理任务和存储任务，通过 channel 解耦各层。
    /// 所有任务在 tokio runtime 中并行运行，此方法立即返回不阻塞。
    pub async fn start(&self) {
        // 创建 channels
        let (collection_tx, collection_rx) = mpsc::channel(COLLECTION_CHANNEL_SIZE);
        let (storage_tx, storage_rx) = mpsc::channel(STORAGE_CHANNEL_SIZE);
        let (broadcast_tx, broadcast_rx) = mpsc::channel(BROADCAST_CHANNEL_SIZE);

        // 获取 tasks mutex lock 并启动所有任务
        let mut tasks = self.tasks.lock().await;

        // 启动命名空间同步任务（独立运行，不阻塞采集）
        tasks.spawn(self.spawn_namespace_sync_task());

        // 启动处理任务
        tasks.spawn(self.spawn_processor_task(collection_rx, storage_tx, broadcast_tx));

        // 启动存储任务
        tasks.spawn(self.spawn_storage_task(storage_rx));

        // 启动广播任务
        tasks.spawn(self.spawn_broadcaster_task(broadcast_rx));

        // 启动调度任务
        tasks.spawn(self.spawn_scheduler_task(collection_tx));

        log::info!("All collection tasks spawned and running in background");
    }

    /// 启动命名空间同步任务（独立运行，不阻塞采集）
    fn spawn_namespace_sync_task(&self) -> impl std::future::Future<Output = ()> {
        let namespace_states = Arc::clone(&self.namespace_states);
        let cancel_token = self.cancel_token.clone();
        /// 同步间隔（秒）
        const SYNC_INTERVAL_SECS: u64 = 5;

        async move {
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
                    _ = cancel_token.cancelled() => {
                        log::info!("Namespace sync task stopped");
                        break;
                    }
                }
            }
        }
    }

    /// 启动调度任务
    fn spawn_scheduler_task(
        &self,
        collection_tx: mpsc::Sender<CollectionMessage>,
    ) -> impl std::future::Future<Output = ()> {
        let namespace_states = Arc::clone(&self.namespace_states);
        let collection_in_progress = Arc::clone(&self.collection_in_progress);
        let config = self.config.clone();
        let cancel_token = self.cancel_token.clone();

        async move {
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
                    _ = cancel_token.cancelled() => {
                        log::info!("Collection scheduler stopped");
                        break;
                    }
                }
            }
        }
    }

    /// 执行一轮采集（只获取快照，不等待同步）
    async fn run_collection_round(
        namespace_states: &Arc<DashMap<String, NamespaceRuntimeState>>,
        collection_tx: &mpsc::Sender<CollectionMessage>,
    ) {
        let round_start = Instant::now();

        // 获取客户端快照（迭代期间持有读锁，clone 数据后立即释放）
        let clients_snapshot: Vec<_> = namespace_states
            .iter()
            .filter_map(|entry| {
                let namespace = entry.key().clone();
                let client = entry.value().client.clone()?;
                Some((namespace, client))
            })
            .collect();

        // 并发采集（无限制，所有命名空间同时开始）
        let mut join_set = JoinSet::new();
        let namespace_count = clients_snapshot.len();

        for (namespace, client) in clients_snapshot {
            let collection_tx = collection_tx.clone();

            join_set.spawn(async move {
                let result = collect_namespace_data(&namespace, &client).await;

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

        log::info!(
            "Collection round completed: {} namespaces in {} ms",
            namespace_count,
            round_start.elapsed().as_millis()
        );
    }

    /// 启动处理任务
    fn spawn_processor_task(
        &self,
        collection_rx: mpsc::Receiver<CollectionMessage>,
        storage_tx: mpsc::Sender<StorageMessage>,
        broadcast_tx: mpsc::Sender<BroadcastMessage>,
    ) -> impl std::future::Future<Output = ()> {
        let namespace_states = Arc::clone(&self.namespace_states);
        let cancel_token = self.cancel_token.clone();

        async move {
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
                    _ = cancel_token.cancelled() => {
                        log::info!(
                            "Data processor stopped (total: {} success, {} failed)",
                            success_count,
                            failure_count
                        );
                        break;
                    }
                }
            }
        }
    }

    /// 处理单个采集消息，立即发送到存储层
    async fn process_collection_message(
        namespace_states: &Arc<DashMap<String, NamespaceRuntimeState>>,
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

                // 获取命名空间状态（DashMap 细粒度锁）
                let (channels, agg_state) = {
                    if let Some(entry) = namespace_states.get(&message.namespace) {
                        (entry.channels.clone(), entry.aggregation_state.clone())
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

                // 更新聚合状态（DashMap 细粒度锁）
                if let Some(mut entry) = namespace_states.get_mut(&message.namespace) {
                    entry.aggregation_state = new_agg_state;
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

                // 清空客户端，下次扫描自动重建（DashMap 细粒度锁）
                if let Some(mut entry) = namespace_states.get_mut(&message.namespace) {
                    entry.client = None;
                    log::info!(
                        "Cleared client for {} due to collection failure",
                        message.namespace
                    );
                }
            }
        }
    }

    /// 启动存储任务（接收单个数据，批量写入）
    fn spawn_storage_task(
        &self,
        storage_rx: mpsc::Receiver<StorageMessage>,
    ) -> impl std::future::Future<Output = ()> {
        let db = Arc::clone(&self.db);
        let cancel_token = self.cancel_token.clone();

        async move {
            log::info!("Started data storage");

            let mut storage_rx = storage_rx;
            let mut realtime_buffer = Vec::new();
            let mut data_10s_buffer = Vec::new();
            let mut data_1m_buffer = Vec::new();
            let mut data_1h_buffer = Vec::new();
            let mut data_1d_buffer = Vec::new();
            const FLUSH_INTERVAL_MS: u64 = 250;
            const MAX_BUFFERED_ROWS: usize = 512;
            const CLEANUP_INTERVAL_SECS: u64 = 30;

            let mut flush_interval = interval(Duration::from_millis(FLUSH_INTERVAL_MS));
            flush_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            flush_interval.tick().await;

            let mut cleanup_interval = interval(Duration::from_secs(CLEANUP_INTERVAL_SECS));
            cleanup_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            cleanup_interval.tick().await;

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

                        if buffered_storage_rows(
                            &realtime_buffer,
                            &data_10s_buffer,
                            &data_1m_buffer,
                            &data_1h_buffer,
                            &data_1d_buffer,
                        ) >= MAX_BUFFERED_ROWS
                        {
                            if let Err(error) = flush_storage_buffers(
                                &db,
                                &realtime_buffer,
                                &data_10s_buffer,
                                &data_1m_buffer,
                                &data_1h_buffer,
                                &data_1d_buffer,
                            ) {
                                log::error!("Failed to store full buffers: {}", error);
                            }

                            realtime_buffer.clear();
                            data_10s_buffer.clear();
                            data_1m_buffer.clear();
                            data_1h_buffer.clear();
                            data_1d_buffer.clear();
                        }
                    }
                    // 固定周期批量写入，避免高吞吐时被持续重置成“空闲后再写”。
                    _ = flush_interval.tick() => {
                        if storage_buffers_empty(
                            &realtime_buffer,
                            &data_10s_buffer,
                            &data_1m_buffer,
                            &data_1h_buffer,
                            &data_1d_buffer,
                        ) {
                            continue;
                        }

                        if let Err(error) = flush_storage_buffers(
                            &db,
                            &realtime_buffer,
                            &data_10s_buffer,
                            &data_1m_buffer,
                            &data_1h_buffer,
                            &data_1d_buffer,
                        ) {
                            log::error!("Failed to store batch data: {}", error);
                        }

                        // 清空缓冲区
                        realtime_buffer.clear();
                        data_10s_buffer.clear();
                        data_1m_buffer.clear();
                        data_1h_buffer.clear();
                        data_1d_buffer.clear();
                    }
                    // 定时批量清理，避免在插入事务内叠加删除压力。
                    _ = cleanup_interval.tick() => {
                        let cleanup_started_at = Instant::now();
                        match db.cleanup_expired_data(Utc::now().timestamp_millis()) {
                            Ok(deleted_rows) if deleted_rows > 0 => {
                                log::debug!(
                                    "Cleaned up {} expired rows in {} ms",
                                    deleted_rows,
                                    cleanup_started_at.elapsed().as_millis()
                                );
                            }
                            Ok(_) => {}
                            Err(error) => {
                                log::error!("Failed to cleanup expired data: {}", error);
                            }
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        // 关闭前写入剩余数据
                        if !storage_buffers_empty(
                            &realtime_buffer,
                            &data_10s_buffer,
                            &data_1m_buffer,
                            &data_1h_buffer,
                            &data_1d_buffer,
                        ) {
                            if let Err(error) = flush_storage_buffers(
                                &db,
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
        }
    }

    /// 启动广播任务
    fn spawn_broadcaster_task(
        &self,
        broadcast_rx: mpsc::Receiver<BroadcastMessage>,
    ) -> impl std::future::Future<Output = ()> {
        let _namespace_states = Arc::clone(&self.namespace_states);
        let cancel_token = self.cancel_token.clone();

        async move {
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
                    _ = cancel_token.cancelled() => {
                        log::info!("Broadcaster stopped");
                        break;
                    }
                }
            }
        }
    }

    /// 订阅指定命名空间和分辨率的数据
    pub async fn subscribe(
        &self,
        namespace: &str,
        resolution: Resolution,
    ) -> Option<broadcast::Receiver<TrafficData>> {
        self.namespace_states
            .get(namespace)
            .map(|entry| entry.channels.subscribe(resolution))
    }

    /// 获取所有命名空间列表
    pub async fn get_namespaces(&self) -> Vec<String> {
        self.namespace_states
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// 停止采集器，等待所有任务完成清理
    ///
    /// 这是优雅关闭的正确方式：
    /// 1. 触发取消令牌，所有任务收到信号
    /// 2. 等待所有任务完成（特别是存储任务写入剩余数据）
    /// 3. 确保没有数据丢失
    pub async fn shutdown(&self) -> Result<()> {
        log::info!("Initiating graceful shutdown...");

        // 1. 触发取消令牌，所有任务开始退出
        self.cancel_token.cancel();

        // 2. 等待所有任务完成清理
        let mut tasks = self.tasks.lock().await;
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                log::error!("Task panicked during shutdown: {}", e);
            }
        }

        log::info!("All background tasks stopped gracefully");
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
    states: &Arc<DashMap<String, NamespaceRuntimeState>>,
    desired_namespaces: &[String],
) {
    // 移除不存在的命名空间
    let to_remove: Vec<String> = states
        .iter()
        .map(|entry| entry.key().clone())
        .filter(|k| !desired_namespaces.contains(k))
        .collect();

    for namespace in to_remove {
        if states.remove(&namespace).is_some() {
            log::info!("Removed namespace: {}", namespace);
        }
    }

    // 找出需要重建客户端的命名空间（client 为 None 但状态存在）
    let to_rebuild: Vec<String> = states
        .iter()
        .filter(|entry| entry.client.is_none())
        .map(|entry| entry.key().clone())
        .collect();

    // 重建客户端
    for namespace in to_rebuild {
        if let Some(mut entry) = states.get_mut(&namespace) {
            match entry.rebuild_client().await {
                Ok(()) => {
                    log::info!("Rebuilt netlink client for namespace: {}", namespace);
                }
                Err(error) => {
                    log::error!("Failed to rebuild client for {}: {}", namespace, error);
                }
            }
        }
    }

    // 找出真正新的命名空间（不存在于 states 中）
    let new_namespaces: Vec<String> = desired_namespaces
        .iter()
        .filter(|ns| !states.contains_key(*ns))
        .cloned()
        .collect();

    // 为新命名空间创建完整的状态（包含客户端）
    for namespace in new_namespaces {
        match NamespaceRuntimeState::new(&namespace).await {
            Ok(state) => {
                states.insert(namespace.clone(), state);
                log::info!("Created namespace state with client: {}", namespace);
            }
            Err(error) => {
                log::error!(
                    "Failed to create namespace state for {}: {}",
                    namespace,
                    error
                );
            }
        }
    }
}

/// 关闭所有客户端
#[allow(dead_code)]
async fn shutdown_all_clients(states: &Arc<DashMap<String, NamespaceRuntimeState>>) {
    let count = states
        .iter_mut()
        .filter_map(|mut entry| entry.client.take())
        .count();
    log::info!("Cleared {} namespace clients (RAII cleanup)", count);
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

fn storage_buffers_empty(
    realtime_buffer: &[RawTrafficData],
    data_10s_buffer: &[RawTrafficData],
    data_1m_buffer: &[RawTrafficData],
    data_1h_buffer: &[RawTrafficData],
    data_1d_buffer: &[RawTrafficData],
) -> bool {
    realtime_buffer.is_empty()
        && data_10s_buffer.is_empty()
        && data_1m_buffer.is_empty()
        && data_1h_buffer.is_empty()
        && data_1d_buffer.is_empty()
}

fn buffered_storage_rows(
    realtime_buffer: &[RawTrafficData],
    data_10s_buffer: &[RawTrafficData],
    data_1m_buffer: &[RawTrafficData],
    data_1h_buffer: &[RawTrafficData],
    data_1d_buffer: &[RawTrafficData],
) -> usize {
    realtime_buffer.len()
        + data_10s_buffer.len()
        + data_1m_buffer.len()
        + data_1h_buffer.len()
        + data_1d_buffer.len()
}

fn flush_storage_buffers(
    db: &Database,
    realtime_buffer: &[RawTrafficData],
    data_10s_buffer: &[RawTrafficData],
    data_1m_buffer: &[RawTrafficData],
    data_1h_buffer: &[RawTrafficData],
    data_1d_buffer: &[RawTrafficData],
) -> Result<()> {
    let write_started_at = Instant::now();
    let raw_count = realtime_buffer.len();
    let agg_10s_count = data_10s_buffer.len();
    let agg_1m_count = data_1m_buffer.len();
    let agg_1h_count = data_1h_buffer.len();
    let agg_1d_count = data_1d_buffer.len();
    let total_count = raw_count + agg_10s_count + agg_1m_count + agg_1h_count + agg_1d_count;

    db.insert_round_batch(
        realtime_buffer,
        data_10s_buffer,
        data_1m_buffer,
        data_1h_buffer,
        data_1d_buffer,
    )?;

    log::debug!(
        "Stored {} records in {} ms (raw={} 10s={} 1m={} 1h={} 1d={})",
        total_count,
        write_started_at.elapsed().as_millis(),
        raw_count,
        agg_10s_count,
        agg_1m_count,
        agg_1h_count,
        agg_1d_count
    );

    Ok(())
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
