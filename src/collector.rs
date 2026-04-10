use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};

use crate::database::Database;
use crate::models::*;
use crate::netlink_client::NamespaceNetlinkClient;

const INTERVAL_10S: i64 = 10_000;
const INTERVAL_1M: i64 = 60_000;
const INTERVAL_1H: i64 = 3_600_000;
const INTERVAL_1D: i64 = 86_400_000;
const MAX_NAMESPACE_CONCURRENCY: usize = 20;

struct ResolutionChannels {
    realtime: broadcast::Sender<TrafficData>,
    agg_10s: broadcast::Sender<TrafficData>,
    agg_1m: broadcast::Sender<TrafficData>,
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

pub struct TrafficCollector {
    db: Arc<Database>,
    config: CollectorConfig,
    namespaces: Arc<RwLock<Vec<String>>>,
    channels: Arc<RwLock<HashMap<String, ResolutionChannels>>>,
    aggregation_state: Arc<RwLock<HashMap<String, NamespaceAggregationState>>>,
    clients: Arc<RwLock<HashMap<String, Arc<NamespaceNetlinkClient>>>>,
    collection_in_progress: Arc<AtomicBool>,
    shutdown: broadcast::Sender<()>,
}

impl TrafficCollector {
    pub fn new(db: Arc<Database>, config: CollectorConfig) -> Result<Self> {
        let (shutdown, _) = broadcast::channel(1);
        let namespaces = Arc::new(RwLock::new(vec!["default".to_string()]));
        let channels = Arc::new(RwLock::new(HashMap::new()));
        let aggregation_state = Arc::new(RwLock::new(HashMap::new()));
        let clients = Arc::new(RwLock::new(HashMap::new()));
        let collection_in_progress = Arc::new(AtomicBool::new(false));

        Ok(TrafficCollector {
            db,
            config,
            namespaces,
            channels,
            aggregation_state,
            clients,
            collection_in_progress,
            shutdown,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.start_collection_scheduler().await
    }

    async fn start_collection_scheduler(&self) -> Result<()> {
        let db = Arc::clone(&self.db);
        let channels = Arc::clone(&self.channels);
        let namespaces = Arc::clone(&self.namespaces);
        let aggregation_state = Arc::clone(&self.aggregation_state);
        let clients = Arc::clone(&self.clients);
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

                        let desired_namespaces = match scan_namespaces() {
                            Ok(namespaces_from_scan) => namespaces_from_scan,
                            Err(error) => {
                                log::error!("Failed to scan namespaces for this tick: {}", error);
                                namespaces.read().await.clone()
                            }
                        };

                        synchronize_namespace_runtime_state(
                            &namespaces,
                            &channels,
                            &aggregation_state,
                            &clients,
                            &desired_namespaces,
                        )
                        .await;

                        let namespaces_snapshot = namespaces.read().await.clone();
                        let results = collect_namespace_round(&namespaces_snapshot, Arc::clone(&clients)).await;

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
                                Err(error) => {
                                    failures += 1;
                                    log::error!("Failed to collect data for {}: {}", namespace, error);
                                }
                            }
                        }

                        if let Err(error) = db.insert_round_batch(
                            &raw_data,
                            &data_10s,
                            &data_1m,
                            &data_1h,
                            &data_1d,
                        ) {
                            log::error!("Failed to store round batch data: {}", error);
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
                        shutdown_all_clients(&clients).await;
                        break;
                    }
                }
            }
        });

        Ok(())
    }

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

    pub async fn get_namespaces(&self) -> Vec<String> {
        self.namespaces.read().await.clone()
    }

    pub async fn stop(&self) -> Result<()> {
        self.shutdown
            .send(())
            .context("Failed to send shutdown signal")?;
        shutdown_all_clients(&self.clients).await;
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

async fn synchronize_namespace_runtime_state(
    namespaces: &Arc<RwLock<Vec<String>>>,
    channels: &Arc<RwLock<HashMap<String, ResolutionChannels>>>,
    aggregation_state: &Arc<RwLock<HashMap<String, NamespaceAggregationState>>>,
    clients: &Arc<RwLock<HashMap<String, Arc<NamespaceNetlinkClient>>>>,
    desired_namespaces: &[String],
) {
    {
        let mut namespaces_guard = namespaces.write().await;
        if *namespaces_guard != desired_namespaces {
            log::info!(
                "Namespaces changed: {:?} -> {:?}",
                *namespaces_guard,
                desired_namespaces
            );
            *namespaces_guard = desired_namespaces.to_vec();
        }
    }

    let desired_set: HashSet<String> = desired_namespaces.iter().cloned().collect();

    {
        let mut channels_guard = channels.write().await;
        let removed_channels: Vec<String> = channels_guard
            .keys()
            .filter(|namespace| !desired_set.contains(*namespace))
            .cloned()
            .collect();
        for namespace in removed_channels {
            channels_guard.remove(&namespace);
            log::info!("Removed channels for namespace: {}", namespace);
        }

        for namespace in desired_namespaces {
            if !channels_guard.contains_key(namespace) {
                channels_guard.insert(namespace.clone(), ResolutionChannels::new());
                log::info!("Created multi-resolution channels for namespace: {}", namespace);
            }
        }
    }

    {
        let mut aggregation_guard = aggregation_state.write().await;
        let removed_states: Vec<String> = aggregation_guard
            .keys()
            .filter(|namespace| !desired_set.contains(*namespace))
            .cloned()
            .collect();
        for namespace in removed_states {
            aggregation_guard.remove(&namespace);
            log::info!("Removed aggregation state for namespace: {}", namespace);
        }

        for namespace in desired_namespaces {
            aggregation_guard
                .entry(namespace.clone())
                .or_insert_with(NamespaceAggregationState::default);
        }
    }

    let (removed_clients, namespaces_to_create) = {
        let mut clients_guard = clients.write().await;
        let stale_namespaces: Vec<String> = clients_guard
            .iter()
            .filter_map(|(namespace, client)| {
                if !desired_set.contains(namespace) {
                    Some(namespace.clone())
                } else if client.is_closed() {
                    log::warn!("Recreating closed netlink client for namespace: {}", namespace);
                    Some(namespace.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut removed_clients = Vec::new();
        for namespace in stale_namespaces {
            if let Some(client) = clients_guard.remove(&namespace) {
                removed_clients.push((namespace, client));
            }
        }

        let namespaces_to_create = desired_namespaces
            .iter()
            .filter(|namespace| !clients_guard.contains_key(*namespace))
            .cloned()
            .collect::<Vec<_>>();

        (removed_clients, namespaces_to_create)
    };

    shutdown_clients(removed_clients).await;

    let mut created_clients = Vec::new();
    for namespace in namespaces_to_create {
        match NamespaceNetlinkClient::new(namespace.clone()).await {
            Ok(client) => {
                log::info!("Created long-lived netlink client for namespace: {}", namespace);
                created_clients.push((namespace, Arc::new(client)));
            }
            Err(error) => {
                log::error!("Failed to create netlink client for {}: {}", namespace, error);
            }
        }
    }

    if !created_clients.is_empty() {
        let mut clients_guard = clients.write().await;
        for (namespace, client) in created_clients {
            clients_guard.entry(namespace).or_insert(client);
        }
    }
}

async fn shutdown_all_clients(
    clients: &Arc<RwLock<HashMap<String, Arc<NamespaceNetlinkClient>>>>,
) {
    let drained_clients = {
        let mut clients_guard = clients.write().await;
        clients_guard.drain().collect::<Vec<_>>()
    };

    shutdown_clients(drained_clients).await;
}

async fn shutdown_clients(clients: Vec<(String, Arc<NamespaceNetlinkClient>)>) {
    for (namespace, client) in clients {
        if let Err(error) = client.shutdown().await {
            log::error!("Failed to shut down netlink client for {}: {}", namespace, error);
        } else {
            log::info!("Shut down netlink client for namespace: {}", namespace);
        }
    }
}

async fn collect_namespace_round(
    namespaces: &[String],
    clients: Arc<RwLock<HashMap<String, Arc<NamespaceNetlinkClient>>>>,
) -> Vec<(String, Result<TrafficData>)> {
    let mut results = Vec::with_capacity(namespaces.len());
    let semaphore = Arc::new(Semaphore::new(MAX_NAMESPACE_CONCURRENCY));
    let client_snapshot = {
        let clients_guard = clients.read().await;
        namespaces
            .iter()
            .map(|namespace| (namespace.clone(), clients_guard.get(namespace).cloned()))
            .collect::<Vec<_>>()
    };
    let mut join_set = JoinSet::new();
    let collect_started_at = Instant::now();

    for (namespace, client) in client_snapshot {
        let semaphore = Arc::clone(&semaphore);
        join_set.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("collection semaphore closed unexpectedly");

            let result = match client {
                Some(client) => collect_namespace_data(&namespace, &client).await,
                None => Err(anyhow!(
                    "no netlink client available for namespace {}",
                    namespace
                )),
            };

            (namespace, result)
        });
    }

    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok((namespace, result)) => results.push((namespace, result)),
            Err(error) => {
                log::error!("Collection task failed: {}", error);
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

async fn collect_namespace_data(
    namespace: &str,
    client: &NamespaceNetlinkClient,
) -> Result<TrafficData> {
    let timestamp = Utc::now();
    let interfaces = client
        .collect_interfaces()
        .await
        .with_context(|| format!("failed to collect interface stats for namespace {}", namespace))?;

    Ok(TrafficData {
        namespace: namespace.to_string(),
        timestamp: timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
        timestamp_ms: timestamp.timestamp_millis(),
        interfaces,
        resolution: None,
    })
}

fn should_aggregate(current_ts: i64, last_ts: Option<i64>, interval_ms: i64) -> bool {
    let current_aligned = align_timestamp(current_ts, interval_ms);

    match last_ts {
        None => true,
        Some(last) => current_aligned > last,
    }
}

fn align_timestamp(timestamp_ms: i64, interval_ms: i64) -> i64 {
    (timestamp_ms / interval_ms) * interval_ms
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
}
