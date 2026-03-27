use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::interval;

use crate::database::Database;
use crate::models::*;

const INTERVAL_10S: i64 = 10; // 10秒
const INTERVAL_1M: i64 = 60;  // 1分钟

/// 数据聚合器
pub struct DataAggregator {
    db: Arc<Database>,
    interval_secs: u64,
    shutdown: broadcast::Sender<()>,
}

impl DataAggregator {
    /// 创建新的聚合器
    pub fn new(db: Arc<Database>, interval_secs: u64) -> Result<Self> {
        let (shutdown, _) = broadcast::channel(1);

        Ok(DataAggregator {
            db,
            interval_secs,
            shutdown,
        })
    }

    /// 启动聚合器
    pub async fn start(&self) -> Result<()> {
        let db = Arc::clone(&self.db);
        let interval_secs = self.interval_secs;
        let mut shutdown_rx = self.shutdown.subscribe();

        tokio::spawn(async move {
            let mut aggregate_interval = interval(Duration::from_secs(interval_secs));

            log::info!("Data aggregator started with interval {}s", interval_secs);

            loop {
                tokio::select! {
                    _ = aggregate_interval.tick() => {
                        if let Err(e) = aggregate_all_namespaces(&db).await {
                            log::error!("Aggregation failed: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Data aggregator stopped");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// 停止聚合器
    pub fn stop(&self) -> Result<()> {
        self.shutdown.send(()).context("Failed to send shutdown signal")?;
        Ok(())
    }
}

/// 聚合所有命名空间的数据
async fn aggregate_all_namespaces(db: &Arc<Database>) -> Result<()> {
    let namespaces = db.get_namespaces().context("Failed to get namespaces")?;

    for namespace in namespaces {
        if let Err(e) = aggregate_namespace_data(db, &namespace).await {
            log::error!("Failed to aggregate data for {}: {}", namespace, e);
        }
    }

    Ok(())
}

/// 聚合单个命名空间的数据
async fn aggregate_namespace_data(db: &Arc<Database>, namespace: &str) -> Result<()> {
    let now_ms = Utc::now().timestamp_millis();

    // 生成10秒聚合数据
    aggregate_to_10s(db, namespace, now_ms).await?;

    // 生成1分钟聚合数据
    aggregate_to_1m(db, namespace, now_ms).await?;

    Ok(())
}

/// 生成10秒聚合数据
async fn aggregate_to_10s(db: &Arc<Database>, namespace: &str, now_ms: i64) -> Result<()> {
    let one_hour_ago_ms = now_ms - (60 * 60 * 1000);

    // 获取最近1小时的原始数据
    let raw_data = db
        .get_raw_data_for_aggregation(namespace, one_hour_ago_ms)
        .context("Failed to get raw data for 10s aggregation")?;

    if raw_data.is_empty() {
        return Ok(());
    }

    // 按10秒分组
    let mut groups_10s: HashMap<i64, AggregationGroup> = HashMap::new();

    for data in raw_data {
        // 计算所属的10秒区间
        let group_key = (data.timestamp_ms / (INTERVAL_10S * 1000)) * (INTERVAL_10S * 1000);

        let group = groups_10s.entry(group_key).or_insert_with(|| {
            AggregationGroup {
                rx_speeds: Vec::new(),
                tx_speeds: Vec::new(),
                rx_dropped_list: Vec::new(),
                tx_dropped_list: Vec::new(),
                timestamp: data.timestamp.clone(),
            }
        });

        // 计算速度（需要上一条数据）
        // 这里简化处理，实际速度计算需要上一条数据
        // 在实际实现中，应该在采集时就计算好速度
        for iface in &data.interfaces {
            if let Some(rx_speed) = iface.rx_speed {
                group.rx_speeds.push(rx_speed as f64);
            }
            if let Some(tx_speed) = iface.tx_speed {
                group.tx_speeds.push(tx_speed as f64);
            }
        }

        if data.ppp0.available {
            if let Some(rx_dropped) = data.ppp0.rx_dropped {
                group.rx_dropped_list.push(rx_dropped);
            }
            if let Some(tx_dropped) = data.ppp0.tx_dropped {
                group.tx_dropped_list.push(tx_dropped);
            }
        }
    }

    // 写入10秒聚合表
    for (group_key, group) in groups_10s {
        if group.rx_speeds.is_empty() {
            continue;
        }

        let rx_speed_avg = group.rx_speeds.iter().sum::<f64>() / group.rx_speeds.len() as f64;
        let tx_speed_avg = group.tx_speeds.iter().sum::<f64>() / group.tx_speeds.len() as f64;

        let rx_dropped_sum = group.rx_dropped_list.iter().max().copied().unwrap_or(0);
        let tx_dropped_sum = group.tx_dropped_list.iter().max().copied().unwrap_or(0);

        let aggregated = AggregatedData {
            namespace: namespace.to_string(),
            timestamp: group.timestamp,
            timestamp_ms: group_key,
            rx_speed_avg,
            tx_speed_avg,
            rx_dropped_sum,
            tx_dropped_sum,
            sample_count: group.rx_speeds.len() as u32,
            resolution: Some("10s".to_string()),
        };

        db.insert_10s_aggregated(&aggregated)
            .with_context(|| format!("Failed to insert 10s aggregated data for {}", namespace))?;
    }

    Ok(())
}

/// 生成1分钟聚合数据
async fn aggregate_to_1m(db: &Arc<Database>, namespace: &str, now_ms: i64) -> Result<()> {
    let three_hours_ago_ms = now_ms - (3 * 60 * 60 * 1000);

    // 获取最近3小时的10秒聚合数据
    let data_10s = db
        .get_10s_data_for_aggregation(namespace, three_hours_ago_ms)
        .context("Failed to get 10s data for 1m aggregation")?;

    if data_10s.is_empty() {
        return Ok(());
    }

    // 按1分钟分组
    let mut groups_1m: HashMap<i64, OneMinuteGroup> = HashMap::new();

    for data in data_10s {
        let group_key = (data.timestamp_ms / (INTERVAL_1M * 1000)) * (INTERVAL_1M * 1000);

        let group = groups_1m.entry(group_key).or_insert_with(|| OneMinuteGroup {
            rx_speeds: Vec::new(),
            tx_speeds: Vec::new(),
            rx_dropped: Vec::new(),
            tx_dropped: Vec::new(),
            timestamps: Vec::new(),
        });

        group.rx_speeds.push(data.rx_speed_avg);
        group.tx_speeds.push(data.tx_speed_avg);
        group.rx_dropped.push(data.rx_dropped_sum);
        group.tx_dropped.push(data.tx_dropped_sum);
        group.timestamps.push(data.timestamp);
    }

    // 写入1分钟聚合表
    for (group_key, group) in groups_1m {
        if group.rx_speeds.is_empty() {
            continue;
        }

        let rx_speed_avg = group.rx_speeds.iter().sum::<f64>() / group.rx_speeds.len() as f64;
        let tx_speed_avg = group.tx_speeds.iter().sum::<f64>() / group.tx_speeds.len() as f64;

        let rx_dropped_sum = group.rx_dropped.iter().max().copied().unwrap_or(0);
        let tx_dropped_sum = group.tx_dropped.iter().max().copied().unwrap_or(0);

        let timestamp = group.timestamps.last().cloned().unwrap_or_default();

        let aggregated = AggregatedData {
            namespace: namespace.to_string(),
            timestamp,
            timestamp_ms: group_key,
            rx_speed_avg,
            tx_speed_avg,
            rx_dropped_sum,
            tx_dropped_sum,
            sample_count: group.rx_speeds.len() as u32,
            resolution: Some("1m".to_string()),
        };

        db.insert_1m_aggregated(&aggregated)
            .with_context(|| format!("Failed to insert 1m aggregated data for {}", namespace))?;
    }

    Ok(())
}

/// 10秒聚合组
struct AggregationGroup {
    rx_speeds: Vec<f64>,
    tx_speeds: Vec<f64>,
    rx_dropped_list: Vec<u64>,
    tx_dropped_list: Vec<u64>,
    timestamp: String,
}

/// 1分钟聚合组
struct OneMinuteGroup {
    rx_speeds: Vec<f64>,
    tx_speeds: Vec<f64>,
    rx_dropped: Vec<u64>,
    tx_dropped: Vec<u64>,
    timestamps: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_aggregation_group() {
        let mut group = AggregationGroup {
            rx_speeds: vec![100.0, 200.0, 300.0],
            tx_speeds: vec![50.0, 100.0, 150.0],
            rx_dropped_list: vec![10, 20, 30],
            tx_dropped_list: vec![5, 10, 15],
            timestamp: "2024-01-01 00:00:00".to_string(),
        };

        assert_eq!(group.rx_speeds.len(), 3);
        assert_eq!(group.tx_speeds.len(), 3);
    }
}
