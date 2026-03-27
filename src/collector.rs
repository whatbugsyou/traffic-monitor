use anyhow::{Context, Result};
use chrono::Utc;
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
    data_sender: broadcast::Sender<TrafficData>,
    shutdown: broadcast::Sender<()>,
}

impl TrafficCollector {
    /// 创建新的采集器
    pub fn new(db: Arc<Database>, config: CollectorConfig) -> Result<Self> {
        let (data_sender, _) = broadcast::channel(1000);
        let (shutdown, _) = broadcast::channel(1);

        let namespaces = Arc::new(RwLock::new(Vec::new()));

        Ok(TrafficCollector {
            db,
            config,
            namespaces,
            data_sender,
            shutdown,
        })
    }

    /// 启动采集器
    pub async fn start(&self) -> Result<()> {
        // 发现所有命名空间
        self.discover_namespaces().await?;

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
        let data_sender = self.data_sender.clone();
        let mut shutdown_rx = self.shutdown.subscribe();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut collect_interval = interval(Duration::from_secs(config.interval_secs));
            let mut last_10s_ts: Option<i64> = None;
            let mut last_1m_ts: Option<i64> = None;
            let mut last_1h_ts: Option<i64> = None;
            let mut last_1d_ts: Option<i64> = None;

            log::info!("Started collector for namespace: {}", namespace);

            loop {
                tokio::select! {
                    _ = collect_interval.tick() => {
                        match collect_namespace_data(&namespace).await {
                            Ok(data) => {
                                let timestamp_ms = data.timestamp_ms;

                                // 存储原始数据
                                if let Err(e) = db.insert_traffic_data(&data) {
                                    log::error!("Failed to store data for {}: {}", namespace, e);
                                }

                                // 检查是否到达10秒聚合点
                                if should_aggregate(timestamp_ms, last_10s_ts, INTERVAL_10S) {
                                    if let Err(e) = db.insert_10s_aggregated(&data) {
                                        log::error!("Failed to store 10s aggregated data for {}: {}", namespace, e);
                                    }
                                    last_10s_ts = Some(align_timestamp(timestamp_ms, INTERVAL_10S));
                                    log::debug!("Saved 10s aggregated data for {} at {}", namespace, data.timestamp);
                                }

                                // 检查是否到达1分钟聚合点
                                if should_aggregate(timestamp_ms, last_1m_ts, INTERVAL_1M) {
                                    if let Err(e) = db.insert_1m_aggregated(&data) {
                                        log::error!("Failed to store 1m aggregated data for {}: {}", namespace, e);
                                    }
                                    last_1m_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1M));
                                    log::debug!("Saved 1m aggregated data for {} at {}", namespace, data.timestamp);
                                }

                                // 检查是否到达1小时聚合点
                                if should_aggregate(timestamp_ms, last_1h_ts, INTERVAL_1H) {
                                    if let Err(e) = db.insert_1h_aggregated(&data) {
                                        log::error!("Failed to store 1h aggregated data for {}: {}", namespace, e);
                                    }
                                    last_1h_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1H));
                                    log::debug!("Saved 1h aggregated data for {} at {}", namespace, data.timestamp);
                                }

                                // 检查是否到达1天聚合点
                                if should_aggregate(timestamp_ms, last_1d_ts, INTERVAL_1D) {
                                    if let Err(e) = db.insert_1d_aggregated(&data) {
                                        log::error!("Failed to store 1d aggregated data for {}: {}", namespace, e);
                                    }
                                    last_1d_ts = Some(align_timestamp(timestamp_ms, INTERVAL_1D));
                                    log::debug!("Saved 1d aggregated data for {} at {}", namespace, data.timestamp);
                                }

                                // 广播数据
                                if let Err(e) = data_sender.send(data) {
                                    log::error!("Failed to broadcast data for {}: {}", namespace, e);
                                }
                            }
                            Err(e) => {
                                log::error!("Failed to collect data for {}: {}", namespace, e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Collector stopped for namespace: {}", namespace);
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// 获取数据接收器
    pub fn subscribe(&self) -> broadcast::Receiver<TrafficData> {
        self.data_sender.subscribe()
    }

    /// 获取命名空间列表
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

    // 采集 ppp0 数据
    let ppp0 = collect_ppp0_stats(namespace).await?;

    let data = TrafficData {
        namespace: namespace.to_string(),
        timestamp,
        timestamp_ms,
        interfaces,
        ppp0,
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
        // 其他命名空间：使用 ip netns exec
        collect_interfaces_via_ip_command(namespace, &mut interfaces).await?;
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

/// 通过 ip netns exec 命令读取接口数据
async fn collect_interfaces_via_ip_command(
    namespace: &str,
    interfaces: &mut Vec<InterfaceStats>,
) -> Result<()> {
    use std::process::Command;

    let output = Command::new("ip")
        .args(["netns", "exec", namespace, "ls", "/sys/class/net"])
        .output()
        .context("Failed to execute ip netns exec command")?;

    if !output.status.success() {
        return Ok(());
    }

    let dev_list = String::from_utf8_lossy(&output.stdout);
    let devices: Vec<&str> = dev_list.split_whitespace().collect();

    for dev in devices {
        // 排除 lo、VLAN 子接口、veth 别名
        if dev == "lo" || dev.contains('.') || dev.contains('@') {
            continue;
        }

        // 读取 rx_bytes
        let rx_output = Command::new("ip")
            .args([
                "netns",
                "exec",
                namespace,
                "cat",
                &format!("/sys/class/net/{}/statistics/rx_bytes", dev),
            ])
            .output()
            .context("Failed to read rx_bytes")?;

        let rx_bytes = String::from_utf8_lossy(&rx_output.stdout)
            .trim()
            .parse()
            .unwrap_or(0);

        // 读取 tx_bytes
        let tx_output = Command::new("ip")
            .args([
                "netns",
                "exec",
                namespace,
                "cat",
                &format!("/sys/class/net/{}/statistics/tx_bytes", dev),
            ])
            .output()
            .context("Failed to read tx_bytes")?;

        let tx_bytes = String::from_utf8_lossy(&tx_output.stdout)
            .trim()
            .parse()
            .unwrap_or(0);

        interfaces.push(InterfaceStats {
            name: dev.to_string(),
            rx_bytes,
            tx_bytes,
            rx_speed: None,
            tx_speed: None,
        });
    }

    Ok(())
}

/// 采集 ppp0 接口统计信息
async fn collect_ppp0_stats(namespace: &str) -> Result<Ppp0Stats> {
    use std::process::Command;

    // 检查 ppp0 是否存在
    let ppp0_exists = if namespace == "default" {
        Path::new("/sys/class/net/ppp0").exists()
    } else {
        Command::new("ip")
            .args([
                "netns",
                "exec",
                namespace,
                "test",
                "-d",
                "/sys/class/net/ppp0",
            ])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    };

    if !ppp0_exists {
        return Ok(Ppp0Stats::unavailable());
    }

    // 使用 ifconfig 读取详细统计
    let output = if namespace == "default" {
        Command::new("ifconfig")
            .arg("ppp0")
            .output()
            .context("Failed to execute ifconfig ppp0")?
    } else {
        Command::new("ip")
            .args(["netns", "exec", namespace, "ifconfig", "ppp0"])
            .output()
            .context("Failed to execute ifconfig ppp0 in namespace")?
    };

    if !output.status.success() {
        return Ok(Ppp0Stats::unavailable());
    }

    parse_ifconfig_output(&String::from_utf8_lossy(&output.stdout))
}

/// 解析 ifconfig 输出
fn parse_ifconfig_output(output: &str) -> Result<Ppp0Stats> {
    let mut stats = Ppp0Stats::unavailable();
    stats.available = true;

    for line in output.lines() {
        if line.contains("RX packets") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            stats.rx_packets = parts.get(2).and_then(|s| s.parse().ok());
            stats.rx_errors = parts.get(4).and_then(|s| s.parse().ok());
            stats.rx_dropped = parts.get(6).and_then(|s| s.parse().ok());
            stats.rx_overruns = parts.get(8).and_then(|s| s.parse().ok());
            stats.rx_frame = parts.get(10).and_then(|s| s.parse().ok());
        } else if line.contains("TX packets") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            stats.tx_packets = parts.get(2).and_then(|s| s.parse().ok());
            stats.tx_errors = parts.get(4).and_then(|s| s.parse().ok());
            stats.tx_dropped = parts.get(6).and_then(|s| s.parse().ok());
            stats.tx_overruns = parts.get(8).and_then(|s| s.parse().ok());
            stats.tx_carrier = parts.get(10).and_then(|s| s.parse().ok());
        }
    }

    Ok(stats)
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

    #[test]
    fn test_parse_ifconfig_output() {
        let output = r#"
ppp0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 10.0.0.1  netmask 255.255.255.0  broadcast 10.0.0.255
        RX packets 12345  errors 10  dropped 5  overruns 2  frame 1
        TX packets 9876  errors 5  dropped 3  overruns 1  carrier 0
"#;

        let stats = parse_ifconfig_output(output).unwrap();
        assert!(stats.available);
        assert_eq!(stats.rx_packets, Some(12345));
        assert_eq!(stats.rx_dropped, Some(5));
        assert_eq!(stats.tx_packets, Some(9876));
        assert_eq!(stats.tx_dropped, Some(3));
    }
}
