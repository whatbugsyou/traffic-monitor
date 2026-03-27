use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::interval;

use crate::database::Database;
use crate::models::*;

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
            let entries = fs::read_dir(netns_dir)
                .context("Failed to read /var/run/netns directory")?;

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
            let mut prev_data: Option<TrafficData> = None;

            log::info!("Started collector for namespace: {}", namespace);

            loop {
                tokio::select! {
                    _ = collect_interval.tick() => {
                        match collect_namespace_data(&namespace, prev_data.as_ref()).await {
                            Ok(data) => {
                                // 存储到数据库
                                if let Err(e) = db.insert_traffic_data(&data) {
                                    log::error!("Failed to store data for {}: {}", namespace, e);
                                }

                                // 广播数据
                                if let Err(e) = data_sender.send(data.clone()) {
                                    log::error!("Failed to broadcast data for {}: {}", namespace, e);
                                }

                                prev_data = Some(data);
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
        self.shutdown.send(()).context("Failed to send shutdown signal")?;
        Ok(())
    }
}

/// 采集指定命名空间的数据
async fn collect_namespace_data(
    namespace: &str,
    prev_data: Option<&TrafficData>,
) -> Result<TrafficData> {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let timestamp_ms = Utc::now().timestamp_millis();

    // 采集接口数据
    let interfaces = collect_interfaces(namespace).await?;

    // 采集 ppp0 数据
    let ppp0 = collect_ppp0_stats(namespace).await?;

    let mut data = TrafficData {
        namespace: namespace.to_string(),
        timestamp,
        timestamp_ms,
        interfaces,
        ppp0,
        resolution: Some("1s".to_string()),
    };

    // 计算速度（需要上一条数据）
    if let Some(prev) = prev_data {
        calculate_speeds(&mut data, prev);
    }

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
fn collect_interfaces_from_sysfs(base_path: &str, interfaces: &mut Vec<InterfaceStats>) -> Result<()> {
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
    let check_output = if namespace == "default" {
        Command::new("test")
            .args(["-d", "/sys/class/net/ppp0"])
            .output()
            .ok()
    } else {
        Command::new("ip")
            .args(["netns", "exec", namespace, "test", "-d", "/sys/class/net/ppp0"])
            .output()
            .ok()
    };

    let ppp0_exists = check_output.map(|o| o.status.success()).unwrap_or(false);

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

/// 计算速度
fn calculate_speeds(current: &mut TrafficData, prev: &TrafficData) {
    let interval_ms = current.timestamp_ms - prev.timestamp_ms;

    if interval_ms <= 0 {
        return;
    }

    // 计算接口速度
    for iface in &mut current.interfaces {
        if let Some(prev_iface) = prev.interfaces.iter().find(|i| i.name == iface.name) {
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
        }
    }

    // 计算丢包增量
    if current.ppp0.available && prev.ppp0.available {
        current.ppp0.rx_dropped_inc = Some(
            current
                .ppp0
                .rx_dropped
                .unwrap_or(0)
                .saturating_sub(prev.ppp0.rx_dropped.unwrap_or(0)),
        );

        current.ppp0.tx_dropped_inc = Some(
            current
                .ppp0
                .tx_dropped
                .unwrap_or(0)
                .saturating_sub(prev.ppp0.tx_dropped.unwrap_or(0)),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_discover_namespaces() {
        let config = DatabaseConfig::default();
        let db = Arc::new(Database::new(config).unwrap());

        let collector_config = CollectorConfig::default();
        let collector = TrafficCollector::new(db, collector_config).unwrap();

        collector.discover_namespaces().await.unwrap();

        let namespaces = collector.get_namespaces().await;
        assert!(!namespaces.is_empty());
        assert!(namespaces.contains(&"default".to_string()));
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
