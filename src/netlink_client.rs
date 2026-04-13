use crate::models::RawInterfaceStats;
use anyhow::{anyhow, Result};

#[cfg(target_os = "linux")]
mod imp {
    use super::*;
    use futures_util::stream::TryStreamExt;
    use rtnetlink::{
        new_connection,
        packet_route::link::{LinkAttribute, LinkMessage, Stats, Stats64},
        Handle,
    };
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    };
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;

    use crate::netns;

    pub struct NamespaceNetlinkClient {
        namespace: String,
        handle: Handle,
        collect_lock: Mutex<()>,
        connection_task: StdMutex<Option<JoinHandle<()>>>,
        closed: Arc<AtomicBool>,
    }

    impl NamespaceNetlinkClient {
        pub async fn new(namespace: String) -> Result<Self> {
            let (connection, handle, _) = netns::run_in_namespace(&namespace, || {
                new_connection().context("failed to create rtnetlink connection")
            })
            .with_context(|| {
                format!(
                    "failed to initialize netlink client in namespace {}",
                    namespace
                )
            })?;

            let closed = Arc::new(AtomicBool::new(false));
            let closed_for_task = Arc::clone(&closed);
            let namespace_for_task = namespace.clone();

            let connection_task = tokio::spawn(async move {
                connection.await;
                closed_for_task.store(true, Ordering::Release);
                log::warn!(
                    "Long-lived rtnetlink connection task exited for namespace {}",
                    namespace_for_task
                );
            });

            Ok(Self {
                namespace,
                handle,
                collect_lock: Mutex::new(()),
                connection_task: StdMutex::new(Some(connection_task)),
                closed,
            })
        }

        pub async fn collect_interfaces(&self) -> Result<Vec<RawInterfaceStats>> {
            let _guard = self.collect_lock.lock().await;

            if self.is_closed() {
                return Err(anyhow!(
                    "netlink client is closed for namespace {}",
                    self.namespace
                ));
            }

            load_link_stats(self.handle.clone()).await.with_context(|| {
                format!(
                    "failed to collect interface stats for namespace {}",
                    self.namespace
                )
            })
        }

        pub async fn shutdown(&self) -> Result<()> {
            let join_handle = {
                let mut guard = self
                    .connection_task
                    .lock()
                    .map_err(|_| anyhow!("netlink client connection task mutex poisoned"))?;
                guard.take()
            };

            self.closed.store(true, Ordering::Release);

            if let Some(join_handle) = join_handle {
                join_handle.abort();
                match join_handle.await {
                    Ok(()) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        return Err(anyhow!(
                            "failed to stop rtnetlink connection task for namespace {}: {}",
                            self.namespace,
                            error
                        ));
                    }
                }
            }

            Ok(())
        }

        pub fn is_closed(&self) -> bool {
            if self.closed.load(Ordering::Acquire) {
                return true;
            }

            match self.connection_task.lock() {
                Ok(guard) => match guard.as_ref() {
                    Some(handle) => handle.is_finished(),
                    None => true,
                },
                Err(_) => true,
            }
        }
    }

    async fn load_link_stats(handle: Handle) -> Result<Vec<RawInterfaceStats>> {
        let mut interfaces = Vec::new();
        let mut links = handle.link().get().execute();

        while let Some(message) = links
            .try_next()
            .await
            .context("failed to receive rtnetlink link message")?
        {
            if let Some(interface) = interface_stats_from_link_message(message) {
                interfaces.push(interface);
            }
        }

        interfaces.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(interfaces)
    }

    fn interface_stats_from_link_message(message: LinkMessage) -> Option<RawInterfaceStats> {
        interface_stats_from_link_attributes(message.attributes)
    }

    fn interface_stats_from_link_attributes(
        attributes: Vec<LinkAttribute>,
    ) -> Option<RawInterfaceStats> {
        let mut interface_name: Option<String> = None;
        let mut stats64: Option<NetlinkInterfaceCounters> = None;
        let mut stats32: Option<NetlinkInterfaceCounters> = None;

        for attribute in attributes {
            match attribute {
                LinkAttribute::IfName(name) => interface_name = Some(name),
                LinkAttribute::Stats64(stats) => stats64 = Some(stats.into()),
                LinkAttribute::Stats(stats) => stats32 = Some(stats.into()),
                _ => {}
            }
        }

        interface_stats_from_counters(interface_name, stats64, stats32)
    }

    fn interface_stats_from_counters(
        interface_name: Option<String>,
        stats64: Option<NetlinkInterfaceCounters>,
        stats32: Option<NetlinkInterfaceCounters>,
    ) -> Option<RawInterfaceStats> {
        let name = interface_name?;
        if name == "lo" {
            return None;
        }

        let counters = stats64.or(stats32)?;

        Some(RawInterfaceStats {
            name,
            rx_bytes: counters.rx_bytes,
            tx_bytes: counters.tx_bytes,
            rx_dropped: counters.rx_dropped,
            tx_dropped: counters.tx_dropped,
        })
    }

    #[derive(Debug, Default, Clone, Copy)]
    struct NetlinkInterfaceCounters {
        rx_bytes: u64,
        tx_bytes: u64,
        rx_dropped: u64,
        tx_dropped: u64,
    }

    impl From<Stats64> for NetlinkInterfaceCounters {
        fn from(stats: Stats64) -> Self {
            Self {
                rx_bytes: stats.rx_bytes,
                tx_bytes: stats.tx_bytes,
                rx_dropped: stats.rx_dropped,
                tx_dropped: stats.tx_dropped,
            }
        }
    }

    impl From<Stats> for NetlinkInterfaceCounters {
        fn from(stats: Stats) -> Self {
            Self {
                rx_bytes: stats.rx_bytes as u64,
                tx_bytes: stats.tx_bytes as u64,
                rx_dropped: stats.rx_dropped as u64,
                tx_dropped: stats.tx_dropped as u64,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_interface_stats_from_counters_prefers_stats64() {
            let interface = interface_stats_from_counters(
                Some("eth0".to_string()),
                Some(NetlinkInterfaceCounters {
                    rx_bytes: 1000,
                    tx_bytes: 2000,
                    rx_dropped: 10,
                    tx_dropped: 20,
                }),
                Some(NetlinkInterfaceCounters {
                    rx_bytes: 10,
                    tx_bytes: 20,
                    rx_dropped: 1,
                    tx_dropped: 2,
                }),
            )
            .unwrap();

            assert_eq!(interface.name, "eth0");
            assert_eq!(interface.rx_bytes, 1000);
            assert_eq!(interface.tx_bytes, 2000);
            assert_eq!(interface.rx_dropped, 10);
            assert_eq!(interface.tx_dropped, 20);
        }

        #[test]
        fn test_interface_stats_from_counters_falls_back_to_stats32() {
            let interface = interface_stats_from_counters(
                Some("eth0.1".to_string()),
                None,
                Some(NetlinkInterfaceCounters {
                    rx_bytes: 500,
                    tx_bytes: 600,
                    rx_dropped: 3,
                    tx_dropped: 4,
                }),
            )
            .unwrap();

            assert_eq!(interface.name, "eth0.1");
            assert_eq!(interface.rx_bytes, 500);
            assert_eq!(interface.tx_bytes, 600);
            assert_eq!(interface.rx_dropped, 3);
            assert_eq!(interface.tx_dropped, 4);
        }

        #[test]
        fn test_interface_stats_from_counters_ignores_loopback() {
            let interface = interface_stats_from_counters(
                Some("lo".to_string()),
                Some(NetlinkInterfaceCounters {
                    rx_bytes: 100,
                    tx_bytes: 200,
                    rx_dropped: 1,
                    tx_dropped: 2,
                }),
                None,
            );

            assert!(interface.is_none());
        }

        #[test]
        fn test_interface_stats_from_counters_requires_stats() {
            let interface = interface_stats_from_counters(Some("eth0".to_string()), None, None);

            assert!(interface.is_none());
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::*;

    pub struct NamespaceNetlinkClient {
        namespace: String,
    }

    impl NamespaceNetlinkClient {
        pub async fn new(namespace: String) -> Result<Self> {
            Err(anyhow!(
                "long-lived rtnetlink clients are only supported on linux (namespace: {})",
                namespace
            ))
        }

        pub async fn collect_interfaces(&self) -> Result<Vec<RawInterfaceStats>> {
            Err(anyhow!(
                "long-lived rtnetlink collection is only supported on linux (namespace: {})",
                self.namespace
            ))
        }

        pub async fn shutdown(&self) -> Result<()> {
            Ok(())
        }

        pub fn is_closed(&self) -> bool {
            true
        }
    }
}

pub use imp::NamespaceNetlinkClient;
