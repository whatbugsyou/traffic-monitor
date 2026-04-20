use anyhow::{anyhow, Context, Result};
use std::fs;

#[cfg(target_os = "linux")]
use std::fs::File;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
pub fn current_tid() -> libc::pid_t {
    unsafe { libc::syscall(libc::SYS_gettid) as libc::pid_t }
}

#[cfg(not(target_os = "linux"))]
pub fn current_tid() -> libc::pid_t {
    unsafe { libc::getpid() }
}

pub fn read_thread_net_dev(tid: libc::pid_t) -> Result<String> {
    let path = format!("/proc/self/task/{}/net/dev", tid);
    fs::read_to_string(&path).with_context(|| format!("failed to read {}", path))
}

pub fn read_current_thread_net_dev() -> Result<String> {
    read_thread_net_dev(current_tid())
}

#[cfg(target_os = "linux")]
pub struct NamespaceGuard {
    original_ns: File,
    original_tid: libc::pid_t,
    target_namespace: Option<String>,
    restored: bool,
}

#[cfg(target_os = "linux")]
impl NamespaceGuard {
    pub fn enter(namespace: &str) -> Result<Option<Self>> {
        if namespace == "default" {
            return Ok(None);
        }

        let original_tid = current_tid();
        let original_ns =
            File::open("/proc/self/ns/net").context("failed to open current network namespace")?;
        let target_path = format!("/var/run/netns/{}", namespace);
        let target_ns = File::open(&target_path)
            .with_context(|| format!("failed to open target network namespace {}", target_path))?;

        unsafe {
            if libc::setns(target_ns.as_raw_fd(), libc::CLONE_NEWNET) != 0 {
                return Err(anyhow!(
                    "failed to switch to network namespace {}: {}",
                    namespace,
                    std::io::Error::last_os_error()
                ));
            }
        }

        Ok(Some(Self {
            original_ns,
            original_tid,
            target_namespace: Some(namespace.to_string()),
            restored: false,
        }))
    }

    pub fn restore(&mut self) -> Result<()> {
        if self.restored {
            return Ok(());
        }

        unsafe {
            if libc::setns(self.original_ns.as_raw_fd(), libc::CLONE_NEWNET) != 0 {
                return Err(anyhow!(
                    "failed to restore network namespace on tid {} from {:?}: {}",
                    self.original_tid,
                    self.target_namespace,
                    std::io::Error::last_os_error()
                ));
            }
        }

        self.restored = true;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Drop for NamespaceGuard {
    fn drop(&mut self) {
        if self.restored {
            return;
        }

        if let Err(error) = self.restore() {
            log::error!("{}", error);
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
pub struct NamespaceGuard {
    _target_namespace: Option<String>,
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
impl NamespaceGuard {
    pub fn enter(namespace: &str) -> Result<Option<Self>> {
        if namespace == "default" {
            return Ok(None);
        }

        Err(anyhow!(
            "network namespaces are only supported on Linux (requested: {})",
            namespace
        ))
    }

    pub fn restore(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
impl Drop for NamespaceGuard {
    fn drop(&mut self) {}
}

#[cfg(target_os = "linux")]
pub fn run_in_namespace<T, F>(namespace: &str, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let tid = current_tid();
    let _guard = NamespaceGuard::enter(namespace)?;

    f().with_context(|| {
        format!(
            "failed to execute operation in namespace {} on tid {}",
            namespace, tid
        )
    })
}

#[cfg(not(target_os = "linux"))]
pub fn run_in_namespace<T, F>(namespace: &str, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    if namespace != "default" {
        return Err(anyhow!(
            "network namespaces are only supported on Linux (requested: {})",
            namespace
        ));
    }

    f()
}

pub fn read_thread_net_dev_in_namespace(namespace: &str) -> Result<String> {
    run_in_namespace(namespace, read_current_thread_net_dev).with_context(|| {
        format!(
            "failed to read thread network stats in namespace {}",
            namespace
        )
    })
}
