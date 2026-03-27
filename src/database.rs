use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, Row};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::models::*;

/// 数据库管理器
#[derive(Debug)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
    config: DatabaseConfig,
}

impl Database {
    /// 创建新的数据库连接
    pub fn new(config: DatabaseConfig) -> Result<Self> {
        let db_path = Path::new(&config.db_path);

        // 确保数据库目录存在
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create database directory: {:?}", parent))?;
        }

        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open database: {}", config.db_path))?;

        let db = Database {
            conn: Arc::new(Mutex::new(conn)),
            config,
        };

        db.initialize()?;
        Ok(db)
    }

    /// 初始化数据库表结构
    fn initialize(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        // 创建原始数据表
        conn.execute(
            "CREATE TABLE IF NOT EXISTS traffic_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL DEFAULT 'default',
                timestamp TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(namespace, timestamp_ms)
            )",
            [],
        )
        .context("Failed to create traffic_history table")?;

        // 创建10秒聚合表
        conn.execute(
            "CREATE TABLE IF NOT EXISTS traffic_history_10s (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL DEFAULT 'default',
                timestamp TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                rx_speed_avg REAL NOT NULL DEFAULT 0,
                tx_speed_avg REAL NOT NULL DEFAULT 0,
                rx_dropped_sum INTEGER NOT NULL DEFAULT 0,
                tx_dropped_sum INTEGER NOT NULL DEFAULT 0,
                sample_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(namespace, timestamp_ms)
            )",
            [],
        )
        .context("Failed to create traffic_history_10s table")?;

        // 创建1分钟聚合表
        conn.execute(
            "CREATE TABLE IF NOT EXISTS traffic_history_1m (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL DEFAULT 'default',
                timestamp TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                rx_speed_avg REAL NOT NULL DEFAULT 0,
                tx_speed_avg REAL NOT NULL DEFAULT 0,
                rx_dropped_sum INTEGER NOT NULL DEFAULT 0,
                tx_dropped_sum INTEGER NOT NULL DEFAULT 0,
                sample_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(namespace, timestamp_ms)
            )",
            [],
        )
        .context("Failed to create traffic_history_1m table")?;

        // 创建索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_history_namespace_ts
             ON traffic_history(namespace, timestamp_ms)",
            [],
        )
        .context("Failed to create index on traffic_history")?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_10s_namespace_ts
             ON traffic_history_10s(namespace, timestamp_ms)",
            [],
        )
        .context("Failed to create index on traffic_history_10s")?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_1m_namespace_ts
             ON traffic_history_1m(namespace, timestamp_ms)",
            [],
        )
        .context("Failed to create index on traffic_history_1m")?;

        // 创建触发器自动清理旧数据
        self.create_cleanup_triggers(&conn)?;

        Ok(())
    }

    /// 创建自动清理触发器
    fn create_cleanup_triggers(&self, conn: &Connection) -> Result<()> {
        // 原始数据保留5分钟
        let retention_raw_ms = self.config.retention_raw_minutes as i64 * 60 * 1000;
        conn.execute(
            &format!(
                "CREATE TRIGGER IF NOT EXISTS cleanup_raw_data
                 AFTER INSERT ON traffic_history
                 BEGIN
                     DELETE FROM traffic_history
                     WHERE timestamp_ms < (NEW.timestamp_ms - {});
                 END",
                retention_raw_ms
            ),
            [],
        )
        .context("Failed to create cleanup trigger for traffic_history")?;

        // 10秒聚合数据保留1小时
        let retention_10s_ms = self.config.retention_10s_hours as i64 * 60 * 60 * 1000;
        conn.execute(
            &format!(
                "CREATE TRIGGER IF NOT EXISTS cleanup_10s_data
                 AFTER INSERT ON traffic_history_10s
                 BEGIN
                     DELETE FROM traffic_history_10s
                     WHERE timestamp_ms < (NEW.timestamp_ms - {});
                 END",
                retention_10s_ms
            ),
            [],
        )
        .context("Failed to create cleanup trigger for traffic_history_10s")?;

        // 1分钟聚合数据保留3小时
        let retention_1m_ms = self.config.retention_1m_hours as i64 * 60 * 60 * 1000;
        conn.execute(
            &format!(
                "CREATE TRIGGER IF NOT EXISTS cleanup_1m_data
                 AFTER INSERT ON traffic_history_1m
                 BEGIN
                     DELETE FROM traffic_history_1m
                     WHERE timestamp_ms < (NEW.timestamp_ms - {});
                 END",
                retention_1m_ms
            ),
            [],
        )
        .context("Failed to create cleanup trigger for traffic_history_1m")?;

        Ok(())
    }

    /// 插入原始流量数据
    pub fn insert_traffic_data(&self, data: &TrafficData) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        let data_json = serde_json::to_string(data).context("Failed to serialize traffic data")?;

        conn.execute(
            "INSERT OR REPLACE INTO traffic_history
             (namespace, timestamp, timestamp_ms, data)
             VALUES (?1, ?2, ?3, ?4)",
            params![data.namespace, data.timestamp, data.timestamp_ms, data_json],
        )
        .context("Failed to insert traffic data")?;

        Ok(())
    }

    /// 批量插入原始流量数据
    pub fn batch_insert_traffic_data(&self, data_list: &[TrafficData]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let tx = conn
            .unchecked_transaction()
            .context("Failed to start transaction")?;

        for data in data_list {
            let data_json =
                serde_json::to_string(data).context("Failed to serialize traffic data")?;

            tx.execute(
                "INSERT OR REPLACE INTO traffic_history
                 (namespace, timestamp, timestamp_ms, data)
                 VALUES (?1, ?2, ?3, ?4)",
                params![data.namespace, data.timestamp, data.timestamp_ms, data_json],
            )
            .context("Failed to insert traffic data in batch")?;
        }

        tx.commit().context("Failed to commit transaction")?;
        Ok(())
    }

    /// 获取所有命名空间
    pub fn get_namespaces(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare("SELECT DISTINCT namespace FROM traffic_history ORDER BY namespace")
            .context("Failed to prepare statement")?;

        let namespaces = stmt
            .query_map([], |row| row.get(0))
            .context("Failed to query namespaces")?
            .collect::<std::result::Result<Vec<String>, _>>()
            .context("Failed to collect namespaces")?;

        Ok(namespaces)
    }

    /// 获取当前数据（最新的数据）
    pub fn get_current_data(&self, namespace: &str) -> Result<Option<TrafficData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT data FROM traffic_history
             WHERE namespace = ?1
             ORDER BY timestamp_ms DESC LIMIT 1",
            )
            .context("Failed to prepare statement")?;

        let result = stmt
            .query_row(params![namespace], |row| {
                let data_json: String = row.get(0)?;
                Ok(data_json)
            })
            .optional()
            .context("Failed to query current data")?;

        match result {
            Some(data_json) => {
                let data: TrafficData = serde_json::from_str(&data_json)
                    .context("Failed to deserialize traffic data")?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    /// 根据时间范围获取历史数据
    pub fn get_history_by_duration(
        &self,
        namespace: &str,
        duration_minutes: u32,
    ) -> Result<Vec<TrafficData>> {
        let now_ms = Utc::now().timestamp_millis();
        let since_ms = now_ms - (duration_minutes as i64 * 60 * 1000);

        if duration_minutes <= 5 {
            // 5分钟内：使用原始数据
            self.get_raw_history(namespace, since_ms)
        } else if duration_minutes <= 60 {
            // 1小时内：使用10秒聚合数据
            self.get_10s_aggregated_history(namespace, since_ms)
        } else {
            // 超过1小时：使用1分钟聚合数据
            self.get_1m_aggregated_history(namespace, since_ms)
        }
    }

    /// 获取原始历史数据
    fn get_raw_history(&self, namespace: &str, since_ms: i64) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT data FROM traffic_history
             WHERE namespace = ?1 AND timestamp_ms > ?2
             ORDER BY timestamp_ms ASC",
            )
            .context("Failed to prepare statement")?;

        let data_list = stmt
            .query_map(params![namespace, since_ms], |row| {
                let data_json: String = row.get(0)?;
                Ok(data_json)
            })
            .context("Failed to query raw history")?
            .collect::<std::result::Result<Vec<String>, _>>()
            .context("Failed to collect raw history")?;

        let mut result = Vec::new();
        let mut prev_data: Option<TrafficData> = None;

        for data_json in data_list {
            let mut data: TrafficData =
                serde_json::from_str(&data_json).context("Failed to deserialize traffic data")?;

            data.resolution = Some("1s".to_string());

            // 计算速度
            if let Some(prev) = &prev_data {
                let interval_ms = data.timestamp_ms - prev.timestamp_ms;
                if interval_ms > 0 {
                    for iface in &mut data.interfaces {
                        if let Some(prev_iface) =
                            prev.interfaces.iter().find(|i| i.name == iface.name)
                        {
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
                    if data.ppp0.available && prev.ppp0.available {
                        data.ppp0.rx_dropped_inc = Some(
                            data.ppp0
                                .rx_dropped
                                .unwrap_or(0)
                                .saturating_sub(prev.ppp0.rx_dropped.unwrap_or(0)),
                        );
                        data.ppp0.tx_dropped_inc = Some(
                            data.ppp0
                                .tx_dropped
                                .unwrap_or(0)
                                .saturating_sub(prev.ppp0.tx_dropped.unwrap_or(0)),
                        );
                    }
                }
            }

            result.push(data.clone());
            prev_data = Some(data);
        }

        Ok(result)
    }

    /// 获取10秒聚合历史数据
    fn get_10s_aggregated_history(
        &self,
        namespace: &str,
        since_ms: i64,
    ) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,
                    rx_dropped_sum, tx_dropped_sum, sample_count
             FROM traffic_history_10s
             WHERE namespace = ?1 AND timestamp_ms > ?2
             ORDER BY timestamp_ms ASC",
            )
            .context("Failed to prepare statement")?;

        let aggregated_list = stmt
            .query_map(params![namespace, since_ms], |row| {
                Ok(self.row_to_aggregated_data(row))
            })
            .context("Failed to query 10s aggregated history")?
            .collect::<std::result::Result<Vec<AggregatedData>, _>>()
            .context("Failed to collect 10s aggregated history")?;

        Ok(aggregated_list
            .into_iter()
            .map(|agg| {
                TrafficData {
                    namespace: agg.namespace,
                    timestamp: agg.timestamp,
                    timestamp_ms: agg.timestamp_ms,
                    interfaces: vec![], // 聚合数据不包含接口详情
                    ppp0: Ppp0Stats::unavailable(),
                    resolution: Some("10s".to_string()),
                }
            })
            .collect())
    }

    /// 获取1分钟聚合历史数据
    fn get_1m_aggregated_history(
        &self,
        namespace: &str,
        since_ms: i64,
    ) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,
                    rx_dropped_sum, tx_dropped_sum, sample_count
             FROM traffic_history_1m
             WHERE namespace = ?1 AND timestamp_ms > ?2
             ORDER BY timestamp_ms ASC",
            )
            .context("Failed to prepare statement")?;

        let aggregated_list = stmt
            .query_map(params![namespace, since_ms], |row| {
                Ok(self.row_to_aggregated_data(row))
            })
            .context("Failed to query 1m aggregated history")?
            .collect::<std::result::Result<Vec<AggregatedData>, _>>()
            .context("Failed to collect 1m aggregated history")?;

        Ok(aggregated_list
            .into_iter()
            .map(|agg| TrafficData {
                namespace: agg.namespace,
                timestamp: agg.timestamp,
                timestamp_ms: agg.timestamp_ms,
                interfaces: vec![],
                ppp0: Ppp0Stats::unavailable(),
                resolution: Some("1m".to_string()),
            })
            .collect())
    }

    /// 将数据库行转换为聚合数据
    fn row_to_aggregated_data(&self, row: &Row) -> AggregatedData {
        AggregatedData {
            namespace: row.get(0).unwrap_or_default(),
            timestamp: row.get(1).unwrap_or_default(),
            timestamp_ms: row.get(2).unwrap_or(0),
            rx_speed_avg: row.get(3).unwrap_or(0.0),
            tx_speed_avg: row.get(4).unwrap_or(0.0),
            rx_dropped_sum: row.get(5).unwrap_or(0),
            tx_dropped_sum: row.get(6).unwrap_or(0),
            sample_count: row.get(7).unwrap_or(0),
            resolution: None,
        }
    }

    /// 插入10秒聚合数据
    pub fn insert_10s_aggregated(&self, data: &AggregatedData) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "INSERT OR REPLACE INTO traffic_history_10s
             (namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,
              rx_dropped_sum, tx_dropped_sum, sample_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                data.namespace,
                data.timestamp,
                data.timestamp_ms,
                data.rx_speed_avg,
                data.tx_speed_avg,
                data.rx_dropped_sum,
                data.tx_dropped_sum,
                data.sample_count
            ],
        )
        .context("Failed to insert 10s aggregated data")?;

        Ok(())
    }

    /// 插入1分钟聚合数据
    pub fn insert_1m_aggregated(&self, data: &AggregatedData) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "INSERT OR REPLACE INTO traffic_history_1m
             (namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,
              rx_dropped_sum, tx_dropped_sum, sample_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                data.namespace,
                data.timestamp,
                data.timestamp_ms,
                data.rx_speed_avg,
                data.tx_speed_avg,
                data.rx_dropped_sum,
                data.tx_dropped_sum,
                data.sample_count
            ],
        )
        .context("Failed to insert 1m aggregated data")?;

        Ok(())
    }

    /// 获取指定时间范围内的原始数据（用于聚合）
    pub fn get_raw_data_for_aggregation(
        &self,
        namespace: &str,
        since_ms: i64,
    ) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT data FROM traffic_history
             WHERE namespace = ?1 AND timestamp_ms > ?2
             ORDER BY timestamp_ms ASC",
            )
            .context("Failed to prepare statement")?;

        let data_list = stmt
            .query_map(params![namespace, since_ms], |row| {
                let data_json: String = row.get(0)?;
                Ok(data_json)
            })
            .context("Failed to query raw data for aggregation")?
            .collect::<std::result::Result<Vec<String>, _>>()
            .context("Failed to collect raw data for aggregation")?;

        data_list
            .into_iter()
            .map(|data_json| {
                serde_json::from_str(&data_json).context("Failed to deserialize traffic data")
            })
            .collect()
    }

    /// 获取指定时间范围内的10秒聚合数据（用于生成1分钟聚合）
    pub fn get_10s_data_for_aggregation(
        &self,
        namespace: &str,
        since_ms: i64,
    ) -> Result<Vec<AggregatedData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,
                    rx_dropped_sum, tx_dropped_sum, sample_count
             FROM traffic_history_10s
             WHERE namespace = ?1 AND timestamp_ms > ?2
             ORDER BY timestamp_ms ASC",
            )
            .context("Failed to prepare statement")?;

        stmt.query_map(params![namespace, since_ms], |row| {
            Ok(self.row_to_aggregated_data(row))
        })
        .context("Failed to query 10s data for aggregation")?
        .collect::<std::result::Result<Vec<AggregatedData>, _>>()
        .context("Failed to collect 10s data for aggregation")
    }

    /// 清理旧数据
    pub fn cleanup_old_data(&self, hours: u32) -> Result<usize> {
        let cutoff_ms = Utc::now().timestamp_millis() - (hours as i64 * 60 * 60 * 1000);

        let conn = self.conn.lock().unwrap();

        let deleted_raw = conn
            .execute(
                "DELETE FROM traffic_history WHERE timestamp_ms < ?1",
                params![cutoff_ms],
            )
            .context("Failed to cleanup traffic_history")?;

        let deleted_10s = conn
            .execute(
                "DELETE FROM traffic_history_10s WHERE timestamp_ms < ?1",
                params![cutoff_ms],
            )
            .context("Failed to cleanup traffic_history_10s")?;

        let deleted_1m = conn
            .execute(
                "DELETE FROM traffic_history_1m WHERE timestamp_ms < ?1",
                params![cutoff_ms],
            )
            .context("Failed to cleanup traffic_history_1m")?;

        Ok(deleted_raw + deleted_10s + deleted_1m)
    }

    /// 获取数据库统计信息
    pub fn get_stats(&self) -> Result<DatabaseStats> {
        let conn = self.conn.lock().unwrap();

        let raw_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM traffic_history", [], |row| row.get(0))
            .unwrap_or(0);

        let aggregated_10s_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM traffic_history_10s", [], |row| {
                row.get(0)
            })
            .unwrap_or(0);

        let aggregated_1m_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM traffic_history_1m", [], |row| {
                row.get(0)
            })
            .unwrap_or(0);

        let namespace_count: i64 = conn
            .query_row(
                "SELECT COUNT(DISTINCT namespace) FROM traffic_history",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        Ok(DatabaseStats {
            raw_count,
            aggregated_10s_count,
            aggregated_1m_count,
            namespace_count,
        })
    }
}

/// 数据库统计信息
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub raw_count: i64,
    pub aggregated_10s_count: i64,
    pub aggregated_1m_count: i64,
    pub namespace_count: i64,
}
