use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

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

        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA temp_store = MEMORY;
            PRAGMA cache_size = -64000;
            PRAGMA busy_timeout = 5000;
            ",
        )
        .context("Failed to apply SQLite pragmas")?;

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
                data TEXT NOT NULL,
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
                data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(namespace, timestamp_ms)
            )",
            [],
        )
        .context("Failed to create traffic_history_1m table")?;

        // 创建1小时聚合表
        conn.execute(
            "CREATE TABLE IF NOT EXISTS traffic_history_1h (
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
        .context("Failed to create traffic_history_1h table")?;

        // 创建1天聚合表
        conn.execute(
            "CREATE TABLE IF NOT EXISTS traffic_history_1d (
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
        .context("Failed to create traffic_history_1d table")?;

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

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_1h_namespace_ts
             ON traffic_history_1h(namespace, timestamp_ms)",
            [],
        )
        .context("Failed to create index on traffic_history_1h")?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_1d_namespace_ts
             ON traffic_history_1d(namespace, timestamp_ms)",
            [],
        )
        .context("Failed to create index on traffic_history_1d")?;

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

        // 1小时聚合数据保留7天
        let retention_1h_ms = self.config.retention_1h_days as i64 * 24 * 60 * 60 * 1000;
        conn.execute(
            &format!(
                "CREATE TRIGGER IF NOT EXISTS cleanup_1h_data
                 AFTER INSERT ON traffic_history_1h
                 BEGIN
                     DELETE FROM traffic_history_1h
                     WHERE timestamp_ms < (NEW.timestamp_ms - {});
                 END",
                retention_1h_ms
            ),
            [],
        )
        .context("Failed to create cleanup trigger for traffic_history_1h")?;

        // 1天聚合数据保留30天
        let retention_1d_ms = self.config.retention_1d_days as i64 * 24 * 60 * 60 * 1000;
        conn.execute(
            &format!(
                "CREATE TRIGGER IF NOT EXISTS cleanup_1d_data
                 AFTER INSERT ON traffic_history_1d
                 BEGIN
                     DELETE FROM traffic_history_1d
                     WHERE timestamp_ms < (NEW.timestamp_ms - {});
                 END",
                retention_1d_ms
            ),
            [],
        )
        .context("Failed to create cleanup trigger for traffic_history_1d")?;

        Ok(())
    }

    /// 插入一轮采集产生的所有数据
    pub fn insert_round_batch(
        &self,
        raw_data: &[TrafficData],
        data_10s: &[TrafficData],
        data_1m: &[TrafficData],
        data_1h: &[TrafficData],
        data_1d: &[TrafficData],
    ) -> Result<()> {
        let started_at = Instant::now();
        let mut conn = self.conn.lock().unwrap();
        let lock_elapsed_ms = started_at.elapsed().as_millis();

        let serialize_started_at = Instant::now();
        let raw_payloads = serialize_payloads(raw_data)?;
        let payload_10s = serialize_payloads(data_10s)?;
        let payload_1m = serialize_payloads(data_1m)?;
        let payload_1h = serialize_payloads(data_1h)?;
        let payload_1d = serialize_payloads(data_1d)?;
        let serialize_elapsed_ms = serialize_started_at.elapsed().as_millis();

        let execute_started_at = Instant::now();
        let tx = conn.transaction().context("Failed to start transaction")?;

        insert_rows(&tx, "traffic_history", &raw_payloads)?;
        insert_rows(&tx, "traffic_history_10s", &payload_10s)?;
        insert_rows(&tx, "traffic_history_1m", &payload_1m)?;
        insert_rows(&tx, "traffic_history_1h", &payload_1h)?;
        insert_rows(&tx, "traffic_history_1d", &payload_1d)?;

        tx.commit().context("Failed to commit transaction")?;
        let execute_elapsed_ms = execute_started_at.elapsed().as_millis();

        log::debug!(
            "Database timing [round] lock={} ms serialize={} ms execute={} ms raw={} 10s={} 1m={} 1h={} 1d={}",
            lock_elapsed_ms,
            serialize_elapsed_ms,
            execute_elapsed_ms,
            raw_data.len(),
            data_10s.len(),
            data_1m.len(),
            data_1h.len(),
            data_1d.len()
        );

        Ok(())
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

    /// 根据时间范围获取历史数据（自动选择合适的表）
    pub fn get_history_data(
        &self,
        namespace: &str,
        start_time: &str,
        end_time: &str,
    ) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();
        let start_ts = parse_timestamp(start_time)?;
        let end_ts = parse_timestamp(end_time)?;

        let (table_name, _) = self.get_appropriate_table(start_ts, end_ts);

        query_data_range(&conn, table_name, namespace, start_ts, end_ts)
    }

    /// 根据持续时间获取历史数据
    pub fn get_history_by_duration(
        &self,
        namespace: &str,
        duration_minutes: u32,
    ) -> Result<Vec<TrafficData>> {
        let now_ms = Utc::now().timestamp_millis();
        let since_ms = now_ms - (duration_minutes as i64 * 60 * 1000);

        let table_name = if duration_minutes <= 5 {
            "traffic_history"
        } else if duration_minutes <= 60 {
            "traffic_history_10s"
        } else if duration_minutes <= 180 {
            "traffic_history_1m"
        } else if duration_minutes <= 1440 {
            "traffic_history_1h"
        } else {
            "traffic_history_1d"
        };

        let conn = self.conn.lock().unwrap();
        query_data_range(
            &conn,
            table_name,
            namespace,
            since_ms,
            now_ms,
        )
    }

    /// 获取原始数据（详细数据）
    pub fn get_raw_data(
        &self,
        namespace: &str,
        start_time: &str,
        end_time: &str,
    ) -> Result<Vec<TrafficData>> {
        self.get_history_data(namespace, start_time, end_time)
    }

    /// 获取10秒聚合数据
    pub fn get_10s_data(
        &self,
        namespace: &str,
        start_time: &str,
        end_time: &str,
    ) -> Result<Vec<TrafficData>> {
        self.get_aggregated_data(namespace, start_time, end_time, "traffic_history_10s")
    }

    /// 获取1分钟聚合数据
    pub fn get_1m_data(
        &self,
        namespace: &str,
        start_time: &str,
        end_time: &str,
    ) -> Result<Vec<TrafficData>> {
        self.get_aggregated_data(namespace, start_time, end_time, "traffic_history_1m")
    }

    /// 获取1小时聚合数据
    pub fn get_1h_data(
        &self,
        namespace: &str,
        start_time: &str,
        end_time: &str,
    ) -> Result<Vec<TrafficData>> {
        self.get_aggregated_data(namespace, start_time, end_time, "traffic_history_1h")
    }

    /// 获取1天聚合数据
    pub fn get_1d_data(
        &self,
        namespace: &str,
        start_time: &str,
        end_time: &str,
    ) -> Result<Vec<TrafficData>> {
        self.get_aggregated_data(namespace, start_time, end_time, "traffic_history_1d")
    }

    /// 获取聚合数据
    fn get_aggregated_data(
        &self,
        namespace: &str,
        start_time: &str,
        end_time: &str,
        table_name: &str,
    ) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();
        let start_ts = parse_timestamp(start_time)?;
        let end_ts = parse_timestamp(end_time)?;

        query_data_range(&conn, table_name, namespace, start_ts, end_ts)
    }

    /// 获取最新的历史数据时间范围
    pub fn get_latest_history_range(
        &self,
        namespace: &str,
    ) -> Result<Option<(String, String)>> {
        let conn = self.conn.lock().unwrap();

        let tables = [
            "traffic_history",
            "traffic_history_10s",
            "traffic_history_1m",
            "traffic_history_1h",
            "traffic_history_1d",
        ];

        for table in &tables {
            if let Some((min_ts, max_ts)) = query_table_time_range(&conn, table, namespace)? {
                return Ok(Some((
                    format_timestamp(min_ts),
                    format_timestamp(max_ts),
                )));
            }
        }

        Ok(None)
    }

    /// 获取适合时间范围的表
    fn get_appropriate_table(&self, start_ts: i64, end_ts: i64) -> (&'static str, i64) {
        let duration_ms = end_ts - start_ts;

        if duration_ms <= 10 * 60 * 1000 {
            ("traffic_history", 1)
        } else if duration_ms <= 60 * 60 * 1000 {
            ("traffic_history_10s", 10)
        } else if duration_ms <= 6 * 60 * 60 * 1000 {
            ("traffic_history_1m", 60)
        } else if duration_ms <= 24 * 60 * 60 * 1000 {
            ("traffic_history_1h", 60 * 60)
        } else {
            ("traffic_history_1d", 24 * 60 * 60)
        }
    }
}

fn serialize_payloads(data_list: &[TrafficData]) -> Result<Vec<(String, String, i64, String)>> {
    data_list
        .iter()
        .map(|data| {
            Ok::<_, anyhow::Error>((
                data.namespace.clone(),
                data.timestamp.clone(),
                data.timestamp_ms,
                serde_json::to_string(data).context("Failed to serialize traffic data")?,
            ))
        })
        .collect()
}

fn insert_rows(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    payloads: &[(String, String, i64, String)],
) -> Result<()> {
    if payloads.is_empty() {
        return Ok(());
    }

    let sql = format!(
        "INSERT OR REPLACE INTO {} (namespace, timestamp, timestamp_ms, data)
         VALUES (?1, ?2, ?3, ?4)",
        table
    );
    let mut stmt = tx.prepare(&sql).context("Failed to prepare batch insert")?;
    for (namespace, timestamp, timestamp_ms, data_json) in payloads {
        stmt.execute(params![namespace, timestamp, timestamp_ms, data_json])
            .with_context(|| format!("Failed to insert batch data into {}", table))?;
    }
    Ok(())
}

fn query_data_range(
    conn: &Connection,
    table: &str,
    namespace: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<TrafficData>> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT data FROM {}
             WHERE namespace = ?1 AND timestamp_ms >= ?2 AND timestamp_ms <= ?3
             ORDER BY timestamp_ms ASC",
            table
        ))
        .with_context(|| format!("Failed to prepare statement for table {}", table))?;

    let rows = stmt
        .query_map(params![namespace, start_ts, end_ts], |row| {
            let data_json: String = row.get(0)?;
            let data: TrafficData = serde_json::from_str(&data_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
            Ok(data)
        })
        .context("Failed to query history data")?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row.context("Failed to parse row")?);
    }

    Ok(result)
}

fn query_table_time_range(
    conn: &Connection,
    table: &str,
    namespace: &str,
) -> Result<Option<(i64, i64)>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT MIN(timestamp_ms), MAX(timestamp_ms) FROM {} WHERE namespace = ?1",
        table
    ))?;

    let result = stmt
        .query_row(params![namespace], |row| {
            let min_ts: Option<i64> = row.get(0)?;
            let max_ts: Option<i64> = row.get(1)?;
            Ok((min_ts, max_ts))
        })
        .optional()?;

    Ok(result.and_then(|(min_ts, max_ts)| match (min_ts, max_ts) {
        (Some(min_ts), Some(max_ts)) => Some((min_ts, max_ts)),
        _ => None,
    }))
}

/// 解析时间戳字符串
fn parse_timestamp(timestamp_str: &str) -> Result<i64> {
    let dt = chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S")
        .with_context(|| format!("Failed to parse timestamp: {}", timestamp_str))?;
    Ok(Utc.from_utc_datetime(&dt).timestamp_millis())
}

/// 格式化时间戳
fn format_timestamp(timestamp_ms: i64) -> String {
    let dt = Utc
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(|| Utc.timestamp_millis_opt(0).single().unwrap());
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_creation() {
        let config = DatabaseConfig {
            db_path: "data/test_creation.db".to_string(),
            ..Default::default()
        };
        let db = Database::new(config);
        assert!(db.is_ok());
    }

    #[test]
    fn test_insert_and_query() {
        let config = DatabaseConfig {
            db_path: "data/test.db".to_string(),
            ..Default::default()
        };
        let db = Database::new(config).unwrap();

        let data = TrafficData {
            namespace: "test".to_string(),
            timestamp: "2024-01-01 00:00:00".to_string(),
            timestamp_ms: 1704067200000,
            interfaces: vec![InterfaceStats {
                name: "eth0".to_string(),
                rx_bytes: 1024,
                tx_bytes: 2048,
                rx_dropped: 0,
                tx_dropped: 0,
                rx_speed: Some(100),
                tx_speed: Some(200),
                rx_dropped_speed: Some(0),
                tx_dropped_speed: Some(0),
            }],
            resolution: None,
        };

        db.insert_round_batch(std::slice::from_ref(&data), &[], &[], &[], &[])
            .unwrap();

        let result = db.get_current_data("test").unwrap();
        assert!(result.is_some());
        let result_data = result.unwrap();
        assert_eq!(result_data.namespace, data.namespace);
        assert_eq!(result_data.timestamp, data.timestamp);
        assert_eq!(result_data.timestamp_ms, data.timestamp_ms);
        assert_eq!(result_data.interfaces.len(), 1);
        assert_eq!(result_data.interfaces[0].name, "eth0");
        assert_eq!(result_data.interfaces[0].rx_bytes, 1024);
        assert_eq!(result_data.interfaces[0].tx_bytes, 2048);
    }
}
