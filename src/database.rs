use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
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

        // 创建10秒聚合表（结构与原始数据表相同）
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

        // 创建1分钟聚合表（结构与原始数据表相同）
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

        // 创建1小时聚合表（结构与原始数据表相同）
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

        // 创建1天聚合表（结构与原始数据表相同）
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

    /// 插入10秒聚合数据（原始快照）
    pub fn insert_10s_aggregated(&self, data: &TrafficData) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        let data_json = serde_json::to_string(data).context("Failed to serialize traffic data")?;

        conn.execute(
            "INSERT OR REPLACE INTO traffic_history_10s
             (namespace, timestamp, timestamp_ms, data)
             VALUES (?1, ?2, ?3, ?4)",
            params![data.namespace, data.timestamp, data.timestamp_ms, data_json],
        )
        .context("Failed to insert 10s aggregated data")?;

        Ok(())
    }

    /// 插入1分钟聚合数据（原始快照）
    pub fn insert_1m_aggregated(&self, data: &TrafficData) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        let data_json = serde_json::to_string(data).context("Failed to serialize traffic data")?;

        conn.execute(
            "INSERT OR REPLACE INTO traffic_history_1m
             (namespace, timestamp, timestamp_ms, data)
             VALUES (?1, ?2, ?3, ?4)",
            params![data.namespace, data.timestamp, data.timestamp_ms, data_json],
        )
        .context("Failed to insert 1m aggregated data")?;

        Ok(())
    }

    /// 插入1小时聚合数据（原始快照）
    pub fn insert_1h_aggregated(&self, data: &TrafficData) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        let data_json = serde_json::to_string(data).context("Failed to serialize traffic data")?;

        conn.execute(
            "INSERT OR REPLACE INTO traffic_history_1h
             (namespace, timestamp, timestamp_ms, data)
             VALUES (?1, ?2, ?3, ?4)",
            params![data.namespace, data.timestamp, data.timestamp_ms, data_json],
        )
        .context("Failed to insert 1h aggregated data")?;

        Ok(())
    }

    /// 插入1天聚合数据（原始快照）
    pub fn insert_1d_aggregated(&self, data: &TrafficData) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        let data_json = serde_json::to_string(data).context("Failed to serialize traffic data")?;

        conn.execute(
            "INSERT OR REPLACE INTO traffic_history_1d
             (namespace, timestamp, timestamp_ms, data)
             VALUES (?1, ?2, ?3, ?4)",
            params![data.namespace, data.timestamp, data.timestamp_ms, data_json],
        )
        .context("Failed to insert 1d aggregated data")?;

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
        } else if duration_minutes <= 180 {
            // 3小时内：使用1分钟聚合数据
            self.get_1m_aggregated_history(namespace, since_ms)
        } else if duration_minutes <= 1440 {
            // 24小时内：使用1小时聚合数据
            self.get_1h_aggregated_history(namespace, since_ms)
        } else {
            // 超过24小时：使用1天聚合数据
            self.get_1d_aggregated_history(namespace, since_ms)
        }
    }

    /// 获取原始历史数据（查询时计算速度）
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

        // 解析原始数据
        parse_data_list(data_list, "1s")
    }

    /// 获取1小时聚合历史数据（查询时计算速度）
    fn get_1h_aggregated_history(
        &self,
        namespace: &str,
        since_ms: i64,
    ) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT data FROM traffic_history_1h
             WHERE namespace = ?1 AND timestamp_ms > ?2
             ORDER BY timestamp_ms ASC",
            )
            .context("Failed to prepare statement")?;

        let data_list = stmt
            .query_map(params![namespace, since_ms], |row| {
                let data_json: String = row.get(0)?;
                Ok(data_json)
            })
            .context("Failed to query 1h aggregated history")?
            .collect::<std::result::Result<Vec<String>, _>>()
            .context("Failed to collect 1h aggregated history")?;

        // 解析原始数据
        parse_data_list(data_list, "1h")
    }

    /// 获取1天聚合历史数据（查询时计算速度）
    fn get_1d_aggregated_history(
        &self,
        namespace: &str,
        since_ms: i64,
    ) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT data FROM traffic_history_1d
             WHERE namespace = ?1 AND timestamp_ms > ?2
             ORDER BY timestamp_ms ASC",
            )
            .context("Failed to prepare statement")?;

        let data_list = stmt
            .query_map(params![namespace, since_ms], |row| {
                let data_json: String = row.get(0)?;
                Ok(data_json)
            })
            .context("Failed to query 1d aggregated history")?
            .collect::<std::result::Result<Vec<String>, _>>()
            .context("Failed to collect 1d aggregated history")?;

        // 解析原始数据
        parse_data_list(data_list, "1d")
    }

    /// 获取10秒聚合历史数据（查询时计算速度）
    fn get_10s_aggregated_history(
        &self,
        namespace: &str,
        since_ms: i64,
    ) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT data FROM traffic_history_10s
             WHERE namespace = ?1 AND timestamp_ms > ?2
             ORDER BY timestamp_ms ASC",
            )
            .context("Failed to prepare statement")?;

        let data_list = stmt
            .query_map(params![namespace, since_ms], |row| {
                let data_json: String = row.get(0)?;
                Ok(data_json)
            })
            .context("Failed to query 10s aggregated history")?
            .collect::<std::result::Result<Vec<String>, _>>()
            .context("Failed to collect 10s aggregated history")?;

        // 解析原始数据
        parse_data_list(data_list, "10s")
    }

    /// 获取1分钟聚合历史数据（查询时计算速度）
    fn get_1m_aggregated_history(
        &self,
        namespace: &str,
        since_ms: i64,
    ) -> Result<Vec<TrafficData>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT data FROM traffic_history_1m
             WHERE namespace = ?1 AND timestamp_ms > ?2
             ORDER BY timestamp_ms ASC",
            )
            .context("Failed to prepare statement")?;

        let data_list = stmt
            .query_map(params![namespace, since_ms], |row| {
                let data_json: String = row.get(0)?;
                Ok(data_json)
            })
            .context("Failed to query 1m aggregated history")?
            .collect::<std::result::Result<Vec<String>, _>>()
            .context("Failed to collect 1m aggregated history")?;

        // 解析原始数据
        parse_data_list(data_list, "1m")
    }
}

/// 解析数据列表（只解析 JSON，不计算速度）
fn parse_data_list(data_json_list: Vec<String>, resolution: &str) -> Result<Vec<TrafficData>> {
    data_json_list
        .into_iter()
        .map(|data_json| {
            let mut data: TrafficData =
                serde_json::from_str(&data_json).context("Failed to deserialize traffic data")?;
            data.resolution = Some(resolution.to_string());
            Ok(data)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_creation() {
        let config = DatabaseConfig::default();
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
                rx_bytes: 1000,
                tx_bytes: 500,
                rx_dropped: 0,
                tx_dropped: 0,
                rx_speed: None,
                tx_speed: None,
                rx_dropped_speed: None,
                tx_dropped_speed: None,
            }],

            resolution: Some("1s".to_string()),
        };

        db.insert_traffic_data(&data).unwrap();

        let result = db.get_current_data("test").unwrap();
        assert!(result.is_some());
    }
}
