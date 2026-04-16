use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::models::{DatabaseConfig, RawTrafficData, TrafficData};

static IN_MEMORY_DB_COUNTER: AtomicU64 = AtomicU64::new(0);
const HISTORY_TABLES: [(&str, &str); 5] = [
    ("traffic_history", "idx_history_timestamp_ms"),
    ("traffic_history_10s", "idx_10s_timestamp_ms"),
    ("traffic_history_1m", "idx_1m_timestamp_ms"),
    ("traffic_history_1h", "idx_1h_timestamp_ms"),
    ("traffic_history_1d", "idx_1d_timestamp_ms"),
];

/// 数据库管理器
#[derive(Debug)]
pub struct Database {
    writer_conn: Arc<Mutex<Connection>>,
    reader_conn: Arc<Mutex<Connection>>,
    config: DatabaseConfig,
}

impl Database {
    /// 创建新的数据库连接
    pub fn new(config: DatabaseConfig) -> Result<Self> {
        if config.db_path != ":memory:" {
            let db_path = Path::new(&config.db_path);

            // 确保数据库目录存在
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create database directory: {:?}", parent)
                })?;
            }
        }

        let target = resolve_connection_target(&config.db_path);

        let writer_conn = open_connection(&target)
            .with_context(|| format!("Failed to open writer database: {}", config.db_path))?;
        apply_connection_pragmas(&writer_conn).context("Failed to apply writer SQLite pragmas")?;

        let db = Database {
            writer_conn: Arc::new(Mutex::new(writer_conn)),
            reader_conn: Arc::new(Mutex::new(open_connection(&target).with_context(|| {
                format!("Failed to open reader database: {}", config.db_path)
            })?)),
            config,
        };

        db.initialize()?;
        {
            let reader_conn = db.reader_conn.lock().unwrap();
            apply_connection_pragmas(&reader_conn)
                .context("Failed to apply reader SQLite pragmas")?;
        }
        Ok(db)
    }

    /// 初始化数据库表结构
    fn initialize(&self) -> Result<()> {
        let mut conn = self.writer_conn.lock().unwrap();
        let tx = conn
            .transaction()
            .context("Failed to start schema initialization transaction")?;

        for (table, timestamp_index) in HISTORY_TABLES {
            ensure_history_table_schema(&tx, table)
                .with_context(|| format!("Failed to ensure schema for {}", table))?;
            tx.execute(
                &format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {}(timestamp_ms)",
                    timestamp_index, table
                ),
                [],
            )
            .with_context(|| format!("Failed to create timestamp index on {}", table))?;
        }

        // UNIQUE(namespace, timestamp_ms) 已经隐式提供复合索引，不再额外维护同列索引。
        self.drop_redundant_namespace_indexes(&tx)?;
        self.drop_legacy_cleanup_triggers(&tx)?;
        tx.commit()
            .context("Failed to commit schema initialization transaction")?;

        Ok(())
    }

    /// 移除与 UNIQUE(namespace, timestamp_ms) 重复的旧索引，减少写入放大。
    fn drop_redundant_namespace_indexes(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "
            DROP INDEX IF EXISTS idx_history_namespace_ts;
            DROP INDEX IF EXISTS idx_10s_namespace_ts;
            DROP INDEX IF EXISTS idx_1m_namespace_ts;
            DROP INDEX IF EXISTS idx_1h_namespace_ts;
            DROP INDEX IF EXISTS idx_1d_namespace_ts;
            ",
        )
        .context("Failed to drop redundant namespace indexes")?;

        Ok(())
    }

    /// 移除旧版本逐条触发清理，避免写入时叠加删除压力。
    fn drop_legacy_cleanup_triggers(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "
            DROP TRIGGER IF EXISTS cleanup_raw_data;
            DROP TRIGGER IF EXISTS cleanup_10s_data;
            DROP TRIGGER IF EXISTS cleanup_1m_data;
            DROP TRIGGER IF EXISTS cleanup_1h_data;
            DROP TRIGGER IF EXISTS cleanup_1d_data;
            ",
        )
        .context("Failed to drop legacy cleanup triggers")?;

        Ok(())
    }

    /// 插入一轮采集产生的所有数据（原始累计值）
    pub fn insert_round_batch(
        &self,
        raw_data: &[RawTrafficData],
        data_10s: &[RawTrafficData],
        data_1m: &[RawTrafficData],
        data_1h: &[RawTrafficData],
        data_1d: &[RawTrafficData],
    ) -> Result<()> {
        let started_at = Instant::now();

        let serialize_started_at = Instant::now();
        let raw_payloads = serialize_payloads(raw_data)?;
        let payload_10s = serialize_payloads(data_10s)?;
        let payload_1m = serialize_payloads(data_1m)?;
        let payload_1h = serialize_payloads(data_1h)?;
        let payload_1d = serialize_payloads(data_1d)?;
        let serialize_elapsed_ms = serialize_started_at.elapsed().as_millis();

        let mut conn = self.writer_conn.lock().unwrap();
        let lock_elapsed_ms = started_at.elapsed().as_millis();

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

    /// 批量清理超出保留时间的数据，避免在插入路径上放大写入压力。
    pub(crate) fn cleanup_expired_data(&self, now_ms: i64) -> Result<usize> {
        let mut conn = self.writer_conn.lock().unwrap();
        let tx = conn
            .transaction()
            .context("Failed to start cleanup transaction")?;

        let mut deleted_rows = 0usize;
        deleted_rows += delete_expired_rows(
            &tx,
            "traffic_history",
            now_ms - (self.config.retention_raw_minutes as i64 * 60 * 1000),
        )?;
        deleted_rows += delete_expired_rows(
            &tx,
            "traffic_history_10s",
            now_ms - (self.config.retention_10s_hours as i64 * 60 * 60 * 1000),
        )?;
        deleted_rows += delete_expired_rows(
            &tx,
            "traffic_history_1m",
            now_ms - (self.config.retention_1m_hours as i64 * 60 * 60 * 1000),
        )?;
        deleted_rows += delete_expired_rows(
            &tx,
            "traffic_history_1h",
            now_ms - (self.config.retention_1h_days as i64 * 24 * 60 * 60 * 1000),
        )?;
        deleted_rows += delete_expired_rows(
            &tx,
            "traffic_history_1d",
            now_ms - (self.config.retention_1d_days as i64 * 24 * 60 * 60 * 1000),
        )?;

        tx.commit()
            .context("Failed to commit cleanup transaction")?;
        Ok(deleted_rows)
    }

    /// 获取当前数据（最新的数据，转换为带速度字段的 TrafficData）
    pub fn get_current_data(&self, namespace: &str) -> Result<Option<TrafficData>> {
        let conn = self.reader_conn.lock().unwrap();

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
                // 从数据库读取原始数据，转换为 TrafficData
                let raw_data: RawTrafficData = serde_json::from_str(&data_json)
                    .context("Failed to deserialize raw traffic data")?;
                Ok(Some(TrafficData::from(raw_data)))
            }
            None => Ok(None),
        }
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

        let conn = self.reader_conn.lock().unwrap();
        query_data_range(&conn, table_name, namespace, since_ms, now_ms)
    }
}

fn resolve_connection_target(db_path: &str) -> String {
    if db_path == ":memory:" {
        let id = IN_MEMORY_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("file:traffic-monitor-{}?mode=memory&cache=shared", id)
    } else {
        db_path.to_string()
    }
}

fn open_connection(target: &str) -> Result<Connection> {
    Connection::open_with_flags(
        target,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("Failed to open SQLite connection: {}", target))
}

fn apply_connection_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -64000;
        PRAGMA busy_timeout = 5000;
        ",
    )
    .context("Failed to apply SQLite pragmas")
}

fn ensure_history_table_schema(conn: &Connection, table: &str) -> Result<()> {
    match table_sql(conn, table)? {
        None => create_history_table(conn, table)
            .with_context(|| format!("Failed to create {}", table))?,
        Some(sql) if is_desired_history_table_schema(&sql) => {}
        Some(_) => migrate_history_table(conn, table)
            .with_context(|| format!("Failed to migrate legacy schema for {}", table))?,
    }

    Ok(())
}

fn table_sql(conn: &Connection, table: &str) -> Result<Option<String>> {
    conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
        params![table],
        |row| row.get(0),
    )
    .optional()
    .context("Failed to query table schema")
}

fn is_desired_history_table_schema(sql: &str) -> bool {
    let normalized = sql
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();

    normalized.contains("primarykey(namespace,timestamp_ms)")
        && normalized.contains("withoutrowid")
        && !normalized.contains("autoincrement")
        && !normalized.contains("created_at")
        && !normalized.contains("idintegerprimarykey")
}

fn create_history_table(conn: &Connection, table: &str) -> Result<()> {
    conn.execute(&history_table_sql(table), [])
        .with_context(|| format!("Failed to create table {}", table))?;
    Ok(())
}

fn migrate_history_table(conn: &Connection, table: &str) -> Result<()> {
    let migrated_table = format!("{}_migrated", table);

    conn.execute(&format!("DROP TABLE IF EXISTS {}", migrated_table), [])
        .with_context(|| format!("Failed to drop temporary table for {}", table))?;
    conn.execute(&history_table_sql(&migrated_table), [])
        .with_context(|| format!("Failed to create temporary table for {}", table))?;
    conn.execute(
        &format!(
            "INSERT INTO {} (namespace, timestamp, timestamp_ms, data)
             SELECT namespace, timestamp, timestamp_ms, data FROM {}",
            migrated_table, table
        ),
        [],
    )
    .with_context(|| format!("Failed to copy data from {}", table))?;
    conn.execute(&format!("DROP TABLE {}", table), [])
        .with_context(|| format!("Failed to drop legacy table {}", table))?;
    conn.execute(
        &format!("ALTER TABLE {} RENAME TO {}", migrated_table, table),
        [],
    )
    .with_context(|| format!("Failed to rename migrated table for {}", table))?;

    Ok(())
}

fn history_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (
            namespace TEXT NOT NULL DEFAULT 'default',
            timestamp TEXT NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            data TEXT NOT NULL,
            PRIMARY KEY(namespace, timestamp_ms)
        ) WITHOUT ROWID",
        table
    )
}

fn serialize_payloads(data_list: &[RawTrafficData]) -> Result<Vec<(String, String, i64, String)>> {
    data_list
        .iter()
        .map(|data| {
            Ok::<_, anyhow::Error>((
                data.namespace.clone(),
                data.timestamp.clone(),
                data.timestamp_ms,
                serde_json::to_string(data).context("Failed to serialize raw traffic data")?,
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
        "INSERT INTO {} (namespace, timestamp, timestamp_ms, data)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(namespace, timestamp_ms) DO UPDATE SET
             timestamp = excluded.timestamp,
             data = excluded.data",
        table
    );
    let mut stmt = tx.prepare(&sql).context("Failed to prepare batch insert")?;
    for (namespace, timestamp, timestamp_ms, data_json) in payloads {
        stmt.execute(params![namespace, timestamp, timestamp_ms, data_json])
            .with_context(|| format!("Failed to insert batch data into {}", table))?;
    }
    Ok(())
}

fn delete_expired_rows(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    cutoff_ms: i64,
) -> Result<usize> {
    let sql = format!("DELETE FROM {} WHERE timestamp_ms < ?1", table);
    tx.execute(&sql, params![cutoff_ms])
        .with_context(|| format!("Failed to cleanup expired rows from {}", table))
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
            // 从数据库读取原始数据，转换为 TrafficData
            let raw_data: RawTrafficData = serde_json::from_str(&data_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
            Ok(TrafficData::from(raw_data))
        })
        .context("Failed to query history data")?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row.context("Failed to parse row")?);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_db_path(prefix: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "{}-{}.db",
            prefix,
            TEST_DB_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn test_database_creation() {
        let config = DatabaseConfig {
            db_path: temp_db_path("traffic-monitor-test-creation"),
            ..Default::default()
        };
        let db = Database::new(config);
        assert!(db.is_ok());
    }

    #[test]
    fn test_insert_and_query() {
        let config = DatabaseConfig {
            db_path: temp_db_path("traffic-monitor-test-insert"),
            ..Default::default()
        };
        let db = Database::new(config).unwrap();

        let data = RawTrafficData {
            namespace: "test".to_string(),
            timestamp: "2024-01-01 00:00:00".to_string(),
            timestamp_ms: 1704067200000,
            interfaces: vec![crate::models::RawInterfaceStats {
                name: "eth0".to_string(),
                rx_bytes: 1024,
                tx_bytes: 2048,
                rx_dropped: 0,
                tx_dropped: 0,
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

    #[test]
    fn test_insert_round_batch_updates_existing_row_on_conflict() {
        let config = DatabaseConfig {
            db_path: ":memory:".to_string(),
            ..Default::default()
        };
        let db = Database::new(config).unwrap();

        let original = RawTrafficData {
            namespace: "test".to_string(),
            timestamp: "2024-01-01 00:00:00".to_string(),
            timestamp_ms: 1_704_067_200_000,
            interfaces: vec![crate::models::RawInterfaceStats {
                name: "eth0".to_string(),
                rx_bytes: 1_024,
                tx_bytes: 2_048,
                rx_dropped: 0,
                tx_dropped: 0,
            }],
            resolution: None,
        };
        let updated = RawTrafficData {
            namespace: "test".to_string(),
            timestamp: "2024-01-01 00:00:00".to_string(),
            timestamp_ms: original.timestamp_ms,
            interfaces: vec![crate::models::RawInterfaceStats {
                name: "eth0".to_string(),
                rx_bytes: 4_096,
                tx_bytes: 8_192,
                rx_dropped: 1,
                tx_dropped: 2,
            }],
            resolution: None,
        };

        db.insert_round_batch(std::slice::from_ref(&original), &[], &[], &[], &[])
            .unwrap();

        db.insert_round_batch(std::slice::from_ref(&updated), &[], &[], &[], &[])
            .unwrap();

        let conn = db.writer_conn.lock().unwrap();
        let row_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM traffic_history WHERE namespace = ?1 AND timestamp_ms = ?2",
                params![updated.namespace, updated.timestamp_ms],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(row_count, 1);

        drop(conn);

        let result = db.get_current_data("test").unwrap().unwrap();
        assert_eq!(result.interfaces[0].rx_bytes, 4_096);
        assert_eq!(result.interfaces[0].tx_bytes, 8_192);
    }

    #[test]
    fn test_history_table_schema_uses_composite_primary_key() {
        let config = DatabaseConfig {
            db_path: ":memory:".to_string(),
            ..Default::default()
        };
        let db = Database::new(config).unwrap();

        let conn = db.writer_conn.lock().unwrap();
        let sql = table_sql(&conn, "traffic_history")
            .unwrap()
            .expect("traffic_history schema should exist");
        let normalized = sql
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>()
            .to_ascii_lowercase();

        assert!(normalized.contains("primarykey(namespace,timestamp_ms)"));
        assert!(normalized.contains("withoutrowid"));
        assert!(!normalized.contains("autoincrement"));
        assert!(!normalized.contains("created_at"));
    }

    #[test]
    fn test_redundant_namespace_indexes_are_removed() {
        let config = DatabaseConfig {
            db_path: ":memory:".to_string(),
            ..Default::default()
        };
        let db = Database::new(config).unwrap();

        let conn = db.writer_conn.lock().unwrap();
        let mut stmt = conn
            .prepare("PRAGMA index_list('traffic_history')")
            .unwrap();
        let index_names: Vec<String> = stmt
            .query_map([], |row| row.get(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap();

        assert!(index_names
            .iter()
            .any(|name| name == "idx_history_timestamp_ms"));
        assert!(!index_names
            .iter()
            .any(|name| name == "idx_history_namespace_ts"));
    }

    #[test]
    fn test_legacy_schema_is_migrated_and_data_preserved() {
        let db_path = temp_db_path("traffic-monitor-test-migrate");
        let legacy_conn = Connection::open(&db_path).unwrap();
        legacy_conn
            .execute_batch(
                "
                CREATE TABLE traffic_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    namespace TEXT NOT NULL DEFAULT 'default',
                    timestamp TEXT NOT NULL,
                    timestamp_ms INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(namespace, timestamp_ms)
                );
                CREATE INDEX idx_history_namespace_ts
                ON traffic_history(namespace, timestamp_ms);
                ",
            )
            .unwrap();

        let legacy_data = RawTrafficData {
            namespace: "test".to_string(),
            timestamp: "2024-01-01 00:00:00".to_string(),
            timestamp_ms: 1_704_067_200_000,
            interfaces: vec![crate::models::RawInterfaceStats {
                name: "eth0".to_string(),
                rx_bytes: 123,
                tx_bytes: 456,
                rx_dropped: 0,
                tx_dropped: 0,
            }],
            resolution: None,
        };
        legacy_conn
            .execute(
                "INSERT INTO traffic_history (namespace, timestamp, timestamp_ms, data)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    legacy_data.namespace,
                    legacy_data.timestamp,
                    legacy_data.timestamp_ms,
                    serde_json::to_string(&legacy_data).unwrap()
                ],
            )
            .unwrap();
        drop(legacy_conn);

        let db = Database::new(DatabaseConfig {
            db_path: db_path.clone(),
            ..Default::default()
        })
        .unwrap();

        let migrated = db.get_current_data("test").unwrap().unwrap();
        assert_eq!(migrated.timestamp_ms, legacy_data.timestamp_ms);
        assert_eq!(migrated.interfaces[0].rx_bytes, 123);
        assert_eq!(migrated.interfaces[0].tx_bytes, 456);

        let conn = db.writer_conn.lock().unwrap();
        let sql = table_sql(&conn, "traffic_history")
            .unwrap()
            .expect("traffic_history schema should exist after migration");
        let normalized = sql
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>()
            .to_ascii_lowercase();
        assert!(normalized.contains("primarykey(namespace,timestamp_ms)"));
        assert!(normalized.contains("withoutrowid"));

        drop(conn);
        let _ = fs::remove_file(db_path);
    }

    #[test]
    fn test_cleanup_expired_data() {
        let config = DatabaseConfig {
            db_path: ":memory:".to_string(),
            retention_raw_minutes: 1,
            ..Default::default()
        };
        let db = Database::new(config).unwrap();

        let expired_data = RawTrafficData {
            namespace: "test".to_string(),
            timestamp: "2024-01-01 00:00:00".to_string(),
            timestamp_ms: 1_000,
            interfaces: vec![],
            resolution: None,
        };
        let fresh_data = RawTrafficData {
            namespace: "test".to_string(),
            timestamp: "2024-01-01 00:01:30".to_string(),
            timestamp_ms: 90_000,
            interfaces: vec![],
            resolution: None,
        };

        db.insert_round_batch(std::slice::from_ref(&expired_data), &[], &[], &[], &[])
            .unwrap();
        db.insert_round_batch(std::slice::from_ref(&fresh_data), &[], &[], &[], &[])
            .unwrap();

        let deleted = db.cleanup_expired_data(120_000).unwrap();
        assert_eq!(deleted, 1);

        let result = db.get_current_data("test").unwrap().unwrap();
        assert_eq!(result.timestamp_ms, fresh_data.timestamp_ms);
    }
}
