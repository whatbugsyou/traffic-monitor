use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::models::*;

static IN_MEMORY_DB_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct Database {
    writer_conn: Arc<Mutex<Connection>>,
    reader_conn: Arc<Mutex<Connection>>,
    config: DatabaseConfig,
}

#[derive(Clone, Copy, Debug)]
enum TableKind {
    Raw,
    TenSeconds,
    OneMinute,
    OneHour,
    OneDay,
}

#[derive(Clone, Copy, Debug)]
struct TableSpec {
    kind: TableKind,
    name: &'static str,
    query_index_name: &'static str,
    cleanup_index_name: &'static str,
    cleanup_trigger_name: &'static str,
}

#[derive(Debug)]
struct TableInspection {
    has_created_at: bool,
    has_unique_index: bool,
}

const TABLE_SPECS: [TableSpec; 5] = [
    TableSpec {
        kind: TableKind::Raw,
        name: "traffic_history",
        query_index_name: "idx_history_namespace_ts",
        cleanup_index_name: "idx_history_timestamp_ms",
        cleanup_trigger_name: "cleanup_raw_data",
    },
    TableSpec {
        kind: TableKind::TenSeconds,
        name: "traffic_history_10s",
        query_index_name: "idx_10s_namespace_ts",
        cleanup_index_name: "idx_10s_timestamp_ms",
        cleanup_trigger_name: "cleanup_10s_data",
    },
    TableSpec {
        kind: TableKind::OneMinute,
        name: "traffic_history_1m",
        query_index_name: "idx_1m_namespace_ts",
        cleanup_index_name: "idx_1m_timestamp_ms",
        cleanup_trigger_name: "cleanup_1m_data",
    },
    TableSpec {
        kind: TableKind::OneHour,
        name: "traffic_history_1h",
        query_index_name: "idx_1h_namespace_ts",
        cleanup_index_name: "idx_1h_timestamp_ms",
        cleanup_trigger_name: "cleanup_1h_data",
    },
    TableSpec {
        kind: TableKind::OneDay,
        name: "traffic_history_1d",
        query_index_name: "idx_1d_namespace_ts",
        cleanup_index_name: "idx_1d_timestamp_ms",
        cleanup_trigger_name: "cleanup_1d_data",
    },
];

impl TableSpec {
    fn retention_ms(self, config: &DatabaseConfig) -> i64 {
        match self.kind {
            TableKind::Raw => config.retention_raw_minutes as i64 * 60 * 1000,
            TableKind::TenSeconds => config.retention_10s_hours as i64 * 60 * 60 * 1000,
            TableKind::OneMinute => config.retention_1m_hours as i64 * 60 * 60 * 1000,
            TableKind::OneHour => config.retention_1h_days as i64 * 24 * 60 * 60 * 1000,
            TableKind::OneDay => config.retention_1d_days as i64 * 24 * 60 * 60 * 1000,
        }
    }
}

impl Database {
    pub fn new(config: DatabaseConfig) -> Result<Self> {
        if config.db_path != ":memory:" {
            let db_path = Path::new(&config.db_path);

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

    fn initialize(&self) -> Result<()> {
        let mut conn = self.writer_conn.lock().unwrap();

        for spec in TABLE_SPECS {
            ensure_table_exists(&conn, spec)?;
            let inspection = inspect_table(&conn, spec)?;

            if !inspection.has_created_at || inspection.has_unique_index {
                rebuild_table(&mut conn, spec, inspection.has_created_at)?;
            }
        }

        ensure_indexes(&conn)?;
        self.drop_legacy_cleanup_triggers(&conn)?;

        Ok(())
    }

    fn drop_legacy_cleanup_triggers(&self, conn: &Connection) -> Result<()> {
        for spec in TABLE_SPECS {
            conn.execute_batch(&format!(
                "DROP TRIGGER IF EXISTS {trigger_name};",
                trigger_name = spec.cleanup_trigger_name,
            ))
            .with_context(|| format!("Failed to drop cleanup trigger for {}", spec.name))?;
        }

        Ok(())
    }

    pub fn insert_round_batch(
        &self,
        raw_data: &[TrafficData],
        data_10s: &[TrafficData],
        data_1m: &[TrafficData],
        data_1h: &[TrafficData],
        data_1d: &[TrafficData],
    ) -> Result<()> {
        let started_at = Instant::now();
        let mut conn = self.writer_conn.lock().unwrap();
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

    pub(crate) fn cleanup_expired_data(&self, now_ms: i64) -> Result<usize> {
        let mut conn = self.writer_conn.lock().unwrap();
        let tx = conn
            .transaction()
            .context("Failed to start cleanup transaction")?;

        let mut deleted_rows = 0usize;
        for spec in TABLE_SPECS {
            deleted_rows +=
                delete_expired_rows(&tx, spec.name, now_ms - spec.retention_ms(&self.config))?;
        }

        tx.commit()
            .context("Failed to commit cleanup transaction")?;
        Ok(deleted_rows)
    }

    pub fn get_current_data(&self, namespace: &str) -> Result<Option<TrafficData>> {
        let conn = self.reader_conn.lock().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT data FROM traffic_history
                 WHERE namespace = ?1
                 ORDER BY timestamp_ms DESC, id DESC
                 LIMIT 1",
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

    pub fn get_history_by_duration(
        &self,
        namespace: &str,
        duration_minutes: u32,
    ) -> Result<Vec<TrafficData>> {
        let now_ms = Utc::now().timestamp_millis();
        let since_ms = now_ms - (duration_minutes as i64 * 60 * 1000);

        let table_name = table_name_for_duration(duration_minutes);

        let conn = self.reader_conn.lock().unwrap();
        query_data_range(&conn, table_name, namespace, since_ms, now_ms)
    }
}

fn resolve_connection_target(db_path: &str) -> String {
    if db_path == ":memory:" {
        let id = IN_MEMORY_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("file:traffic-monitor-memory-{id}?mode=memory&cache=shared")
    } else {
        db_path.to_string()
    }
}

fn open_connection(target: &str) -> Result<Connection> {
    let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    if target.starts_with("file:") {
        flags |= OpenFlags::SQLITE_OPEN_URI;
    }

    Connection::open_with_flags(target, flags)
        .with_context(|| format!("Failed to open database connection: {}", target))
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
    .context("Failed to apply SQLite pragmas")?;

    Ok(())
}

fn ensure_table_exists(conn: &Connection, spec: TableSpec) -> Result<()> {
    conn.execute(&create_table_sql(spec.name, true), [])
        .with_context(|| format!("Failed to create {} table", spec.name))?;
    Ok(())
}

fn inspect_table(conn: &Connection, spec: TableSpec) -> Result<TableInspection> {
    let mut has_created_at = false;
    let mut table_info_stmt = conn
        .prepare(&format!("PRAGMA table_info({})", spec.name))
        .with_context(|| format!("Failed to inspect columns for {}", spec.name))?;

    let table_info_rows = table_info_stmt
        .query_map([], |row| row.get::<_, String>(1))
        .with_context(|| format!("Failed to query columns for {}", spec.name))?;

    for column in table_info_rows {
        if column.context("Failed to read table column")? == "created_at" {
            has_created_at = true;
        }
    }

    let mut has_unique_index = false;
    let mut index_list_stmt = conn
        .prepare(&format!("PRAGMA index_list({})", spec.name))
        .with_context(|| format!("Failed to inspect indexes for {}", spec.name))?;

    let index_rows = index_list_stmt
        .query_map([], |row| row.get::<_, i64>(2))
        .with_context(|| format!("Failed to query indexes for {}", spec.name))?;

    for is_unique in index_rows {
        if is_unique.context("Failed to read index metadata")? == 1 {
            has_unique_index = true;
        }
    }

    Ok(TableInspection {
        has_created_at,
        has_unique_index,
    })
}

fn rebuild_table(conn: &mut Connection, spec: TableSpec, has_created_at: bool) -> Result<()> {
    let temp_table_name = format!("{}_non_unique_migrating", spec.name);
    let created_at_expr = if has_created_at {
        "created_at"
    } else {
        "CURRENT_TIMESTAMP"
    };

    let tx = conn
        .transaction()
        .with_context(|| format!("Failed to start migration for {}", spec.name))?;

    tx.execute_batch(&format!("DROP TABLE IF EXISTS {temp_table_name};"))
        .with_context(|| format!("Failed to drop temp migration table for {}", spec.name))?;

    tx.execute(&create_table_sql(&temp_table_name, false), [])
        .with_context(|| format!("Failed to create temp migration table for {}", spec.name))?;

    tx.execute(
        &format!(
            "INSERT INTO {temp_table_name} (id, namespace, timestamp, timestamp_ms, data, created_at)
             SELECT id, namespace, timestamp, timestamp_ms, data, {created_at_expr}
             FROM {table_name}
             ORDER BY id",
            temp_table_name = temp_table_name,
            created_at_expr = created_at_expr,
            table_name = spec.name,
        ),
        [],
    )
    .with_context(|| format!("Failed to copy legacy data for {}", spec.name))?;

    tx.execute_batch(&format!(
        "DROP TABLE {table_name};
         ALTER TABLE {temp_table_name} RENAME TO {table_name};",
        table_name = spec.name,
        temp_table_name = temp_table_name,
    ))
    .with_context(|| format!("Failed to replace legacy table for {}", spec.name))?;

    tx.commit()
        .with_context(|| format!("Failed to commit migration for {}", spec.name))?;

    Ok(())
}

fn ensure_indexes(conn: &Connection) -> Result<()> {
    for spec in TABLE_SPECS {
        conn.execute(
            &format!(
                "CREATE INDEX IF NOT EXISTS {} ON {}(namespace, timestamp_ms)",
                spec.query_index_name, spec.name
            ),
            [],
        )
        .with_context(|| format!("Failed to create query index on {}", spec.name))?;

        conn.execute(
            &format!(
                "CREATE INDEX IF NOT EXISTS {} ON {}(timestamp_ms)",
                spec.cleanup_index_name, spec.name
            ),
            [],
        )
        .with_context(|| format!("Failed to create cleanup index on {}", spec.name))?;
    }

    Ok(())
}

fn create_table_sql(table_name: &str, if_not_exists: bool) -> String {
    let if_not_exists_clause = if if_not_exists { "IF NOT EXISTS " } else { "" };

    format!(
        "CREATE TABLE {if_not_exists_clause}{table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            namespace TEXT NOT NULL DEFAULT 'default',
            timestamp TEXT NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
        if_not_exists_clause = if_not_exists_clause,
        table_name = table_name,
    )
}

fn table_name_for_duration(duration_minutes: u32) -> &'static str {
    if duration_minutes <= 5 {
        "traffic_history"
    } else if duration_minutes <= 60 {
        "traffic_history_10s"
    } else if duration_minutes <= 180 {
        "traffic_history_1m"
    } else if duration_minutes <= 1440 {
        "traffic_history_1h"
    } else {
        "traffic_history_1d"
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
        "INSERT INTO {} (namespace, timestamp, timestamp_ms, data)
         VALUES (?1, ?2, ?3, ?4)",
        table
    );

    let mut stmt = tx
        .prepare_cached(&sql)
        .with_context(|| format!("Failed to prepare statement for {}", table))?;

    for (namespace, timestamp, timestamp_ms, data_json) in payloads {
        stmt.execute(params![namespace, timestamp, timestamp_ms, data_json])
            .with_context(|| format!("Failed to insert row into {}", table))?;
    }

    Ok(())
}

fn delete_expired_rows(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    cutoff_ms: i64,
) -> Result<usize> {
    tx.execute(
        &format!("DELETE FROM {} WHERE timestamp_ms < ?1", table),
        params![cutoff_ms],
    )
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
             ORDER BY timestamp_ms ASC, id ASC",
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_test_db_path(label: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();

        std::env::temp_dir()
            .join(format!("traffic-monitor-{label}-{nanos}.db"))
            .to_string_lossy()
            .into_owned()
    }

    fn cleanup_test_db_files(db_path: &str) {
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(format!("{db_path}-wal"));
        let _ = fs::remove_file(format!("{db_path}-shm"));
    }

    fn has_index(conn: &Connection, table_name: &str, index_name: &str) -> bool {
        let mut stmt = conn
            .prepare(&format!("PRAGMA index_list({table_name})"))
            .expect("failed to prepare index_list");

        let rows = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .expect("failed to query index_list");

        for row in rows {
            if row.expect("failed to read index name") == index_name {
                return true;
            }
        }

        false
    }

    fn has_trigger(conn: &Connection, trigger_name: &str) -> bool {
        let exists: i64 = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = ?1)",
                params![trigger_name],
                |row| row.get(0),
            )
            .expect("failed to query sqlite_master");

        exists == 1
    }

    fn unique_index_count(conn: &Connection, table_name: &str) -> i64 {
        conn.prepare(&format!("PRAGMA index_list({table_name})"))
            .expect("failed to prepare index_list")
            .query_map([], |row| row.get::<_, i64>(2))
            .expect("failed to query index_list")
            .map(|row| row.expect("failed to read unique flag"))
            .filter(|flag| *flag == 1)
            .count() as i64
    }

    fn build_test_data(namespace: &str, timestamp_ms: i64) -> TrafficData {
        TrafficData {
            namespace: namespace.to_string(),
            timestamp: "2024-01-01 00:00:00".to_string(),
            timestamp_ms,
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
        }
    }

    #[test]
    fn test_database_creation() {
        let db_path = temp_test_db_path("creation");
        cleanup_test_db_files(&db_path);

        let config = DatabaseConfig {
            db_path: db_path.clone(),
            ..Default::default()
        };

        let db = Database::new(config).unwrap();
        let conn = db.writer_conn.lock().unwrap();

        assert_eq!(unique_index_count(&conn, "traffic_history"), 0);
        assert!(has_index(
            &conn,
            "traffic_history",
            "idx_history_namespace_ts"
        ));
        assert!(has_index(
            &conn,
            "traffic_history",
            "idx_history_timestamp_ms"
        ));
        assert!(!has_trigger(&conn, "cleanup_raw_data"));
        assert!(!has_trigger(&conn, "cleanup_10s_data"));
        assert!(!has_trigger(&conn, "cleanup_1m_data"));
        assert!(!has_trigger(&conn, "cleanup_1h_data"));
        assert!(!has_trigger(&conn, "cleanup_1d_data"));

        drop(conn);
        drop(db);
        cleanup_test_db_files(&db_path);
    }

    #[test]
    fn test_insert_and_query() {
        let db_path = temp_test_db_path("insert-query");
        cleanup_test_db_files(&db_path);

        let config = DatabaseConfig {
            db_path: db_path.clone(),
            ..Default::default()
        };

        let db = Database::new(config).unwrap();
        let data = build_test_data("test", 1704067200000);

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

        drop(db);
        cleanup_test_db_files(&db_path);
    }

    #[test]
    fn test_drops_legacy_cleanup_triggers() {
        let db_path = temp_test_db_path("drop-trigger");
        cleanup_test_db_files(&db_path);

        let conn = Connection::open(PathBuf::from(&db_path)).unwrap();
        conn.execute(&create_table_sql("traffic_history", true), [])
            .unwrap();
        conn.execute_batch(
            "CREATE TRIGGER cleanup_raw_data
             AFTER INSERT ON traffic_history
             BEGIN
                 DELETE FROM traffic_history WHERE timestamp_ms < (NEW.timestamp_ms - 60000);
             END;",
        )
        .unwrap();
        drop(conn);

        let config = DatabaseConfig {
            db_path: db_path.clone(),
            ..Default::default()
        };

        let db = Database::new(config).unwrap();
        let conn = db.writer_conn.lock().unwrap();

        assert!(!has_trigger(&conn, "cleanup_raw_data"));

        drop(conn);
        drop(db);
        cleanup_test_db_files(&db_path);
    }

    #[test]
    fn test_cleanup_expired_data() {
        let db_path = temp_test_db_path("cleanup");
        cleanup_test_db_files(&db_path);

        let config = DatabaseConfig {
            db_path: db_path.clone(),
            retention_raw_minutes: 1,
            ..Default::default()
        };

        let db = Database::new(config).unwrap();
        let expired = build_test_data("cleanup", 0);
        let retained = build_test_data("cleanup", 60_000);

        db.insert_round_batch(std::slice::from_ref(&expired), &[], &[], &[], &[])
            .unwrap();
        db.insert_round_batch(std::slice::from_ref(&retained), &[], &[], &[], &[])
            .unwrap();

        let deleted = db.cleanup_expired_data(120_000).unwrap();
        let conn = db.reader_conn.lock().unwrap();
        let remaining_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM traffic_history WHERE namespace = ?1",
                params![retained.namespace.as_str()],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(deleted, 1);
        assert_eq!(remaining_count, 1);
        drop(conn);

        let current = db.get_current_data("cleanup").unwrap().unwrap();
        assert_eq!(current.timestamp_ms, retained.timestamp_ms);

        drop(db);
        cleanup_test_db_files(&db_path);
    }

    #[test]
    fn test_migrates_legacy_unique_schema() {
        let db_path = temp_test_db_path("legacy-migration");
        cleanup_test_db_files(&db_path);

        let conn = Connection::open(PathBuf::from(&db_path)).unwrap();
        conn.execute(
            "CREATE TABLE traffic_history (
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
        .unwrap();
        drop(conn);

        let config = DatabaseConfig {
            db_path: db_path.clone(),
            ..Default::default()
        };

        let db = Database::new(config).unwrap();
        let data = build_test_data("legacy", 1704067200000);

        db.insert_round_batch(std::slice::from_ref(&data), &[], &[], &[], &[])
            .unwrap();
        db.insert_round_batch(std::slice::from_ref(&data), &[], &[], &[], &[])
            .unwrap();

        let conn = db.writer_conn.lock().unwrap();
        let duplicate_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM traffic_history WHERE namespace = ?1 AND timestamp_ms = ?2",
                params![data.namespace, data.timestamp_ms],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(duplicate_count, 2);
        assert_eq!(unique_index_count(&conn, "traffic_history"), 0);
        assert!(has_index(
            &conn,
            "traffic_history",
            "idx_history_namespace_ts"
        ));
        assert!(has_index(
            &conn,
            "traffic_history",
            "idx_history_timestamp_ms"
        ));

        drop(conn);
        drop(db);
        cleanup_test_db_files(&db_path);
    }
}
