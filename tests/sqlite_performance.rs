use rusqlite::{params, Connection};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const SECOND_MS: i64 = 1_000;
const TEN_SECONDS_MS: i64 = 10_000;
const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;
const BASE_TS_MS: i64 = 40 * DAY_MS;

const DEFAULT_NAMESPACE_COUNT: usize = 300;
const DEFAULT_SAMPLES: usize = 1;
const FIXED_TIMESTAMP_TEXT: &str = "2026-01-01T00:00:00Z";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SchemaCase {
    WriteOptimized,
    IndexedNonUnique,
    LegacyUniqueReplaceIndexed,
}

impl SchemaCase {
    fn all() -> [Self; 3] {
        [
            Self::WriteOptimized,
            Self::IndexedNonUnique,
            Self::LegacyUniqueReplaceIndexed,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Self::WriteOptimized => "write_optimized",
            Self::IndexedNonUnique => "indexed_non_unique",
            Self::LegacyUniqueReplaceIndexed => "legacy_unique_replace_indexed",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::WriteOptimized => "当前写优化 schema：无 UNIQUE、无二级索引、INSERT",
            Self::IndexedNonUnique => "折中 schema：无 UNIQUE、保留二级索引、INSERT",
            Self::LegacyUniqueReplaceIndexed => {
                "历史 schema：UNIQUE(namespace, timestamp_ms) + 二级索引 + INSERT OR REPLACE"
            }
        }
    }

    fn has_unique_constraint(self) -> bool {
        matches!(self, Self::LegacyUniqueReplaceIndexed)
    }

    fn has_secondary_indexes(self) -> bool {
        matches!(
            self,
            Self::IndexedNonUnique | Self::LegacyUniqueReplaceIndexed
        )
    }

    fn insert_sql(self, table_name: &str) -> String {
        let verb = match self {
            Self::WriteOptimized | Self::IndexedNonUnique => "INSERT",
            Self::LegacyUniqueReplaceIndexed => "INSERT OR REPLACE",
        };

        format!(
            "{verb} INTO {table_name} (namespace, timestamp, timestamp_ms, data)
             VALUES (?1, ?2, ?3, ?4)"
        )
    }

    fn create_table_sql(self, table_name: &str) -> String {
        let unique_clause = if self.has_unique_constraint() {
            ",\n                UNIQUE(namespace, timestamp_ms)"
        } else {
            ""
        };

        format!(
            "CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL DEFAULT 'default',
                timestamp TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP{unique_clause}
            )"
        )
    }

    fn from_token(token: &str) -> Option<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "write_optimized" | "current" | "optimized" => Some(Self::WriteOptimized),
            "indexed_non_unique" | "indexed" | "non_unique_indexed" => {
                Some(Self::IndexedNonUnique)
            }
            "legacy_unique_replace_indexed" | "legacy" | "old" => {
                Some(Self::LegacyUniqueReplaceIndexed)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoundProfile {
    RealtimeOnly,
    TenSecondTick,
    OneMinuteTick,
    OneHourTick,
    OneDayTick,
}

impl RoundProfile {
    fn all() -> [Self; 5] {
        [
            Self::RealtimeOnly,
            Self::TenSecondTick,
            Self::OneMinuteTick,
            Self::OneHourTick,
            Self::OneDayTick,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Self::RealtimeOnly => "1s_only",
            Self::TenSecondTick => "10s_tick",
            Self::OneMinuteTick => "1m_tick",
            Self::OneHourTick => "1h_tick",
            Self::OneDayTick => "1d_tick",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::RealtimeOnly => "只写入 1s 原始表",
            Self::TenSecondTick => "写入 1s + 10s",
            Self::OneMinuteTick => "写入 1s + 10s + 1m",
            Self::OneHourTick => "写入 1s + 10s + 1m + 1h",
            Self::OneDayTick => "写入 1s + 10s + 1m + 1h + 1d",
        }
    }

    fn max_level(self) -> u8 {
        match self {
            Self::RealtimeOnly => 1,
            Self::TenSecondTick => 2,
            Self::OneMinuteTick => 3,
            Self::OneHourTick => 4,
            Self::OneDayTick => 5,
        }
    }

    fn round_ts_ms(self) -> i64 {
        match self {
            Self::RealtimeOnly => BASE_TS_MS + SECOND_MS,
            Self::TenSecondTick => BASE_TS_MS + TEN_SECONDS_MS,
            Self::OneMinuteTick => BASE_TS_MS + MINUTE_MS,
            Self::OneHourTick => BASE_TS_MS + HOUR_MS,
            Self::OneDayTick => BASE_TS_MS + DAY_MS,
        }
    }

    fn writes(self, spec: TableSpec) -> bool {
        spec.level <= self.max_level()
    }



    fn from_token(token: &str) -> Option<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "1s_only" | "1s" | "realtime" => Some(Self::RealtimeOnly),
            "10s_tick" | "10s" => Some(Self::TenSecondTick),
            "1m_tick" | "1m" => Some(Self::OneMinuteTick),
            "1h_tick" | "1h" => Some(Self::OneHourTick),
            "1d_tick" | "1d" => Some(Self::OneDayTick),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct TableSpec {
    name: &'static str,
    index_name: &'static str,
    trigger_name: &'static str,
    interval_ms: i64,
    retention_ms: i64,
    level: u8,
}

#[derive(Debug)]
struct BenchConfig {
    namespace_count: usize,
    samples: usize,
    enable_cleanup_triggers: bool,
    enable_cleanup_timestamp_indexes: bool,
    cases: Vec<SchemaCase>,
    rounds: Vec<RoundProfile>,
}

#[derive(Debug)]
struct Payloads {
    realtime: String,
    ten_seconds: String,
    one_minute: String,
    one_hour: String,
    one_day: String,
}

#[derive(Debug)]
struct SampleResult {
    elapsed_ms: f64,
    inserted_rows: usize,
}

#[derive(Debug)]
struct AggregateResult {
    inserted_rows: usize,
    avg_ms: f64,
    min_ms: f64,
    max_ms: f64,
    avg_us_per_row: f64,
}

#[test]
#[ignore = "manual benchmark: cargo test --test sqlite_performance -- --ignored --nocapture"]
fn sqlite_steady_state_realistic_volume_report() {
    let config = load_config();
    let payloads = Payloads::new();
    let namespaces = build_namespaces(config.namespace_count);

    print_benchmark_header(&config);

    for case in &config.cases {
        println!();
        println!("=== schema: {} ===", case.name());
        println!("{}", case.description());
        println!(
            "{:<14} {:<28} {:>12} {:>12} {:>12} {:>12} {:>14}",
            "round",
            "writes",
            "rows/round",
            "avg_ms",
            "min_ms",
            "max_ms",
            "avg_us/row"
        );
        println!("{}", "-".repeat(110));

        for round in &config.rounds {
            let result = benchmark_round(
                *case,
                *round,
                config.samples,
                &namespaces,
                &payloads,
                config.enable_cleanup_triggers,
                config.enable_cleanup_timestamp_indexes,
            );
            println!(
                "{:<14} {:<28} {:>12} {:>12.2} {:>12.2} {:>12.2} {:>14.2}",
                round.name(),
                round.description(),
                result.inserted_rows,
                result.avg_ms,
                result.min_ms,
                result.max_ms,
                result.avg_us_per_row
            );
        }
    }
}

fn load_config() -> BenchConfig {
    let namespace_count =
        parse_positive_usize("TM_BENCH_NAMESPACES", DEFAULT_NAMESPACE_COUNT);
    let samples = parse_positive_usize("TM_BENCH_SAMPLES", DEFAULT_SAMPLES);
    let enable_cleanup_triggers = parse_enable_cleanup_triggers();
    let enable_cleanup_timestamp_indexes = parse_enable_cleanup_timestamp_indexes();
    let cases = parse_cases();
    let rounds = parse_rounds();

    BenchConfig {
        namespace_count,
        samples,
        enable_cleanup_triggers,
        enable_cleanup_timestamp_indexes,
        cases,
        rounds,
    }
}

fn parse_positive_usize(var_name: &str, default_value: usize) -> usize {
    match env::var(var_name) {
        Ok(raw) => raw.parse::<usize>().unwrap_or_else(|_| {
            panic!("{var_name} must be a positive integer, got `{raw}`");
        }),
        Err(_) => default_value,
    }
}

fn parse_enable_cleanup_triggers() -> bool {
    match env::var("TM_BENCH_TRIGGERS") {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "on" | "enabled" => true,
            "0" | "false" | "off" | "disabled" => false,
            _ => panic!(
                "Unknown TM_BENCH_TRIGGERS value `{raw}`. Valid values: on, off, true, false, 1, 0"
            ),
        },
        Err(_) => true,
    }
}

fn parse_enable_cleanup_timestamp_indexes() -> bool {
    match env::var("TM_BENCH_CLEANUP_TS_INDEXES") {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "on" | "enabled" => true,
            "0" | "false" | "off" | "disabled" => false,
            _ => panic!(
                "Unknown TM_BENCH_CLEANUP_TS_INDEXES value `{raw}`. Valid values: on, off, true, false, 1, 0"
            ),
        },
        Err(_) => false,
    }
}

fn parse_cases() -> Vec<SchemaCase> {
    match env::var("TM_BENCH_CASES") {
        Ok(raw) => parse_case_list(&raw),
        Err(_) => SchemaCase::all().into_iter().collect(),
    }
}

fn parse_case_list(raw: &str) -> Vec<SchemaCase> {
    if raw.trim().is_empty() || raw.trim().eq_ignore_ascii_case("all") {
        return SchemaCase::all().into_iter().collect();
    }

    let mut cases = Vec::new();
    for token in raw.split(',').map(|item| item.trim()).filter(|item| !item.is_empty()) {
        let case = SchemaCase::from_token(token).unwrap_or_else(|| {
            panic!(
                "Unknown case `{token}`. Valid values: write_optimized, indexed_non_unique, legacy_unique_replace_indexed, all"
            )
        });

        if !cases.contains(&case) {
            cases.push(case);
        }
    }

    if cases.is_empty() {
        panic!("TM_BENCH_CASES produced an empty case list");
    }

    cases
}

fn parse_rounds() -> Vec<RoundProfile> {
    match env::var("TM_BENCH_ROUNDS") {
        Ok(raw) => parse_round_list(&raw),
        Err(_) => RoundProfile::all().into_iter().collect(),
    }
}

fn parse_round_list(raw: &str) -> Vec<RoundProfile> {
    if raw.trim().is_empty() || raw.trim().eq_ignore_ascii_case("all") {
        return RoundProfile::all().into_iter().collect();
    }

    let mut rounds = Vec::new();
    for token in raw.split(',').map(|item| item.trim()).filter(|item| !item.is_empty()) {
        let round = RoundProfile::from_token(token).unwrap_or_else(|| {
            panic!("Unknown round `{token}`. Valid values: 1s_only, 10s_tick, 1m_tick, 1h_tick, 1d_tick, all");
        });

        if !rounds.contains(&round) {
            rounds.push(round);
        }
    }

    if rounds.is_empty() {
        panic!("TM_BENCH_ROUNDS produced an empty round list");
    }

    rounds
}

fn print_benchmark_header(config: &BenchConfig) {
    let case_names = config
        .cases
        .iter()
        .map(|case| case.name())
        .collect::<Vec<_>>()
        .join(", ");
    let round_names = config
        .rounds
        .iter()
        .map(|round| round.name())
        .collect::<Vec<_>>()
        .join(", ");

    println!("=== SQLite steady-state realistic volume benchmark ===");
    println!(
        "namespaces: {} (默认值来自文档中的 300 命名空间，可用 TM_BENCH_NAMESPACES 覆盖)",
        config.namespace_count
    );
    println!("samples per round: {}", config.samples);
    println!(
        "cleanup triggers: {}",
        if config.enable_cleanup_triggers {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "cleanup timestamp indexes: {}",
        if config.enable_cleanup_timestamp_indexes {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!("cases: {}", case_names);
    println!("rounds: {}", round_names);
    println!();

    let mut total_rows = 0usize;
    println!(
        "{:<18} {:>18} {:>18}",
        "table", "rows/ns", "steady_rows_total"
    );
    println!("{}", "-".repeat(58));

    for spec in table_specs() {
        let per_namespace = steady_state_rows_per_namespace(spec);
        let table_total = per_namespace * config.namespace_count;
        total_rows += table_total;
        println!(
            "{:<18} {:>18} {:>18}",
            spec.name, per_namespace, table_total
        );
    }

    println!("{}", "-".repeat(58));
    println!("{:<18} {:>18} {:>18}", "total", "-", total_rows);
}

fn benchmark_round(
    case: SchemaCase,
    round: RoundProfile,
    samples: usize,
    namespaces: &[String],
    payloads: &Payloads,
    enable_cleanup_triggers: bool,
    enable_cleanup_timestamp_indexes: bool,
) -> AggregateResult {
    let mut elapsed_values_ms = Vec::with_capacity(samples);
    let mut inserted_rows = 0usize;

    for sample_index in 0..samples {
        let sample = benchmark_round_sample(
            case,
            round,
            namespaces,
            payloads,
            sample_index,
            enable_cleanup_triggers,
            enable_cleanup_timestamp_indexes,
        );
        inserted_rows = sample.inserted_rows;
        elapsed_values_ms.push(sample.elapsed_ms);
    }

    let min_ms = elapsed_values_ms
        .iter()
        .copied()
        .fold(f64::INFINITY, f64::min);
    let max_ms = elapsed_values_ms
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let avg_ms = elapsed_values_ms.iter().sum::<f64>() / elapsed_values_ms.len() as f64;
    let avg_us_per_row = (avg_ms * 1000.0) / inserted_rows as f64;

    AggregateResult {
        inserted_rows,
        avg_ms,
        min_ms,
        max_ms,
        avg_us_per_row,
    }
}

fn benchmark_round_sample(
    case: SchemaCase,
    round: RoundProfile,
    namespaces: &[String],
    payloads: &Payloads,
    sample_index: usize,
    enable_cleanup_triggers: bool,
    enable_cleanup_timestamp_indexes: bool,
) -> SampleResult {
    let db_path = temp_db_path(case.name(), round.name(), sample_index);
    cleanup_db_files(&db_path);

    let mut conn = Connection::open(&db_path).expect("failed to open sqlite database");
    apply_prefill_pragmas(&conn);
    create_tables(&conn, case);
    prefill_steady_state(&mut conn, case, round, namespaces, payloads);
    create_secondary_indexes(&conn, case);
    if enable_cleanup_timestamp_indexes {
        create_cleanup_timestamp_indexes(&conn);
    }
    apply_measurement_pragmas(&conn);
    if enable_cleanup_triggers {
        create_cleanup_triggers(&conn);
    }
    verify_pre_round_state(&conn, round, namespaces.len());

    let result = measure_round(&mut conn, case, round, namespaces, payloads);
    verify_post_round_state(
        &conn,
        round,
        namespaces.len(),
        enable_cleanup_triggers,
    );

    drop(conn);
    cleanup_db_files(&db_path);

    result
}

fn apply_prefill_pragmas(conn: &Connection) {
    conn.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -64000;
        PRAGMA locking_mode = EXCLUSIVE;
        PRAGMA busy_timeout = 5000;
        ",
    )
    .expect("failed to apply fast setup pragmas");
}

fn apply_measurement_pragmas(conn: &Connection) {
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -64000;
        PRAGMA locking_mode = NORMAL;
        PRAGMA busy_timeout = 5000;
        ",
    )
    .expect("failed to apply measurement pragmas");
}

fn create_tables(conn: &Connection, case: SchemaCase) {
    for spec in table_specs() {
        let sql = case.create_table_sql(spec.name);
        conn.execute(&sql, [])
            .unwrap_or_else(|err| panic!("failed to create table {}: {err}", spec.name));
    }
}

fn create_secondary_indexes(conn: &Connection, case: SchemaCase) {
    if !case.has_secondary_indexes() {
        return;
    }

    for spec in table_specs() {
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}(namespace, timestamp_ms)",
            spec.index_name, spec.name
        );

        conn.execute(&sql, [])
            .unwrap_or_else(|err| panic!("failed to create index on {}: {err}", spec.name));
    }
}

fn create_cleanup_timestamp_indexes(conn: &Connection) {
    for spec in table_specs() {
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_cleanup_timestamp_ms ON {}(timestamp_ms)",
            spec.name, spec.name
        );

        conn.execute(&sql, []).unwrap_or_else(|err| {
            panic!(
                "failed to create cleanup timestamp index on {}: {err}",
                spec.name
            )
        });
    }
}

fn create_cleanup_triggers(conn: &Connection) {
    for spec in table_specs() {
        let sql = format!(
            "CREATE TRIGGER IF NOT EXISTS {trigger_name}
             AFTER INSERT ON {table_name}
             BEGIN
                 DELETE FROM {table_name}
                 WHERE timestamp_ms < (NEW.timestamp_ms - {retention_ms});
             END",
            trigger_name = spec.trigger_name,
            table_name = spec.name,
            retention_ms = spec.retention_ms,
        );

        conn.execute_batch(&sql)
            .unwrap_or_else(|err| panic!("failed to create trigger on {}: {err}", spec.name));
    }
}

fn prefill_steady_state(
    conn: &mut Connection,
    case: SchemaCase,
    round: RoundProfile,
    namespaces: &[String],
    payloads: &Payloads,
) {
    let tx = conn.transaction().expect("failed to begin prefill transaction");

    for spec in table_specs() {
        let latest_existing_ts = previous_aligned(round.round_ts_ms(), spec.interval_ms);
        let earliest_existing_ts = latest_existing_ts - spec.retention_ms;
        let insert_sql = case.insert_sql(spec.name);
        let payload = payloads.for_table(spec);

        let mut stmt = tx
            .prepare_cached(&insert_sql)
            .unwrap_or_else(|err| panic!("failed to prepare prefill statement for {}: {err}", spec.name));

        let mut ts = earliest_existing_ts;
        while ts <= latest_existing_ts {
            for namespace in namespaces {
                stmt.execute(params![
                    namespace.as_str(),
                    FIXED_TIMESTAMP_TEXT,
                    ts,
                    payload
                ])
                .unwrap_or_else(|err| {
                    panic!(
                        "failed to prefill table {} at ts {} for namespace {}: {err}",
                        spec.name, ts, namespace
                    )
                });
            }
            ts += spec.interval_ms;
        }
    }

    tx.commit().expect("failed to commit prefill transaction");
}

fn measure_round(
    conn: &mut Connection,
    case: SchemaCase,
    round: RoundProfile,
    namespaces: &[String],
    payloads: &Payloads,
) -> SampleResult {
    let started = Instant::now();
    let tx = conn.transaction().expect("failed to begin measured transaction");
    let mut inserted_rows = 0usize;

    for spec in table_specs() {
        if !round.writes(spec) {
            continue;
        }

        let insert_sql = case.insert_sql(spec.name);
        let payload = payloads.for_table(spec);
        let mut stmt = tx
            .prepare_cached(&insert_sql)
            .unwrap_or_else(|err| panic!("failed to prepare measured statement for {}: {err}", spec.name));

        for namespace in namespaces {
            stmt.execute(params![
                namespace.as_str(),
                FIXED_TIMESTAMP_TEXT,
                round.round_ts_ms(),
                payload
            ])
            .unwrap_or_else(|err| {
                panic!(
                    "failed to insert measured row into {} for namespace {}: {err}",
                    spec.name, namespace
                )
            });
            inserted_rows += 1;
        }
    }

    tx.commit()
        .expect("failed to commit measured transaction");

    SampleResult {
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        inserted_rows,
    }
}

fn verify_pre_round_state(conn: &Connection, round: RoundProfile, namespace_count: usize) {
    for spec in table_specs() {
        let expected_rows = steady_state_rows_per_namespace(spec) * namespace_count;
        let expected_max_ts = previous_aligned(round.round_ts_ms(), spec.interval_ms);

        let actual_rows = row_count(conn, spec.name);
        let actual_max_ts = max_timestamp(conn, spec.name);

        assert_eq!(
            actual_rows as usize, expected_rows,
            "unexpected pre-round row count for {}",
            spec.name
        );
        assert_eq!(
            actual_max_ts,
            Some(expected_max_ts),
            "unexpected pre-round max timestamp for {}",
            spec.name
        );
    }
}

fn verify_post_round_state(
    conn: &Connection,
    round: RoundProfile,
    namespace_count: usize,
    enable_cleanup_triggers: bool,
) {
    for spec in table_specs() {
        let steady_rows = steady_state_rows_per_namespace(spec) * namespace_count;
        let expected_rows = if enable_cleanup_triggers || !round.writes(spec) {
            steady_rows
        } else {
            steady_rows + namespace_count
        };
        let expected_max_ts = if round.writes(spec) {
            round.round_ts_ms()
        } else {
            previous_aligned(round.round_ts_ms(), spec.interval_ms)
        };

        let actual_rows = row_count(conn, spec.name);
        let actual_max_ts = max_timestamp(conn, spec.name);

        assert_eq!(
            actual_rows as usize, expected_rows,
            "unexpected post-round row count for {}",
            spec.name
        );
        assert_eq!(
            actual_max_ts,
            Some(expected_max_ts),
            "unexpected post-round max timestamp for {}",
            spec.name
        );
    }
}

fn row_count(conn: &Connection, table_name: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table_name}"), [], |row| {
        row.get(0)
    })
    .unwrap_or_else(|err| panic!("failed to count rows from {table_name}: {err}"))
}

fn max_timestamp(conn: &Connection, table_name: &str) -> Option<i64> {
    conn.query_row(
        &format!("SELECT MAX(timestamp_ms) FROM {table_name}"),
        [],
        |row| row.get(0),
    )
    .unwrap_or_else(|err| panic!("failed to fetch max timestamp from {table_name}: {err}"))
}

fn previous_aligned(round_ts_ms: i64, interval_ms: i64) -> i64 {
    ((round_ts_ms - 1) / interval_ms) * interval_ms
}

fn steady_state_rows_per_namespace(spec: TableSpec) -> usize {
    ((spec.retention_ms / spec.interval_ms) + 1) as usize
}

fn build_namespaces(namespace_count: usize) -> Vec<String> {
    (0..namespace_count)
        .map(|index| format!("ns{:03}", index))
        .collect()
}

impl Payloads {
    fn new() -> Self {
        Self {
            realtime: build_payload("1s"),
            ten_seconds: build_payload("10s"),
            one_minute: build_payload("1m"),
            one_hour: build_payload("1h"),
            one_day: build_payload("1d"),
        }
    }

    fn for_table(&self, spec: TableSpec) -> &str {
        match spec.level {
            1 => &self.realtime,
            2 => &self.ten_seconds,
            3 => &self.one_minute,
            4 => &self.one_hour,
            5 => &self.one_day,
            _ => unreachable!("unknown table level {}", spec.level),
        }
    }
}

fn build_payload(resolution: &str) -> String {
    let interfaces = (0..4)
        .map(|index| {
            format!(
                "{{\"name\":\"eth{idx}\",\"rx_bytes\":123456789,\"tx_bytes\":987654321,\"rx_dropped\":0,\"tx_dropped\":0,\"rx_speed\":null,\"tx_speed\":null,\"rx_dropped_speed\":null,\"tx_dropped_speed\":null}}",
                idx = index
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "{{\"namespace\":\"ns000\",\"timestamp\":\"2026-01-01T00:00:00Z\",\"timestamp_ms\":0,\"resolution\":\"{resolution}\",\"interfaces\":[{interfaces}]}}"
    )
}

fn table_specs() -> [TableSpec; 5] {
    [
        TableSpec {
            name: "traffic_history",
            index_name: "idx_history_namespace_ts",
            trigger_name: "cleanup_raw_data",
            interval_ms: SECOND_MS,
            retention_ms: 5 * MINUTE_MS,
            level: 1,
        },
        TableSpec {
            name: "traffic_history_10s",
            index_name: "idx_10s_namespace_ts",
            trigger_name: "cleanup_10s_data",
            interval_ms: TEN_SECONDS_MS,
            retention_ms: HOUR_MS,
            level: 2,
        },
        TableSpec {
            name: "traffic_history_1m",
            index_name: "idx_1m_namespace_ts",
            trigger_name: "cleanup_1m_data",
            interval_ms: MINUTE_MS,
            retention_ms: 3 * HOUR_MS,
            level: 3,
        },
        TableSpec {
            name: "traffic_history_1h",
            index_name: "idx_1h_namespace_ts",
            trigger_name: "cleanup_1h_data",
            interval_ms: HOUR_MS,
            retention_ms: 7 * DAY_MS,
            level: 4,
        },
        TableSpec {
            name: "traffic_history_1d",
            index_name: "idx_1d_namespace_ts",
            trigger_name: "cleanup_1d_data",
            interval_ms: DAY_MS,
            retention_ms: 30 * DAY_MS,
            level: 5,
        },
    ]
}

fn temp_db_path(case_name: &str, round_name: &str, sample_index: usize) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();

    std::env::temp_dir().join(format!(
        "traffic-monitor-{case_name}-{round_name}-sample{sample_index}-{nanos}.db"
    ))
}

fn cleanup_db_files(db_path: &Path) {
    let _ = fs::remove_file(db_path);

    if let Some(db_str) = db_path.to_str() {
        let _ = fs::remove_file(format!("{db_str}-wal"));
        let _ = fs::remove_file(format!("{db_str}-shm"));
    }
}
