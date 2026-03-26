#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite 数据库初始化脚本
用于流量监控数据存储

分层存储策略：
- traffic_history: 原始数据，1秒/点，保留5分钟（300条）
- traffic_history_10s: 10秒聚合数据，保留1小时（360条）
- traffic_history_1m: 1分钟聚合数据，保留3小时（180条）
"""

import os
import sqlite3
import time

# 数据库配置
DB_PATH = "/root/traffic-monitor/data/traffic_monitor.db"

# 数据保留配置
RAW_RECORDS_PER_NAMESPACE = 300  # 原始数据：5分钟
RECORDS_10S_PER_NAMESPACE = 360  # 10秒聚合：1小时
RECORDS_1M_PER_NAMESPACE = 180  # 1分钟聚合：3小时

# 聚合间隔（秒）
INTERVAL_10S = 10
INTERVAL_1M = 60


def migrate_database():
    """迁移旧版数据库结构（如有必要）"""
    if not os.path.exists(DB_PATH):
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 检查 traffic_history 表是否存在
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='traffic_history'
    """)
    if cursor.fetchone() is None:
        conn.close()
        return

    # 检查是否已有 namespace 列
    cursor.execute("PRAGMA table_info(traffic_history)")
    columns = [row[1] for row in cursor.fetchall()]

    if "namespace" not in columns:
        print("检测到旧版表结构，正在迁移...")

        # 添加 namespace 列
        cursor.execute("""
            ALTER TABLE traffic_history
            ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default'
        """)

        # 删除旧的索引
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='index' AND tbl_name='traffic_history'
        """)
        old_indexes = [row[0] for row in cursor.fetchall()]

        for idx_name in old_indexes:
            if not idx_name.startswith("sqlite_"):
                cursor.execute('DROP INDEX IF EXISTS "{}"'.format(idx_name))

        # 重建表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS traffic_history_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL DEFAULT 'default',
                timestamp TEXT NOT NULL,
                timestamp_ms INTEGER NOT NULL,
                data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (namespace, timestamp_ms)
            )
        """)

        cursor.execute("""
            INSERT INTO traffic_history_new
                (id, namespace, timestamp, timestamp_ms, data, created_at)
            SELECT id, namespace, timestamp, timestamp_ms, data, created_at
            FROM traffic_history
        """)

        cursor.execute("DROP TABLE traffic_history")
        cursor.execute("ALTER TABLE traffic_history_new RENAME TO traffic_history")

        conn.commit()
        print("✓ 数据库迁移完成：已添加 namespace 列")
    else:
        print("数据库结构已是最新版本，无需迁移")

    conn.close()


def init_database():
    """初始化数据库"""
    print("正在初始化数据库...")

    # 先执行迁移
    migrate_database()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ═══════════════════════════════════════════
    # 1. 原始数据表（1秒/点，保留5分钟）
    # ═══════════════════════════════════════════
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS traffic_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            namespace TEXT NOT NULL DEFAULT 'default',
            timestamp TEXT NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (namespace, timestamp_ms)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_namespace_timestamp_ms
        ON traffic_history(namespace, timestamp_ms)
    """)

    # 删除旧触发器
    cursor.execute("DROP TRIGGER IF EXISTS auto_cleanup")

    # 创建自动清理触发器（每个命名空间保留300条）
    cursor.execute(
        """
        CREATE TRIGGER IF NOT EXISTS auto_cleanup
        AFTER INSERT ON traffic_history
        WHEN (
            SELECT COUNT(*) FROM traffic_history
            WHERE namespace = NEW.namespace
        ) > %d
        BEGIN
            DELETE FROM traffic_history
            WHERE id = (
                SELECT id FROM traffic_history
                WHERE namespace = NEW.namespace
                ORDER BY timestamp_ms ASC
                LIMIT 1
            );
        END
    """
        % RAW_RECORDS_PER_NAMESPACE
    )

    # ═══════════════════════════════════════════
    # 2. 10秒聚合表（保留1小时）
    # ═══════════════════════════════════════════
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS traffic_history_10s (
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
            UNIQUE (namespace, timestamp_ms)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_10s_namespace_timestamp_ms
        ON traffic_history_10s(namespace, timestamp_ms)
    """)

    cursor.execute("DROP TRIGGER IF EXISTS auto_cleanup_10s")

    cursor.execute(
        """
        CREATE TRIGGER IF NOT EXISTS auto_cleanup_10s
        AFTER INSERT ON traffic_history_10s
        WHEN (
            SELECT COUNT(*) FROM traffic_history_10s
            WHERE namespace = NEW.namespace
        ) > %d
        BEGIN
            DELETE FROM traffic_history_10s
            WHERE id = (
                SELECT id FROM traffic_history_10s
                WHERE namespace = NEW.namespace
                ORDER BY timestamp_ms ASC
                LIMIT 1
            );
        END
    """
        % RECORDS_10S_PER_NAMESPACE
    )

    # ═══════════════════════════════════════════
    # 3. 1分钟聚合表（保留3小时）
    # ═══════════════════════════════════════════
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS traffic_history_1m (
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
            UNIQUE (namespace, timestamp_ms)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_1m_namespace_timestamp_ms
        ON traffic_history_1m(namespace, timestamp_ms)
    """)

    cursor.execute("DROP TRIGGER IF EXISTS auto_cleanup_1m")

    cursor.execute(
        """
        CREATE TRIGGER IF NOT EXISTS auto_cleanup_1m
        AFTER INSERT ON traffic_history_1m
        WHEN (
            SELECT COUNT(*) FROM traffic_history_1m
            WHERE namespace = NEW.namespace
        ) > %d
        BEGIN
            DELETE FROM traffic_history_1m
            WHERE id = (
                SELECT id FROM traffic_history_1m
                WHERE namespace = NEW.namespace
                ORDER BY timestamp_ms ASC
                LIMIT 1
            );
        END
    """
        % RECORDS_1M_PER_NAMESPACE
    )

    conn.commit()
    conn.close()

    print("✓ 数据库初始化完成: %s" % DB_PATH)
    print(
        "✓ 原始数据表: 每个命名空间保留 %d 条（约 %.1f 分钟）"
        % (RAW_RECORDS_PER_NAMESPACE, RAW_RECORDS_PER_NAMESPACE / 60.0)
    )
    print(
        "✓ 10秒聚合表: 每个命名空间保留 %d 条（约 %.1f 小时）"
        % (RECORDS_10S_PER_NAMESPACE, RECORDS_10S_PER_NAMESPACE * 10 / 3600.0)
    )
    print(
        "✓ 1分钟聚合表: 每个命名空间保留 %d 条（约 %.1f 小时）"
        % (RECORDS_1M_PER_NAMESPACE, RECORDS_1M_PER_NAMESPACE / 60.0)
    )


def aggregate_data(namespace=None):
    """
    聚合数据：从原始数据生成10秒和1分钟聚合数据
    可以指定命名空间，默认处理所有命名空间
    """
    if not os.path.exists(DB_PATH):
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 获取需要处理的命名空间
    if namespace:
        namespaces = [namespace]
    else:
        cursor.execute("SELECT DISTINCT namespace FROM traffic_history")
        namespaces = [row[0] for row in cursor.fetchall()]

    import json

    for ns in namespaces:
        # ═══════════════════════════════════════════
        # 生成10秒聚合数据
        # ═══════════════════════════════════════════
        # 获取最近一分钟的原始数据，按10秒分组
        now_ms = int(time.time() * 1000)
        one_hour_ago_ms = now_ms - (60 * 60 * 1000)

        cursor.execute(
            """
            SELECT data, timestamp_ms FROM traffic_history
            WHERE namespace = ? AND timestamp_ms > ?
            ORDER BY timestamp_ms ASC
        """,
            (ns, one_hour_ago_ms),
        )
        rows = cursor.fetchall()

        if rows:
            # 按10秒分组
            groups_10s = {}
            for data_json, ts_ms in rows:
                try:
                    data = json.loads(data_json)
                    # 计算所属的10秒区间
                    group_key = (ts_ms // (INTERVAL_10S * 1000)) * (INTERVAL_10S * 1000)

                    if group_key not in groups_10s:
                        groups_10s[group_key] = {
                            "rx_speeds": [],
                            "tx_speeds": [],
                            "rx_dropped": [],
                            "tx_dropped": [],
                            "timestamp": None,
                        }

                    # 计算速度（需要上一条数据）
                    groups_10s[group_key]["timestamp"] = data.get("timestamp")
                    # 这里简化处理，实际速度计算需要上一条数据
                    if data.get("interfaces"):
                        for iface in data["interfaces"]:
                            groups_10s[group_key]["rx_speeds"].append(
                                iface.get("rx_bytes", 0)
                            )
                            groups_10s[group_key]["tx_speeds"].append(
                                iface.get("tx_bytes", 0)
                            )

                    if data.get("ppp0", {}).get("available"):
                        groups_10s[group_key]["rx_dropped"].append(
                            data["ppp0"].get("rx_dropped", 0)
                        )
                        groups_10s[group_key]["tx_dropped"].append(
                            data["ppp0"].get("tx_dropped", 0)
                        )

                except (json.JSONDecodeError, KeyError):
                    continue

            # 写入10秒聚合表
            for group_key, group_data in groups_10s.items():
                if group_data["rx_speeds"]:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO traffic_history_10s
                        (namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,
                         rx_dropped_sum, tx_dropped_sum, sample_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            ns,
                            group_data["timestamp"],
                            group_key,
                            sum(group_data["rx_speeds"]) / len(group_data["rx_speeds"]),
                            sum(group_data["tx_speeds"]) / len(group_data["tx_speeds"]),
                            max(group_data["rx_dropped"])
                            if group_data["rx_dropped"]
                            else 0,
                            max(group_data["tx_dropped"])
                            if group_data["tx_dropped"]
                            else 0,
                            len(group_data["rx_speeds"]),
                        ),
                    )

        # ═══════════════════════════════════════════
        # 生成1分钟聚合数据（从10秒聚合表）
        # ═══════════════════════════════════════════
        three_hours_ago_ms = now_ms - (3 * 60 * 60 * 1000)

        cursor.execute(
            """
            SELECT rx_speed_avg, tx_speed_avg, rx_dropped_sum, tx_dropped_sum,
                   sample_count, timestamp, timestamp_ms
            FROM traffic_history_10s
            WHERE namespace = ? AND timestamp_ms > ?
            ORDER BY timestamp_ms ASC
        """,
            (ns, three_hours_ago_ms),
        )
        rows_10s = cursor.fetchall()

        if rows_10s:
            # 按1分钟分组
            groups_1m = {}
            for rx_avg, tx_avg, rx_drop, tx_drop, sample_cnt, ts, ts_ms in rows_10s:
                group_key = (ts_ms // (INTERVAL_1M * 1000)) * (INTERVAL_1M * 1000)

                if group_key not in groups_1m:
                    groups_1m[group_key] = {
                        "rx_speeds": [],
                        "tx_speeds": [],
                        "rx_dropped": [],
                        "tx_dropped": [],
                        "timestamps": [],
                    }

                groups_1m[group_key]["rx_speeds"].append(rx_avg)
                groups_1m[group_key]["tx_speeds"].append(tx_avg)
                groups_1m[group_key]["rx_dropped"].append(rx_drop)
                groups_1m[group_key]["tx_dropped"].append(tx_drop)
                groups_1m[group_key]["timestamps"].append(ts)

            # 写入1分钟聚合表
            for group_key, group_data in groups_1m.items():
                if group_data["rx_speeds"]:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO traffic_history_1m
                        (namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,
                         rx_dropped_sum, tx_dropped_sum, sample_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            ns,
                            group_data["timestamps"][-1]
                            if group_data["timestamps"]
                            else None,
                            group_key,
                            sum(group_data["rx_speeds"]) / len(group_data["rx_speeds"]),
                            sum(group_data["tx_speeds"]) / len(group_data["tx_speeds"]),
                            max(group_data["rx_dropped"])
                            if group_data["rx_dropped"]
                            else 0,
                            max(group_data["tx_dropped"])
                            if group_data["tx_dropped"]
                            else 0,
                            len(group_data["rx_speeds"]),
                        ),
                    )

    conn.commit()
    conn.close()


def get_data_by_duration(namespace, duration_minutes):
    """
    根据时间范围获取数据，自动选择合适的数据表

    Args:
        namespace: 命名空间
        duration_minutes: 时间范围（分钟）

    Returns:
        list: 数据列表
    """
    if not os.path.exists(DB_PATH):
        return []

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    now_ms = int(time.time() * 1000)
    since_ms = now_ms - (duration_minutes * 60 * 1000)

    # 根据时间范围选择数据表
    if duration_minutes <= 5:
        # 5分钟内：使用原始数据
        table = "traffic_history"
        cursor.execute(
            """
            SELECT data, timestamp_ms FROM %s
            WHERE namespace = ? AND timestamp_ms > ?
            ORDER BY timestamp_ms ASC
        """
            % table,
            (namespace, since_ms),
        )
        rows = cursor.fetchall()
        conn.close()
        # 返回原始JSON数据
        import json

        return [json.loads(row[0]) for row in rows]

    elif duration_minutes <= 60:
        # 1小时内：使用10秒聚合数据
        table = "traffic_history_10s"
        cursor.execute(
            """
            SELECT namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,
                   rx_dropped_sum, tx_dropped_sum
            FROM %s
            WHERE namespace = ? AND timestamp_ms > ?
            ORDER BY timestamp_ms ASC
        """
            % table,
            (namespace, since_ms),
        )
        rows = cursor.fetchall()
        conn.close()

        # 转换为统一格式
        result = []
        for ns, ts, ts_ms, rx_speed, tx_speed, rx_drop, tx_drop in rows:
            result.append(
                {
                    "namespace": ns,
                    "timestamp": ts,
                    "timestamp_ms": ts_ms,
                    "rx_speed": rx_speed,
                    "tx_speed": tx_speed,
                    "rx_dropped": rx_drop,
                    "tx_dropped": tx_drop,
                    "resolution": "10s",
                }
            )
        return result

    else:
        # 超过1小时：使用1分钟聚合数据
        table = "traffic_history_1m"
        cursor.execute(
            """
            SELECT namespace, timestamp, timestamp_ms, rx_speed_avg, tx_speed_avg,
                   rx_dropped_sum, tx_dropped_sum
            FROM %s
            WHERE namespace = ? AND timestamp_ms > ?
            ORDER BY timestamp_ms ASC
        """
            % table,
            (namespace, since_ms),
        )
        rows = cursor.fetchall()
        conn.close()

        # 转换为统一格式
        result = []
        for ns, ts, ts_ms, rx_speed, tx_speed, rx_drop, tx_drop in rows:
            result.append(
                {
                    "namespace": ns,
                    "timestamp": ts,
                    "timestamp_ms": ts_ms,
                    "rx_speed": rx_speed,
                    "tx_speed": tx_speed,
                    "rx_dropped": rx_drop,
                    "tx_dropped": tx_drop,
                    "resolution": "1m",
                }
            )
        return result


def check_database():
    """检查数据库状态"""
    if not os.path.exists(DB_PATH):
        print("数据库文件不存在: %s" % DB_PATH)
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("\n=== 数据库状态 ===")
    print("数据库路径: %s" % DB_PATH)
    print("数据库大小: %.2f KB" % (os.path.getsize(DB_PATH) / 1024.0))

    # 检查各表
    tables = [
        ("traffic_history", "原始数据", RAW_RECORDS_PER_NAMESPACE),
        ("traffic_history_10s", "10秒聚合", RECORDS_10S_PER_NAMESPACE),
        ("traffic_history_1m", "1分钟聚合", RECORDS_1M_PER_NAMESPACE),
    ]

    for table_name, desc, max_records in tables:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM %s" % table_name)
            total = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT namespace, COUNT(*) as cnt
                FROM %s
                GROUP BY namespace
                ORDER BY namespace
            """
                % table_name
            )
            ns_counts = cursor.fetchall()

            print("\n[%s] %s:" % (desc, table_name))
            print("  总记录数: %d" % total)
            if ns_counts:
                for ns, cnt in ns_counts:
                    print("    [%s]: %d 条" % (ns, cnt))
        else:
            print("\n[%s] %s: 表不存在" % (desc, table_name))

    conn.close()


def cleanup_old_data(hours=3):
    """手动清理超过指定小时数的旧数据"""
    if not os.path.exists(DB_PATH):
        print("数据库文件不存在: %s" % DB_PATH)
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cutoff_ms = int(time.time() * 1000) - (hours * 3600 * 1000)

    tables = ["traffic_history", "traffic_history_10s", "traffic_history_1m"]
    total_deleted = 0

    for table in tables:
        cursor.execute(
            "SELECT COUNT(*) FROM %s WHERE timestamp_ms < ?" % table, (cutoff_ms,)
        )
        old_count = cursor.fetchone()[0]

        if old_count > 0:
            cursor.execute(
                "DELETE FROM %s WHERE timestamp_ms < ?" % table, (cutoff_ms,)
            )
            total_deleted += old_count
            print("✓ 从 %s 清理 %d 条记录" % (table, old_count))

    conn.commit()
    conn.close()

    if total_deleted > 0:
        print("✓ 总计清理 %d 条超过 %d 小时的旧数据" % (total_deleted, hours))
    else:
        print("没有需要清理的旧数据")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "cleanup":
            hours = int(sys.argv[2]) if len(sys.argv) > 2 else 3
            cleanup_old_data(hours)
        elif cmd == "aggregate":
            namespace = sys.argv[2] if len(sys.argv) > 2 else None
            aggregate_data(namespace)
            print("✓ 数据聚合完成")
        elif cmd == "status":
            check_database()
        else:
            print(
                "用法: python init_db.py [cleanup [hours]|aggregate [namespace]|status]"
            )
    else:
        init_database()
        check_database()
