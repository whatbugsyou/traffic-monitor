#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite 数据库初始化脚本
用于流量监控数据存储
- 数据保留时间：3 小时（每个命名空间 10800 条记录）
"""

import os
import sqlite3

# 数据库配置
DB_PATH = "/root/traffic-monitor/data/traffic_monitor.db"

# 数据保留配置
# 采样间隔 1 秒，3 小时 = 3 * 60 * 60 = 10800 条记录
RECORDS_PER_NAMESPACE = 10800


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

        # 删除旧的单列唯一索引（如果存在）
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='index' AND tbl_name='traffic_history'
        """)
        old_indexes = [row[0] for row in cursor.fetchall()]

        for idx_name in old_indexes:
            # 跳过自动生成的主键索引
            if not idx_name.startswith("sqlite_"):
                cursor.execute('DROP INDEX IF EXISTS "{}"'.format(idx_name))

        # 旧表的 UNIQUE(timestamp_ms) 约束内嵌在表定义中，无法直接删除列约束，
        # 需要重建表以去除旧约束并添加新的组合唯一约束
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
        print("✓ 数据库迁移完成：已添加 namespace 列，已重建唯一约束")
    else:
        print("数据库结构已是最新版本，无需迁移")

    conn.close()


def init_database():
    """初始化数据库"""
    print("正在初始化数据库...")

    # 先执行迁移（针对已存在的旧版数据库）
    migrate_database()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 创建表
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

    # 创建索引
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_namespace_timestamp_ms
        ON traffic_history(namespace, timestamp_ms)
    """)

    # 删除旧的触发器（如果存在）
    cursor.execute("DROP TRIGGER IF EXISTS auto_cleanup")

    # 创建自动清理触发器
    # 每个命名空间保留最近 10800 条记录（3小时）
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
        % RECORDS_PER_NAMESPACE
    )

    conn.commit()
    conn.close()

    print("✓ 数据库初始化完成: %s" % DB_PATH)
    print("✓ 表结构已创建（含 namespace 字段）")
    print("✓ 索引已创建（namespace, timestamp_ms）")
    print(
        "✓ 自动清理触发器已创建（每个命名空间保留最近 %d 条记录，约 %.1f 小时）"
        % (RECORDS_PER_NAMESPACE, RECORDS_PER_NAMESPACE / 3600.0)
    )


def check_database():
    """检查数据库状态"""
    if not os.path.exists(DB_PATH):
        print("数据库文件不存在: %s" % DB_PATH)
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM traffic_history")
    total_count = cursor.fetchone()[0]

    db_size = os.path.getsize(DB_PATH)

    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM traffic_history")
    time_range = cursor.fetchone()

    cursor.execute("""
        SELECT namespace, COUNT(*) as cnt
        FROM traffic_history
        GROUP BY namespace
        ORDER BY namespace
    """)
    ns_counts = cursor.fetchall()

    conn.close()

    print("\n=== 数据库状态 ===")
    print("数据库路径: %s" % DB_PATH)
    print("数据库大小: %.2f KB" % (db_size / 1024.0))
    print("记录总数量: %d 条" % total_count)
    if time_range[0] and time_range[1]:
        print("时间范围: %s ~ %s" % (time_range[0], time_range[1]))

    if ns_counts:
        print("\n各命名空间记录数：")
        for ns, cnt in ns_counts:
            hours = cnt / 3600.0
            print("  [%s]: %d 条（约 %.1f 小时）" % (ns, cnt, hours))
    else:
        print("\n暂无记录")


def cleanup_old_data(hours=3):
    """手动清理超过指定小时数的旧数据"""
    if not os.path.exists(DB_PATH):
        print("数据库文件不存在: %s" % DB_PATH)
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 计算截止时间戳（毫秒）
    import time

    cutoff_ms = int(time.time() * 1000) - (hours * 3600 * 1000)

    cursor.execute(
        """
        SELECT COUNT(*) FROM traffic_history
        WHERE timestamp_ms < ?
    """,
        (cutoff_ms,),
    )
    old_count = cursor.fetchone()[0]

    if old_count > 0:
        cursor.execute(
            """
            DELETE FROM traffic_history
            WHERE timestamp_ms < ?
        """,
            (cutoff_ms,),
        )
        conn.commit()
        print("✓ 已清理 %d 条超过 %d 小时的旧数据" % (old_count, hours))
    else:
        print("没有需要清理的旧数据")

    conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "cleanup":
        # 手动清理旧数据
        hours = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        cleanup_old_data(hours)
    else:
        init_database()
        check_database()
