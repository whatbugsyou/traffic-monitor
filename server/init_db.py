#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite 数据库初始化脚本
用于流量监控数据存储
"""

import os
import sqlite3

# 数据库配置
DB_PATH = "/root/traffic-monitor/data/traffic_monitor.db"


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
                cursor.execute(f'DROP INDEX IF EXISTS "{idx_name}"')

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

    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS auto_cleanup
        AFTER INSERT ON traffic_history
        WHEN (
            SELECT COUNT(*) FROM traffic_history
            WHERE namespace = NEW.namespace
        ) > 300
        BEGIN
            DELETE FROM traffic_history
            WHERE id = (
                SELECT id FROM traffic_history
                WHERE namespace = NEW.namespace
                ORDER BY timestamp_ms ASC
                LIMIT 1
            );
        END
    """)

    conn.commit()
    conn.close()

    print(f"✓ 数据库初始化完成: {DB_PATH}")
    print("✓ 表结构已创建（含 namespace 字段）")
    print("✓ 索引已创建（namespace, timestamp_ms）")
    print("✓ 自动清理触发器已创建（每个命名空间各自保留最近 300 条记录）")


def check_database():
    """检查数据库状态"""
    if not os.path.exists(DB_PATH):
        print(f"数据库文件不存在: {DB_PATH}")
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
    print(f"数据库路径: {DB_PATH}")
    print(f"数据库大小: {db_size / 1024:.2f} KB")
    print(f"记录总数量: {total_count} 条")
    if time_range[0] and time_range[1]:
        print(f"时间范围: {time_range[0]} ~ {time_range[1]}")

    if ns_counts:
        print("\n各命名空间记录数：")
        for ns, cnt in ns_counts:
            print(f"  [{ns}]: {cnt} 条")
    else:
        print("\n暂无记录")


if __name__ == "__main__":
    init_database()
    check_database()
