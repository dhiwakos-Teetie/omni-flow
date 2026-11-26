"""Real ClickHouse 24.8 verification script for ReplacingMergeTree CDC edge cases.

Proves against an actual ClickHouse instance:
1. Unmerged data parts cause stale/duplicate rows on raw SELECT (using SYSTEM STOP MERGES).
2. Filtering WHERE is_deleted = 0 before deduplicating causes the Soft-Delete Zombie Row Trap.
3. Identical version_ts (same-ms) collisions yield nondeterministic results under ReplacingMergeTree(version_ts, is_deleted),
   while WAL LSN + Kafka offset deterministic ordering always wins.
4. Background merge + cleanup compacts parts and purges soft-deleted rows.
"""

import sys

import clickhouse_connect

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
DB = "cdc_olap"


def print_rows(label: str, rows) -> None:
    print(f"  {label}:")
    if not rows:
        print("    (0 rows)")
        return
    for r in rows:
        print(f"    {r}")


def setup_tables(ch) -> None:
    ch.command(f"CREATE DATABASE IF NOT EXISTS {DB}")
    ch.command(f"DROP VIEW IF EXISTS {DB}.v_demo_orders_final")
    ch.command(f"DROP VIEW IF EXISTS {DB}.v_demo_orders_argmax")
    ch.command(f"DROP TABLE IF EXISTS {DB}.demo_orders_ts")
    ch.command(f"DROP TABLE IF EXISTS {DB}.demo_orders_lsn")

    table_ddl = """
        CREATE TABLE {db}.{name} (
            order_id UInt32,
            customer_name String,
            amount Float64,
            order_status LowCardinality(String),
            version_ts UInt64,
            wal_lsn UInt64,
            kafka_offset UInt64,
            is_deleted UInt8
        ) ENGINE = ReplacingMergeTree({ver_col}, is_deleted)
        ORDER BY (order_id)
        SETTINGS allow_experimental_replacing_merge_with_cleanup = 1
    """
    ch.command(table_ddl.format(db=DB, name="demo_orders_ts", ver_col="version_ts"))
    ch.command(table_ddl.format(db=DB, name="demo_orders_lsn", ver_col="wal_lsn"))

    ch.command(
        f"""
        CREATE VIEW {DB}.v_demo_orders_final AS
        SELECT order_id, customer_name, amount, order_status, version_ts, wal_lsn, is_deleted
        FROM {DB}.demo_orders_lsn FINAL
        WHERE is_deleted = 0
        """
    )
    ch.command(
        f"""
        CREATE VIEW {DB}.v_demo_orders_argmax AS
        SELECT
            order_id,
            argMax(customer_name, (wal_lsn, kafka_offset)) AS customer_name,
            argMax(amount, (wal_lsn, kafka_offset)) AS amount,
            argMax(order_status, (wal_lsn, kafka_offset)) AS order_status,
            argMax(is_deleted, (wal_lsn, kafka_offset)) AS is_deleted
        FROM {DB}.demo_orders_lsn
        GROUP BY order_id
        HAVING is_deleted = 0
        """
    )


def insert_row(ch, table: str, row: list) -> None:
    ch.insert(
        f"{DB}.{table}",
        [row],
        column_names=[
            "order_id",
            "customer_name",
            "amount",
            "order_status",
            "version_ts",
            "wal_lsn",
            "kafka_offset",
            "is_deleted",
        ],
    )


def main() -> int:
    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        ch.command("SELECT 1")
    except Exception as exc:
        print("❌ Cannot connect to ClickHouse at localhost:8123.")
        print(f"   Detail: {exc}")
        print("   Start Lab 3 ClickHouse first with:")
        print("     cd c:\\Omni-Flow\\lab3-debezium-cdc && docker compose up -d clickhouse")
        return 1

    setup_tables(ch)
    cols = ["demo_orders_ts", "demo_orders_lsn"]

    try:
        for t in cols:
            ch.command(f"SYSTEM STOP MERGES {DB}.{t}")

        print("=" * 72)
        print("Scenario 1: Stale & Duplicate Reads Before Background Merge")
        print("=" * 72)
        batch_1 = [
            [101, "Somchai K.", 10000.0, "PENDING", 1732600001000, 24501000, 10, 0],
            [101, "Somchai K.", 12000.0, "PAID", 1732600002000, 24501120, 11, 0],
            [101, "Somchai K.", 12000.0, "SHIPPED", 1732600003000, 24501288, 12, 0],
            [102, "Suda W.", 8000.0, "PAID", 1732600001500, 24501050, 13, 0],
        ]
        for row in batch_1:
            for t in cols:
                insert_row(ch, t, row)

        parts = ch.query(
            f"SELECT count() FROM system.parts WHERE database='{DB}' AND table='demo_orders_lsn' AND active=1"
        ).result_rows[0][0]
        raw = ch.query(f"SELECT order_id, order_status, amount FROM {DB}.demo_orders_lsn ORDER BY order_id, wal_lsn").result_rows
        final_rows = ch.query(f"SELECT order_id, order_status, amount FROM {DB}.v_demo_orders_final ORDER BY order_id").result_rows
        argmax_rows = ch.query(f"SELECT order_id, order_status, amount FROM {DB}.v_demo_orders_argmax ORDER BY order_id").result_rows

        print(f"  Active unmerged parts: {parts}")
        print_rows("Raw SELECT (duplicate!)", raw)
        print_rows("v_demo_orders_final", final_rows)
        print_rows("v_demo_orders_argmax", argmax_rows)
        assert len(raw) == 4
        assert final_rows == [(101, "SHIPPED", 12000.0), (102, "PAID", 8000.0)]
        assert argmax_rows == final_rows

        print("\n" + "=" * 72)
        print("Scenario 2: Soft-Delete & Zombie Row Trap")
        print("=" * 72)
        delete_row = [101, "Somchai K.", 12000.0, "DELETED", 1732600004000, 24501400, 14, 1]
        for t in cols:
            insert_row(ch, t, delete_row)

        zombie = ch.query(
            f"""
            SELECT order_id, argMax(order_status, (wal_lsn, kafka_offset)) AS status
            FROM {DB}.demo_orders_lsn
            WHERE is_deleted = 0
            GROUP BY order_id
            ORDER BY order_id
            """
        ).result_rows
        safe_final = ch.query(f"SELECT order_id, order_status FROM {DB}.v_demo_orders_final ORDER BY order_id").result_rows
        safe_argmax = ch.query(f"SELECT order_id, order_status FROM {DB}.v_demo_orders_argmax ORDER BY order_id").result_rows

        print_rows("WRONG query (WHERE is_deleted=0 before GROUP BY -> Zombie!)", zombie)
        print_rows("SAFE v_demo_orders_final", safe_final)
        print_rows("SAFE v_demo_orders_argmax", safe_argmax)
        assert zombie == [(101, "SHIPPED"), (102, "PAID")]
        assert safe_final == [(102, "PAID")]
        assert safe_argmax == [(102, "PAID")]

        print("\n" + "=" * 72)
        print("Scenario 3: Same-ms version_ts Collision vs WAL LSN Tie-Breaker")
        print("=" * 72)
        # Newer update (CANCELLED, LSN 24509240) arrives first; older retried update (PAID, LSN 24509100) arrives second in the same ms.
        collision_rows = [
            [201, "Ananda P.", 15000.0, "CANCELLED", 1732600009999, 24509240, 51, 0],
            [201, "Ananda P.", 15000.0, "PAID", 1732600009999, 24509100, 50, 0],
        ]
        for row in collision_rows:
            for t in cols:
                insert_row(ch, t, row)

        ts_result = ch.query(
            f"SELECT order_id, order_status FROM {DB}.demo_orders_ts FINAL WHERE order_id=201 AND is_deleted=0"
        ).result_rows
        lsn_result = ch.query(
            f"SELECT order_id, order_status FROM {DB}.v_demo_orders_argmax WHERE order_id=201"
        ).result_rows

        print_rows("demo_orders_ts FINAL (corrupted by same version_ts!)", ts_result)
        print_rows("v_demo_orders_argmax (deterministic WAL LSN)", lsn_result)
        assert ts_result == [(201, "PAID")]
        assert lsn_result == [(201, "CANCELLED")]

        print("\n" + "=" * 72)
        print("Scenario 4: Background Merge (FINAL) vs Tombstone Purge (FINAL CLEANUP)")
        print("=" * 72)
        for t in cols:
            ch.command(f"SYSTEM START MERGES {DB}.{t}")
            ch.command(f"OPTIMIZE TABLE {DB}.{t} FINAL", settings={"alter_sync": 1})

        merged_parts = ch.query(
            f"SELECT count() FROM system.parts WHERE database='{DB}' AND table='demo_orders_lsn' AND active=1"
        ).result_rows[0][0]
        raw_after_final = ch.query(
            f"SELECT order_id, order_status, is_deleted FROM {DB}.demo_orders_lsn ORDER BY order_id"
        ).result_rows
        print(f"  Active parts after OPTIMIZE FINAL: {merged_parts}")
        print_rows("4a. Raw SELECT after OPTIMIZE FINAL (tombstone still retained by default!)", raw_after_final)
        assert merged_parts == 1
        assert raw_after_final == [(101, "DELETED", 1), (102, "PAID", 0), (201, "CANCELLED", 0)]

        for t in cols:
            ch.command(
                f"OPTIMIZE TABLE {DB}.{t} FINAL CLEANUP",
                settings={"alter_sync": 1},
            )

        raw_after_cleanup = ch.query(
            f"SELECT order_id, order_status, is_deleted FROM {DB}.demo_orders_lsn ORDER BY order_id"
        ).result_rows
        print_rows("4b. Raw SELECT after OPTIMIZE FINAL CLEANUP (tombstone purged from disk!)", raw_after_cleanup)
        assert raw_after_cleanup == [(102, "PAID", 0), (201, "CANCELLED", 0)]
        print("\n✅ All real ClickHouse 24.8 assertions passed!")
        return 0
    finally:
        for t in cols:
            try:
                ch.command(f"SYSTEM START MERGES {DB}.{t}")
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
