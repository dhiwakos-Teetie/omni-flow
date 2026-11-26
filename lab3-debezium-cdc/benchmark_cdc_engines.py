"""ClickHouse 24.8 CDC Engine Benchmark & Edge-Case Proof Lab.

Compares 4 query/storage architectures across 300,000+ CDC rows (200k orders + 80k updates + 20k deletes):
  1A. ReplacingMergeTree + raw argMax + GROUP BY order_id
  1B. ReplacingMergeTree + FINAL
  1C. VersionedCollapsingMergeTree(sign, row_version) -> sum(sign), sum(amount * sign)
  1D. Signed Delta Stream (ENGINE=Null) -> Incremental Materialized View -> SummingMergeTree

Also proves:
  - Test 2: VersionedCollapsingMergeTree handles out-of-order (-1 before +1) arrival cleanly.
  - Test 3: Kafka At-Least-Once duplicate trap (Collapsing doubles revenue vs Replacing is idempotent).
  - Test 4: ClickHouse Query Cache speedup on heavy argMax queries.
"""

import sys
import time
from typing import Dict, List, Tuple

import clickhouse_connect

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
DB = "cdc_olap"


def setup_schema(ch) -> None:
    ch.command(f"CREATE DATABASE IF NOT EXISTS {DB}")
    for obj in (
        f"DROP VIEW IF EXISTS {DB}.mv_bench_kpi_summing",
        f"DROP TABLE IF EXISTS {DB}.bench_kpi_summing",
        f"DROP TABLE IF EXISTS {DB}.bench_delta_input",
        f"DROP TABLE IF EXISTS {DB}.bench_collapsing",
        f"DROP TABLE IF EXISTS {DB}.bench_replacing",
        f"DROP TABLE IF EXISTS {DB}.bench_edge_replacing",
        f"DROP TABLE IF EXISTS {DB}.bench_edge_collapsing",
    ):
        ch.command(obj)

    ch.command(
        f"""
        CREATE TABLE {DB}.bench_replacing (
            order_id UInt32,
            order_date Date,
            category LowCardinality(String),
            amount Float64,
            wal_lsn UInt64,
            is_deleted UInt8
        ) ENGINE = ReplacingMergeTree(wal_lsn, is_deleted)
        ORDER BY (order_id)
        """
    )

    ch.command(
        f"""
        CREATE TABLE {DB}.bench_collapsing (
            order_id UInt32,
            order_date Date,
            category LowCardinality(String),
            amount Float64,
            sign Int8,
            row_version UInt32
        ) ENGINE = VersionedCollapsingMergeTree(sign, row_version)
        ORDER BY (order_id, row_version)
        """
    )

    # ENGINE = Null receives delta stream & fires MV without storing raw duplicates on disk
    ch.command(
        f"""
        CREATE TABLE {DB}.bench_delta_input (
            order_date Date,
            category LowCardinality(String),
            sign Int8,
            amount Float64
        ) ENGINE = Null
        """
    )

    ch.command(
        f"""
        CREATE TABLE {DB}.bench_kpi_summing (
            order_date Date,
            category LowCardinality(String),
            order_count Int64,
            total_revenue Float64
        ) ENGINE = SummingMergeTree()
        ORDER BY (order_date, category)
        """
    )

    ch.command(
        f"""
        CREATE MATERIALIZED VIEW {DB}.mv_bench_kpi_summing
        TO {DB}.bench_kpi_summing AS
        SELECT
            order_date,
            category,
            sum(sign) AS order_count,
            sum(amount * sign) AS total_revenue
        FROM {DB}.bench_delta_input
        GROUP BY order_date, category
        """
    )

    ch.command(
        f"""
        CREATE TABLE {DB}.bench_edge_replacing (
            order_id UInt32,
            amount Float64,
            wal_lsn UInt64,
            is_deleted UInt8
        ) ENGINE = ReplacingMergeTree(wal_lsn, is_deleted)
        ORDER BY (order_id)
        """
    )

    ch.command(
        f"""
        CREATE TABLE {DB}.bench_edge_collapsing (
            order_id UInt32,
            amount Float64,
            sign Int8,
            row_version UInt32
        ) ENGINE = VersionedCollapsingMergeTree(sign, row_version)
        ORDER BY (order_id, row_version)
        """
    )


def populate_cdc_workload(ch) -> None:
    """Populate 200k initial orders + 80k updates + 20k deletes across all 3 models."""
    cats = "['Electronics', 'Accessories', 'Furniture', 'Software', 'Services']"
    base_date = "toDate('2026-11-01')"

    # Batch 1: 200,000 initial INSERTs
    ch.command(
        f"""
        INSERT INTO {DB}.bench_replacing
        SELECT
            toUInt32(number + 1) AS order_id,
            {base_date} + (number % 30) AS order_date,
            {cats}[(number % 5) + 1] AS category,
            1000.0 + ((number + 1) % 50) * 100.0 AS amount,
            toUInt64(1000000 + number + 1) AS wal_lsn,
            0 AS is_deleted
        FROM numbers(200000)
        """
    )
    ch.command(
        f"""
        INSERT INTO {DB}.bench_collapsing
        SELECT
            toUInt32(number + 1) AS order_id,
            {base_date} + (number % 30) AS order_date,
            {cats}[(number % 5) + 1] AS category,
            1000.0 + ((number + 1) % 50) * 100.0 AS amount,
            1 AS sign,
            1 AS row_version
        FROM numbers(200000)
        """
    )
    ch.command(
        f"""
        INSERT INTO {DB}.bench_delta_input
        SELECT
            {base_date} + (number % 30) AS order_date,
            {cats}[(number % 5) + 1] AS category,
            1 AS sign,
            1000.0 + ((number + 1) % 50) * 100.0 AS amount
        FROM numbers(200000)
        """
    )

    # Batch 2: 80,000 UPDATEs (+500 THB on order_id 1..80,000)
    ch.command(
        f"""
        INSERT INTO {DB}.bench_replacing
        SELECT
            toUInt32(number + 1) AS order_id,
            {base_date} + (number % 30) AS order_date,
            {cats}[(number % 5) + 1] AS category,
            1500.0 + ((number + 1) % 50) * 100.0 AS amount,
            toUInt64(2000000 + number + 1) AS wal_lsn,
            0 AS is_deleted
        FROM numbers(80000)
        """
    )
    for sign_val, delta, ver in ((-1, 1000.0, 1), (1, 1500.0, 2)):
        ch.command(
            f"""
            INSERT INTO {DB}.bench_collapsing
            SELECT
                toUInt32(number + 1) AS order_id,
                {base_date} + (number % 30) AS order_date,
                {cats}[(number % 5) + 1] AS category,
                {delta} + ((number + 1) % 50) * 100.0 AS amount,
                {sign_val} AS sign,
                {ver} AS row_version
            FROM numbers(80000)
            """
        )
        ch.command(
            f"""
            INSERT INTO {DB}.bench_delta_input
            SELECT
                {base_date} + (number % 30) AS order_date,
                {cats}[(number % 5) + 1] AS category,
                {sign_val} AS sign,
                {delta} + ((number + 1) % 50) * 100.0 AS amount
            FROM numbers(80000)
            """
        )

    # Batch 3: 20,000 DELETEs (order_id 80,001..100,000)
    ch.command(
        f"""
        INSERT INTO {DB}.bench_replacing
        SELECT
            toUInt32(number + 80001) AS order_id,
            {base_date} + ((number + 80000) % 30) AS order_date,
            {cats}[((number + 80000) % 5) + 1] AS category,
            1000.0 + ((number + 80001) % 50) * 100.0 AS amount,
            toUInt64(3000000 + number + 80001) AS wal_lsn,
            1 AS is_deleted
        FROM numbers(20000)
        """
    )
    ch.command(
        f"""
        INSERT INTO {DB}.bench_collapsing
        SELECT
            toUInt32(number + 80001) AS order_id,
            {base_date} + ((number + 80000) % 30) AS order_date,
            {cats}[((number + 80000) % 5) + 1] AS category,
            1000.0 + ((number + 80001) % 50) * 100.0 AS amount,
            -1 AS sign,
            1 AS row_version
        FROM numbers(20000)
        """
    )
    ch.command(
        f"""
        INSERT INTO {DB}.bench_delta_input
        SELECT
            {base_date} + ((number + 80000) % 30) AS order_date,
            {cats}[((number + 80000) % 5) + 1] AS category,
            -1 AS sign,
            1000.0 + ((number + 80001) % 50) * 100.0 AS amount
        FROM numbers(20000)
        """
    )


def run_timed_query(ch, name: str, sql: str, settings: Dict = None) -> Dict:
    t0 = time.perf_counter()
    res = ch.query(sql, settings=settings or {})
    client_ms = (time.perf_counter() - t0) * 1000.0
    rows = [(r[0], int(r[1]), round(float(r[2]), 2)) for r in res.result_rows]
    read_rows = int((res.summary or {}).get("read_rows", 0))
    return {
        "name": name,
        "query_id": res.query_id,
        "client_ms": client_ms,
        "read_rows": read_rows,
        "rows": rows,
        "active_orders": sum(r[1] for r in rows),
        "total_revenue": round(sum(r[2] for r in rows), 2),
    }


def fetch_query_log_metrics(ch, query_ids: List[str]) -> Dict[str, Tuple[float, float]]:
    ch.command("SYSTEM FLUSH LOGS")
    ids_sql = ", ".join(f"'{qid}'" for qid in query_ids if qid)
    metrics: Dict[str, Tuple[float, float]] = {}
    if not ids_sql:
        return metrics
    qlog = ch.query(
        f"""
        SELECT query_id, query_duration_ms, memory_usage
        FROM system.query_log
        WHERE type = 'QueryFinish' AND query_id IN ({ids_sql})
        """
    )
    for qid, dur_ms, mem_bytes in qlog.result_rows:
        metrics[qid] = (float(dur_ms), float(mem_bytes) / (1024.0 * 1024.0))
    return metrics


def run_test_1_benchmark(ch) -> None:
    print("=" * 84)
    print("TEST 1: 300k-Row CDC Benchmark (200k Inserts + 80k Updates + 20k Deletes)")
    print("=" * 84)

    queries = [
        (
            "1A. Replacing + argMax(GROUP BY order_id)",
            f"""
            SELECT
                category,
                count() AS active_orders,
                sum(amount) AS total_revenue
            FROM (
                SELECT
                    order_id,
                    argMax(category, wal_lsn) AS category,
                    argMax(amount, wal_lsn) AS amount,
                    argMax(is_deleted, wal_lsn) AS is_deleted
                FROM {DB}.bench_replacing
                GROUP BY order_id
                HAVING is_deleted = 0
            )
            GROUP BY category
            ORDER BY category
            """,
        ),
        (
            "1B. Replacing + FINAL",
            f"""
            SELECT
                category,
                count() AS active_orders,
                sum(amount) AS total_revenue
            FROM {DB}.bench_replacing FINAL
            WHERE is_deleted = 0
            GROUP BY category
            ORDER BY category
            """,
        ),
        (
            "1C. VersionedCollapsing (sum(sign))",
            f"""
            SELECT
                category,
                sum(sign) AS active_orders,
                sum(amount * sign) AS total_revenue
            FROM {DB}.bench_collapsing
            GROUP BY category
            ORDER BY category
            """,
        ),
        (
            "1D. Delta MV -> SummingMergeTree",
            f"""
            SELECT
                category,
                sum(order_count) AS active_orders,
                sum(total_revenue) AS total_revenue
            FROM {DB}.bench_kpi_summing
            GROUP BY category
            ORDER BY category
            """,
        ),
    ]

    results = [run_timed_query(ch, name, sql) for name, sql in queries]
    log_metrics = fetch_query_log_metrics(ch, [r["query_id"] for r in results])

    baseline = results[0]["rows"]
    for r in results:
        assert r["rows"] == baseline, f"Mismatch in {r['name']}: {r['rows']} != {baseline}"
        assert r["active_orders"] == 180000, f"Expected 180,000 active orders, got {r['active_orders']}"

    print(f"{'Architecture / Query':<42} | {'Server ms':>9} | {'Rows Read':>10} | {'Peak RAM':>9} | {'Orders':>8} | {'Revenue (THB)':>15}")
    print("-" * 108)
    for r in results:
        srv_ms, mem_mb = log_metrics.get(r["query_id"], (r["client_ms"], 0.0))
        print(
            f"{r['name']:<42} | {srv_ms:>7.1f}ms | {r['read_rows']:>10,} | {mem_mb:>7.2f}MB | {r['active_orders']:>8,} | {r['total_revenue']:>15,.2f}"
        )
    print("✅ All 4 architectures returned 100% identical category counts & revenue (180,000 active orders).")


def run_test_2_out_of_order(ch) -> None:
    print("\n" + "=" * 84)
    print("TEST 2: VersionedCollapsingMergeTree Out-of-Order Arrival (-1 before +1)")
    print("=" * 84)
    # Insert cancel (-1, v1) BEFORE original insert (+1, v1)
    ch.insert(
        f"{DB}.bench_edge_collapsing",
        [[501, 10000.0, -1, 1], [501, 10000.0, 1, 1]],
        column_names=["order_id", "amount", "sign", "row_version"],
    )
    before_merge = ch.query(
        f"SELECT sum(sign), sum(amount * sign) FROM {DB}.bench_edge_collapsing WHERE order_id = 501"
    ).result_rows[0]
    print(f"  Before merge (sum(sign), sum(amount*sign)): {before_merge}")
    assert before_merge == (0, 0.0)

    ch.command(f"OPTIMIZE TABLE {DB}.bench_edge_collapsing FINAL", settings={"alter_sync": 1})
    rows_after_merge = ch.query(
        f"SELECT count() FROM {DB}.bench_edge_collapsing WHERE order_id = 501"
    ).result_rows[0][0]
    print(f"  Physical rows remaining after OPTIMIZE FINAL: {rows_after_merge}")
    assert rows_after_merge == 0
    print("✅ VersionedCollapsingMergeTree collapsed out-of-order (-1/+1) pair completely!")


def run_test_3_kafka_duplicate_trap(ch) -> None:
    print("\n" + "=" * 84)
    print("TEST 3: Kafka At-Least-Once Duplicate Trap (Collapsing vs Replacing)")
    print("=" * 84)
    # Simulate the same Kafka event being replayed twice after consumer restart
    for _ in range(2):
        ch.insert(
            f"{DB}.bench_edge_collapsing",
            [[999, 12000.0, 1, 2]],
            column_names=["order_id", "amount", "sign", "row_version"],
        )
        ch.insert(
            f"{DB}.bench_edge_replacing",
            [[999, 12000.0, 999002, 0]],
            column_names=["order_id", "amount", "wal_lsn", "is_deleted"],
        )

    collapsing_rev = float(
        ch.query(f"SELECT sum(amount * sign) FROM {DB}.bench_edge_collapsing WHERE order_id = 999").result_rows[0][0]
    )
    replacing_rev = float(
        ch.query(f"SELECT sum(amount) FROM {DB}.bench_edge_replacing FINAL WHERE order_id = 999 AND is_deleted = 0").result_rows[0][0]
    )
    print(f"  VersionedCollapsingMergeTree revenue after 2x replay : {collapsing_rev:,.2f} THB (DOUBLED! ❌)")
    print(f"  ReplacingMergeTree FINAL revenue after 2x replay     : {replacing_rev:,.2f} THB (IDEMPOTENT! ✅)")
    assert collapsing_rev == 24000.0
    assert replacing_rev == 12000.0
    print("✅ Proved: Collapsing requires upstream deduplication/tokens, whereas Replacing is naturally idempotent.")


def run_test_4_query_cache(ch) -> None:
    print("\n" + "=" * 84)
    print("TEST 4: ClickHouse Query Cache on Heavy argMax Query (1s Dashboard Refresh)")
    print("=" * 84)
    try:
        ch.command("SYSTEM DROP QUERY CACHE")
    except Exception:
        pass

    heavy_sql = f"""
        SELECT category, count() AS active_orders, sum(amount) AS total_revenue
        FROM (
            SELECT order_id, argMax(category, wal_lsn) AS category,
                   argMax(amount, wal_lsn) AS amount, argMax(is_deleted, wal_lsn) AS is_deleted
            FROM {DB}.bench_replacing
            GROUP BY order_id
            HAVING is_deleted = 0
        )
        GROUP BY category
        ORDER BY category
    """
    cache_settings = {"use_query_cache": 1, "query_cache_ttl": 10}
    miss = run_timed_query(ch, "Cache Miss", heavy_sql, settings=cache_settings)
    hit = run_timed_query(ch, "Cache Hit", heavy_sql, settings=cache_settings)
    print(f"  1st Run (Cache Miss): {miss['client_ms']:.2f} ms | read_rows={miss['read_rows']:,}")
    print(f"  2nd Run (Cache Hit) : {hit['client_ms']:.2f} ms | read_rows={hit['read_rows']:,}")
    assert miss["rows"] == hit["rows"]
    assert hit["client_ms"] < miss["client_ms"]
    print("✅ Query Cache served identical results in milliseconds with zero table re-scan!")


def main() -> int:
    try:
        ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
        ch.command("SELECT 1")
    except Exception as exc:
        print("❌ Cannot connect to ClickHouse at localhost:8123.")
        print(f"   Detail: {exc}")
        print("   Start ClickHouse first:")
        print("     docker compose -f c:\\Omni-Flow\\lab3-debezium-cdc\\docker-compose.yml up -d clickhouse")
        return 1

    setup_schema(ch)
    frozen_tables = ("bench_replacing", "bench_collapsing")

    try:
        for t in frozen_tables:
            ch.command(f"SYSTEM STOP MERGES {DB}.{t}")

        populate_cdc_workload(ch)
        run_test_1_benchmark(ch)
        run_test_2_out_of_order(ch)
        run_test_3_kafka_duplicate_trap(ch)
        run_test_4_query_cache(ch)
        print("\n🎉 All 4 CDC engine benchmark & edge-case tests passed on ClickHouse 24.8!")
        return 0
    finally:
        for t in frozen_tables:
            try:
                ch.command(f"SYSTEM START MERGES {DB}.{t}")
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
