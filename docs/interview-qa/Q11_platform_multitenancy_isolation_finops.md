# Q11: Multi-Tenancy, Resource Isolation & FinOps บน Production

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab3-debezium-cdc`](../../lab3-debezium-cdc) (`system.query_log` Instrumentation) + [`lab2-minio-lakehouse`](../../lab2-minio-lakehouse) (Hot/Cold S3 Tiering) + [`omni-command-center`](../../omni-command-center) (Kafka Cluster & Hardware Telemetry)
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab3-debezium-cdc/benchmark_cdc_engines.py`](../../lab3-debezium-cdc/benchmark_cdc_engines.py#L251-L265) — ตัวอย่างการใช้ `system.query_log` วัด `query_duration_ms`, `memory_usage`, และ `read_rows` ราย Query
>   * [`lab2-minio-lakehouse/lakehouse_sink_worker.py`](../../lab2-minio-lakehouse/lakehouse_sink_worker.py#L151-L175) — การแยก Cold Parquet ลง MinIO S3 เพื่อลดต้นทุน SSD ของ Hot Tier
>   * [`omni-command-center/dashboard.py`](../../omni-command-center/dashboard.py#L48-L86) — การมอนิเตอร์ Hardware CPU/RAM ระดับโหนด
> * **เรื่องที่ต้องดูจากข้อนี้:** การกัน Noisy Neighbor ข้ามทีม/Workload (Read vs Write vs Ad-hoc), ClickHouse Settings Profiles/Quotas, Kafka Client Quotas, การคิด Cost-per-Query จาก `system.query_log` และ 5 คันโยกลดต้นทุน Cloud

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"When multiple teams, dashboards, ad-hoc analysts, and batch/streaming jobs share the same Kafka cluster, ClickHouse, and S3 bucket, a single runaway query or noisy neighbor can starve critical pipelines or blow up cloud bills. How do you isolate compute/I/O resources across workloads, prevent ad-hoc or heavy analytical queries from hurting real-time ingestion, and track/optimize cost per query or per pipeline?"*

---

## 🧩 แยกทบทวนเป็น 5 ข้อย่อย

### ข้อ 11.1 — ทำไม Shared Cluster ถึงล่มจาก Noisy Neighbor?
* **สถานการณ์จริง:** Analyst 1 คนยิง `SELECT * FROM orders_replica FINAL` (หรือ Join ข้ามปีไม่กรอง Partition)
* **ผลกระทบแบบโดมิโน:** ClickHouse โดนกิน RAM จน OOM หรือ CPU พุ่ง 100% ➔ Background Merge ช้าลง ➔ `system.parts` บวมจน Ingestion ติด `Too many parts` ➔ Kafka Consumer Lag พุ่งกระทบทุกทีมที่ใช้ Cluster ร่วมกัน!

### ข้อ 11.2 — ClickHouse Resource Isolation 3 ชั้น (กัน Ad-hoc ทำ Ingestion พัง)
1. **Physical Workload Separation (แยก Node / Replica):**
   * แยก Replica Pool ใน ClickHouse Cluster: **Ingestion & Merge Replicas** (ห้าม BI/Analyst ยิงเข้าตรงๆ), **BI Dashboard Replicas**, และ **Ad-hoc Analytics Replicas** — ทั้งหมดแชร์ข้อมูลผ่าน `ReplicatedMergeTree` + ZooKeeper/ClickHouse Keeper แต่ CPU/RAM แยกขาดจากกัน 100%
2. **Settings Profiles & Quotas (จำกัดเพดานต่อ User / Role):**
   ```sql
   CREATE SETTINGS PROFILE analyst_profile SETTINGS
       max_memory_usage = 8000000000,            -- จำกัด 8 GB ต่อ Query (ไม่ OOM ทั้งเครื่อง)
       max_execution_time = 30,                  -- เกิน 30 วินาทีตัดทิ้งอัตโนมัติ
       max_rows_to_read = 50000000,              -- กัน Full-Table Scan 100M+ แถว
       max_concurrent_queries_for_user = 4,      -- กันยิงรัวพร้อมกัน
       use_query_cache = 1,
       query_cache_ttl = 30;

   CREATE QUOTA analyst_quota FOR KEYED BY user_name
       FOR INTERVAL 1 HOUR MAX queries = 200, read_bytes = 50000000000
       TO role_analysts;
   ```
3. **ClickHouse 24.x Workload Scheduling & I/O Isolation:**
   * ใช้ **Workload Classification & Resource Scheduling** จัด Priority ให้ Background Merges, CDC Ingestion และ Executive Dashboard สูงกว่า Ad-hoc Queries และจำกัดจำนวน Thread (`max_threads`) ของงาน Ad-hoc

### ข้อ 11.3 — Kafka & S3 Multi-Tenancy Isolation
* **Kafka Cluster:**
  * ตั้ง **Kafka Client Quotas** (`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`) ต่อ `client-id` / `user` เพื่อไม่ให้ทีมหนึ่งอัดข้อมูลจน Network Broker เต็มและทำให้ Broker หยุดตอบ Heartbeat
  * แยก **Topic ACLs + Separate Consumer Groups** ชัดเจน และแยก **Tier-0 Critical Topics** (Payments/CDC) ออกจาก Tier-2 Telemetry/Logs บนคนละ Broker/Storage Pool หากสเกลใหญ่
* **S3 / MinIO Object Storage:**
  * แยก **Bucket / Prefix + IAM Policy** ต่อทีมและต่อ Layer (`raw/`, `silver/`, `gold/`, `sandbox/`)
  * ตั้ง **S3 Lifecycle Rules** (Abort incomplete multipart uploads, เปลี่ยนเป็น Glacier/Archive หรือลบ Temp/Sandbox prefix อัตโนมัติหลัง 30 วัน)

### ข้อ 11.4 — FinOps: วัด Cost ต่อ Query / ต่อทีม / ต่อ Pipeline ยังไง?
* **ต่อยอดจาก `system.query_log` ที่ใช้จริงใน [`benchmark_cdc_engines.py`](../../lab3-debezium-cdc/benchmark_cdc_engines.py#L251-L265):**
  * บังคับให้ทุก BI Tool, Python Worker, และ Airflow DAG แนบ Tag ใน `log_comment` (เช่น `{"team":"finance","pipeline":"medallion_gold","dashboard":"exec_kpi"}`)
  * ดึงข้อมูลจาก `system.query_log` มาคำนวณ **Cost Attribution** (คิดน้ำหนักจาก CPU microseconds + RAM GB-seconds + S3/Disk Read Bytes):
    ```sql
    SELECT
        JSONExtractString(log_comment, 'team') AS team,
        JSONExtractString(log_comment, 'dashboard') AS dashboard,
        count() AS query_count,
        round(sum(read_bytes) / 1024 / 1024 / 1024, 2) AS total_read_gb,
        round(sum(query_duration_ms) / 1000.0, 1) AS total_cpu_wall_sec,
        round(max(memory_usage) / 1024 / 1024, 1) AS peak_ram_mb
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= today() - 7
    GROUP BY team, dashboard
    ORDER BY total_read_gb DESC;
    ```
  * ใช้ทำ **Showback / Chargeback Report** รายสัปดาห์เพื่อจับ Top 5 Expensive Queries มา Optimize ทันที!

### ข้อ 11.5 — 5 คันโยกลดต้นทุน Cloud ใน Omni-Flow
1. **Hot/Cold Tiering (`lab2-minio-lakehouse`):** เก็บข้อมูลสด 7–30 วันบน SSD (ClickHouse Hot) แล้วย้ายข้อมูลเก่าลง Object Storage S3 Parquet (Cold) พร้อม **Partition TTL** (`TTL event_time + INTERVAL 30 DAY TO DISK 's3_cold'`)
2. **Columnar Compression & Type Tuning:** ใช้ `LowCardinality(String)` (ลดขนาด String ซ้ำๆ), `ZSTD(3)` และ `Delta` / `DoubleDelta` / `Gorilla` codecs สำหรับ Time-series
3. **Immutable Partition Pruning + Materialized Views:** ลดการสแกนจาก 300k/100M แถวเหลือแค่ 120 แถว (`63ms ➔ 3ms` ตาม Benchmark Lab 3)
4. **ClickHouse Query Cache:** Dashboard รีเฟรชทุกวินาทีคำนวณจริงแค่ 1 ครั้งต่อ TTL (`95.44ms ➔ 2.32ms` ตาม Benchmark Lab 3)
5. **Compaction / Small-File Hygiene:** รวมไฟล์ Parquet ให้ได้ขนาดเหมาะสม (64–256 MB) เพื่อลด S3 GET/LIST API Request Cost

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"เพื่อกันปัญหา Noisy Neighbor ไม่ให้ Ad-hoc Query เดียวมาทำ Real-time Ingestion ล่ม ผมแยกควบคุมทั้ง 3 ชั้นครับ:*
  >
  > *1. **ฝั่ง ClickHouse:** ถ้าคลัสเตอร์ใหญ่ผมแยก **Read Replicas สำหรับ BI/Ad-hoc ออกจาก Write/Merge Replicas** โดยสิ้นเชิง พร้อมตั้ง **Settings Profiles & Quotas** คุมทั้ง `max_memory_usage`, `max_execution_time`, `max_rows_to_read` และ `max_concurrent_queries` ต่อ User/Role เพื่อตัด Query นอกลู่นอกทางก่อนทำเครื่อง OOM ส่วนฝั่ง Kafka ก็ตั้ง **Client Quotas (`producer_byte_rate` / `consumer_byte_rate`)** และ ACL แยก Prefix บน S3 ครับ*
  >
  > *2. **ฝั่ง FinOps & Cost Optimization:** ผมบังคับให้ทุก Dashboard และ Pipeline แนบ Metadata Tag ใน ClickHouse `log_comment` แล้วดึงข้อมูลจาก **`system.query_log`** (แบบที่ผมใช้วัด `memory_usage` กับ `read_rows` ใน `benchmark_cdc_engines.py`) มาคำนวณ Cost-per-Team และจับ Top Expensive Queries รายสัปดาห์ ส่วนการลดค่า Cloud ผมใช้ 5 คันโยกใน Omni-Flow คือ Hot/Cold S3 Tiering + TTL (`lab2`), `LowCardinality` + `ZSTD`/`Delta` codecs, Pre-aggregated `SummingMergeTree` MV (ลดจาก 300k เหลือ 120 แถว), และ **Query Cache** (ลดเวลาจาก 95 ms เหลือ 2.3 ms) ครับ"*

* **🇬🇧 English:**
  > *"To prevent a single runaway query or noisy neighbor from starving critical ingestion, I enforce multi-tenant isolation across all three layers:*
  >
  > *1. **Compute & I/O Isolation:** In ClickHouse, I separate dedicated **Write/Merge Replicas** from **BI and Ad-hoc Read Replicas**, and attach strict **Settings Profiles and Quotas** (`max_memory_usage`, `max_execution_time`, `max_rows_to_read`, and concurrency caps) so runaway queries are killed before triggering OOM or `Too many parts`. On Kafka, I enforce **client byte-rate quotas** (`producer_byte_rate`, `consumer_byte_rate`) and ACLs, and isolate S3 by team prefixes with automated lifecycle rules.*
  >
  > *2. **FinOps & Cost Attribution:** Every dashboard and pipeline tags queries via ClickHouse `log_comment`. We aggregate `system.query_log` metrics—`read_bytes`, `memory_usage`, and `query_duration_ms`, just as I instrumented in `benchmark_cdc_engines.py`—to attribute cost per team/pipeline and surface top offenders weekly. To drive bills down, we combine Hot/Cold S3 Parquet tiering with TTLs (`lab2`), `LowCardinality`/`ZSTD`/`Delta` compression, `SummingMergeTree` MVs, and Query Cache (cutting our heavy query cost from 300k rows / 95 ms to 5 rows / 2.3 ms)."*
