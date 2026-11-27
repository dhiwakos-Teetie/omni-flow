# Q03: ออกแบบ Logic แตก Delta (`-1 / +1`) ใน Omni-Flow + `insert_deduplication_token` Window

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab3-debezium-cdc`](../../lab3-debezium-cdc) *(เชื่อมโยงกับ [`lab1-flink-stream`](../../lab1-flink-stream) และ [`lab4-orchestration-audit`](../../lab4-orchestration-audit))*
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab3-debezium-cdc/init-postgres.sql`](../../lab3-debezium-cdc/init-postgres.sql#L12) — `ALTER TABLE public.orders REPLICA IDENTITY FULL;` (หัวใจที่ทำให้ได้ทั้ง `before` และ `after`)
>   * [`lab3-debezium-cdc/cdc_sink_worker.py`](../../lab3-debezium-cdc/cdc_sink_worker.py#L92-L115) — จุดแยก `op` (`r`, `c`, `u`, `d`) และดึง `before`/`after` payload
>   * [`lab3-debezium-cdc/benchmark_cdc_engines.py`](../../lab3-debezium-cdc/benchmark_cdc_engines.py#L64-L98) — โครงสร้าง `ENGINE = Null` (`bench_delta_input`) ➔ Incremental MV ➔ `SummingMergeTree`
>   * [`lab4-orchestration-audit/orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py) — แนวคิด Key-Level Reconciliation Audit ปิดท้าย
> * **เรื่องที่ต้องดูจาก Lab นี้:** จุดวาง Logic Fan-out `-1 / +1` ที่ชั้น CDC Consumer, การใช้ `ENGINE = Null` + `deduplicate_blocks_in_dependent_materialized_views = 1` + `insert_deduplication_token` และขอบเขตของ Deduplication Window vs `REPLACE PARTITION`

---
ํ
## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"สมมติเราเลือกทางเลือกสำรอง คือใช้ `ReplacingMergeTree + Partition Pruning` บนตารางหลัก แล้วทำ Incremental Materialized View ส่ง Delta (`-1 / +1`) เข้า `SummingMergeTree` สำหรับหน้าจอ Dashboard — ปัญหาที่ Incremental MV เป็น Insert Trigger และอาจนับซ้ำเวลา Update นั้น จะออกแบบ Logic ตรงไหนให้มันส่ง Delta `-1` กับ `+1` ออกมาได้ถูกต้องจริงๆ? และถ้า Replay ด้วย `insert_deduplication_token` ที่มี Deduplication Window จำกัด จะรับประกันความถูกต้อง 100% ยังไง?"*

---

## 🧩 แยกทบทวนเป็นข้อย่อย

### ข้อ 3.1 — วาง Logic แตก Delta `-1 / +1` ไว้ตรงไหนใน Omni-Flow?
* **ดูโค้ดที่:** [`init-postgres.sql`](../../lab3-debezium-cdc/init-postgres.sql#L12) + [`cdc_sink_worker.py`](../../lab3-debezium-cdc/cdc_sink_worker.py#L92-L115)
* **คำตอบ:**
  * **ไม่ผูก Incremental MV ไปเกาะที่ตาราง `orders_replica` (`ReplacingMergeTree`) โดยตรง** เพราะตารางนั้นเก็บแค่ค่า `after` จึงไม่รู้ค่าเดิม
  * วาง Logic ไว้ที่ **ชั้น CDC Consumer (`cdc_sink_worker.py`)** (หรือชั้น Stream Engine อย่าง Apache Flink ใน `lab1-flink-stream` ที่อ่าน `debezium-json` เป็น Changelog `-U / +U / -D` โดยอัตโนมัติ)
  * เพราะใน `init-postgres.sql` เราเปิด **`REPLICA IDENTITY FULL`** ไว้แล้ว Debezium จึงส่งทั้ง **`payload["before"]`** และ **`payload["after"]`** มาให้ครบใน Event เดียว:
    * **`op in ('r', 'c')` (INSERT):** ส่ง **1 แถว** `(+1, after.category, after.status, after.amount)`
    * **`op == 'u'` (UPDATE):** ส่ง **2 แถวใน Batch เดียวกัน**
      1. หักค่าเก่า: `(-1, before.category, before.status, -before.amount)`
      2. บวกค่าใหม่: `(+1, after.category, after.status, +after.amount)`
    * **`op == 'd'` (DELETE):** ส่ง **1 แถว** `(-1, before.category, before.status, -before.amount)`

### ข้อ 3.2 — ตัวอย่าง Trace 1 Order จาก Omni-Flow (`#101`)
สมมติ Order `#101` ถูก `UPDATE` จาก **`PENDING / 10,000 THB`** ➔ **`PAID / 12,000 THB`**:
1. Debezium ส่ง `before={PENDING, 10000}` และ `after={PAID, 12000}`
2. Consumer ยิง 2 แถวเข้า `orders_delta_input` (`ENGINE = Null` — ไม่เปลืองดิสก์เก็บ raw delta แต่ trigger MV ทันที)
3. Incremental MV อัปเดต `orders_kpi_summing` (`SummingMergeTree`):
   * `PENDING`: `1 + (-1) = 0 orders`, `0 THB`
   * `PAID`: `0 + 1 = 1 order`, **`12,000 THB`** (แม้เปลี่ยน `category` หรือ `status` ข้ามถัง ยอดก็ย้ายถังถูกต้อง 100%!)

### ข้อ 3.3 — คุม Idempotency ด้วย `insert_deduplication_token` + `ENGINE = Null`
* **⚠️ จุดตายของ `ENGINE = Null`:** ตาราง `ENGINE = Null` ไม่ได้เก็บ Block บนดิสก์ จึงไม่จำ Token เอง!
* ต้องเปิด Setting: **`deduplicate_blocks_in_dependent_materialized_views = 1`** ควบคู่กับ `replicated_deduplication_window` (หรือ `non_replicated_deduplication_window` บนโหนดเดี่ยว)
* จากนั้นแนบ `insert_deduplication_token` ที่ผูกกับ `(topic, partition, offset_range)` ไปกับทุก Micro-batch ➔ หาก Worker ตายก่อน Commit Offset แล้ว Kafka Replay ซ้ำ ClickHouse จะ Drop Batch ซ้ำทิ้งที่ปลายทาง `SummingMergeTree` ทันที

### ข้อ 3.4 — เมื่อ Deduplication Window มีขอบเขตจำกัด จะคุมความถูกต้อง 100% ยังไง?
* **ข้อจำกัด:** Deduplication Window จำได้แค่ **N blocks ล่าสุดต่อ Partition** (ค่าเริ่มต้น `100` blocks บน ReplicatedMergeTree) ใช้กัน **Short-term Retry / Consumer Crash** เท่านั้น ไม่จำข้ามเดือน
* **ตาข่ายนิรภัย 2 ชั้น:**
  1. **Historical Backfill (เกิน Window):** ไม่พึ่ง Token แต่ใช้วิธี Rebuild ระดับ Partition (**`REPLACE PARTITION`**) จาก `orders_replica` (ที่มี Partition Pruning `toYYYYMM(created_at)`) หรือจาก Lakehouse Cold Tier (`lab2-minio-lakehouse`)
  2. **Daily Reconciliation Audit (แบบ `lab4-orchestration-audit`):** รัน Job เทียบยอด `sum(total_revenue)` ระหว่าง `orders_kpi_summing` กับ `orders_replica FINAL WHERE is_deleted = 0` รายวัน เพื่อยืนยัน 100% Data Accountancy

---

## 🗣️ สคริปต์พร้อมตอบ (60 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"ผมจะไม่เอา Incremental MV ไปเกาะที่ `orders_replica` โดยตรงครับ แต่เพราะใน Lab 3 ผมเปิด `REPLICA IDENTITY FULL` ไว้ที่ Postgres (`init-postgres.sql`) ตัว Debezium จึงส่งทั้ง `before` และ `after` มาให้ครบ ผมจึงวาง Logic แตก Delta `-1/+1` ไว้ที่ชั้น CDC Consumer (`cdc_sink_worker.py`) แล้ว Fan-out เข้าตาราง `ENGINE = Null` ที่มี MV เกาะลง `SummingMergeTree` — จากการ Benchmark จริงบน ClickHouse 24.8 วิธีนี้ลดเวลาจาก 63 ms (RAM 115 MB) เหลือแค่ **3 ms (อ่าน 120 แถว)** ครับ*
  >
  > *ส่วนเรื่อง Idempotency เพราะ `ENGINE = Null` ไม่จำ Block เอง ผมต้องเปิด `deduplicate_blocks_in_dependent_materialized_views = 1` คู่กับ `insert_deduplication_token` เพื่อกัน Short-term Retry/Crash และเนื่องจาก Deduplication Window ของ ClickHouse มีขอบเขตจำกัด ถ้าต้อง Replay ย้อนหลังไกลๆ จะใช้ `REPLACE PARTITION` คู่กับ Daily Reconciliation Audit เทียบยอดกับ `orders_replica FINAL` เพื่อให้ถูกต้อง 100% ครับ"*

* **🇬🇧 English:**
  > *"I wouldn't attach the Incremental MV directly to `orders_replica`, since it only holds post-update state. Because we enabled `REPLICA IDENTITY FULL` in Postgres (`init-postgres.sql`), Debezium delivers both `before` and `after` payloads. I place the `-1/+1` delta fan-out logic inside the CDC consumer (`cdc_sink_worker.py`)—or Flink's `debezium-json` changelog stream—writing signed deltas into an `ENGINE = Null` intake table that feeds a `SummingMergeTree` MV. In my ClickHouse 24.8 benchmark, this dropped query latency from 63 ms to **3 ms**.*
  >
  > *Since an `ENGINE = Null` table doesn't retain block hashes, we must enable `deduplicate_blocks_in_dependent_materialized_views = 1` with an appropriate deduplication window and attach offset-derived `insert_deduplication_token` values for short-term retry safety. For historical replays beyond that finite window, I rely on partition-level rebuilds (`REPLACE PARTITION`) paired with daily reconciliation audits against `orders_replica FINAL`."*
