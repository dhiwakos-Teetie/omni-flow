# Q02: Scale 100M Rows — `argMax` OOM Bottleneck vs `VersionedCollapsingMergeTree` vs `Delta MV`

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab3-debezium-cdc`](../../lab3-debezium-cdc)
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab3-debezium-cdc/benchmark_cdc_engines.py`](../../lab3-debezium-cdc/benchmark_cdc_engines.py) — สคริปต์ Benchmark จริงบน ClickHouse 24.8 (300k CDC rows + Out-of-order + Kafka 2x Replay + Query Cache)
>   * [`lab3-debezium-cdc/init-postgres.sql`](../../lab3-debezium-cdc/init-postgres.sql#L1-L13) — Schema `public.orders` ต้นทาง (จุดที่ต้องเพิ่มคอลัมน์ `row_version` หากเลือกใช้ `VersionedCollapsingMergeTree`)
> * **เรื่องที่ต้องดูจาก Lab นี้:** คอขวด RAM ของ `argMax + GROUP BY order_id`, เหตุผลที่ `PROJECTION` และ Naive MV ใช้ไม่ได้, เทียบ `VersionedCollapsingMergeTree` (เร็วจัดแต่เบิ้ลเมื่อ Kafka Replay) กับ `Delta MV -> SummingMergeTree` และผล Query Cache

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"ข้อ 4 ใช้ `argMax + GROUP BY order_id` แล้วได้ผลเป๊ะเลย แต่ถ้าตาราง `orders_replica` มีข้อมูลเป็นร้อยล้านแถว และ Dashboard ต้องคิวรีทุกวินาที คิวรีแบบ `GROUP BY` ตรงๆ บนตารางดิบจะมีคอขวดตรงไหน และใน ClickHouse จะออกแบบ View หรือ Materialized View / Projection ยังไงให้เร็วขึ้น?"*  
> *(คำถามต่อยอด: แล้วใช้ `VersionedCollapsingMergeTree` แทนเลยดีกว่าไหม?)*

---

## 🧩 แยกทบทวนเป็นข้อย่อย

### ข้อ 2.1 — คอขวดจริงของ `argMax + GROUP BY order_id` บน 100 ล้านแถว
1. **High-Cardinality Hash Table Explosion:** `order_id` มีถึง 100 ล้านคีย์ การทำ `GROUP BY order_id` พร้อม `argMax` หลายคอลัมน์ (โดยเฉพาะ `String` อย่าง `customer_name`) กิน RAM มหาศาล เสี่ยง `MEMORY_LIMIT_EXCEEDED` หรือต้อง Spill ลงดิสก์
2. **Normal `VIEW` = แค่ SQL Macro (ไม่มีการเก็บผลลัพธ์):** ทุกวินาทีที่ Dashboard รีเฟรช ClickHouse ต้องทำ **Full-Table Scan 100 ล้านแถว + คำนวณ `argMax` ซ้ำใหม่ทั้งหมด**

### ข้อ 2.2 — 2 กับดักที่คนสัมภาษณ์วางล่อไว้ (ห้ามหลงกลใช้เด็ดขาด!)
* **❌ กับดักที่ 1 — `PROJECTION` บน `ReplacingMergeTree`:**
  * Projection ทำงานระดับราย Part และ **ไม่รองรับการ Deduplicate ข้าม Parts** (ถูก bypass เมื่อ query ใช้ `FINAL`) ถ้าสร้าง Projection รวมยอดขาย ยอดจะเบิ้ลตามจำนวน `UPDATE`
* **❌ กับดักที่ 2 — Incremental `MATERIALIZED VIEW` จาก `orders_replica` เข้า `SummingMergeTree` ตรงๆ:**
  * Materialized View ใน ClickHouse เป็นแค่ **Insert Trigger** (มองเห็นเฉพาะแถวที่เพิ่ง Insert เข้ามา ไม่เห็นแถวเก่าบนดิสก์) ถ้ายิง `UPDATE` (`10,000 -> 12,000`) เข้า `SummingMergeTree` ตรงๆ ยอดจะบวกกลายเป็น **`22,000` พังทันที!**

### ข้อ 2.3 — ตัวเลข Benchmark จริงบน ClickHouse 24.8 (`benchmark_cdc_engines.py`)
> ชุดทดสอบจริงใน [`benchmark_cdc_engines.py`](../../lab3-debezium-cdc/benchmark_cdc_engines.py): **300,000 CDC Rows** (200k Inserts + 80k Updates + 20k Deletes) = **180,000 Active Orders, ยอดรวม `661,000,000.00 THB` ตรงกันทั้ง 4 แบบ** (ขณะแช่แข็ง Merge ด้วย `SYSTEM STOP MERGES`):

| สถาปัตยกรรม / วิธี Query | Server Time | Rows Read | Peak RAM | ข้อสรุปจากผลวัดจริง |
| :--- | :---: | :---: | :---: | :--- |
| **1A. `ReplacingMergeTree` + `argMax(GROUP BY order_id)`** | `63.0 ms` | `300,000` | **`114.91 MB`** | **กิน RAM สูงสุด!** แค่ 200k orders ยังใช้ 115 MB ถ้า 100M orders = OOM |
| **1B. `ReplacingMergeTree` + `FINAL`** | `9.0 ms` | `339,040` | `2.51 MB` | Merge-on-Read ตาม `ORDER BY (order_id)` ประหยัด RAM กว่า `argMax` ~45x |
| **1C. `VersionedCollapsingMergeTree(sign, row_version)`** | `8.0 ms` | `380,000` | **`0.05 MB`** | กวาดตัวเลขด้วย `sum(sign)` ไม่ต้อง `GROUP BY order_id` **RAM แทบเป็นศูนย์!** |
| **1D. Delta (`ENGINE=Null`) ➔ MV ➔ `SummingMergeTree`** | **`3.0 ms`** | **`120`** | **`0.06 MB`** | **เร็วที่สุด!** อ่านแค่ 120 แถวที่ Pre-aggregate ไว้แล้ว |

* **ผล Edge Cases ที่วัดจริงเพิ่มเติม:**
  * **Test 2 (Out-of-Order `-1` มาก่อน `+1` บน `VersionedCollapsingMergeTree`):** หักล้างได้ `0` และหลัง `OPTIMIZE FINAL` เหลือ `0` แถวบนดิสก์
  * **Test 3 (Kafka 2x Replay — At-Least-Once Duplicate Trap):** `VersionedCollapsingMergeTree` ยอดเบิ้ลเป็น **`24,000.00 THB` ❌** ส่วน `ReplacingMergeTree FINAL` ยังได้ **`12,000.00 THB` ✅**
  * **Test 4 (ClickHouse Query Cache `use_query_cache=1, ttl=10`):** Query 1A ลดจาก **`95.44 ms`** (`read_rows=300,000`) เหลือ **`2.32 ms`** (`read_rows=5`)

### ข้อ 2.4 — สรุปการออกแบบ & ข้อแม้ของ `VersionedCollapsingMergeTree`
* **KPI / Aggregate Dashboard:** ใช้ **Signed Delta (`-1/+1`) ➔ `ENGINE = Null` ➔ Incremental MV ➔ `SummingMergeTree`** (เร็ว `3 ms`) หรือ **`VersionedCollapsingMergeTree`** (`0.05 MB RAM`)
* **Operational / Row-level Query:** แยก Partition ด้วยคีย์ที่ไม่เปลี่ยนค่า (`PARTITION BY toYYYYMM(created_at)` + `do_not_merge_across_partitions_select_final = 1`) เพื่อทำ `FINAL` Partition Pruning
* **High-Concurrency Refresh (1 วินาที):** เสริมด้วย **ClickHouse Query Cache** หรือ **Refreshable Materialized View** (ClickHouse 24.x)
* **⚠️ 2 เงื่อนไขที่ต้องครบก่อนเลือก `VersionedCollapsingMergeTree`:**
  1. ตารางต้นทางใน PostgreSQL ต้องมีคอลัมน์ **`row_version`** (บวก +1 ทุกครั้งที่แก้) เพื่อให้รู้ version ก่อนหน้าตอนส่งแถว `-1` (Debezium `source.lsn` บอกแค่ LSN ปัจจุบัน ไม่บอก LSN เก่า)
  2. ฝั่ง Kafka Sink ต้องคุม Deduplication Token / Exactly-Once แน่นหนา เพราะไม่ทน Kafka Replay ซ้ำ

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"ถ้าระดับ 100 ล้านแถวแล้วยิง `argMax + GROUP BY order_id` ผ่าน Normal View ทุกวินาที คอขวดคือ High-Cardinality Hash Table ที่กิน RAM มหาศาลและต้อง Full Scan ซ้ำทุกวินาทีครับ ซึ่งผมลอง Benchmark จริงบน ClickHouse 24.8 ด้วยข้อมูล CDC 300,000 แถวแล้วพบว่า `argMax` กิน Peak RAM ถึง **114.91 MB (63 ms)** ในขณะที่ `VersionedCollapsingMergeTree` กินแค่ **0.05 MB (8 ms)** ส่วน `Delta MV -> SummingMergeTree` อ่านแค่ **120 แถวใน 3 ms** ครับ*
  >
  > *ทั้งนี้ผมจะเลี่ยง **Projection** เพราะไม่ช่วย Deduplicate ข้าม Parts และเลี่ยง MV แบบธรรมดาจาก `orders_replica` เพราะไม่เห็นค่าเก่าจนยอดเบิ้ล แต่จะเลือก `VersionedCollapsingMergeTree` (ถ้าคุม `row_version` ที่ Postgres และ Deduplication Token ฝั่ง Kafka ได้) หรือใช้ **`ReplacingMergeTree` + `REPLICA IDENTITY FULL` แตก Delta `-1/+1` เข้า `SummingMergeTree`** คู่กับ Query Cache (ลดเวลาจาก 95 ms เหลือ 2.3 ms) ครับ"*

* **🇬🇧 English:**
  > *"At 100M rows, running `argMax` with `GROUP BY order_id` every second hits a massive high-cardinality hash-table bottleneck and triggers repeated full scans because a standard `VIEW` is just a macro. In my ClickHouse 24.8 benchmark across 300k CDC rows, `argMax` consumed **114.91 MB peak RAM (63 ms)**, whereas `VersionedCollapsingMergeTree` used **0.05 MB (8 ms)** and a pre-aggregated **`Delta MV -> SummingMergeTree`** read just **120 rows in 3 ms**.*
  >
  > *I avoid ClickHouse Projections (they don't deduplicate across parts under `FINAL`) and naive MVs on `orders_replica` (insert triggers double-count updates). Instead, for KPI dashboards I either use `VersionedCollapsingMergeTree`—provided Postgres tracks `row_version` and Kafka ingestion enforces deduplication tokens against replay doubling—or emit `-1/+1` deltas from Debezium's `REPLICA IDENTITY FULL` into a `SummingMergeTree` MV, layered with Query Cache (which cut our heavy `argMax` latency from 95.4 ms to 2.3 ms)."*
