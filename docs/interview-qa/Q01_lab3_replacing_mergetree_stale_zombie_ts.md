# Q01: ClickHouse `ReplacingMergeTree(version_ts, is_deleted)` — Stale Read, Zombie Row Trap & Same-`version_ts` Collision

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab3-debezium-cdc`](../../lab3-debezium-cdc)
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab3-debezium-cdc/cdc_sink_worker.py`](../../lab3-debezium-cdc/cdc_sink_worker.py#L23-L35) — นิยามตาราง `orders_replica` ด้วย `ENGINE = ReplacingMergeTree(version_ts, is_deleted) ORDER BY (order_id)`
>   * [`lab3-debezium-cdc/cdc_sink_worker.py`](../../lab3-debezium-cdc/cdc_sink_worker.py#L93) — จุดที่โค้ด Lab ใช้ `ts_ms` เป็น `version_ts`
>   * [`lab3-debezium-cdc/replacing_mergetree_lab.py`](../../lab3-debezium-cdc/replacing_mergetree_lab.py) — Interactive Web Simulator (พอร์ต `8086`) จำลอง 3 สถานการณ์
>   * [`lab3-debezium-cdc/verify_clickhouse_real.py`](../../lab3-debezium-cdc/verify_clickhouse_real.py) — สคริปต์พิสูจน์จริงบน ClickHouse 24.8 ด้วย `SYSTEM STOP MERGES`
> * **เรื่องที่ต้องดูจาก Lab นี้:** พฤติกรรม Merge-on-Read (`FINAL` / `argMax`), กับดัก Zombie Row เมื่อกรอง `WHERE is_deleted = 0` ก่อน Deduplicate, ความต่างระหว่าง `OPTIMIZE FINAL` กับ `FINAL CLEANUP` และวิธีแก้ `version_ts` ชนกันด้วย `wal_lsn` + `kafka_offset`

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"I see you used `ReplacingMergeTree(version_ts, is_deleted)` in ClickHouse for CDC from PostgreSQL WAL via Debezium. In ClickHouse, `ReplacingMergeTree` only deduplicates asynchronously during background part merges—it doesn't guarantee immediate deduplication on `SELECT`. If an analytical dashboard queries `orders_replica` right after an `UPDATE` or soft-delete (`is_deleted = 1`), how do you prevent stale or deleted rows from showing up, and what happens if two updates land with the exact same `version_ts`?"*

---

## 🧩 แยกทบทวนเป็นข้อย่อย

### ข้อ 1.1 — ป้องกัน Stale / Duplicate Rows ก่อน Background Merge ยังไง?
* **ดูโค้ดที่:** [`cdc_sink_worker.py`](../../lab3-debezium-cdc/cdc_sink_worker.py#L23-L35), [`verify_clickhouse_real.py`](../../lab3-debezium-cdc/verify_clickhouse_real.py) (Scenario 1)
* **คำตอบ:**
  1. **ห้ามให้ Dashboard ยิง `SELECT` ตรงเข้า `orders_replica`** (จากการทดลองจริงใน `verify_clickhouse_real.py` ขณะมี 4 Unmerged Parts การยิง Raw `SELECT` คืนข้อมูลซ้ำครบทั้ง 4 แถว!)
  2. **ห้ามสั่ง `OPTIMIZE TABLE ... FINAL` ใน Production** เพื่อหวังให้ข้อมูลสด เพราะบังคับ Disk I/O Merge ทั้งตารางจนระบบช้า
  3. **ให้ครอบด้วย View แทน (เลือกตาม Workload):**
     * **`FINAL` View (Merge-on-Read):**
       ```sql
       SELECT * FROM cdc_olap.orders_replica FINAL WHERE is_deleted = 0;
       ```
     * **`argMax` Aggregation View:**
       ```sql
       SELECT
           order_id,
           argMax(order_status, (wal_lsn, kafka_offset)) AS order_status,
           argMax(amount, (wal_lsn, kafka_offset))       AS amount,
           argMax(is_deleted, (wal_lsn, kafka_offset))   AS is_deleted
       FROM cdc_olap.orders_replica
       GROUP BY order_id
       HAVING is_deleted = 0;
       ```

### ข้อ 1.2 — กับดักสำคัญตอน Soft-Delete: "Zombie Row Trap" คืออะไร?
* **ดูโค้ดที่:** [`verify_clickhouse_real.py`](../../lab3-debezium-cdc/verify_clickhouse_real.py) (Scenario 2) และ [`replacing_mergetree_lab.py`](../../lab3-debezium-cdc/replacing_mergetree_lab.py) (`zombie_trap`)
* **คำตอบ:**
  * **ห้ามกรอง `WHERE is_deleted = 0` ก่อน Deduplicate เด็ดขาด!**
  * ถ้าเขียน `SELECT ... FROM orders_replica WHERE is_deleted = 0 GROUP BY order_id` ตัวแถว Tombstone (`is_deleted = 1`) จะถูก `WHERE` ตัดทิ้งก่อน merge ➔ ทำให้แถวเวอร์ชันเก่า (`SHIPPED`, `is_deleted = 0`) **"ฟื้นคืนชีพ" (Zombie Row)** กลับเข้ามาใน Dashboard ทันที (พิสูจน์แล้วบน ClickHouse 24.8 ว่า Order `#101` ที่ถูกลบแล้วโผล่กลับมาเป็น `SHIPPED` จริง!)
  * ต้อง Deduplicate ให้เสร็จก่อน แล้วค่อยตัด Tombstone ออกด้วย **`HAVING is_deleted = 0`** หรือ **`FINAL WHERE is_deleted = 0`** เท่านั้น

### ข้อ 1.3 — เกร็ดจริงบน ClickHouse 24.8: `OPTIMIZE FINAL` vs `FINAL CLEANUP`
* **ดูโค้ดที่:** [`verify_clickhouse_real.py`](../../lab3-debezium-cdc/verify_clickhouse_real.py) (Scenario 4a & 4b)
* **สิ่งที่พิสูจน์ได้จริงบนเครื่อง:**
  * **4a (`OPTIMIZE TABLE ... FINAL`):** ยุบเหลือ `1` Part และลบเวอร์ชันเก่าทิ้งแล้ว **แต่ ClickHouse 24.8 ยังคงเก็บแถว Tombstone `(101, 'DELETED', 1)` ไว้บนดิสก์ตามค่าเริ่มต้น!**
  * **4b (`OPTIMIZE TABLE ... FINAL CLEANUP`):** ต้องเปิด Table-level Setting `SETTINGS allow_experimental_replacing_merge_with_cleanup = 1` ตอน `CREATE TABLE` ถึงจะลบแถว Tombstone ออกจากดิสก์จริง ➔ ตอกย้ำว่า Dashboard **ต้องอ่านผ่าน View ที่กรอง `is_deleted = 0` เสมอ แม้หลัง Merge แล้วก็ตาม**

### ข้อ 1.4 — จะเกิดอะไรขึ้นถ้า 2 Updates มี `version_ts` เท่ากันเป๊ะ?
* **ดูโค้ดที่:** [`cdc_sink_worker.py`](../../lab3-debezium-cdc/cdc_sink_worker.py#L93) และ [`verify_clickhouse_real.py`](../../lab3-debezium-cdc/verify_clickhouse_real.py) (Scenario 3)
* **คำตอบ:**
  * เมื่อ `ORDER BY (order_id)` และ `version_ts` เท่ากันเป๊ะ ClickHouse จะเก็บ **แถวที่ถูกเขียน/Merge ทีหลังสุดตามลำดับกายภาพ**
  * **ความเสี่ยง:** ถ้าระบบยิง `UPDATE` 2 ครั้งใน millisecond เดียวกัน หรือเกิด Retry / Out-of-order replay ผลลัพธ์จะ **Nondeterministic** (พิสูจน์แล้วใน Scenario 3 ที่ `PAID` เก่าที่ถูก retry ทีหลังมาทับ `CANCELLED` ใหม่)
  * **วิธีแก้ตาม DDIA บทที่ 8 & 11:** ยอมรับตรงๆ ว่าใน `cdc_sink_worker.py` ใช้ `ts_ms` (Wall-clock) เพื่อให้ง่ายต่อการสาธิต แต่บน Production ต้องเปลี่ยนเป็น **PostgreSQL WAL LSN (`payload["source"]["lsn"]`)** ซึ่งเป็น 64-bit Monotonic Sequence หรือใช้คู่ **`(wal_lsn, kafka_offset)`** เพื่อการันตีลำดับแบบ Deterministic 100%

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"คำถามนี้ตรงจุดมากครับ ใน `orders_replica` ผมจะไม่ให้ Dashboard ยิง Query เข้าตารางดิบตรงๆ และจะไม่สั่ง `OPTIMIZE FINAL` ใน Prod เพราะกิน I/O หนัก แต่จะครอบด้วย View โดยใช้ `FINAL WHERE is_deleted = 0` เพื่อให้ ClickHouse ทำ Merge-on-Read ณ ตอนอ่าน หรือใช้ `argMax` View ที่จบด้วย `HAVING is_deleted = 0` ครับ จุดตายที่ต้องระวังคือ **ห้ามใส่ `WHERE is_deleted = 0` ก่อน Deduplicate** เด็ดขาด ไม่งั้น Tombstone จะถูกตัดทิ้งแล้วแถวเก่าจะฟื้นเป็น Zombie Row (และจากการเทสจริงบน ClickHouse 24.8 แม้รัน `OPTIMIZE FINAL` แล้ว ClickHouse ก็ยังเก็บ Tombstone ไว้จนกว่าจะใช้ `FINAL CLEANUP` ครับ)*
  >
  > *ส่วนกรณี 2 Updates ที่ `version_ts` ชนกันเป๊ะ ClickHouse จะหยิบแถวที่เขียนทีหลังตามลำดับกายภาพซึ่งไม่แน่นอนเวลาเกิด Retry ใน Lab ผมใช้ `ts_ms` เพื่อสาธิต แต่บน Production ตาม DDIA บทที่ 8 ผมจะเปลี่ยนไปใช้ **PostgreSQL WAL LSN (`source.lsn`)** หรือคู่ **`(wal_lsn, kafka_offset)`** แทน Wall-Clock เพื่อการันตีลำดับแบบ Deterministic 100% ครับ"*

* **🇬🇧 English:**
  > *"Great catch. On `orders_replica`, I never expose the raw table directly to dashboards and I avoid `OPTIMIZE TABLE FINAL` in production due to disk I/O. Instead, I serve queries through a View using either `FINAL WHERE is_deleted = 0` (merge-on-read) or `argMax(..., version)` with `GROUP BY order_id HAVING is_deleted = 0`. The critical gotcha is never filtering `WHERE is_deleted = 0` before deduplication—otherwise you drop the tombstone and resurrect stale 'zombie rows.' In fact, my tests on ClickHouse 24.8 showed that even after `OPTIMIZE FINAL`, tombstones stay on disk unless `FINAL CLEANUP` is enabled.*
  >
  > *If two updates share the exact same `version_ts`, ClickHouse keeps whichever row lands last in physical write/merge order—which becomes nondeterministic under sub-millisecond bursts or retries. While my lab uses Debezium's `ts_ms` for simplicity, in production (per DDIA Ch. 8) I replace wall-clock timestamps with PostgreSQL's strictly monotonic **WAL LSN (`source.lsn`)** paired with **Kafka offset** for 100% deterministic ordering."*
