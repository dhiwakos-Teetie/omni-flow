# Q05: MinIO S3 Parquet Lakehouse — Compaction Crash, Partial Reads & Schema Evolution

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab2-minio-lakehouse`](../../lab2-minio-lakehouse) *(เชื่อมโยงแนวคิด Field-ID Schema Evolution กับ [`lab5-encoding-evolution`](../../lab5-encoding-evolution))*
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab2-minio-lakehouse/lakehouse_sink_worker.py`](../../lab2-minio-lakehouse/lakehouse_sink_worker.py#L16) — พาธถังจริงในโค้ดคือ `http://minio:9000/omni-lakehouse/snapshots` *(หมายเหตุ: ในโค้ดใช้ชื่อถัง `omni-lakehouse` ไม่ใช่ `lakehouse-cold`)*
>   * [`lab2-minio-lakehouse/lakehouse_sink_worker.py`](../../lab2-minio-lakehouse/lakehouse_sink_worker.py#L134-L197) — ลำดับการเขียน 4 ขั้น (`hot_events` ➔ `s3() Parquet` + `s3_truncate_on_insert = 1` ➔ `lakehouse_snapshots` ➔ `consumer.commit()`)
>   * [`lab2-minio-lakehouse/LAKEHOUSE_TESTING_GUIDE.md`](../../lab2-minio-lakehouse/LAKEHOUSE_TESTING_GUIDE.md#L69-L92) — คำสั่ง Query แบบ Wildcard `s3('.../snapshots/*.parquet')` (จุดเสี่ยงอ่าน Orphan File และพังเมื่อ Schema เปลี่ยน)
> * **เรื่องที่ต้องดูจาก Lab นี้:** S3 Atomic PUT vs Multi-Step Orphan File, Wildcard `*.parquet` vs Catalog-driven Reads (Apache Iceberg / Delta Lake), ClickHouse `input_format_parquet_allow_missing_columns` + `schema_inference_mode = 'union'`, และ Field-ID Schema Evolution

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"In `lab2-minio-lakehouse`, you compact older historical data from ClickHouse's `hot_events` table into Snappy-compressed Apache Parquet files on MinIO S3 (`lakehouse-cold`), track snapshots in `lakehouse_snapshots`, and run Time-Travel SQL queries directly via ClickHouse's `s3()` table function. What happens if a background compaction job crashes halfway through writing a Parquet file to MinIO and a concurrent dashboard or Time-Travel query runs—how do you prevent partial/corrupted reads, and how do you handle schema evolution when historical Parquet files on S3 have an older schema than today's `hot_events`?"*

---

## 🧩 แยกทบทวนเป็น 6 ข้อย่อย

### ข้อ 5.1 — ถ้า Job ตายกลางคันตอนกำลังเขียน Parquet ขึ้น MinIO S3 จะเกิดอะไรขึ้น?
* **ดูโค้ดที่:** [`lakehouse_sink_worker.py`](../../lab2-minio-lakehouse/lakehouse_sink_worker.py#L134-L197) และ [`LAKEHOUSE_TESTING_GUIDE.md`](../../lab2-minio-lakehouse/LAKEHOUSE_TESTING_GUIDE.md#L69-L74)
* **ต้องแยกตอบเป็น 2 ระดับ:**
  1. **ระดับไฟล์เดี่ยวบน S3 / MinIO (ไม่เกิดไฟล์แหว่ง/พังกลางคัน):**
     * S3 `PUT Object` และ `Multipart Upload` เป็น **Atomic ระดับ Object** — ถ้า Job ตายก่อน `CompleteMultipartUpload` ตัว MinIO จะ **ไม่แสดงไฟล์ที่เขียนไม่เสร็จให้ Reader เห็น** (จะค้างเป็นแค่ Uncommitted Multipart Part เบื้องหลัง)
  2. **ระดับ Multi-Step Pipeline ในโค้ดจริง Lab 2 (จุดแผลที่เกิดจริง!):**
     * โค้ดเราทำ 4 ขั้นไม่ Atomic: `hot_events` ➔ `S3 Parquet` ➔ `lakehouse_snapshots` ➔ `consumer.commit()`
     * **ถ้าตายหลังเขียน `snap_0005.parquet` เสร็จ แต่ก่อนลง `lakehouse_snapshots`:** จะเกิด **Orphan / Uncommitted Parquet File** วางค้างอยู่บน S3!
     * **ผลกระทบใน Lab 2:** เพราะใน `LAKEHOUSE_TESTING_GUIDE.md` เรายิง Query แบบ **Wildcard `s3('.../snapshots/*.parquet')`** ตัว Dashboard จะเผลอกวาดอ่านไฟล์ที่ยังไม่ Commit เข้าไปนับรวมด้วยทันที (Dirty / Duplicate Read) และเมื่อ Worker รีสตาร์ท Kafka จะ Replay 20 แถวเดิมลง `hot_events` (ซึ่งเป็น `MergeTree()` ธรรมดา) จนแถวเบิ้ลใน Hot Tier ด้วย!

### ข้อ 5.2 — วิธีป้องกัน Partial / Uncommitted / Dirty Reads บน Production (3 กลไก)
1. **Write-Before-Commit + Catalog-Driven Reads (ห้ามสแกน Wildcard `*.parquet` ตรงๆ!):**
   * **เขียนด้วยชื่อไฟล์ Immutable + UUID** (เช่น `snap_0005_<uuid>.parquet` — ห้ามเขียนทับด้วย `s3_truncate_on_insert = 1` เพื่อไม่ให้ทับไฟล์ที่คนอื่นกำลังอ่าน)
   * **Commit ที่ Metadata Catalog เป็นจุดเดียวสุดท้าย:** บันทึกชื่อไฟล์ลง Catalog (`lakehouse_snapshots` หรือ **Apache Iceberg Manifest / Delta `_delta_log`**) หลังจาก S3 `PUT` สำเร็จ 100% แล้วเท่านั้น
   * **ฝั่งอ่าน (Snapshot Isolation):** Reader อ่านเฉพาะรายชื่อไฟล์ที่ Commit แล้วใน Snapshot ของ Catalog เท่านั้น ➔ ต่อให้มี Orphan file วางค้างอยู่บน S3 Reader จะมองไม่เห็นเลย!
2. **Garbage Collection (Vacuum) & S3 Lifecycle:**
   * ตั้ง S3 Lifecycle Rule **`AbortIncompleteMultipartUpload`** (เช่น 1 วัน) ล้างชิ้นส่วนอัปโหลดที่ตายกลางทาง
   * รัน Job **Vacuum** ลบ Orphan Parquet files ที่ไม่มีชื่ออยู่ใน Metadata Catalog และเก่ากว่า Retention window
3. **คุม Idempotency ของ Hot Tier + Eviction ที่ปลอดภัย:**
   * เปลี่ยน `hot_events` จาก `MergeTree()` เป็น `ReplacingMergeTree` (หรือใช้ Deduplication Token) และลบข้อมูลเก่าออกจาก Hot Tier ด้วย Partition TTL **หลังจาก** Snapshot นั้น Commit ลง Cold S3 สำเร็จแล้วเท่านั้น

### ข้อ 5.3 — ทำไม `s3('.../snapshots/*.parquet')` ถึงพังเมื่อ Schema ของ `hot_events` เปลี่ยน?
* **ดูโค้ดที่:** [`lakehouse_sink_worker.py`](../../lab2-minio-lakehouse/lakehouse_sink_worker.py#L160-L172)
* **สาเหตุ:**
  * Parquet เป็น **Self-describing file** (เก็บ Schema ไว้ที่ Footer ของใครของมัน)
  * สมมติไฟล์เก่า `snap_0001.parquet` มี 10 คอลัมน์ (v1) แต่วันนี้ `hot_events` เพิ่มคอลัมน์ใหม่ `currency String` หรือเปลี่ยน Type `voltage` จาก `Float32` ➔ `Float64` (v2)
  * ตอน ClickHouse รัน `s3('.../snapshots/*.parquet', ..., 'Parquet')` โดยค่าเริ่มต้น ClickHouse จะอนุมาน Schema จากไฟล์แรกที่เจอ (v1) ➔ พอ Query เรียก `currency` หรือไปชนไฟล์ v2 ที่มี Type ต่างกัน ➔ **Query พังด้วย `UNKNOWN_IDENTIFIER` หรือ `TYPE_MISMATCH`**

### ข้อ 5.4 — วิธีแก้ Schema Evolution บน ClickHouse `s3()` (Immediate Fix)
* ระบุ **Superset Schema** ชัดเจนตอนอ่าน + เปิด Setting ของ ClickHouse 24.8:
  ```sql
  SELECT snapshot_id, region, revenue_thb, currency
  FROM s3(
      'http://minio:9000/omni-lakehouse/snapshots/*.parquet',
      'minioadmin', 'minioadmin123', 'Parquet',
      'snapshot_id UInt32, region String, revenue_thb Float64, currency Nullable(String)'
  )
  SETTINGS
      input_format_parquet_allow_missing_columns = 1,
      schema_inference_mode = 'union';
  ```
  * `input_format_parquet_allow_missing_columns = 1`: ไฟล์เก่า v1 ที่ไม่มี `currency` จะถูกเติมเป็น `NULL` / Default อัตโนมัติ (Backward Compatible)
  * `schema_inference_mode = 'union'`: รวม Schema จากทุกไฟล์แบบ Union แทนที่จะดูแค่ไฟล์แรก
* **ตอนรวม Hot + Cold (`UNION ALL` View):** กำหนดคอลัมน์ + Type (`CAST`) ให้ตรงกันทั้ง 2 ฝั่ง และใช้ Cutoff Timestamp ชัดเจน (`event_time >= cutoff` จาก Hot + `event_time < cutoff` จาก Cold) เพื่อไม่ให้นับซ้ำ

### ข้อ 5.5 — วิธีแก้ระดับ Enterprise Production: Apache Iceberg (Field-ID Schema Evolution — เชื่อม DDIA บทที่ 4 & Lab 5)
* **ดูโค้ดเชื่อมโยง:** [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py)
* **จุดอ่อนที่ `allow_missing_columns` ยังแก้ไม่ได้:** การ **Rename Column** (`revenue_thb` ➔ `net_revenue_thb`) หรือ **Drop แล้วสร้างคอลัมน์ชื่อเดิมซ้ำ** เพราะ Parquet เปลือยๆ จับคู่ด้วย **ชื่อคอลัมน์ (Name-based)**
* **ทางแก้ระดับ Production (Apache Iceberg / Delta Column Mapping):**
  * Iceberg กำหนด **Immutable Field ID (`1, 2, 3...`)** ให้ทุกคอลัมน์ใน Parquet Metadata (หลักการเดียวกับ Field Tag ใน Protobuf ที่ผ่าดูใน Lab 5 / DDIA บทที่ 4)
  * จึงทำ **Add, Drop, Rename, Reorder, และ Safe Type Widening (`Int32 -> Int64`, `Float32 -> Float64`)** ได้แบบ **Metadata-only (Zero-Rewrite)** ไม่ต้องเขียนไฟล์ Parquet เก่าบน S3 ใหม่เลย
  * ClickHouse 24.8 รองรับ **`icebergS3(...)` / `ENGINE = Iceberg`** อ่านตรงจาก MinIO ได้ครบทั้ง **Atomic Snapshot Isolation + Time-Travel + Field-ID Schema Evolution** ในตัวเดียว

### ข้อ 5.6 — กฎเหล็ก Schema Evolution ที่ทำได้ vs ห้ามทำบน Cold S3 Parquet
* ✅ **ทำได้ปลอดภัย (Metadata-only):** Add optional/nullable column, Type widening (`Int32 -> Int64`, `Float32 -> Float64`), Rename (เมื่อใช้ Iceberg Field ID)
* ❌ **ห้ามทำตรงๆ (Breaking Changes):** Narrowing type (`Float64 -> Int32`), เปลี่ยน `String -> Int`, เปลี่ยนความหมายคอลัมน์เดิม ➔ ต้องใช้แพทเทิร์น **Expand & Contract** (เพิ่มคอลัมน์ใหม่แทน) หรือทำ Controlled Backfill Rewrite เป็น Snapshot ใหม่

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"ขอแยกตอบเป็น 2 ชั้นครับ: ระดับไฟล์เดี่ยวบน MinIO/S3 การทำ `PUT` หรือ `Multipart Upload` เป็น **Atomic ระดับ Object** อยู่แล้วครับ ดังนั้นถ้า Job ตายกลางคันตอนอัปโหลด MinIO จะไม่เปิดให้ Reader เห็นไฟล์ที่เขียนครึ่งๆ กลางๆ (แค่ตั้ง S3 Lifecycle `AbortIncompleteMultipartUpload` กวาดชิ้นส่วนค้างทิ้ง)*
  >
  > *แต่จุดเสี่ยงจริงอยู่ในระดับ Multi-Step ครับ — ในโค้ด Lab 2 (`lakehouse_sink_worker.py`) ผมเขียนลง `hot_events` ➔ เขียน S3 Parquet (`omni-lakehouse/snapshots/snap_XXXX.parquet`) ➔ แล้วค่อยลง Catalog `lakehouse_snapshots` ถ้าตายหลังเขียน S3 แต่ก่อนลง Catalog จะมี **Orphan File** ค้างอยู่ และเพราะใน Lab ใช้ Wildcard `s3('.../snapshots/*.parquet')` Query จะเผลออ่านไฟล์ที่ยังไม่ Commit เข้าไปด้วยครับ — บน Production ผมจึง **ไม่สแกน Wildcard ตรงๆ** แต่เขียนไฟล์ด้วย Immutable UUID แล้ว **Commit ลง Metadata Catalog เป็นจุดเดียวสุดท้าย** (หรือใช้ **Apache Iceberg** บน MinIO + `icebergS3()` ใน ClickHouse) เพื่อให้ Reader อ่านเฉพาะไฟล์ที่ Commit แล้วได้ **Snapshot Isolation 100%** พร้อมรัน Vacuum เก็บ Orphan files*
  >
  > *ส่วนเรื่อง **Schema Evolution** ระหว่าง `hot_events` รุ่นใหม่กับ Parquet รุ่นเก่าบน S3: ระดับเฉพาะหน้าบน ClickHouse `s3()` ผมระบุ Superset Schema และเปิด `input_format_parquet_allow_missing_columns = 1` คู่กับ `schema_inference_mode = 'union'` เพื่อให้ไฟล์เก่าเติมค่า `NULL`/Default ได้โดยไม่พัง แต่สำหรับ Production ที่ต้องรองรับทั้ง Rename/Reorder ด้วย ผมจะใช้ **Apache Iceberg** เพราะผูกคอลัมน์ด้วย **Field ID** (หลักการเดียวกับ Protobuf Field Tag ใน Lab 5 / DDIA บทที่ 4) ทำให้วิวัฒนาการ Schema ได้แบบ Zero-Rewrite ไม่ต้องไปเขียนไฟล์เก่าบน S3 ใหม่ครับ"*

* **🇬🇧 English:**
  > *"I separate the crash scenario into two layers. At the individual object level, S3/MinIO `PUT` and multipart uploads are **atomic**—if a worker crashes mid-upload, readers never see a half-written Parquet file, and an S3 lifecycle rule (`AbortIncompleteMultipartUpload`) cleans up incomplete parts.*
  >
  > *The real risk is multi-step atomicity. In my Lab 2 `lakehouse_sink_worker.py`, we write to `hot_events`, export Parquet to `omni-lakehouse/snapshots/`, and finally register it in `lakehouse_snapshots`. Crashing between step 2 and step 3 leaves an **orphan Parquet file** that a wildcard `s3('.../snapshots/*.parquet')` query would mistakenly read. In production, I eliminate wildcard scans: writers emit immutable UUID-named Parquet files and make the **catalog commit the single atomic visibility point**—ideally using **Apache Iceberg** with ClickHouse's `icebergS3()`—giving readers true snapshot isolation while a scheduled vacuum purges orphan files.*
  >
  > *For schema evolution across older S3 Parquet files and newer `hot_events`, immediately in ClickHouse `s3()` I pass an explicit superset schema with `input_format_parquet_allow_missing_columns = 1` and `schema_inference_mode = 'union'` so legacy files safely default missing columns to `NULL`. For full production evolution—including column renames and safe type widening without rewriting historical files—I rely on **Apache Iceberg's immutable Field IDs**, applying the same principle as Protobuf field tags from DDIA Chapter 4 and my Lab 5."*
