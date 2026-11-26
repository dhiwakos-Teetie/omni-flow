# Q04: Apache Flink — 5s Watermark, 12s Late Data, 10s Checkpoint & Smurfing (`15,000 × 2`)

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab1-flink-stream`](../../lab1-flink-stream) *(เชื่อมโยงแนวคิด Audit กับ [`lab4-orchestration-audit`](../../lab4-orchestration-audit))*
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab1-flink-stream/flink_job.sql`](../../lab1-flink-stream/flink_job.sql#L1) — `execution.checkpointing.interval = '10s'`
>   * [`lab1-flink-stream/flink_job.sql`](../../lab1-flink-stream/flink_job.sql#L11) — `WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND`
>   * [`lab1-flink-stream/flink_job.sql`](../../lab1-flink-stream/flink_job.sql#L36-L48) — `TUMBLE(event_ts, INTERVAL '10' SECOND)`, `GROUP BY category`, และกฎ Fraud `amount >= 25000`
>   * [`lab1-flink-stream/payment_producer.py`](../../lab1-flink-stream/payment_producer.py#L62-L71) — payload ปัจจุบันใช้ `key=category` และยังไม่มี `user_id` / `card_hash`
>   * [`lab1-flink-stream/clickhouse_sink.py`](../../lab1-flink-stream/clickhouse_sink.py#L21-L32) — ตารางปลายทาง `flink_window_summary` ใช้ `ReplacingMergeTree(ingested_at) ORDER BY (window_start, category)`
> * **เรื่องที่ต้องดูจาก Lab นี้:** กลไกปิด Window ด้วย Watermark (`Current Watermark >= window_end`), ความต่างระหว่าง Watermark กับ Checkpoint, วิธีป้องกัน Late Data และวิธีแก้ Smurfing Attack ข้ามขอบ Tumbling Window

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"In `lab1-flink-stream`, you set a 5-second watermark (`event_ts - INTERVAL '5' SECOND`) on a 10-second tumbling window for `raw_payments`, flagging fraud when `amount >= 25,000 THB` with 10-second checkpoints. What happens to a payment event that arrives 12 seconds late due to a mobile network drop—does it get dropped by default, or can it still affect fraud alerts or totals? And how would you handle a user splitting a 30,000 THB fraud attempt into two 15,000 THB transactions right across a 10-second window boundary?"*

---

## 🧩 แยกทบทวนเป็น 6 ข้อย่อย

### ข้อ 4.1 — Watermark 5 วินาที ทำงานยังไง?
* **ดูโค้ดที่:** [`lab1-flink-stream/flink_job.sql`](../../lab1-flink-stream/flink_job.sql#L11) (`WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND`)
* **หลักการ:**
  * สูตร: `Current Watermark = max(event_ts ที่เคยเห็น) - 5 วินาที`
  * Watermark คือนาฬิกาที่บอก Flink ว่า *"ข้อมูลที่เกิดก่อนเวลานี้น่าจะมาครบหมดแล้ว ปิด Window ได้เลย"*
* **จุดต้องระวัง (Bonus):** ในโค้ด Lab 1 ยังไม่ได้ตั้ง `table.exec.source.idle-timeout` — บน Prod ถ้า Kafka Partition ใดเงียบไม่มีข้อมูลเข้า Watermark จะค้างไม่ยอมขยับ

### ข้อ 4.2 — Payment มาสาย 12 วินาที โดน Drop ทิ้งเลยไหม? (ต้องแยก 2 กรณี!)
* **ดูโค้ดที่:** [`lab1-flink-stream/flink_job.sql`](../../lab1-flink-stream/flink_job.sql#L11-L48)
* **กฎตัดสินของ Flink:** Flink **ไม่ได้เช็กว่า** `มาสายเกิน 5 วิ -> Drop` แต่เช็กว่า **`Current Watermark >= window_end` หรือยัง!**
* **ตัวอย่างบน Window `[10:00:00, 10:00:10)`** (Window นี้จะปิดเมื่อมี Event เวลา `10:00:15` เข้ามา เพราะ `WM = 10:00:10`):
  * **กรณี A (เกิดท้าย Window — โดน DROP ❌):** เกิดตอน `10:00:08` ➔ มาสาย 12 วิ (ถึงตอน `10:00:20`, `WM = 10:00:15 >= 10:00:10`) ➔ Window ปิดไปแล้ว ➔ **Flink SQL Drop ทิ้งเงียบๆ ไม่เข้าทั้ง `total_amount` และ `high_risk_count`**
  * **กรณี B (เกิดต้น Window — ยังรอด ✅):** เกิดตอน `10:00:01` ➔ มาสาย 12 วิ (ถึงตอน `10:00:13`, `WM = 10:00:08 < 10:00:10`) ➔ Window ยังไม่ปิด ➔ **ยังไม่โดน Drop และถูกนับรวมตามปกติ**

### ข้อ 4.3 — Checkpoint 10 วินาที เกี่ยวกับการรอ Late Data ไหม?
* **ดูโค้ดที่:** [`lab1-flink-stream/flink_job.sql`](../../lab1-flink-stream/flink_job.sql#L1) (`SET 'execution.checkpointing.interval' = '10s';`)
* **คำตอบสั้นจำง่าย:** **ไม่เกี่ยวกันเลย!**
  * **Checkpoint (10s)** = การ Snapshot State ของ Flink ลง Storage เพื่อ **กู้คืนเวลา TaskManager ล่ม (Fault Tolerance / Recovery)**
  * **Watermark (5s)** = ตัวคุมเวลาปิด Window และตัดสินว่า Event ไหนสายเกินไป (Event-Time Progress)

### ข้อ 4.4 — บน Production จะกันไม่ให้ Late Data ทำ Fraud Alert หรือยอดเงินหายยังไง?
* **ดูโค้ดที่:** [`lab1-flink-stream/flink_job.sql`](../../lab1-flink-stream/flink_job.sql#L44) และ [`lab1-flink-stream/clickhouse_sink.py`](../../lab1-flink-stream/clickhouse_sink.py#L21-L32)
* **วิธีแก้ 3 ชั้น:**
  1. **Fraud รายธุรกรรม (`amount >= 25,000`):** แยกออกจาก Tumbling Window ไปเป็น **Stateless Real-time Filter** ยิง Alert ทันที ➔ ต่อให้มาสาย 12 วิ ก็ไม่โดน Drop Alert
  2. **Windowed Totals:** เปลี่ยนไปใช้ Flink DataStream API ตั้ง **`allowedLateness`** (เช่น 60 วินาที) ให้ Re-emit ยอดอัปเดตเมื่อมีข้อมูลมาสาย ➔ ฝั่งปลายทางใน `clickhouse_sink.py` สร้างตาราง `flink_window_summary` เป็น **`ReplacingMergeTree(ingested_at) ORDER BY (window_start, category)`** ไว้แล้ว จึง Upsert ทับยอด Window เดิมได้ทันที
  3. **สายเกิน `allowedLateness`:** ส่งออก **`sideOutputLateData`** ลง Late-Data Kafka Topic / DLQ แล้วให้ Batch Audit (แบบ Lab 4) เก็บตกยอดบัญชี

### ข้อ 4.5 — ทำไมโจรซอยยอด `15,000 × 2` ข้ามขอบ Window 10 วิ ถึงหลุดจากโค้ด Lab 1? (Smurfing Attack)
* **ดูโค้ดที่:** [`lab1-flink-stream/payment_producer.py`](../../lab1-flink-stream/payment_producer.py#L62-L71) และ [`lab1-flink-stream/flink_job.sql`](../../lab1-flink-stream/flink_job.sql#L36-L48)
* **หลุดเพราะ 2 จุดในโค้ดจริง (ยอมรับตรงๆ ได้เลย):**
  1. **ขอบ Tumbling Window ตายตัว:** `TUMBLE(10s)` ตัดเป็น `[00..10)` กับ `[10..20)` ➔ รูด `15,000` ตอนวินาทีที่ `09` และ `15,000` ตอนวินาทีที่ `11` (ห่างกันแค่ 2 วิ) ถูกแยกอยู่คนละกล่อง ทำให้ยอดแต่ละกล่องไม่ถึง `25,000`
  2. **ยังไม่มี `user_id` Key:** ใน `payment_producer.py` ตอนนี้ใช้ `key=category` และใน `flink_job.sql` ทำ `GROUP BY category` + เช็กยอดเดี่ยว `amount >= 25000` ยังไม่ได้รวมยอดรายคน

### ข้อ 4.6 — วิธีจับ Smurfing Attack (`15,000 × 2`) บน Production (3 ท่าเลือกใช้ตามโจทย์)
* **ดูโค้ดที่:** [`lab1-flink-stream/flink_job.sql`](../../lab1-flink-stream/flink_job.sql#L36-L48)
* **ขั้นเตรียมก่อน (Prerequisite):** เพิ่ม `user_id` / `card_hash` ใน payload และใช้เป็น **Kafka Partition Key** เพื่อให้ทุกธุรกรรมของคนเดียวกันเข้า Flink Subtask เดียวกัน
* **เลือกใช้ 1 ใน 3 ท่านี้:**
  1. **Flink SQL `OVER RANGE` Rolling Window (คมที่สุด — ไม่มีขอบ Window ตายตัว!):**
     ```sql
     SELECT
         user_id,
         tx_id,
         event_ts,
         amount,
         SUM(amount) OVER (
             PARTITION BY user_id
             ORDER BY event_ts
             RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
         ) AS rolling_1m_sum
     FROM raw_payments;
     ```
     คิดยอดย้อนหลัง 1 นาทีนับจาก `event_ts` ของทุก Event ➔ `00:09 (15k)` + `00:11 (15k)` = **`30,000`** จับได้ทันที!
  2. **Sliding / Hopping Window (`HOP`):** เช่น `HOP(event_ts, INTERVAL '2' SECOND, INTERVAL '60' SECOND)` `GROUP BY user_id HAVING SUM(amount) >= 25000` — ข้อเสียคือ State ใหญ่ขึ้นตามความถี่การซ้อนทับ
  3. **Session Window (`SESSION`) หรือ `KeyedProcessFunction`:** เหมาะกับโจรที่ซอยยอดต่อเนื่องหลายนาที หรือกฎซับซ้อน (เช่น ยอดรวม + เปลี่ยนประเทศใน 5 นาที) โดยเก็บ State ใน RocksDB พร้อมตั้ง **State TTL** กัน RAM/Disk เต็ม

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"ใน `flink_job.sql` ของ Lab 1 ตอนนี้ ถ้าธุรกรรมมาสาย 12 วินาทีจน Watermark (`event_ts - 5s`) วิ่งเลย `window_end` ไปแล้ว **Flink SQL จะ Drop ทิ้งโดยค่าเริ่มต้นครับ** ไม่เข้าทั้ง `total_amount` และ `high_risk_count` — ยกเว้นกรณีเกิดต้น Window เช่นวินาทีที่ 1 แล้วมาถึงวินาทีที่ 13 ซึ่ง Watermark เพิ่งอยู่ที่วินาทีที่ 8 ยังไม่เกินขอบ 10 วิ อันนั้นยังรอดครับ ส่วน Checkpoint ทุก 10 วิเป็นเรื่อง Fault Recovery ไม่ใช่การรอ Late Data*
  >
  > *บน Production ผมจะแยก Fraud Alert รายธุรกรรมออกมาเป็น Stateless Filter เพื่อไม่ให้ Late Data ทำให้ Alert หาย ส่วนยอด Windowed Totals ใช้ DataStream API ตั้ง `allowedLateness` (ซึ่งตาราง `flink_window_summary` ใน `clickhouse_sink.py` ผมตั้งเป็น `ReplacingMergeTree` ไว้แล้วจึงรองรับการอัปเดตยอดย้อนหลังได้) คู่กับ `sideOutputLateData` ส่งเข้า DLQ เพื่อทำ Reconciliation*
  >
  > *ส่วนเคสโจรซอยยอด 15,000 × 2 ข้ามขอบ 10 วินาที: ในโค้ด Lab 1 ปัจจุบันจับไม่ได้ครับ เพราะ `TUMBLE(10s)` มีขอบตายตัวและยังไม่ได้ Group by `user_id` — บน Production ผมจะเพิ่ม `user_id`/`card_hash` เป็น Partition Key แล้วใช้ **Flink SQL `OVER` Rolling Range Window** (`RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW`) ที่คิดยอดย้อนหลังนับจาก `event_ts` ของทุก Event โดยไม่มีขอบ Window ตายตัว หรือใช้ **Session Window / `KeyedProcessFunction`** สำหรับกฎ Multi-event ที่ซับซ้อนครับ"*

* **🇬🇧 English:**
  > *"In my current Lab 1 `flink_job.sql`, once the watermark (`event_ts - 5s`) passes `window_end`, a 12-second-late event is **dropped by default**—affecting neither `total_amount` nor `high_risk_count` (unless it occurred early in the window, e.g., second 1 arriving at second 13 when the watermark is only at second 8). And the 10-second checkpointing interval is strictly for state recovery, not lateness tolerance.*
  >
  > *In production, I'd decouple per-transaction fraud alerts (`>= 25,000`) into a stateless filter so late events still trigger alerts, while using `allowedLateness` on the DataStream API—since my `flink_window_summary` sink in `clickhouse_sink.py` already uses `ReplacingMergeTree(ingested_at)` keyed by `(window_start, category)` to absorb late updates—plus `sideOutputLateData` routed to a DLQ for reconciliation.*
  >
  > *For two 15,000 THB split transactions straddling a 10-second tumbling boundary: honestly, my current `TUMBLE(10s)` grouped by `category` misses it due to fixed window boundaries and missing `user_id` keying. In production, I'd partition by `user_id` or `card_hash` and replace the tumbling window with an event-time **`OVER` rolling range window** (`RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW`)—eliminating boundary blind spots—or use a **Session Window / `KeyedProcessFunction`** with RocksDB state for multi-minute structuring patterns."*
