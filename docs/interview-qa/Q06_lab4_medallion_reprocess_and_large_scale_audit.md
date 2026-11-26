# Q06: Medallion DAG Reprocessing & Scaling 50M-Row Key-Level Audit

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab4-orchestration-audit`](../../lab4-orchestration-audit) *(เชื่อมโยงแนวคิด Partition Swap กับ [`lab2-minio-lakehouse`](../../lab2-minio-lakehouse))*
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab4-orchestration-audit/orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L56-L101) — DDL ของ `silver_clean_events` (`ReplacingMergeTree`), `quarantine_bad_rows` (`MergeTree` ธรรมดา ไม่มีสถานะ!), และ `gold_executive_mart` (`ReplacingMergeTree`)
>   * [`lab4-orchestration-audit/orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L141-L185) — Step 1–2: ดึง `WHERE seq_no > last_seq` (ยังไม่มีเพดานบน `batch_max_seq`) และกฎ `rev < 0`
>   * [`lab4-orchestration-audit/orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L254-L288) — Step 3: Key-Level Double `LEFT JOIN` Audit (จุดที่จะ OOM เมื่อ Batch มีหลายสิบล้านแถว)
>   * [`lab4-orchestration-audit/orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L297-L310) — Step 4: Full Recompute `gold_executive_mart` จาก `silver_clean_events FINAL`
> * **เรื่องที่ต้องดูจาก Lab นี้:** Quarantine State Machine (`OPEN -> PROMOTED_TO_SILVER`), `dq_rule_version`, Atomic `REPLACE PARTITION`, กับดัก OOM ของ Hash Join ใน ClickHouse, และ Two-Tier `bitXor(cityHash64(...))` Audit

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"In `lab4-orchestration-audit`, you built a Medallion pipeline (`bronze_raw_events -> silver_clean_events -> gold_executive_mart`) with an Automated Data Quality Gate that routes invalid records (like null IDs or negative amounts) to `quarantine_bad_rows` instead of halting the pipeline, and runs a key-level `LEFT JOIN` audit verifying `Bronze = Silver + Quarantine` in `dag_run_history`. What happens if a business rule changes mid-day—for example, a negative amount becomes valid for refund/chargeback events—and you need to reprocess quarantined or historical rows without corrupting `gold_executive_mart` or breaking `Bronze = Silver + Quarantine` accountancy? Also, how do you scale a key-level `LEFT JOIN` reconciliation audit when a batch has tens of millions of rows?"*

---

## 🧩 แยกทบทวนเป็น 6 ข้อย่อย

### ข้อ 6.1 — ถ้ากฎเปลี่ยนกลางวัน (ยอดติดลบ = Refund ใช้ได้แล้ว) แล้ว Reprocess แถวจาก Quarantine กลับเข้า Silver จะเกิดอะไรขึ้นในโค้ด Lab 4 เดิม?
* **ดูโค้ดที่:** [`orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L72-L101) และ [L297–310](../../lab4-orchestration-audit/orchestrator_app.py#L297-L310)
* **ต้องแยกตอบ 2 ส่วนตามโค้ดจริง:**
  1. **ฝั่ง `gold_executive_mart` (ไม่นับเบิ้ล แต่ต้องระวังความหมาย):** เพราะ Step 4 (L297–310) ทำ **Full Recompute** จาก `silver_clean_events FINAL GROUP BY plant` ลง `ReplacingMergeTree(updated_at) ORDER BY (plant)` — ดังนั้นเมื่อแถว Refund ถูกส่งเข้า `silver_clean_events` ด้วย `(seq_no, event_id)` เดิม ยอด `total_clean_revenue_thb` จะถูกคำนวณใหม่แบบหักยอด Refund ให้อัตโนมัติ *(แต่บน Prod ควรแยกคอลัมน์ `gross_revenue` กับ `refund_amount` ให้ชัดเจน)*
  2. **ฝั่งสมการ `Bronze = Silver + Quarantine` (พังทันทีในโค้ด Lab 4 เดิม! ❌):** เพราะ `quarantine_bad_rows` ใน L84 เป็น **`MergeTree()` ธรรมดาที่ไม่มีคอลัมน์สถานะ** — ถ้าเอาแถว Refund ไปใส่ใน `silver_clean_events` โดยไม่ปิดสถานะใน Quarantine แถว `event_id` เดียวกันจะโผล่ **ทั้ง 2 ตารางพร้อมกัน** ➔ `Silver + Quarantine > Bronze` (นับซ้ำทางบัญชี!)

### ข้อ 6.2 — วิธีออกแบบ Reprocessing บน Production ให้ `Gold` ไม่พัง และ `Bronze = Silver + Quarantine` ยังเป๊ะ 100%
1. **Immutable Bronze + Rule Versioning (`dq_rule_version`):**
   * เก็บ `dq_rule_version` (เช่น `v1`, `v2`) ไว้ใน `silver_clean_events`, `quarantine_bad_rows`, และ `dag_run_history` — Bronze ห้ามแก้ไขเด็ดขาด ใช้เป็น Source of Truth สำหรับ Reprocess เสมอ
2. **ทำ State Machine บน `quarantine_bad_rows` (ไม่ลบประวัติ แต่ปิดเคสได้!):**
   * เปลี่ยน `quarantine_bad_rows` เป็น `ReplacingMergeTree(updated_at) ORDER BY (seq_no, event_id)` + เพิ่มคอลัมน์ `quarantine_status` (`'OPEN'`, `'PROMOTED_TO_SILVER'`, `'PERMANENTLY_REJECTED'`)
   * ตอน Reprocess แถว Refund ที่ผ่านกฎ `v2` แล้ว:
     * ยิงเข้า `silver_clean_events` (`dq_rule_version = 'v2'`)
     * ยิงอัปเดตสถานะใน `quarantine_bad_rows` เป็น **`PROMOTED_TO_SILVER`** (เก็บ Audit Trail ไว้ครบว่าเคยติดกักกันด้วยกฎ `v1`)
   * **สมการ Audit ที่ถูกต้องตลอดกาล:**
     * `Bronze = Silver (Active) + Quarantine (status IN ('OPEN', 'PERMANENTLY_REJECTED'))`
     * และเช็กเงื่อนไขเพิ่มว่า **`Silver ∩ Quarantine(OPEN) = ∅`** (ห้ามมี Key เดียวกันค้างอยู่ทั้ง 2 ฝั่ง!)
3. **Atomic Partition Rebuild บน `gold_executive_mart`:**
   * แทนที่จะ Full Scan `silver_clean_events` ทั้งหมดทุกครั้ง ให้ Partition Silver & Gold ตาม `event_date` ➔ คำนวณเฉพาะวันที่ได้รับผลกระทบลง Staging Table แล้วใช้ **`ALTER TABLE gold_executive_mart REPLACE PARTITION '2026-11-26' FROM gold_staging`** ➔ Dashboard ไม่เห็นข้อมูลครึ่งๆ กลางๆ (Atomic Swap) และไม่กระทบวันอื่น

### ข้อ 6.3 — แผลซ่อนอีก 1 จุดในโค้ด Lab 4 (พูดดักไว้ กรรมการชอบมาก!)
* **ดูโค้ดที่:** [`orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L155) และ [L262](../../lab4-orchestration-audit/orchestrator_app.py#L262)
* ใน Step 1 อ่าน `WHERE seq_no > last_seq` แล้วใน Step 3 Audit อ่าน `WHERE seq_no > last_seq` ซ้ำ **โดยไม่มีเพดานบน (`AND seq_no <= batch_max_seq`)** — ถ้ามีข้อมูลใหม่ไหลเข้า Bronze ระหว่าง Step 2 ➔ Step 3 ตัว Audit จะฟ้อง `missing_in_downstream` ปลอม!
* **วิธีแก้:** ล็อกกรอบ Batch ชัดเจนด้วย **`batch_id`** หรือ **`WHERE seq_no > last_seq AND seq_no <= batch_max_seq`**

### ข้อ 6.4 — ทำไม Key-Level `LEFT JOIN` Audit ใน Lab 4 ถึงพังเมื่อ Batch มีหลายสิบล้านแถว?
* **ดูโค้ดที่:** [`orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L254-L275)
* **3 คอขวดจริงใน ClickHouse เมื่อ Batch = 30–50 ล้านแถว:**
  1. **Hash Join OOM (`MEMORY_LIMIT_EXCEEDED`):** ClickHouse `LEFT JOIN` ค่าเริ่มต้น (`join_algorithm = 'hash'`) จะโหลดฝั่งขวาทั้งหมด (`silver` + `quarantine` กว่า 50 ล้าน `event_id` String!) เข้า **RAM Hash Table** ➔ กิน RAM หลายสิบ GB จนตาย
  2. **Double `FINAL` Penalty:** โค้ดเรียก `bronze_raw_events FINAL` และ `silver_clean_events FINAL` ทั้ง 2 ฝั่งก่อน Join ➔ CPU + RAM คูณสอง
  3. **Shuffle Overhead บน Cluster:** ถ้าทำ Distributed Join ข้าม Shard โดยไม่ Co-locate ตาม `event_id` จะส่งข้อมูลข้าม Network มหาศาล

### ข้อ 6.5 — วิธี Scale Reconciliation Audit สำหรับ Batch ระดับหลายสิบล้านแถว (Two-Tier Audit + ClickHouse Join Tuning)
1. **🏆 ท่าที่ 1 (เร็วสุด & กิน RAM O(1) — ไม่ต้อง `JOIN` ในภาวะปกติ!): Two-Tier Hash Checksum Audit**
   * **Tier 1 (รันทุก Batch 50 ล้านแถว — ใช้ RAM แค่หลัก MB จบในเสี้ยววินาที):**
     * เปรียบเทียบ **4 ค่า Aggregate** ระหว่าง `Bronze` vs `Silver + Quarantine(OPEN)` ของ `batch_id` นั้น (Order-Independent — ไม่ขึ้นกับลำดับแถว!):
       1. `count()` (จำนวนแถว)
       2. `sum(revenue_thb)` (ยอดเงินรวม)
       3. **`bitXor(cityHash64(event_id))`** (XOR Hash ของทุก Key — หายหรือเกินแม้แต่ 1 Key ค่าจะไม่ตรงทันที!)
       4. **`bitXor(cityHash64(event_id, toString(round(revenue_thb, 2))))`** (จับยอดเงินเพี้ยนระดับแถวโดยไม่ต้อง Join!)
     ```sql
     SELECT
         count() AS cnt,
         round(sum(revenue_thb), 2) AS total_rev,
         bitXor(cityHash64(event_id)) AS key_xor_hash,
         bitXor(cityHash64(event_id, toString(round(revenue_thb, 2)))) AS row_xor_hash
     FROM audit_olap.bronze_raw_events
     WHERE batch_id = 1001;
     ```
   * **Tier 2 (Drill-down เฉพาะตอน Tier 1 ไม่ตรง!):**
     * แตก `GROUP BY modulo(cityHash64(event_id), 64) AS bucket_id` เพื่อหาว่า **Bucket ไหน** ที่ XOR ไม่ตรง ➔ แล้วค่อยทำ Key-Level `LEFT ANTI JOIN` **เฉพาะ Bucket ที่มีปัญหา** (ลดขนาด Join จาก 50 ล้านแถว เหลือ < 1 ล้านแถว!)
2. **⚙️ ท่าที่ 2 (ถ้าจำเป็นต้องทำ Full Key-Level Join จริงใน ClickHouse):**
   * เปลี่ยนอัลกอริทึม Join เป็น **`SETTINGS join_algorithm = 'grace_hash'`** (แบ่ง Hash Bucket แล้ว Spill ลงดิสก์อัตโนมัติ ไม่ OOM) หรือ **`join_algorithm = 'full_sorting_merge'`** (ถ้าตารางเรียงตาม `event_id` อยู่แล้ว ทำ Streaming Merge Join กิน RAM ต่ำมาก)

### ข้อ 6.6 — แยก "ข้อมูลพัง (Data Defect)" ออกจาก "กฎธุรกิจ (Business Policy)"
* ในโค้ด Lab 4 ([`orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L181)) เราเหมาว่า `rev < 0` เป็น Poison Row ทันที
* บน Production ควรแยก **Schema/Integrity Defect** (เช่น `event_id IS NULL`, `temp = 999.9C`) ออกจาก **Domain Rule** ด้วยการมี `event_type` (`SALE`, `REFUND`, `CHARGEBACK`) ตั้งแต่ Bronze ➔ `SALE` ห้ามติดลบ แต่ `REFUND` ติดลบได้อย่างถูกต้องตั้งแต่แรก

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"คำถามนี้จี้ตรงจุดอ่อนในโค้ด Lab 4 (`orchestrator_app.py`) ของผมเลยครับ: ฝั่ง `gold_executive_mart` จริงๆ ไม่นับเบิ้ลเพราะผมทำ Full Recompute จาก `silver_clean_events FINAL` แต่ฝั่งสมการ `Bronze = Silver + Quarantine` จะพังทันทีถ้าเอาแถว Refund เข้า Silver เพราะ `quarantine_bad_rows` ใน Lab ผมเป็น `MergeTree()` ธรรมดาที่ไม่มีสถานะ*
  >
  > *บน Production ผมจะแก้ 3 จุดครับ: หนึ่งคือเก็บ Bronze แบบ Immutable และติด `dq_rule_version` (`v1`, `v2`) ไว้ทุกชั้น; สองคือเปลี่ยน `quarantine_bad_rows` เป็น `ReplacingMergeTree` ที่มี **State Machine (`OPEN` ➔ `PROMOTED_TO_SILVER`)** เพื่อเก็บประวัติไว้ครบแต่ไม่นับซ้ำในสมการ (`Bronze = Silver + Quarantine(OPEN)`); และสามคือ Partition `gold_executive_mart` ตามวันแล้วใช้ **`ALTER TABLE ... REPLACE PARTITION`** จาก Staging Table เพื่อ Atomic Swap เฉพาะวันที่โดน Reprocess โดยไม่กระทบ Dashboard*
  >
  > *ส่วนเรื่อง Scale Key-Level `LEFT JOIN` Audit เมื่อ Batch มีหลายสิบล้านแถว: ใน ClickHouse การทำ Hash Join 50 ล้าน String Key จะกิน RAM จน OOM แน่นอนครับ ผมจะเปลี่ยนเป็น **Two-Tier Audit** — รอบปกติ (Tier 1) ไม่ทำ Join เลย แต่ใช้ **`count()`, `sum()`, และ `bitXor(cityHash64(event_id, amount))`** เทียบระหว่าง Bronze กับ Silver+Quarantine ซึ่งกิน RAM ระดับ MB และจับแถวหาย/ยอดเพี้ยนได้ครบทุก Key แล้วค่อยทำ Bucketed Drill-down (`cityHash64(event_id) % 64`) หรือเปิด `join_algorithm = 'grace_hash'` / `'full_sorting_merge'` เฉพาะรอบที่ Tier 1 ฟ้อง Discrepancy เท่านั้นครับ"*

* **🇬🇧 English:**
  > *"You spotted a real limitation in my Lab 4 `orchestrator_app.py`. While `gold_executive_mart` won't double-count because Step 4 recomputes it from `silver_clean_events FINAL`, promoting a quarantined refund row directly to Silver would break `Bronze = Silver + Quarantine` since my lab's `quarantine_bad_rows` is a plain `MergeTree` with no lifecycle state.*
  >
  > *In production, I keep Bronze immutable, version my DQ rules (`dq_rule_version`), and turn `quarantine_bad_rows` into a `ReplacingMergeTree` with a **state machine (`OPEN` -> `PROMOTED_TO_SILVER`)** so audit history stays intact while accountancy cleanly holds `Bronze = Silver + Quarantine(OPEN)`. For Gold, I partition by date and run an atomic **`REPLACE PARTITION`** from a staging table for only the affected dates.*
  >
  > *To scale reconciliation to tens of millions of rows without hitting ClickHouse hash-join OOM, I switch to a **Two-Tier Audit**: Tier 1 avoids joins altogether by comparing `count()`, `sum(revenue)`, and order-independent **`bitXor(cityHash64(event_id, amount))`** across Bronze vs. Silver + Open Quarantine in O(1) memory. Only if Tier 1 detects a mismatch do we trigger Tier 2—bucketing by `cityHash64(event_id) % 64` or using ClickHouse's `grace_hash` / `full_sorting_merge` join to pinpoint the exact missing keys."*
