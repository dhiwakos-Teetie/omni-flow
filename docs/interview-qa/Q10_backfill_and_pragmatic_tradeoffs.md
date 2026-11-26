# Q10: 6-Month Non-Disruptive Backfill & Pragmatic Engineering Trade-offs (Final Boss)

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** ครอบคลุมทั้ง [`lab2-minio-lakehouse`](../../lab2-minio-lakehouse) (Cold S3 Source สำหรับ Backfill) + [`lab1-flink-stream`](../../lab1-flink-stream) (Live Stream) + [`lab4-orchestration-audit`](../../lab4-orchestration-audit) (Batch Medallion & Audit)
> * **ไฟล์ที่ต้องเปิดดูคู่กัน:**
>   * [`lab2-minio-lakehouse/LAKEHOUSE_TESTING_GUIDE.md`](../../lab2-minio-lakehouse/LAKEHOUSE_TESTING_GUIDE.md#L62-L92) — การอ่าน Historical S3 Parquet โดยไม่แตะ OLTP
>   * [`docs/INTERVIEW_SCRIPT_THAI_ENG.md`](../INTERVIEW_SCRIPT_THAI_ENG.md#L101-L117) — หลักคิด *"Right Tool for the Right Job"*
> * **เรื่องที่ต้องดูจากข้อนี้:** เหตุผลที่ห้ามยิง Backfill เข้า Live Kafka Topic, แผน Backfill 6 เดือนแบบ Isolated + Cutover + Atomic Swap, และตารางตัดสินใจ Latency vs Consistency vs Cost vs Ops Complexity

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"In a real-world data platform combining streaming and historical lakehouse/warehouse layers, your streaming pipeline gives low-latency signals while batch/lakehouse jobs handle heavy aggregations, backfills, or reconciliations. Suppose you need to backfill six months of historical events after a logic or schema fix, or you have to justify why you didn't over-engineer every table for sub-second streaming—how do you design idempotent, non-disruptive historical backfills alongside live streaming, and how do you make pragmatic trade-offs between latency, consistency, cost, and operational complexity?"*

---

## 🧩 แยกทบทวนเป็น 6 ข้อย่อย

### ข้อ 10.1 — ทำไม "ห้าม" ยิงข้อมูล Backfill 6 เดือนเข้า Live Kafka Topic ตรงๆ?
ถ้าเผลออัดข้อมูล 6 เดือนเข้า Live Topic ที่มี Flink / ClickHouse เกาะอยู่ จะเกิด **4 หายนะพร้อมกัน:**
1. **Resource Starvation:** Consumer Lag พุ่งเป็นร้อยล้านแถว ➔ Live Fraud Alert (`lab1`) ที่ต้องออกใน 1 วิ กลายเป็นดีเลย์หลายชั่วโมง
2. **Watermark & Window พัง (`lab1-flink-stream`):** ข้อมูลเก่าย้อนหลัง 6 เดือนจะต่ำกว่า Current Watermark ของ Live Stream ➔ **โดน Flink Drop ทิ้งเกลี้ยง!**
3. **ClickHouse `Too many parts` & Disk Thrashing:** การเขียนข้อมูลกระจายข้าม 180 วันพร้อมกัน จะเปิด Partition นับร้อยพร้อมกันจนกิน RAM/Disk I/O จนระบบล่ม
4. **Side-Effect ระเบิด:** Downstream ที่ส่ง Email / Push Notification จะยิงแจ้งเตือนย้อนหลัง 6 เดือนใส่ลูกค้า!

### ข้อ 10.2 — ออกแบบ 6-Month Backfill ขนานกับ Live Streaming แบบ Idempotent & Non-Disruptive (5 ขั้นตอน)
1. **อ่านต้นทางจาก Immutable Cold Storage (`lab2` S3 Parquet / Iceberg):** ห้ามไปรบกวน Production PostgreSQL OLTP เด็ดขาด
2. **แยกท่อประมวลผล (Isolated Compute + Unified Codebase):**
   * ใช้ Logic v2 เดียวกันแต่รันใน **Flink Batch Execution Mode (`execution.runtime-mode = BATCH`)** หรือ Spark/ClickHouse Backfill Pool ที่ **แยก Queue/Resource ออกจาก Live Streaming 100%** (ไม่แย่ง CPU/RAM กับ Live)
3. **Chunk ทีละ Partition (`event_date` / รายสัปดาห์) ลง Staging Table:**
   * แบ่ง Backfill 180 วันเป็น Job ย่อยทีละวัน/สัปดาห์ เขียนลงตาราง Staging (`mart_v2_staging`) ➔ คุมหน่วยความจำได้, พังวันไหนรันซ้ำเฉพาะวันนั้นได้ (**Idempotent per partition!**)
4. **กำหนดจุดตัด Cutover (`cutoff_ts`) ชัดเจน:**
   * **ก่อน `cutoff_ts`:** ใช้ผลจาก Isolated Backfill (Logic v2)
   * **ตั้งแต่ `cutoff_ts` เป็นต้นไป:** Deploy Live Stream v2 ควบคู่ (Dual-run ช่วงสั้นๆ) แล้วให้ Live Stream v2 รับช่วงต่อ — ช่วงทับซ้อน 5–10 นาทีตัดซ้ำด้วย Deterministic Key (`event_id` / `ReplacingMergeTree`)
5. **Shadow Audit & Atomic Cutover:**
   * รัน Reconciliation (`lab4` — Two-Tier `bitXor` + Metric Diff) ตรวจว่ายอด `v2` ต่างจาก `v1` เฉพาะจุดที่ตั้งใจแก้ ➔ แล้วสลับขึ้นใช้งานจริงด้วย **`ALTER TABLE ... REPLACE PARTITION`** ทีละเดือน หรือ **Atomic View Swap (`CREATE OR REPLACE VIEW`)** ➔ Zero Downtime และ Rollback กลับได้ใน 1 วินาที!

### ข้อ 10.3 — กรณี Backfill ที่มีการ "เปลี่ยน Schema" ด้วย (v1 ➔ v2) ทำยังไง?
* ใช้ **Blue-Green Table + View Swap** คู่กับ **Expand & Contract** (จาก [`Q07`](Q07_lab5_protobuf_avro_wire_evolution.md)):
  * ปล่อย `table_v1` ให้ Dashboard เก่าทำงานไปตามปกติระหว่างที่ Backfill 6 เดือนลง `table_v2` (Schema ใหม่)
  * เมื่อ `table_v2` ครบและผ่าน Audit ➔ สลับ Serving View ชี้ไป `table_v2` ➔ เก็บ `table_v1` ไว้เป็น Rollback Safety Net 7 วันค่อย Drop

### ข้อ 10.4 — ทำไมไม่ Over-Engineer ทุกตารางเป็น Sub-Second Streaming? (4 แกนตัดสินใจระดับ Principal)
> **กฎทอง:** *"Latency ไม่ได้ฟรี — ทุกครั้งที่บีบ Latency จาก รายวัน ➔ รายนาที ➔ เสี้ยววินาที ต้นทุน Infra และความซับซ้อนในการดูแล (Ops Complexity) จะพุ่งแบบทวีคูณ!"*

1. **Business SLA & ROI ของความสด:**
   * **Fraud Detection (`lab1`):** ช้า 5 วิ = เงินหายจริง ➔ **คุ้มค่ากับ Flink Sub-second!**
   * **Executive / Financial Mart (`lab4`):** ผู้บริหารเปิดดูวันละ 2 ครั้งตอนเช้า/เย็น ➔ ทำ **Batch รายชั่วโมง/รายวัน** ถูกกว่า 10 เท่า, ง่ายต่อการ Audit ครบ 100%, และ Backfill ง่ายกว่ามาก
2. **Completeness vs Speed (DDIA บทที่ 10 vs 11):**
   * Streaming ต้องรับมือ Watermark, Late Data, State Recovery, Out-of-order — ถ้า Business รอได้ 15 นาที–1 วัน Batch จะให้ความถูกต้องครบถ้วน (Completeness) ด้วยโค้ดที่ซับซ้อนน้อยกว่าเยอะ
3. **Storage & Compute Cost (`lab2` Tiering):**
   * ข้อมูลสด 7–30 วันอยู่บน ClickHouse SSD (Hot) ที่เหลือ 6 เดือน–5 ปีลง S3 Parquet (Cold) ไม่ต้องจ่ายค่า SSD แพงๆ สำหรับข้อมูลที่นานๆ Query ที
4. **Team Cognitive Load (Maintainability — DDIA บทที่ 1):**
   * สถาปัตยกรรมที่ดีที่สุดไม่ใช่ระบบที่ใส่เทคโนโลยีหรูสุด แต่คือ **ระบบที่เรียบง่ายที่สุดที่ยังตอบโจทย์ SLA ได้ และทีมสามารถตื่นมาแก้ตอนตี 3 ได้โดยไม่พัง!**

### ข้อ 10.5 — ตาราง Decision Matrix (จับคู่ Use Case กับ Stack ใน Omni-Flow)

| Use Case / SLA | เลือกใช้ชั้นไหนใน Omni-Flow | เหตุผลเชิง Trade-off |
| :--- | :--- | :--- |
| **Fraud Alert / IoT Anomaly (< 1–5s)** | Kafka + Flink (`lab1`) | มูลค่าธุรกิจขึ้นกับความเร็ว ➔ ยอมจ่าย Ops Complexity ของ Stateful Streaming |
| **Operational Dashboard (วินาที–นาที)** | Debezium CDC + ClickHouse (`lab3`) | อ่านยอดรวมเร็ว (`Delta MV` 3ms / `Query Cache`) แต่แยก OLTP Primary ไว้ให้ Read-Your-Writes |
| **Executive Mart / Financial Audit (15m–1d)** | Medallion Batch DAG + DQ Gate (`lab4`) | ต้องการ 100% Completeness, Quarantine, Audit ง่าย และ Reprocess ได้สบาย |
| **Historical 6m–5y / Backfill / ML** | MinIO S3 Parquet Lakehouse (`lab2`) | ต้นทุน Storage ต่ำสุด, Immutable Source สำหรับ Backfill โดยไม่กวน OLTP |

### ข้อ 10.6 — สรุปภาพรวมคัมภีร์ 10 ข้อ (`Q01`–`Q10`)
1. Storage & ClickHouse Internals ([`Q01`](Q01_lab3_replacing_mergetree_stale_zombie_ts.md), [`Q02`](Q02_lab3_100m_scale_argmax_collapsing_delta_mv.md), [`Q03`](Q03_lab3_delta_fanout_and_dedup_token_window.md))
2. Stateful Stream Processing ([`Q04`](Q04_lab1_flink_late_data_watermark_smurfing.md))
3. Lakehouse, Atomicity & Schema Evolution ([`Q05`](Q05_lab2_minio_lakehouse_crash_and_schema_evolution.md), [`Q07`](Q07_lab5_protobuf_avro_wire_evolution.md))
4. Medallion Reprocessing & Large-Scale Audit ([`Q06`](Q06_lab4_medallion_reprocess_and_large_scale_audit.md))
5. Surge Incident Triage & Recovery ([`Q08`](Q08_command_center_surge_triage_backpressure_recovery.md))
6. Distributed Systems Theory — Consistency, Outbox, Clocks & Fencing ([`Q09`](Q09_ddia_consistency_outbox_monotonic_fencing.md))
7. Historical Backfill & Pragmatic Trade-offs (**`Q10` — ข้อนี้**)

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"สำหรับการ Backfill ย้อนหลัง 6 เดือนควบคู่กับ Live Streaming กฎข้อแรกของผมคือ **ห้ามอัดข้อมูล 6 เดือนเข้า Live Kafka Topic ตรงๆ เด็ดขาด** ครับ เพราะจะทำให้ Lag พุ่งจน Live Fraud Alert ช้า, ทำ Flink Watermark พังจนโดน Drop, และถล่ม ClickHouse จนติด `Too many parts`*
  >
  > *ผมจะแยกเป็น **Isolated Backfill Pipeline** โดยอ่านจาก **Immutable Cold S3 Parquet (`lab2`)** ไม่ไปกวน Postgres OLTP ➔ รัน Logic v2 เดียวกันใน Flink Batch Mode (หรือ Backfill Pool แยกต่างหาก) ➔ แบ่ง Chunk เขียนทีละวันลง Staging Table (`table_v2_staging`) เพื่อให้ Idempotent พังวันไหนรันซ้ำได้เฉพาะวันนั้น ➔ กำหนดจุดตัด `cutoff_ts` ชัดเจนให้ Live Stream v2 รับช่วงต่อ ➔ ตรวจ Shadow Audit (`lab4`) เทียบผลให้ชัวร์แล้วค่อยทำ **Atomic Swap ด้วย `REPLACE PARTITION` หรือ View Swap** แบบ Zero-Downtime ครับ*
  >
  > *ส่วนเหตุผลที่ผมไม่ Over-engineer ทุกตารางเป็น Sub-second Streaming เพราะ **Latency ไม่ได้ฟรีครับ** — ผมตัดสินใจจาก 4 แกนคือ **Business SLA, Completeness, Cost และ Team Maintainability** อะไรที่ช้าแล้วเสียเงินจริงอย่าง Fraud Detection (`lab1`) ถึงจะยอมจ่ายความซับซ้อนของ Flink ส่วนรายงานผู้บริหารหรือบัญชี (`lab4`) ที่ดูเป็นรอบและต้องการความครบถ้วน 100% การใช้ Batch Medallion + Cold S3 Lakehouse (`lab2`) ถูกกว่า เสถียรกว่า และดูแลตอนตี 3 ง่ายกว่ามากครับ"*

* **🇬🇧 English:**
  > *"For a six-month historical backfill alongside live streaming, my first rule is **never replay six months into the live Kafka topic**—that explodes consumer lag for live alerts, breaks Flink watermarks, and triggers 'too many parts' in ClickHouse.*
  >
  > *Instead, I run an **isolated backfill pipeline** reading from our immutable **S3 Parquet cold layer (`lab2`)** so we never touch the OLTP Postgres primary. I execute the same v2 logic in Flink Batch mode (or a separate resource pool), writing day-by-day chunks idempotently into a staging table (`table_v2_staging`). We define a crisp `cutoff_ts` where the live v2 stream takes over (deduplicating the brief overlap by `event_id`), validate diffs via reconciliation (`lab4`), and perform an **atomic `REPLACE PARTITION` or View swap** for zero-downtime cutover and instant rollback.*
  >
  > *As for why I don't over-engineer every table for sub-second streaming: **low latency isn't free**. I balance business SLA, completeness, cloud cost, and team maintainability. Fraud detection (`lab1`) justifies Flink's operational complexity because seconds cost real money; executive marts and financial audits (`lab4`) prioritize 100% completeness and easy reprocessing, where hourly/daily Medallion batch backed by cold S3 Parquet (`lab2`) is far cheaper, simpler, and safer to operate at 3 AM."*
