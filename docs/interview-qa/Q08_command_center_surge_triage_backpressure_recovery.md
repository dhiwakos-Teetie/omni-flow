# Q08: `omni-command-center` + End-to-End Pipeline — Traffic Surge Triage, Safe Backpressure & Coordinated Recovery

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`omni-command-center`](../../omni-command-center) *(เชื่อมโยงครบทั้ง [`lab1-flink-stream`](../../lab1-flink-stream), [`lab2-minio-lakehouse`](../../lab2-minio-lakehouse), [`lab3-debezium-cdc`](../../lab3-debezium-cdc), และ [`lab4-orchestration-audit`](../../lab4-orchestration-audit))*
> * **หมายเหตุความซื่อสัตย์ต่อโค้ด:** ใน Repo จริงแต่ละ Lab แยกรันคนละ Docker Compose เพื่อประหยัด RAM โดยมี `omni-command-center` เป็น Control Plane สำหรับ Kafka + ClickHouse — คำตอบนี้เชื่อมทั้ง Telemetry ที่มีจริงในโค้ด และ Playbook ระดับ Production เมื่อนำทุกชั้นมาต่อร่วมกัน
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`omni-command-center/dashboard.py`](../../omni-command-center/dashboard.py#L90-L111) — ตัวแปร Telemetry หลัก (`producer_tps`, `consumer_tps`, `consumer_lag`, per-partition `leader`/`isr`/`lag`/`pct`, CPU/RAM)
>   * [`omni-command-center/consumer.py`](../../omni-command-center/consumer.py#L120-L123) — State ใน Python RAM (`window_state`, `device_cep_streak`)
>   * [`omni-command-center/consumer.py`](../../omni-command-center/consumer.py#L181-L213) — การ Pause/Resume ช่วง `NETWORK_GAP` และ Manual Offset Commit
>   * [`omni-command-center/consumer.py`](../../omni-command-center/consumer.py#L295-L319) — Micro-batch flush (`>= 200` rows / `0.12s`)
>   * [`omni-command-center/producer.py`](../../omni-command-center/producer.py#L26) และ [L123–L132](../../omni-command-center/producer.py#L123-L132) — `acks=all`, `enable.idempotence=True`, และจุดอ่อน `replication_factor=1`
> * **เรื่องที่ต้องดูจาก Lab นี้:** การไล่หาคอขวดแบบ Downstream-to-Upstream, การใช้ Kafka เป็น Shock Absorber, Flink Unaligned Checkpoints, Load Shedding แบบ Priority Tiering, และลำดับ Recovery 4 เฟส

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"In `omni-command-center`, you bring together your streaming, CDC, lakehouse, and orchestration/audit layers into a single operational control plane. Suppose a real-time surge hits the system—causing Kafka consumer lag to spike, Flink checkpoints or ClickHouse merges to slow down, and a downstream dashboard or audit check to report stale or inconsistent metrics. How do you pinpoint where the bottleneck actually lives across the pipeline, apply backpressure or load shedding safely without dropping critical data, and coordinate recovery across components so the system stabilizes cleanly?"*

---

## 🧩 แยกทบทวนเป็น 6 ข้อย่อย

### ข้อ 8.1 — ขั้นที่ 1: ไล่หาคอขวดตัวจริงยังไง? (กฎทอง: "ไล่ย้อนทวนน้ำ — Downstream ➔ Upstream")
* **หลักคิด:** อย่าเพิ่งโทษ Kafka หรือรีบเพิ่ม Consumer! เพราะถ้า **ปลายน้ำ (ClickHouse)** ช้า มันจะดัน Backpressure ย้อนขึ้นไปทำให้ Flink Checkpoint ช้า และทำให้ Kafka Consumer Lag พุ่งเป็นโดมิโน
* **ไล่ตรวจ 4 ชั้นตามลำดับ (จากปลายน้ำย้อนกลับต้นน้ำ):**
  1. **ชั้น ClickHouse (ปลายน้ำสุด):**
     * เช็ก `system.parts` (ถ้า Active Parts ต่อ Partition พุ่ง >300–1,000 แปลว่าติด **`Too many parts`** จาก Batch เล็กเกินไป)
     * เช็ก `system.merges` / Disk I/O / `system.processes` (ดูว่ามี Query หนักๆ อย่าง `FINAL` / `argMax` จาก Dashboard มาแย่ง CPU/RAM กับฝั่ง Write ไหม)
  2. **ชั้น Apache Flink (`lab1-flink-stream` — กลางน้ำ):**
     * ดู **Backpressure (`outPoolUsage` vs `inPoolUsage`)** บน Flink UI (`:8081`):
       * ถ้า Sink Operator ติด `outPoolUsage = 100%` ➔ คอขวดอยู่ที่ **ClickHouse / Kafka Sink** (ไม่ใช่ Flink)
       * ถ้า `inPoolUsage` สูง + Checkpoint Alignment Duration ช้า/Timeout + RocksDB Disk I/O พุ่ง ➔ คอขวดอยู่ที่ **State โตหรือ Data Skew ใน Flink**
  3. **ชั้น Kafka & Partition Skew ([`dashboard.py`](../../omni-command-center/dashboard.py#L99-L103)):**
     * ดู Per-Partition `lag`, `pct`, `leader`, `isr`:
       * **1 Partition Lag พุ่งอยู่ตัวเดียว** (เช่น `P0 = 90%`) ➔ **Hot Key / Partition Skew!** (เพิ่ม Consumer ก็ไม่ช่วย เพราะ 1 Partition อ่านได้แค่ 1 Consumer)
       * **ทุก Partition Lag พุ่งพร้อมกัน + `isr < RF`** ➔ Broker ล่ม / Disk เต็ม / Consumer ช้ากว่า `producer_tps`
  4. **ชั้น Audit & Dashboard (`p95/p99` & `source_audit_kappa`):**
     * แยกให้ขาดว่าข้อมูลค้าง (Stale) เพราะ **Lag ยังไม่ถึง ClickHouse** (`ingested_at - sent_at` สูง) หรือถึงแล้วแต่ **Merge / MV / DAG ยังรันไม่ทัน**

### ข้อ 8.2 — ขั้นที่ 2: ใช้ Backpressure อย่างไรให้ระบบรอด โดย "ไม่ทิ้งข้อมูลสำคัญ (Zero Critical Data Loss)"
1. **ใช้ Kafka เป็น Shock Absorber (ดิสก์ถูกและทนทานกว่า RAM!):**
   * ยอมให้ **Kafka Consumer Lag โตขึ้นชั่วคราวอย่างควบคุมได้** แทนที่จะยิงถล่ม ClickHouse จน `Too many parts` หรือ OOM
   * ฝั่ง Consumer ([`consumer.py`](../../omni-command-center/consumer.py#L295)): **ขยาย Batch Size ให้ใหญ่ขึ้น + Flush ห่างขึ้น** (เช่นจาก `200` แถว/`0.12s` ➔ `10,000–50,000` แถว/`1–3s`) เพื่อลดจำนวน Parts ใน ClickHouse ลง 50–100 เท่า และตั้ง `max.poll.interval.ms` ให้พอเพื่อไม่ให้ Consumer หลุด Group จนเกิด Rebalance Storm
2. **Flink Backpressure Survival (`lab1-flink-stream`):**
   * เปิด **Unaligned Checkpoints** (`execution.checkpointing.unaligned.enabled = true`) ให้ Checkpoint Barrier แซงคิวผ่าน Buffer ที่ติด Backpressure ได้ ไม่โดน Checkpoint Timeout
   * ใช้ **Incremental Checkpoints** (RocksDB) ลดเวลาเขียน State
3. **ClickHouse Write Protection & Workload Isolation:**
   * เปิด `async_insert = 1` + `wait_for_async_insert = 1` (รวม Mini-batches ฝั่ง Server แต่ยังการันตี Durability)
   * แยก Quota / Resource Limits ฝั่ง Read (`max_concurrent_queries`, `max_memory_usage`) ไม่ให้ Dashboard Query มาฆ่า Background Merge และ Ingestion

### ข้อ 8.3 — ขั้นที่ 3: Load Shedding อย่างปลอดภัย (ลดโหลดอะไรได้ vs อะไรห้ามทิ้งเด็ดขาด!)
แบ่งงานเป็น **3 ระดับความสำคัญ (Priority Tiering):**
* 🔴 **Tier 0 — ห้าม Drop / ห้ามหยุดเด็ดขาด:**
  * Kafka Event Ingestion (`acks=all`), Debezium WAL CDC (`lab3`), Payment/Fraud Alert (`lab1`), และ Bronze Raw Archive (`lab2`/`lab4`) — ข้อมูลดิบต้องปลอดภัย 100%
* 🟡 **Tier 1 — ชะลอความถี่ได้ชั่วคราว (Defer, Don't Drop):**
  * ขยาย **ClickHouse Query Cache TTL** บน Dashboard (เช่น `5s` ➔ `30–60s`) หรือลดความถี่ Auto-refresh เพื่อคืน CPU/RAM ให้ ClickHouse Merge
  * หยุดพัก Medallion Batch DAG (`lab4`) และ Cold S3 Compaction (`lab2`) ชั่วคราว (ข้อมูลดิบอยู่ใน Bronze/Kafka ครบแล้ว ค่อยรันเก็บตกทีหลัง)
* 🟢 **Tier 2 — Graceful Degradation (ตัดโหลดหนักชั่วคราว):**
  * ปิด Ad-hoc Heavy BI Queries (`FINAL` / `argMax` บนตารางใหญ่) เสิร์ฟเฉพาะ Pre-aggregated KPI (`SummingMergeTree`) พร้อมขึ้น **Freshness Banner ("ข้อมูลดีเลย์ X นาที")** เพื่อไม่ให้ผู้บริหารตัดสินใจจากตัวเลขไม่ครบ!

### ข้อ 8.4 — แผลในโค้ดจริงของ `omni-command-center` ที่ต้องยอมรับตรงๆ
* **ดูโค้ดที่:** [`consumer.py`](../../omni-command-center/consumer.py#L120-L123), [L295](../../omni-command-center/consumer.py#L295), และ [`producer.py`](../../omni-command-center/producer.py#L26)
1. `consumer.py` L295 flush ทุก `200` แถว / `0.12s` ➔ ตอน Surge หนักๆ จะสร้าง Part ถี่เกินไปใน ClickHouse
2. `window_state` และ `device_cep_streak` เก็บใน **Python RAM** ➔ ถ้าเกิด Rebalance หรือ Worker ล่มกลาง Surge ค่า 5s Window ใน RAM จะหาย (ต้องใช้ Flink RocksDB Checkpointing ใน Lab 1)
3. `producer.py` L26 สร้าง Topic ด้วย `replication_factor=1` ➔ บน 3-Broker Production Cluster ต้องเป็น `RF=3` และ `min.insync.replicas=2`

### ข้อ 8.5 — ขั้นที่ 4: ลำดับ Coordinate Recovery 4 เฟส (กัน Thundering Herd!)
**ห้ามเปิดทุกบริการพร้อมกันตูมเดียว** — ให้ฟื้นทีละชั้นตามลำดับนี้:
1. **Phase 1 — ฟื้น Storage (`ClickHouse`) ก่อนเพื่อน:**
   * กันโหลด Read หนักๆ ไว้ก่อน ปล่อยให้ Background Merges เคลียร์ `system.parts` ลงสู่ระดับปกติและ Disk/CPU นิ่ง
2. **Phase 2 — Controlled Lag Drain (`Kafka` ➔ `Flink` / `CDC`):**
   * ขยาย Consumer Replicas (สูงสุดเท่าจำนวน Partition — ถ้าติด Hot Key ใช้ **Key Salting** เช่น `device_id + rand%N` ตาม DDIA บทที่ 6)
   * ปล่อย Consumer & Flink ค่อยๆ ระบาย Lag ด้วย Micro-batch ใหญ่ พร้อมมอนิเตอร์ **Lag Burn-down Rate**, **Checkpoint Duration**, และ `p99` จน Lag กลับเป็น `0`
3. **Phase 3 — Catch-up Batch & Audit (`lab2` + `lab4`):**
   * เปิด Medallion DAG (`lab4`) และ Cold Compaction (`lab2`) เก็บตกงานค้าง แล้วรัน **Reconciliation Audit** (`Bronze = Silver + Quarantine` + `source_audit_kappa` vs `sensor_events_kappa`) จนขึ้น **`RECONCILED_OK`**
4. **Phase 4 — ปลดล็อก Full Dashboard & Post-mortem:**
   * ปลด Stale Banner + เปิด Ad-hoc BI กลับปกติ + ปรับ Alert Threshold / Auto-scaling Policy

### ข้อ 8.6 — ตาราง Runbook กวาดตา 5 วินาที (อาการ ➔ ต้นตอ ➔ แก้ทันที)

| อาการที่เห็นบน Control Plane | ต้นตอที่เป็นไปได้มากสุด | มาตรการแก้ทันที |
| :--- | :--- | :--- |
| Kafka Lag พุ่ง **ทุก Partition** + ClickHouse `system.parts` พุ่งสูง | ClickHouse `Too many parts` / Merge ไม่ทัน | ขยาย Batch Size (`10k–50k`), เปิด `async_insert`, พัก Heavy BI Read ชั่วคราว |
| Kafka Lag พุ่ง **แค่ 1 Partition (`pct = 90%`)** | **Hot Key / Partition Skew** | Key Salting (`key + salt`) หรือแยก Hot Key ออกไปอีก Topic |
| Flink Checkpoint Timeout แต่ ClickHouse ปกติ | State โต / Backpressure ขวาง Barrier | เปิด **Unaligned Checkpoints** + Incremental RocksDB + ตรวจ Skew |
| Lag = 0 แต่ Dashboard & Audit ฟ้องไม่ตรง | Merge/MV ค้าง หรือ DAG Batch ยังไม่จบ | ตรวจ `FINAL` View, รัน Two-Tier `bitXor` Audit, และขึ้น Freshness Banner |

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"ขอออกตัวตรงๆ ก่อนครับว่าใน Repo Omni-Flow ผมแยกรันแต่ละ Lab คนละ Docker เพื่อประหยัด RAM โดยมี `omni-command-center` เป็น Control Plane วัด `producer_tps`, `consumer_tps`, per-partition `lag`/`isr`, และ `p50/p95/p99` — แต่ถ้าต้องรับมือ Traffic Surge บนระบบ Production ที่ต่อครบทั้งชุด ผมใช้แนวทาง 3 ขั้นครับ:*
  >
  > *1. **หาคอขวดแบบไล่ย้อนทวนน้ำ (Downstream ➔ Upstream):** ผมจะเช็ก **ClickHouse** ก่อน (`system.parts`, `system.merges`, `system.processes`) ว่าติด `Too many parts` หรือโดน Dashboard Query แย่ง CPU ไหม ➔ แล้วย้อนมาดู **Flink Backpressure** (`outPoolUsage` vs `inPoolUsage` + Checkpoint alignment) ➔ แล้วค่อยดู **Kafka Per-Partition Lag** ใน Control Plane เพื่อแยกว่าเป็น Hot Key Skew (1 partition พุ่ง) หรือ Consumer ช้าทั้งกลุ่ม*
  >
  > *2. **Backpressure & Safe Load Shedding โดยไม่ทิ้งข้อมูลสำคัญ:** ผมใช้ **Kafka เป็น Shock Absorber** ปล่อยให้ Lag โตชั่วคราวแทนการถล่ม ClickHouse, ขยาย Micro-batch Size ให้ใหญ่ขึ้น (จาก 200 ใน Lab เป็น 10k–50k แถว) + เปิด Flink **Unaligned Checkpoints** ส่วน Load Shedding จะทำเฉพาะ **Tier ล่าง** — ห้าม Drop Raw Event/CDC/Fraud เด็ดขาด แต่ชะลอ Batch DAG, ขยาย Query Cache TTL, ปิด Ad-hoc BI ชั่วคราว และขึ้น **Freshness Banner** แทนการโชว์เลขเพี้ยนครับ*
  >
  > *3. **Coordinate Recovery 4 เฟส (กัน Thundering Herd):** ฟื้น ClickHouse ให้ Merge นิ่งก่อน ➔ ค่อยๆ Drain Kafka Lag & Flink (พร้อม Key Salting ถ้าติด Hot Key) จน Lag เป็น 0 ➔ รัน Medallion DAG & Reconciliation Audit (`Bronze = Silver + Quarantine`) จนขึ้น `RECONCILED_OK` ➔ แล้วค่อยปลดล็อก Full Dashboard กลับมาปกติครับ"*

* **🇬🇧 English:**
  > *"First, transparently: in my Omni-Flow repo each lab runs independently to conserve local RAM, with `omni-command-center` acting as my Kafka + ClickHouse control plane tracking `producer_tps`, `consumer_tps`, per-partition lag/ISR, and `p50/p95/p99`. Across a unified production pipeline hit by a surge, I follow a structured three-part playbook:*
  >
  > *1. **Pinpoint the bottleneck downstream-to-upstream:** I inspect **ClickHouse first** (`system.parts` for 'too many parts', `system.merges`, and `system.processes` for heavy BI queries starving writes), then check **Flink backpressure** (`outPoolUsage` vs. `inPoolUsage` and checkpoint alignment), and finally inspect **per-partition Kafka lag** to separate single-partition hot-key skew from cluster-wide consumer lag.*
  >
  > *2. **Apply safe backpressure & tiered load shedding:** I treat **Kafka as our durable shock absorber** rather than crashing ClickHouse—increasing consumer micro-batch sizes (from 200 rows in my lab to 10k–50k rows), enabling Flink **unaligned checkpoints**, and isolating write vs. read quotas. I never drop Tier-0 raw events, CDC, or fraud alerts; instead, I shed Tier-1/2 load by raising Query Cache TTL, pausing non-urgent batch compaction/DAGs, throttling ad-hoc `FINAL` queries, and displaying a freshness banner instead of serving inconsistent numbers.*
  >
  > *3. **Coordinated 4-phase recovery:** Stabilize ClickHouse merges first -> drain Kafka/Flink lag in a controlled manner (applying key salting if hot-key skewed) until lag hits zero -> run catch-up Medallion DAG and reconciliation audits (`RECONCILED_OK`) -> and finally lift dashboard throttling."*
