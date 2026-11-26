# Q12: Data Observability, Silent Data Corruption Detection & Blameless Post-Mortems

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab4-orchestration-audit`](../../lab4-orchestration-audit) (DQ Gate & Reconciliation) + [`omni-command-center`](../../omni-command-center) (Lag & Latency Percentiles) + [`lab3-debezium-cdc`](../../lab3-debezium-cdc)
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab4-orchestration-audit/orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L176-L288) — DQ Gate ปัจจุบันเช็กแค่แถวต่อแถว (`temp`, `rev < 0`) และนับยอด `Bronze = Silver + Quarantine`
>   * [`omni-command-center/dashboard.py`](../../omni-command-center/dashboard.py#L90-L111) — Telemetry ฝั่ง Consumer Lag และ Freshness
> * **เรื่องที่ต้องดูจากข้อนี้:** เหตุผลที่ `Bronze = Silver + Quarantine` ยังไม่พอจับ Silent Corruption, 5 เสาหลักของ Data Observability, Circuit Breaker ก่อน Publish Gold, วิธีกัน Alert Fatigue และโครงสร้าง Blameless Post-Mortem

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"The scariest failures in data pipelines aren't loud crashes—they're silent data corruption or gradual drift where jobs stay green (`200 OK` / DAG success) while numbers quietly go wrong. Beyond batch-level `Bronze = Silver + Quarantine` counts, how do you detect silent failures, schema/distribution drift, and freshness regressions early, and how do you run blameless post-mortems that turn incidents into permanent guardrails?"*

---

## 🧩 แยกทบทวนเป็น 5 ข้อย่อย

### ข้อ 12.1 — ทำไม `Bronze = Silver + Quarantine` (`Q06`) ถึงยังจับ "Silent Corruption" ไม่ได้?
* **ดูโค้ดที่:** [`lab4-orchestration-audit/orchestrator_app.py`](../../lab4-orchestration-audit/orchestrator_app.py#L176-L288)
* **3 ตัวอย่างที่ DAG ขึ้นเขียว `RECONCILED_OK` 100% แต่ข้อมูลธุรกิจพังยับ:**
  1. **Unit Mismatch / Bug (x100 หรือสลับหน่วย):** แอปส่งยอดเป็น "สตางค์" แทน "บาท" (`10000` กลายเป็น `1000000` — ยังเป็นบวก จึงผ่าน DQ Gate `rev >= 0` ฉลุย และยอด `Bronze = Silver` ตรงกันเป๊ะ!)
  2. **Upstream Silence (0 Rows):** Producer หรือ Webhook ตายเงียบๆ ➔ `Bronze = 0`, `Silver = 0`, `Quarantine = 0` ➔ **`0 == 0 + 0` ขึ้น `RECONCILED_OK`!**
  3. **Gradual Distribution / Null-Rate Drift:** ฟิลด์ `category` หรือ `customer_segment` ที่เคยเป็น `'UNKNOWN'` 1% ค่อยๆ พุ่งเป็น 45% เพราะ Upstream แก้ Mapping โดยไม่แจ้ง ➔ Pipeline เขียวตลอดสัปดาห์แต่รายงานผู้บริหารพัง!

### ข้อ 12.2 — 5 เสาหลักของ Data Observability (ตรวจจับก่อนผู้บริหารเปิดเจอ)
1. **Freshness (ความสด):** เทียบ `now() - max(event_time)` และ `consumer_lag` กับ SLA (เช่น Alert ทันทีถ้าข้อมูลขาดช่วงเกิน 5 นาที ไม่ใช่รอ Job crash)
2. **Volume (ปริมาณข้อมูล):** เทียบ `row_count` ของ Batch/ชั่วโมงนี้กับ **Rolling Baseline (เช่นค่าเฉลี่ย + SD ของวันและชั่วโมงเดียวกันใน 4 สัปดาห์ล่าสุด)** ➔ จับเคส `0 rows` หรือ Drop 80% ได้ทันที
3. **Schema (โครงสร้าง):** ตรวจจับ Column add/drop/type change ก่อนเข้า Bronze/Silver (Schema Registry / Contract Tests)
4. **Distribution & Semantics (การกระจายตัวของค่า — ตัวจับ Silent Corruption!):**
   * รัน Statistical Check บน ClickHouse เทียบกับค่าประวัติศาสตร์ 7–28 วัน (เช่น Null/Unknown rate, `quantile(0.50)`, `quantile(0.99)`, Zero-value ratio, และ Cardinality `uniqExact`):
   ```sql
   -- ตัวอย่าง Drift Detector บน Silver ก่อน Publish ขึ้น Gold
   SELECT
       countIf(category = 'UNKNOWN' OR customer_name = '') / count() AS unknown_rate,
       quantileExact(0.50)(revenue_thb) AS p50_rev,
       quantileExact(0.99)(revenue_thb) AS p99_rev,
       uniqExact(device_id) AS active_devices
   FROM audit_olap.silver_clean_events
   WHERE dag_run_id = 42;
   ```
   หาก `p50_rev` กระโดด 100x หรือ `unknown_rate` กระโดดจาก `0.01` เป็น `0.40` ➔ **Flag Anomaly ทันที!**
5. **End-to-End Lineage:** รู้ทันทีว่าตาราง Silver นี้เลี้ยง Gold Table ไหนและ Dashboard ไหน เพื่อแจ้งเตือนผู้ใช้งานตรงจุด

### ข้อ 12.3 — Circuit Breaker & Canary / Shadow Checks (หยุดก่อนพังถึงหน้าผู้บริหาร)
* **Write-Audit-Publish (WAP) / Circuit Breaker:**
  * อย่ายิงลง `gold_executive_mart` ทันทีที่ `Bronze = Silver + Quarantine` ผ่าน — ให้เขียนลง **Gold Staging** ก่อน แล้วรัน **Distribution & Volume Anomaly Check**
  * ถ้าเข้าเงื่อนไข Critical Drift (เช่น ยอดขายรวมกระโดด > 5x หรือหาย > 80% เมื่อเทียบกับสัปดาห์ก่อน) ➔ **เบรกการ Publish (Hold `gold_v_current` ไว้ที่ Snapshot เดิมที่สะอาด)** + ยิง Alert ให้วิศวกรตรวจสอบ!
* **Canary Synthetic Probes:** ส่งธุรกรรมทดสอบ (Canary Heartbeat Event) ผ่านระบบจริงทุก 1 นาที ➔ ถ้า Canary หายหรือค่าเพี้ยนที่ Gold แปลว่า Pipeline ป่วยแม้ Traffic จริงเงียบ

### ข้อ 12.4 — แก้ปัญหา Alert Fatigue (คนเลิกอ่าน Alert เพราะดังบ่อยเกิน)
1. **แยก Severity ชัดเจน (Page vs Ticket):**
   * 🔴 **PagerDuty / ปลุกตี 3 (Symptom-based):** เฉพาะปัญหาที่กระทบ SLA ผู้ใช้งาน/รายได้จริง (เช่น Gold Freshness ขาด > 15 นาที, Fraud Alert Pipeline หยุด, Reconciliation Discrepancy เงินหาย)
   * 🟡 **Slack / Daily Digest (Warning):** เช่น Minor Cardinality shift, Consumer lag พุ่งชั่วคราวแต่กำลังลดลง
2. **Dynamic / Seasonality-Aware Thresholds:** ห้ามตั้ง Fixed Threshold แบบ `rows < 1000` เพราะ Traffic ตี 3 กับเที่ยงวันไม่เท่ากัน — ให้เทียบกับ **ชั่วโมงเดียวกันของวันเดียวกันในสัปดาห์ก่อน (Day-over-Week)** และใช้ `N consecutive breaches` (เช่น ผิดปกติติดกัน 2–3 รอบ) เพื่อตัด Noise

### ข้อ 12.5 — Blameless Post-Mortem ที่เปลี่ยนอุบัติเหตุเป็น Guardrail ถาวร
* **หลักการ:** *"Blame systems and missing guardrails, never people"* (คนทำพลาดได้เสมอ ถ้าระบบปล่อยให้พลาดง่าย แปลว่า Guardrail ของแพลตฟอร์มยังไม่ดีพอ)
* **โครงสร้าง Post-Mortem 5 ส่วน (ทำภายใน 48 ชม. หลังแก้จบ):**
  1. **Timeline & Detection Gap:** เกิดอะไรขึ้น, ใครตรวจเจอ (ถ้า Business User เจอก่อน Monitoring ของทีม = ต้องแก้ Observability ด่วน!)
  2. **Customer / Business Impact:** ตัวเลขเพี้ยนกี่ชั่วโมง กระทบ Dashboard/ลูกค้ากี่ราย
  3. **Systemic Root Cause (5 Whys):** ไล่หาช่องโหว่เชิงระบบ ไม่จบแค่ *"เพราะ Dev ลืมเช็ก"*
  4. **What Went Well:** เช่น Quarantine/Snapshot Rollback ช่วยให้กู้คืนได้ใน 5 นาที
  5. **Permanent Guardrails (Action Items ที่มี Owner + Due Date):**
     * เปลี่ยนความผิดพลาดให้เป็น **Automated Gate** ถาวร เช่น เพิ่ม **CI Schema Gate (`FULL_TRANSITIVE`)**, เพิ่ม **Distribution Circuit Breaker** ก่อน Publish Gold, เพิ่ม **ClickHouse Quota** (`Q11`), หรือทำ **Automated Backfill Runbook** (`Q10`)

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"ใน Lab 4 แม้สมการ `Bronze = Silver + Quarantine` จะการันตีว่าแถวไม่หาย แต่ยังจับ **Silent Corruption** ไม่ได้ครับ เช่น เคสแอปส่งยอดเป็นสตางค์แทนบาท (100x), ต้นทางส่ง 0 แถว (`0 = 0 + 0`), หรือ `UNKNOWN` rate พุ่งจาก 1% เป็น 40% ซึ่ง DAG จะยังขึ้นเขียวอยู่*
  >
  > *วิธีแก้ผมจะวาง **5 เสาหลักของ Data Observability** ครอบไว้ครับ ได้แก่ **Freshness**, **Volume** (เทียบ Rolling Baseline ตามช่วงเวลา/วันเดียวกันของสัปดาห์), **Schema**, **Distribution** (เช็ก Null/Unknown rate, p50/p99, Cardinality บน Silver ก่อนขึ้น Gold), และ **Lineage** คู่กับแพทเทิร์น **Write-Audit-Publish (Circuit Breaker)** — ถ้าเจอยอดเพี้ยนรุนแรง ให้เบรกการสลับขึ้น `gold_executive_mart` ไว้ก่อนไม่ให้ผู้บริหารเห็นตัวเลขพัง และใช้ Dynamic Threshold + แยก Page เฉพาะ Symptom ที่กระทบ SLA เพื่อกัน Alert Fatigue*
  >
  > *ส่วนเวลาเกิด Incident ผมจัด **Blameless Post-Mortem** ที่เน้นแก้ที่ระบบและ Guardrail ไม่โทษคนครับ — วิเคราะห์ 5 Whys ว่าทำไม Monitoring จับไม่ได้ก่อน User แล้วแปลงเป็น Action Items ที่มี Owner ชัดเจน เช่น เพิ่ม Distribution Check ใน WAP Gate, ล็อก CI Schema Registry, หรือตั้ง Resource Quota เพื่อไม่ให้ปัญหาเดิมเกิดซ้ำได้อีกครับ"*

* **🇬🇧 English:**
  > *"Even when `Bronze = Silver + Quarantine` holds in Lab 4, pipelines can still suffer silent corruption—like an upstream 100x unit bug, silent zero-row ingestion (`0 = 0 + 0`), or a gradual `UNKNOWN` rate drift from 1% to 40% while every job reports green.*
  >
  > *To catch these early, I layer the **five pillars of Data Observability**—Freshness, Volume (compared against day-of-week seasonal baselines), Schema, Distribution (null/unknown rates, `p50/p99` shifts, and cardinality on Silver), and Lineage—enforced via a **Write-Audit-Publish circuit breaker** that holds Gold publication if distribution checks trip. To prevent alert fatigue, we page only on customer/SLA-impacting symptoms using seasonality-aware thresholds, routing minor anomalies to Slack digests.*
  >
  > *When an incident happens, we run a **blameless post-mortem** focused on systemic gaps rather than human error—specifically asking why our guardrails didn't catch it before users did—and turn root causes into permanent automated protections like CI schema compatibility gates, distribution circuit breakers, or ClickHouse resource quotas."*
