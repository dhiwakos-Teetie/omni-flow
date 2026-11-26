# Q09: DDIA Ch. 5, 7, 8 — Consistency Models, Transactional Outbox, Monotonic Clocks & Fencing Tokens

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab5-encoding-evolution`](../../lab5-encoding-evolution) (DDIA Ch. 5 & Ch. 8 Interactive Simulator) + [`lab3-debezium-cdc`](../../lab3-debezium-cdc) (Transactional Outbox) + [`omni-command-center`](../../omni-command-center)
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L1010-L1067) — จำลองครบทั้ง 3 Replication Lag Anomalies (`READ_OWN_WRITES`, `MONOTONIC_READS`, `CONSISTENT_PREFIX`)
>   * [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L1321-L1395) — จำลอง Clock Skew (`LWW_CLOCK_SKEW`, `SPANNER_TRUETIME`) และ Consensus (`SPLIT_BRAIN_NO_QUORUM`, `RAFT_PAXOS_MAJORITY`)
>   * [`lab3-debezium-cdc/init-postgres.sql`](../../lab3-debezium-cdc/init-postgres.sql#L14-L23) — ตาราง `public.outbox_events` พร้อม `REPLICA IDENTITY FULL`
>   * [`omni-command-center/producer.py`](../../omni-command-center/producer.py#L129) — `enable.idempotence = True` (กลไก Producer Epoch ซึ่งเป็น Fencing Token ในตัวของ Kafka)
> * **เรื่องที่ต้องดูจาก Lab นี้:** วิธีรับประกัน Read-Your-Writes / Monotonic Reads / Consistent Prefix, การป้องกัน Dual-Write Loss ด้วย Transactional Outbox, การแยกใช้ Monotonic Clock vs Time-of-Day Clock, และการกัน Zombie Worker ด้วย Fencing Token

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"Suppose a user updates a profile or order in PostgreSQL and immediately refreshes a downstream view or API backed by async replication/streaming—only to see stale or out-of-order data, or a partial write across two tables when a worker crashes mid-batch. Beyond basic deduplication tokens or `ReplacingMergeTree`, how do you guarantee Read-Your-Writes, Monotonic Reads, and Consistent Prefix across asynchronous read replicas or streaming views, how does a Transactional Outbox prevent dual-write loss when a worker dies mid-batch, and how do you protect shared state against clock drift and stale workers using monotonic clocks and fencing tokens?"*

---

## 🧩 แยกทบทวนเป็น 6 ข้อย่อย

### ข้อ 9.1 — รับประกัน Read-Your-Writes Consistency ข้าม Async Replica / Streaming View ยังไง?
* **ดูโค้ดที่:** [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L1012-L1028) (`READ_OWN_WRITES`)
* **4 กลไกที่ใช้จริง (เลือกตามประเภทหน้าจอ):**
  1. **Route Owner-Editable Reads ไป Primary (Leader) เสมอ:** ข้อมูลที่เจ้าตัวแก้เองได้ (เช่น หน้า Profile หรือ *My Order Detail*) ให้อ่านตรงจาก **PostgreSQL Primary** ส่วนคนอื่นมาดูค่อยอ่าน Replica/Streaming View
  2. **Time-Window Sticky Routing:** หลัง Write ภายใน **30–60 วินาที** ให้ Route คำขออ่านของ User นั้นไปที่ Primary ชั่วคราว พ้นช่วง Replication Lag ค่อยกลับไปอ่าน Replica
  3. **Client-Passed Logical Watermark (`min_lsn`):** ตอน `COMMIT` ใน Postgres คืน `pg_current_wal_lsn()` กลับไป ➔ ตอนเรียก Downstream View ให้แนบ `min_lsn` ไปด้วย ➔ Downstream เช็กว่า `synced_lsn >= min_lsn` หรือยัง ถ้ายังให้ **Bounded Wait (~200ms)** หรือ **Fallback ไป Primary** *(หากเป็น Cross-device มือถือ+เว็บ ต้องเก็บ `last_write_lsn` ไว้ที่ Redis/Server Session ไม่ใช่แค่ LocalStorage!)*
  4. **CQRS Boundary:** แยก Transactional UI (Read-Your-Writes บน Postgres) ออกจาก Analytical Dashboard (Eventual Consistency บน ClickHouse + แสดง `Last Synced At`)

### ข้อ 9.2 — รับประกัน Monotonic Reads (เห็นของใหม่แล้วห้ามเห็นของเก่ากว่าเหมือนย้อนเวลา) ยังไง?
* **ดูโค้ดที่:** [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L1030-L1045) (`MONOTONIC_READS`)
* **สาเหตุที่เกิด:** รีเฟรชรอบ 1 โดนสุ่มไป Replica A (ตามทัน) ➔ รอบ 2 โดนสุ่มไป Replica B (ล้าหลังกว่า 3 วิ) ➔ ข้อมูลที่เพิ่งเห็นหายวับ!
* **วิธีแก้:**
  1. **Sticky Replica Routing:** แฮช `user_id` ผูกให้ผู้ใช้แต่ละคนอ่านจาก **Replica เครื่องเดิมเสมอ** (สลับเฉพาะเมื่อเครื่องนั้นตาย)
  2. **Session High-Water Mark (`last_seen_lsn`):** ถ้าต้อง Failover ข้ามเครื่อง ให้แนบ `last_seen_lsn` ห้ามเสิร์ฟข้อมูลจาก Replica ที่ `synced_lsn < last_seen_lsn`

### ข้อ 9.3 — รับประกัน Consistent Prefix Reads (ห้ามเห็น "คำตอบ" โผล่ก่อน "คำถาม") ยังไง?
* **ดูโค้ดที่:** [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L1048-L1066) (`CONSISTENT_PREFIX`)
* **สาเหตุที่เกิด:** ในระบบ Sharded DB / Multi-partition Kafka ถ้าเหตุ (`Order Created` / คำถาม) กับผล (`Order Paid` / คำตอบ) ถูกส่งไปคนละ Partition ที่มี Lag ต่างกัน คนอ่านอาจเห็น `Order Paid` ก่อน `Order Created`!
* **วิธีแก้:**
  1. **Co-locate Causal Entities ใน Partition เดียวกัน:** ใช้ Aggregate Key เดียวกัน (เช่น `order_id` หรือ `conversation_id`) เป็น Partition Key ทั้งฝั่ง DB และ Kafka ➔ เหตุและผลวิ่งเข้าคิว FIFO เดียวกัน เรียงลำดับเป๊ะ 100%
  2. **Causal Dependency / Version Vectors:** หากจำเป็นต้องข้าม Partition ให้แนบ `causal_parent_id` / Version Vector เพื่อให้ Downstream รอจนกว่าเหตุจะมาถึงก่อนแสดงผล

### ข้อ 9.4 — Transactional Outbox ป้องกัน Dual-Write Loss ตอน Worker ตายกลาง Batch ยังไง?
* **ดูโค้ดที่:** [`lab3-debezium-cdc/init-postgres.sql`](../../lab3-debezium-cdc/init-postgres.sql#L14-L23) (`public.outbox_events`)
* **ทำไม Dual-Write ธรรมดาถึงพัง:**
  * ถ้าเขียน Postgres แล้วค่อยส่ง Kafka ➔ ตายกลางคัน = **Event หายถาวร**
  * ถ้าส่ง Kafka ก่อนแล้ว Postgres Rollback ➔ เกิด **Ghost Event** ใน Kafka ทั้งที่ข้อมูลไม่เคยถูกบันทึก!
* **วิธีที่ Transactional Outbox ปิดแผลขาด 100% (Local ACID Atomicity):**
  ```sql
  BEGIN;
    UPDATE public.orders SET order_status = 'PAID' WHERE order_id = 101;
    INSERT INTO public.outbox_events (aggregate_type, aggregate_id, event_type, payload)
    VALUES ('ORDER', 101, 'ORDER_PAID', '{"order_id": 101, "amount": 12000}');
  COMMIT;
  ```
  1. **All-or-Nothing:** Business Table (`orders`) และ `outbox_events` ถูกบันทึกใน **PostgreSQL Local Transaction เดียวกัน** — ไม่มีทางเกิด Partial Write ระหว่าง 2 ตาราง (ถ้า Worker ตายก่อน `COMMIT` Postgres Rollback ทั้งคู่; ถ้าตายหลัง `COMMIT` ทั้งคู่ถูกเขียนลง WAL แล้ว 100%)
  2. **Async Relay ผ่าน Debezium WAL:** Debezium อ่าน `outbox_events` จาก WAL (ใช้ Outbox Event Router SMT ตั้ง Key = `aggregate_id`) ส่งต่อเข้า Kafka ➔ **ไม่พึ่ง App Worker ที่ตายไปแล้ว** และฝั่ง Downstream ใช้ `event_id` ของ Outbox เป็น Idempotency Key!
  * *(ความซื่อสัตย์ต่อโค้ด Lab 3: ใน `init-postgres.sql` สร้างตาราง `outbox_events` พร้อม `REPLICA IDENTITY FULL` ไว้แล้ว แต่ใน `register_connector.py` ยังฟังเฉพาะ `orders` — บน Prod แค่เพิ่ม `public.outbox_events` + Outbox Router SMT!)*

### ข้อ 9.5 — ป้องกัน Clock Drift ด้วย Monotonic Clocks vs Time-of-Day Clocks (DDIA บทที่ 8)
* **ดูโค้ดที่:** [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L1321-L1356) (`LWW_CLOCK_SKEW` vs `SPANNER_TRUETIME`)
* **แยกหน้าที่นาฬิกา 2 แบบให้แม่น:**
  * **Time-of-Day Clock (`time.time()` / NTP):** กระโดดเดินหน้า-ถอยหลังได้เมื่อ NTP ซิงก์ ➔ **ใช้แค่เป็นป้ายเวลาแสดงผลเท่านั้น!** ห้ามใช้วัด Lease/Timeout และห้ามใช้ตัดสิน Last-Write-Wins (LWW) ข้ามเครื่อง (เครื่อง B เร็วกว่า 2 วิ จะทำให้ค่าเก่าเขียนทับค่าใหม่หายเงียบๆ!)
  * **Monotonic Clock (`time.monotonic()` / `System.nanoTime()`):** รับประกันว่าเดินหน้าอย่างเดียว ➔ **ต้องใช้สำหรับวัด Duration, Lock Lease Timeout, Heartbeat และ Backoff ภายใน Node เดียว** (แต่เทียบข้าม Node ไม่ได้)
  * **การเรียงลำดับข้าม Node:** ใช้ **Logical Sequence** จาก Leader/Consensus (WAL LSN, Kafka Offset, Raft Term+Index) หรือ TrueTime Uncertainty Wait-out

### ข้อ 9.6 — ป้องกัน Stale / Zombie Worker เขียนทับ Shared State ด้วย Fencing Tokens (DDIA บทที่ 8)
* **สถานการณ์พัง (Stop-the-World GC Pause / Network Delay):**
  1. Worker A ถือ Distributed Lock (Lease 10 วิ) แล้วเกิด **GC Pause ค้างไป 15 วิ**
  2. Lock หมดอายุ ➔ Consensus/Lock Service (ZooKeeper/etcd/Raft) ยก Lock ให้ **Worker B**
  3. Worker B เขียนค่าใหม่ลง Storage เสร็จ ➔ Worker A ตื่นจาก GC Pause ยังคิดว่าตัวเองถือ Lock อยู่ ➔ **ยิง Write เก่ามาทับข้อมูลของ Worker B พัง!**
* **วิธีป้องกันขาด 100% — Fencing Token (Monotonic Epoch Number):**
  1. ทุกครั้งที่ Lock Service แจก Lock จะออก **Fencing Token ที่เพิ่มขึ้นเสมอ** (Worker A ได้ `token = 33`, Worker B ได้ `token = 34`)
  2. ทุก Write ที่ยิงเข้า Storage ต้องแนบ `fencing_token` ไปด้วย
  3. **Storage เป็นคนตรวจและปฏิเสธเอง (Server-side Check):**
     ```sql
     UPDATE shared_state
     SET value = 'worker_a_stale_val', last_fencing_token = 33
     WHERE resource_id = 'job-1' AND last_fencing_token < 33;
     -- Worker B เขียน token=34 ไปแล้ว -> คำสั่งของ Zombie Worker A (token=33) ถูกปฏิเสธทันที (0 rows updated)!
     ```
  * *(เชื่อมโค้ดจริงใน Omni-Flow: Kafka ใช้หลักการเดียวกันเป๊ะใน `enable.idempotence = True` ([`omni-command-center/producer.py`](../../omni-command-center/producer.py#L129)) ผ่าน **Producer Epoch** และ **KRaft Leader Epoch** เพื่อกัน Zombie Producer / Zombie Leader!)*

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"คำถามนี้ตรงกับแก่น DDIA บทที่ 5, 7 และ 8 ที่ผมทำ Simulator ไว้ใน Lab 5 และวางตาราง `outbox_events` ไว้ใน Lab 3 เลยครับ ผมแยกจัดการ 3 ส่วนดังนี้:*
  >
  > *1. **3 Consistency Guarantees ข้าม Async Replica/View (DDIA บทที่ 5):** สำหรับ **Read-Your-Writes** ผมแยกตาม CQRS — หน้าจอที่เจ้าตัวเพิ่งแก้ (เช่น Profile / My Order) ให้ Route ไปอ่าน PostgreSQL Primary ตรงๆ หรือใช้ Time-Window Routing / แนบ `min_lsn` Watermark ให้ Downstream รอสั้นๆ หรือ Fallback ไป Primary; สำหรับ **Monotonic Reads** ผมแฮช `user_id` ผูกให้อ่านจาก Replica เครื่องเดิมเสมอ (Sticky Routing + `last_seen_lsn`); และสำหรับ **Consistent Prefix** ผม Co-locate เหตุและผลของ Aggregate เดียวกัน (เช่น `order_id`) ไว้ใน Partition เดียวกันทั้ง DB และ Kafka*
  >
  > *2. **ป้องกัน Dual-Write Loss ตอน Worker ตายกลางคันด้วย Transactional Outbox (DDIA บทที่ 7 & 11):** แทนที่ App จะเขียน Postgres แล้วค่อยส่ง Kafka ซึ่งเสี่ยงหายหรือเกิด Ghost Event ผมเขียนทั้ง Business Table (`orders`) และ `outbox_events` (`lab3/init-postgres.sql`) ใน **Local ACID Transaction เดียวกัน** — มันจะ Commit หรือ Rollback พร้อมกัน 100% แล้วปล่อยให้ Debezium อ่านจาก PostgreSQL WAL ส่งต่อเข้า Kafka แม้ App Worker ตายทันทีหลัง Commit ก็ไม่มีข้อมูลหล่นหายครับ*
  >
  > *3. **กัน Clock Drift & Zombie Worker (DDIA บทที่ 8):** ผมไม่ใช้ NTP Time-of-Day Clock วัด Lease หรือตัดสิน LWW ข้ามเครื่องเด็ดขาด แต่ใช้ **Monotonic Clock** วัด Timeout ภายในเครื่องเดียว, ใช้ **Logical Sequence** (WAL LSN / Kafka Offset) เรียงลำดับข้ามเครื่อง, และกัน Zombie Worker ที่ค้างจาก GC Pause ด้วย **Fencing Token** (เลข Epoch เพิ่มทางเดียวที่ Lock Service แจก แล้วให้ **Storage ฝั่งปลายทาง** เป็นคนปฏิเสธ Write ที่มี Token ต่ำกว่า — แบบเดียวกับ Producer Epoch ใน Kafka `enable.idempotence=True` ครับ)"*

* **🇬🇧 English:**
  > *"This maps directly to DDIA Chapters 5, 7, and 8—which I modeled in Lab 5's simulator and Lab 3's `outbox_events` schema. I solve it across three layers:*
  >
  > *1. **Replication-lag consistency guarantees:** For **Read-Your-Writes**, I route owner-modified reads (like profile or order detail) to the PostgreSQL primary, or return a `min_lsn` watermark token so downstream views either wait briefly until caught up or fall back to primary. For **Monotonic Reads**, I hash `user_id` to sticky replicas (guarded by a session `last_seen_lsn` on failover). For **Consistent Prefix**, I co-locate causally related events in the same DB/Kafka partition by aggregate key (e.g., `order_id`) or track causal dependencies across partitions.*
  >
  > *2. **Preventing dual-write loss with Transactional Outbox:** Instead of updating Postgres and publishing to Kafka in two non-atomic steps, I write both the business table (`orders`) and `outbox_events` inside a single **local ACID transaction**. Either both commit or both roll back; Debezium then relays the outbox rows reliably from the WAL even if the application worker crashes right after `COMMIT`.*
  >
  > *3. **Defending shared state against clock drift and stale workers:** I never use NTP time-of-day clocks for leases or cross-node LWW ordering. I use **monotonic clocks** strictly for local lease/timeout durations, logical sequences (WAL LSN / Kafka offset) for ordering, and **fencing tokens**—monotonically increasing epochs issued by the lock/consensus service and **enforced by the storage layer itself** to reject writes from GC-paused zombie workers, just like Kafka's Producer Epoch under `enable.idempotence=True`."*
