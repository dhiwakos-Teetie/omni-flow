# Q07: Binary Encoding & Schema Evolution — Protobuf Wire Breakage, Zero-Downtime Upgrade & Avro Writer's Schema

[← กลับสารบัญคัมภีร์ OMNI_FLOW_INTERVIEW_BIBLE.md](../OMNI_FLOW_INTERVIEW_BIBLE.md)

> **📍 อ้างอิง Lab ที่ต้องเปิดดู (Lab Reference)**
> * **Lab หลัก:** [`lab5-encoding-evolution`](../../lab5-encoding-evolution)
> * **ไฟล์ & บรรทัดที่ต้องเปิดดู:**
>   * [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L84-L103) — โค้ดผ่าไบต์จริงของ **Protobuf (`33B`: `0x0A`, `0x10`, `0x1A`)** เทียบกับ **Avro (`32B`: packed bytes ไร้ Tag/ไร้ Wire Type)**
>   * [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L200-L282) — จำลอง Rolling Upgrade (`ADD_OPTIONAL`, `ADD_REQUIRED`, `RENAME_FIELD`)
>   * [`lab5-encoding-evolution/encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L320-L398) — จำลองกับดัก Database Read-Modify-Write (Unknown field หายเงียบๆ)
> * **เรื่องที่ต้องดูจาก Lab นี้:** Protobuf Key Byte `(tag << 3) | wire_type`, การชนกันของ Wire Type `0` (`int32`) vs `2` (`string`), แพทเทิร์น Expand & Contract + `reserved`, และเหตุผลระดับไบต์ที่ Avro ต้องการ Writer's Schema

---

## 🎙️ โจทย์สัมภาษณ์ (Interviewer Prompt)

> *"In `lab5-encoding-evolution`, you benchmarked JSON, MessagePack, Avro, and Protocol Buffers (`encoding_app.py`) and demonstrated forward and backward schema compatibility using Field Tags. Suppose a production Kafka topic is shared by 50 microservices—some running v1 producers/consumers and others rolling out v2—and someone proposes renaming an existing field or changing its data type from `int32` to `string` while keeping the same Protobuf Field Tag. What breaks on the wire, how do you safely evolve a field's type or meaning without downtime, and why can't Avro decode a binary payload without the writer's schema while Protobuf can?"*

---

## 🧩 แยกทบทวนเป็น 6 ข้อย่อย

### ข้อ 7.1 — เปลี่ยนชื่อฟิลด์ (Rename) โดยใช้ Protobuf Field Tag เดิม ➔ พังบน Wire ไหม?
* **ดูโค้ดที่:** [`encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L260-L282) (`RENAME_FIELD`)
* **แยกตอบ 2 มุมให้คม:**
  1. **บน Binary Wire — ไม่พัง 100% ✅:** ในไบนารีของ Protobuf ไม่มีชื่อฟิลด์อยู่เลย มีแค่หมายเลข Tag (เช่น `0x0A` = Tag `#1`, String) ➔ Service v1 ยังอ่าน Tag `#1` เข้า `userName` ส่วน Service v2 อ่าน Tag `#1` เข้า `fullName` ได้สมบูรณ์
  2. **⚠️ แต่จุดที่พังนอก Binary Wire:**
     * **ProtoJSON / REST Gateway:** ถ้ามี Service แปลง Protobuf เป็น JSON (Proto3 JSON mapping) ชื่อคีย์จะเปลี่ยนจาก `"userName"` ➔ `"fullName"` ทำให้ Downstream JSON consumer พัง
     * **Source-code Breaking:** 50 microservices ที่ compile `.proto` รุ่นใหม่จะต้องแก้ชื่อ getter ในโค้ดด้วย (แม้ Binary wire ไม่พัง)

### ข้อ 7.2 — เปลี่ยน Type จาก `int32` ➔ `string` บน Protobuf Field Tag เดิม ➔ พังยังไงบน Wire?
* **ดูโค้ดที่:** [`encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L84-L91) (`0x10` สำหรับ `favoriteNumber` Tag `#2`)
* **พังทันทีระดับ Wire Type Mismatch! ❌**
  * ใน Protobuf ไบต์หัวฟิลด์คำนวณจาก `(field_tag << 3) | wire_type`:
    * เดิม Tag `#2` เป็น **`int32`** ➔ Key Byte = `(2 << 3) | 0` = **`0x10`** (**Wire Type `0` = Varint**) ตามด้วยไบต์ Varint
    * พอเปลี่ยน Tag `#2` เป็น **`string`** ➔ Key Byte = `(2 << 3) | 2` = **`0x12`** (**Wire Type `2` = Length-Delimited**) ตามด้วย `[length][UTF-8 bytes]`
  * **ผลกระทบระหว่าง Rolling Upgrade ของ 50 Microservices:**
    * **v1 Consumer อ่าน v2 Producer (`0x12`):** v1 คาดหวัง Wire Type `0` แต่เจอ Wire Type `2` ➔ บาง Parser โยน **`InvalidProtocolBufferException`** หรือใน Proto3 Parser จะตีความว่าเป็น **Unknown Field แล้วข้ามทิ้ง** ทำให้ v1 ได้ค่า Default **`0` เงียบๆ (Silent Data Corruption!)**
    * **v2 Consumer อ่าน v1 Producer (`0x10`):** v2 คาดหวัง Wire Type `2` แต่เจอ Wire Type `0` ➔ โยน Error หรือได้ `""` (empty string) เงียบๆ!

### ข้อ 7.3 — วิธีเปลี่ยน Type หรือความหมายของฟิลด์แบบ Zero-Downtime บน Kafka (Expand & Contract + `reserved`)
* **ห้ามเปลี่ยน Type บน Tag เดิมเด็ดขาด — ใช้ 4 ขั้นตอน Zero-Downtime:**
  1. **Expand (เพิ่ม Tag ใหม่):** คง Tag `#2` (`int32`) ไว้เหมือนเดิม + ติด `[deprecated = true]` แล้วเพิ่มฟิลด์ใหม่ด้วย **Tag ใหม่** เช่น `string favorite_number_str = 5;` (Optional)
  2. **Dual-Write (อัปเกรด Producer ก่อน):** Producer v2 เขียนค่าลง **ทั้ง Tag `#2` และ Tag `#5`** พร้อมกัน เพื่อให้ v1 Consumer ยังทำงานต่อได้
  3. **Migrate Readers (ทยอยอัปเกรด 50 Consumer):** Consumer v2 อ่าน Tag `#5` เป็นหลัก ถ้าว่าง (มาจาก v1 Producer) ให้ Fallback แปลงจาก Tag `#2`
  4. **Contract & Reserve (ล็อก Tag เก่าถาวร):** เมื่อยืนยันว่าไม่มี v1 เหลืออยู่แล้ว ค่อยหยุดเขียน Tag `#2` และลบทิ้งพร้อมประกาศ:
     ```protobuf
     reserved 2;
     reserved "favorite_number";
     ```
     เพื่อป้องกันไม่ให้ใครในอนาคตเผลอเอา Tag `#2` กลับมาใช้ซ้ำจนชนกับข้อมูลประวัติศาสตร์ใน Kafka / Lakehouse!
* **ด่านกันพลาดอัตโนมัติ:** ผูก **Schema Registry** ตั้ง Compatibility เป็น **`FULL_TRANSITIVE`** (และรัน `buf breaking` ใน CI) เพื่อบล็อก PR ที่แก้ Type หรือ Reuse Tag ตั้งแต่ก่อน Deploy!

### ข้อ 7.4 — ทำไม Avro ถึง Decode Binary Payload โดยไม่มี Writer's Schema ไม่ได้ แต่ Protobuf ทำได้?
* **ดูโค้ดผ่าไบต์เทียบตรงๆ ที่:** [`encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L84-L103) (`protobuf_bytes` 33B vs `avro_bytes` 32B)
* **Protobuf (`33 ไบต์` — Self-Framing Wire Format):**
  * ทุกฟิลด์มี **Key Byte (`Field Tag + Wire Type`)** นำหน้าเสมอ (`0x0A`, `0x10`, `0x1A`)
  * แม้ไม่มี `.proto` Schema เลย ตัว Decoder (`protoc --decode_raw`) ก็ยัง **รู้ขอบเขตและวิธีข้ามทุกฟิลด์ได้** เพราะ Wire Type บอกวิธีอ่านความยาว (Wire Type `0` = อ่าน Varint จนบิตบนสุดเป็น `0`, Wire Type `2` = อ่านความยาว `N` ไบต์แล้วกระโดดข้าม `N` ไบต์) ➔ จึง Decode โครงสร้างดิบได้ (`1: "Martin", 2: 1337`) และข้าม Unknown Fields ได้เอง
* **Avro (`32 ไบต์` — Pure Packed Values, Zero Tags, Zero Wire Types):**
  * ดูใน `encoding_app.py` L95–102: Avro เก็บเฉพาะ **เนื้อข้อมูลล้วนๆ เรียงต่อกันติดเป็นพืด** (`len + "Martin" + union_idx(0x00) + zigzag(1337) + ...`) **ไม่มีทั้งชื่อฟิลด์ ไม่มีทั้งหมายเลข Tag และไม่มี Wire Type!**
  * ถ้าไม่มี **Writer's Schema** ตัว Decoder **ไม่รู้แม้กระทั่งว่าฟิลด์ที่ 1 จบที่ไบต์ไหนและฟิลด์ที่ 2 เป็น Type อะไร** (อ่านหลุดเฟรมพังทั้ง Payload ทันที!)
  * **วิธีที่ Avro ส่ง Writer's Schema ในโลกจริง (DDIA บทที่ 4):**
    * **Kafka Streaming:** ใส่ **Magic Byte (`0x00`) + 4-byte Schema ID** ไว้หัวข้อความ (Confluent Wire Format) ให้ Consumer ดึง Writer's Schema จาก **Schema Registry** มาแคชไว้ แล้วทำ Schema Resolution เทียบกับ Reader's Schema
    * **Batch File (`.avro`):** เก็บ Writer's Schema ไว้ครั้งเดียวที่ Header ของไฟล์

### ข้อ 7.5 — ระวังกับดักต่อเนื่อง: "Database Read-Modify-Write Trap"
* **ดูโค้ดที่:** [`encoding_app.py`](../../lab5-encoding-evolution/encoding_app.py#L320-L398)
* ระหว่าง Rolling Upgrade ถ้า Service v1 อ่านข้อมูลที่มี Tag `#5` (เขียนโดย v2) มาแปลงผ่าน DTO/JSON แล้วเขียนทับลง DB โดยไม่เก็บ Unknown Fields ไว้ ➔ ค่าใน Tag `#5` จะ **หายไปเงียบๆ!**

### ข้อ 7.6 — ตารางเทียบสั้นๆ: Protobuf vs Avro บน Kafka 50 Microservices

| มิติ | Protocol Buffers (`33B`) | Apache Avro (`32B`) |
| :--- | :--- | :--- |
| **มี Tag / Wire Type ใน Payload?** | **มี** `(tag << 3) | wire_type` | **ไม่มีเลย** (เล็กสุดแต่ต้องมี Writer's Schema เสมอ) |
| **Decode ดิบโดยไม่มี Schema?** | **ได้** (`protoc --decode_raw` รู้ขอบเขตไบต์) | **ไม่ได้เลย** (ไม่รู้จุดตัดฟิลด์) |
| **Rename Field** | ปลอดภัยบน Binary Wire (แต่ระวัง ProtoJSON) | ต้องใส่ **`aliases: ["oldName"]`** ใน Reader's Schema |
| **จุดเด่นสุด** | Cross-service RPC/Microservices, ข้าม Unknown Field ง่าย | Big Data / Dynamic Schemas (เช่น Debezium / Hive / Hadoop) |

---

## 🗣️ สคริปต์พร้อมตอบ (60–90 วินาที)

* **🇹🇭 ภาษาไทย:**
  > *"ขอแยกเป็น 3 ประเด็นตามที่ผมผ่าไบต์จริงไว้ใน `encoding_app.py` (Lab 5) ครับ:*
  >
  > *1. **เรื่อง Rename vs เปลี่ยน Type (`int32` ➔ `string`) บน Tag เดิม:** ถ้าแค่ **Rename** บน Binary Wire ของ Protobuf จะไม่พังครับ เพราะในไบนารีส่งแค่ Field Tag ไม่มีชื่อฟิลด์ (แต่ต้องระวังถ้ามีตัวแปลงเป็น ProtoJSON) — **แต่ถ้าเปลี่ยน `int32` เป็น `string` บน Tag เดิม พังทันทีครับ!** เพราะ Key Byte จะเปลี่ยนจาก Wire Type `0` (Varint เช่น `0x10`) เป็น Wire Type `2` (Length-delimited เช่น `0x12`) ทำให้ 50 Microservices ที่ยังเป็น v1 โยน `InvalidProtocolBufferException` หรือข้ามฟิลด์นั้นทิ้งจนอ่านได้ค่า Default `0` เงียบๆ (Silent Data Loss)*
  >
  > *2. **วิธีเปลี่ยน Type แบบ Zero-Downtime:** ผมจะใช้แพทเทิร์น **Expand & Contract** — ห้ามแตะ Tag เก่า แต่เพิ่ม **Tag ใหม่** (`#5`) แบบ Optional ➔ ให้ Producer ทำ **Dual-Write** ทั้ง Tag เก่าและใหม่ ➔ ทยอยย้าย Consumer 50 ตัวไปอ่าน Tag ใหม่ (พร้อม Fallback) ➔ พอไม่มี v1 เหลือค่อยลบ Tag เก่าและใส่ **`reserved 2;`** ล็อกไม่ให้ใครเอาหมายเลขเดิมกลับมาใช้ซ้ำ ควบคู่กับเปิด Schema Registry โหมด **`FULL_TRANSITIVE`** ใน CI เพื่อบล็อก Breaking Change ตั้งแต่ต้นทาง*
  >
  > *3. **ทำไม Avro Decode โดยไม่มี Writer's Schema ไม่ได้ แต่ Protobuf ทำได้:** จากไบต์จริงใน Lab 5 (Protobuf 33B vs Avro 32B) ตัว **Protobuf แนบ `(Tag + Wire Type)` ไว้หัวทุกฟิลด์** Decoder จึงรู้ความยาวและขอบเขตของทุกฟิลด์ได้เองแม้ไม่มี `.proto` ส่วน **Avro ตัดทั้งชื่อฟิลด์ หมายเลข Tag และ Wire Type ออกหมด** เก็บเฉพาะเนื้อข้อมูลเรียงติดกันเป็นพืด ถ้าไม่มี Writer's Schema (ซึ่งบน Kafka จะส่งเป็น **4-byte Schema ID** ไว้หัวข้อความไปดึงจาก Schema Registry) ตัว Decoder จะไม่รู้แม้กระทั่งว่าฟิลด์แรกจบที่ไบต์ไหนครับ"*

* **🇬🇧 English:**
  > *"Let me break this down from the wire-level bytes I implemented in Lab 5 (`encoding_app.py`):*
  >
  > *1. **Renaming vs. changing `int32` to `string` on the same Protobuf tag:** Renaming is binary-wire safe because Protobuf only transmits field tags, not names (though watch out for ProtoJSON gateways and SDK compile breaks). Changing `int32` to `string` on the same tag **breaks immediately**: the key byte changes from Wire Type `0` (Varint, e.g. `0x10`) to Wire Type `2` (Length-delimited, `0x12`), causing v1 consumers to either throw a parse exception or treat it as an unknown field and silently default to `0`.*
  >
  > *2. **Zero-downtime evolution across 50 microservices:** I use **Expand & Contract**—add a new optional field with a **new tag** (e.g., tag `#5`), mark the old tag `deprecated`, dual-write both fields from v2 producers, migrate all consumers to read tag `#5` with fallback to tag `#2`, and finally retire tag `#2` using **`reserved 2;`** so it can never be reused against historical Kafka data—enforced automatically with Schema Registry `FULL_TRANSITIVE` checks in CI.*
  >
  > *3. **Why Avro requires the writer's schema while Protobuf doesn't:** Protobuf is self-framing—every field is prefixed with `(tag << 3) | wire_type`, so a raw decoder always knows field boundaries and can skip unknown fields. Avro strips out all field names, tags, and wire types to pack raw values tightly (32B vs. 33B in my benchmark). Without the writer's schema—typically fetched via a 4-byte Schema Registry ID header in Kafka—an Avro decoder can't even tell where one field ends and the next begins."*
