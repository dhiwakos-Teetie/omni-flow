from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(
    title="Lab 3 Deep-Dive: ClickHouse ReplacingMergeTree(version_ts, is_deleted) Simulator",
    description=(
        "Interactive simulation & proof of ClickHouse ReplacingMergeTree merge-on-read, "
        "soft-delete zombie row trap, and deterministic tie-breaking with WAL LSN + Kafka offset."
    ),
)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@dataclass
class PartRow:
    """Single row stored inside an immutable MergeTree data part."""

    order_id: int
    customer_name: str
    product_name: str
    amount: float
    order_status: str
    cdc_op: str
    version_ts: int
    wal_lsn: int
    kafka_offset: int
    is_deleted: int
    insert_seq: int
    part_id: str = ""


@dataclass
class DataPart:
    """Immutable ClickHouse MergeTree data part created per INSERT batch."""

    part_id: str
    level: int
    rows: List[PartRow]


class CustomEventRequest(BaseModel):
    """Payload for inserting a custom CDC event as a new unmerged ClickHouse part."""

    order_id: int = Field(default=101, ge=1)
    customer_name: str = Field(default="Somchai K.")
    product_name: str = Field(default="MacBook Pro M3")
    amount: float = Field(default=12000.0, ge=0.0)
    order_status: str = Field(default="PAID")
    cdc_op: Literal["c", "u", "d"] = "u"
    same_ms_collision: bool = False


class MergeRequest(BaseModel):
    """Options for background part compaction simulation."""

    cleanup_deleted: bool = True


def _pick_latest_by_key(
    rows: List[PartRow],
    key_fn,
    pre_filter_active: bool = False,
) -> List[PartRow]:
    """Deduplicate rows per order_id using key_fn, optionally pre-filtering deleted rows (unsafe)."""
    source = [r for r in rows if r.is_deleted == 0] if pre_filter_active else rows
    best_by_order: Dict[int, PartRow] = {}
    for row in source:
        current = best_by_order.get(row.order_id)
        if current is None or key_fn(row) > key_fn(current):
            best_by_order[row.order_id] = row
    return [best_by_order[oid] for oid in sorted(best_by_order.keys()) if best_by_order[oid].is_deleted == 0]


def _rows_signature(rows: List[PartRow]) -> List[Tuple[int, str, float, int]]:
    """Canonical signature to compare query output against ground truth."""
    return [
        (r.order_id, r.order_status, round(r.amount, 2), r.is_deleted)
        for r in sorted(rows, key=lambda x: x.order_id)
    ]


class ClickHouseReplacingEngine:
    """In-memory model of ClickHouse ReplacingMergeTree(version_ts, is_deleted) parts & queries."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.parts: List[DataPart] = []
        self.part_counter: int = 0
        self.seq_counter: int = 0
        self.next_lsn: int = 24501000
        self.next_offset: int = 10
        self.last_ts: int = 1732600001000
        self.active_scenario: str = "empty"
        self.scenario_title: str = "Empty Table (Ready)"
        self.scenario_explanation: str = "กดปุ่ม Scenario ด้านบนเพื่อจำลองการทำงานของ ClickHouse ReplacingMergeTree"
        self.interview_takeaway: str = ""
        self.event_log: List[str] = []

    def insert_part(
        self,
        *,
        order_id: int,
        customer_name: str,
        product_name: str,
        amount: float,
        order_status: str,
        cdc_op: str,
        version_ts: int,
        wal_lsn: int,
        kafka_offset: int,
        is_deleted: int,
        note: str = "",
    ) -> DataPart:
        """Append a new immutable MergeTree data part (simulating an asynchronous CDC flush)."""
        self.part_counter += 1
        self.seq_counter += 1
        part_id = f"all_{self.part_counter}_{self.part_counter}_0"
        row = PartRow(
            order_id=order_id,
            customer_name=customer_name,
            product_name=product_name,
            amount=amount,
            order_status=order_status,
            cdc_op=cdc_op,
            version_ts=version_ts,
            wal_lsn=wal_lsn,
            kafka_offset=kafka_offset,
            is_deleted=is_deleted,
            insert_seq=self.seq_counter,
            part_id=part_id,
        )
        part = DataPart(part_id=part_id, level=0, rows=[row])
        self.parts.append(part)
        self.last_ts = max(self.last_ts, version_ts)
        self.next_lsn = max(self.next_lsn, wal_lsn + 100)
        self.next_offset = max(self.next_offset, kafka_offset + 1)
        op_tag = "DELETE(Tombstone)" if is_deleted == 1 else f"{cdc_op.upper()}->{order_status}"
        self.event_log.append(
            f"[{part_id}] Order #{order_id} {op_tag} | amount={amount:,.0f} | ts={version_ts} | lsn={wal_lsn} | off={kafka_offset}"
            + (f" ({note})" if note else "")
        )
        return part

    def _all_rows(self) -> List[PartRow]:
        return [row for part in self.parts for row in part.rows]

    def load_scenario_1_stale_read(self) -> None:
        self.reset()
        self.active_scenario = "1-stale-read"
        self.scenario_title = "Scenario 1: Stale & Duplicate Reads ก่อน Background Merge"
        self.scenario_explanation = (
            "จำลองเหตุการณ์หลัง INSERT + UPDATE 2 ครั้งของ Order #101 และ INSERT Order #102 "
            "ข้อมูลถูกเขียนลง 4 Parts แยกกันและ ClickHouse ยังไม่ Merge ใน Background"
        )
        self.interview_takeaway = (
            "ห้ามให้ Dashboard ยิง SELECT ตรงเข้า orders_replica เพราะจะเห็นข้อมูลซ้ำทุก Part และยอดเงินเบิ้ล "
            "ต้องครอบด้วย View ที่ใช้ FINAL (Merge-on-Read) หรือ argMax(..., version) เสมอ"
        )
        self.insert_part(
            order_id=101,
            customer_name="Somchai K.",
            product_name="MacBook Pro M3",
            amount=10000.0,
            order_status="PENDING",
            cdc_op="c",
            version_ts=1732600001000,
            wal_lsn=24501000,
            kafka_offset=10,
            is_deleted=0,
            note="initial insert",
        )
        self.insert_part(
            order_id=101,
            customer_name="Somchai K.",
            product_name="MacBook Pro M3",
            amount=12000.0,
            order_status="PAID",
            cdc_op="u",
            version_ts=1732600002000,
            wal_lsn=24501120,
            kafka_offset=11,
            is_deleted=0,
            note="update #1",
        )
        self.insert_part(
            order_id=101,
            customer_name="Somchai K.",
            product_name="MacBook Pro M3",
            amount=12000.0,
            order_status="SHIPPED",
            cdc_op="u",
            version_ts=1732600003000,
            wal_lsn=24501288,
            kafka_offset=12,
            is_deleted=0,
            note="update #2 latest",
        )
        self.insert_part(
            order_id=102,
            customer_name="Suda W.",
            product_name='4K Monitor 27"',
            amount=8000.0,
            order_status="PAID",
            cdc_op="c",
            version_ts=1732600001500,
            wal_lsn=24501050,
            kafka_offset=13,
            is_deleted=0,
            note="another order",
        )

    def load_scenario_2_zombie_trap(self) -> None:
        self.load_scenario_1_stale_read()
        self.active_scenario = "2-zombie-trap"
        self.scenario_title = "Scenario 2: Soft-Delete & กับดัก Zombie Row Trap"
        self.scenario_explanation = (
            "Order #101 ถูกลบจาก PostgreSQL (Debezium ส่ง op='d' ➔ is_deleted=1 Tombstone เข้า Part 5) "
            "หากเผลอใส่ WHERE is_deleted = 0 ก่อน Deduplicate จะตัด Tombstone ทิ้ง ทำให้แถวเก่าฟื้นเป็น Zombie Row!"
        )
        self.interview_takeaway = (
            "กฎเหล็กของ Soft-Delete CDC: ห้ามกรอง WHERE is_deleted = 0 ก่อน Deduplicate เด็ดขาด! "
            "ต้องใช้ FINAL WHERE is_deleted = 0 หรือ GROUP BY ... HAVING argMax(is_deleted, ver) = 0 เท่านั้น"
        )
        self.insert_part(
            order_id=101,
            customer_name="Somchai K.",
            product_name="MacBook Pro M3",
            amount=12000.0,
            order_status="DELETED",
            cdc_op="d",
            version_ts=1732600004000,
            wal_lsn=24501400,
            kafka_offset=14,
            is_deleted=1,
            note="soft-delete tombstone",
        )

    def load_scenario_3_same_ts_collision(self) -> None:
        self.reset()
        self.active_scenario = "3-same-ts-collision"
        self.scenario_title = "Scenario 3: Same-ms version_ts Collision (กับดัก Wall-Clock)"
        self.scenario_explanation = (
            "Order #201 ถูกแก้ 2 ครั้งภายใน 1 millisecond เดียวกัน (ts=1732600009999 เท่ากันเป๊ะ) "
            "และเกิด Retry ทำให้ UPDATE เก่า (PAID, lsn=24509100) ถูกเขียนลง Part 2 ทีหลัง UPDATE ใหม่ (CANCELLED, lsn=24509240)"
        )
        self.interview_takeaway = (
            "เมื่อ version_ts ชนกันเป๊ะ ClickHouse จะเก็บแถวที่ถูกเขียน/Merge ทีหลังสุด (Nondeterministic)! "
            "วิธีแก้ระดับ Production ตาม DDIA บทที่ 8 & 11 คือเลิกใช้ Wall-Clock ts_ms แล้วใช้ PostgreSQL WAL LSN + Kafka Offset แทน"
        )
        self.insert_part(
            order_id=201,
            customer_name="Ananda P.",
            product_name="Ergonomic Chair",
            amount=15000.0,
            order_status="CANCELLED",
            cdc_op="u",
            version_ts=1732600009999,
            wal_lsn=24509240,
            kafka_offset=51,
            is_deleted=0,
            note="newer LSN arrived 1st",
        )
        self.insert_part(
            order_id=201,
            customer_name="Ananda P.",
            product_name="Ergonomic Chair",
            amount=15000.0,
            order_status="PAID",
            cdc_op="u",
            version_ts=1732600009999,
            wal_lsn=24509100,
            kafka_offset=50,
            is_deleted=0,
            note="older LSN retried 2nd (same ms!)",
        )

    def run_background_merge(self, cleanup_deleted: bool = True) -> None:
        """Simulate ClickHouse background merge of all existing parts into a single compacted part."""
        rows = self._all_rows()
        if not rows:
            return
        best_by_order: Dict[int, PartRow] = {}
        # ReplacingMergeTree(version_ts, is_deleted) compares version_ts; on tie, keeps last row in physical order (insert_seq)
        for row in rows:
            current = best_by_order.get(row.order_id)
            if current is None or (row.version_ts, row.insert_seq) > (current.version_ts, current.insert_seq):
                best_by_order[row.order_id] = row

        merged_rows: List[PartRow] = []
        for oid in sorted(best_by_order.keys()):
            winner = best_by_order[oid]
            if cleanup_deleted and winner.is_deleted == 1:
                continue
            merged_part_id = f"all_1_{self.part_counter}_1"
            merged_rows.append(
                PartRow(
                    order_id=winner.order_id,
                    customer_name=winner.customer_name,
                    product_name=winner.product_name,
                    amount=winner.amount,
                    order_status=winner.order_status,
                    cdc_op=winner.cdc_op,
                    version_ts=winner.version_ts,
                    wal_lsn=winner.wal_lsn,
                    kafka_offset=winner.kafka_offset,
                    is_deleted=winner.is_deleted,
                    insert_seq=winner.insert_seq,
                    part_id=merged_part_id,
                )
            )

        merged_part_id = f"all_1_{self.part_counter}_1"
        self.parts = [DataPart(part_id=merged_part_id, level=1, rows=merged_rows)] if merged_rows else []
        self.event_log.append(
            f"[MERGE] Background merge compacted all parts -> {len(merged_rows)} row(s) remaining (cleanup_deleted={cleanup_deleted})"
        )

    def add_custom_event(self, req: CustomEventRequest) -> None:
        is_deleted = 1 if req.cdc_op == "d" else 0
        status = "DELETED" if is_deleted == 1 else req.order_status
        if req.same_ms_collision and self.parts:
            ts = self.last_ts
            # Simulate an older retried event landing later with the exact same ms timestamp
            lsn = max(1, self.next_lsn - 150)
            offset = max(0, self.next_offset - 2)
            note = "custom same-ms older LSN retry"
        else:
            ts = self.last_ts + 1000
            lsn = self.next_lsn + 120
            offset = self.next_offset + 1
            note = "custom event"

        self.insert_part(
            order_id=req.order_id,
            customer_name=req.customer_name,
            product_name=req.product_name,
            amount=req.amount,
            order_status=status,
            cdc_op=req.cdc_op,
            version_ts=ts,
            wal_lsn=lsn,
            kafka_offset=offset,
            is_deleted=is_deleted,
            note=note,
        )

    def _build_query_result(
        self,
        title: str,
        sql: str,
        rows: List[PartRow],
        truth_rows: List[PartRow],
        pass_msg: str,
        fail_msg: str,
    ) -> Dict:
        is_correct = _rows_signature(rows) == _rows_signature(truth_rows)
        total_revenue = round(sum(r.amount for r in rows), 2)
        return {
            "title": title,
            "sql": sql,
            "rows": [asdict(r) for r in rows],
            "row_count": len(rows),
            "total_revenue": total_revenue,
            "is_correct": is_correct,
            "verdict": pass_msg if is_correct else fail_msg,
        }

    def snapshot(self) -> Dict:
        all_rows = self._all_rows()

        # Ground truth uses strictly monotonic (wal_lsn, kafka_offset) & post-dedup tombstone filter
        truth_rows = _pick_latest_by_key(
            all_rows,
            key_fn=lambda r: (r.wal_lsn, r.kafka_offset),
            pre_filter_active=False,
        )

        # Query 1: Raw SELECT without FINAL or argMax
        raw_rows = sorted(all_rows, key=lambda r: (r.order_id, r.insert_seq))

        # Query 2: Zombie Row Trap (WHERE is_deleted = 0 BEFORE GROUP BY / deduplication)
        zombie_rows = _pick_latest_by_key(
            all_rows,
            key_fn=lambda r: (r.wal_lsn, r.kafka_offset),
            pre_filter_active=True,
        )

        # Query 3: FINAL using version_ts (vulnerable to same-ms tie-breaker nondeterminism)
        final_ts_rows = _pick_latest_by_key(
            all_rows,
            key_fn=lambda r: (r.version_ts, r.insert_seq),
            pre_filter_active=False,
        )

        # Query 4: Production argMax View using (wal_lsn, kafka_offset) + HAVING is_deleted = 0
        argmax_lsn_rows = truth_rows

        queries = {
            "raw_select": self._build_query_result(
                title="1. Raw SELECT (อันตราย: ไม่อ่านผ่าน FINAL/View)",
                sql="SELECT order_id, order_status, amount, is_deleted\nFROM cdc_olap.orders_replica\nWHERE is_deleted = 0;",
                rows=raw_rows if len(all_rows) == 0 else [r for r in raw_rows if r.is_deleted == 0],
                truth_rows=truth_rows,
                pass_msg="✅ บังเอิญตรงเพราะมีแค่ 1 Part ต่อ 1 Order (หลัง Merge แล้ว)",
                fail_msg="❌ ผิดพลาด! เห็นแถวเก่า/ซ้ำจากหลาย Parts ที่ยังไม่ Merge และเสี่ยง Zombie Row",
            ),
            "zombie_trap": self._build_query_result(
                title="2. Zombie Row Trap (กรอง is_deleted=0 ก่อน Deduplicate)",
                sql="SELECT order_id,\n  argMax(order_status, (wal_lsn, kafka_offset)) AS order_status,\n  argMax(amount, (wal_lsn, kafka_offset)) AS amount\nFROM cdc_olap.orders_replica\nWHERE is_deleted = 0  -- ❌ ตัด Tombstone ทิ้งก่อน merge!\nGROUP BY order_id;",
                rows=zombie_rows,
                truth_rows=truth_rows,
                pass_msg="✅ ยังไม่เจอปัญหาเพราะยังไม่มีแถว DELETE (ลองกด Scenario 2 เพื่อดูกับดัก!)",
                fail_msg="❌ พัง! แถว Tombstone (is_deleted=1) ถูก WHERE ตัดทิ้งก่อน ➔ แถวเก่าฟื้นเป็น Zombie Row!",
            ),
            "final_ts": self._build_query_result(
                title="3. SELECT ... FINAL บน ReplacingMergeTree(version_ts, is_deleted)",
                sql="SELECT order_id, order_status, amount, version_ts, is_deleted\nFROM cdc_olap.orders_replica FINAL\nWHERE is_deleted = 0;",
                rows=final_ts_rows,
                truth_rows=truth_rows,
                pass_msg="✅ ถูกต้อง! FINAL ทำ Merge-on-Read และตัดแถวที่ลบหลัง Deduplicate แล้ว",
                fail_msg="❌ พังเพราะ version_ts ชนกันใน ms เดียวกัน! ClickHouse หยิบแถวที่เขียนทีหลัง (PAID เก่าทับ CANCELLED)",
            ),
            "argmax_lsn": self._build_query_result(
                title="4. Production View: argMax(..., (wal_lsn, kafka_offset)) + HAVING",
                sql="SELECT order_id,\n  argMax(order_status, (wal_lsn, kafka_offset)) AS order_status,\n  argMax(amount, (wal_lsn, kafka_offset)) AS amount,\n  argMax(is_deleted, (wal_lsn, kafka_offset)) AS is_deleted\nFROM cdc_olap.orders_replica\nGROUP BY order_id\nHAVING is_deleted = 0;",
                rows=argmax_lsn_rows,
                truth_rows=truth_rows,
                pass_msg="✅ ถูกต้อง 100%! ชนะทุกกรณี ทั้งก่อน Merge, Soft-Delete และ Retry ใน ms เดียวกัน",
                fail_msg="❌ ไม่ตรงกับความจริง",
            ),
        }

        return {
            "active_scenario": self.active_scenario,
            "scenario_title": self.scenario_title,
            "scenario_explanation": self.scenario_explanation,
            "interview_takeaway": self.interview_takeaway,
            "part_count": len(self.parts),
            "total_stored_rows": len(all_rows),
            "parts": [
                {
                    "part_id": p.part_id,
                    "level": p.level,
                    "rows": [asdict(r) for r in p.rows],
                }
                for p in self.parts
            ],
            "truth": {
                "rows": [asdict(r) for r in truth_rows],
                "row_count": len(truth_rows),
                "total_revenue": round(sum(r.amount for r in truth_rows), 2),
            },
            "queries": queries,
            "event_log": self.event_log[-12:],
        }


engine = ClickHouseReplacingEngine()
engine.load_scenario_1_stale_read()


@app.get("/")
def serve_home():
    return FileResponse(STATIC_DIR / "replacing_mergetree_demo.html")


@app.get("/api/state")
def get_state():
    return engine.snapshot()


@app.post("/api/reset")
def reset_state():
    engine.reset()
    return engine.snapshot()


@app.post("/api/scenario/1-stale-read")
def run_scenario_1():
    engine.load_scenario_1_stale_read()
    return engine.snapshot()


@app.post("/api/scenario/2-zombie-trap")
def run_scenario_2():
    engine.load_scenario_2_zombie_trap()
    return engine.snapshot()


@app.post("/api/scenario/3-same-ts-collision")
def run_scenario_3():
    engine.load_scenario_3_same_ts_collision()
    return engine.snapshot()


@app.post("/api/action/merge")
def run_merge(req: Optional[MergeRequest] = None):
    cleanup = True if req is None else req.cleanup_deleted
    engine.run_background_merge(cleanup_deleted=cleanup)
    return engine.snapshot()


@app.post("/api/action/custom-event")
def post_custom_event(req: CustomEventRequest):
    engine.add_custom_event(req)
    return engine.snapshot()


if __name__ == "__main__":
    uvicorn.run("replacing_mergetree_lab:app", host="0.0.0.0", port=8086, reload=False)
