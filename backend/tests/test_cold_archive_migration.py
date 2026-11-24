"""冷档分区引用迁移的端到端测试。

覆盖场景：
- 老档 A 有时钟依据且产物可复查，确认覆盖某日 10:00-10:10；
- 老档 B 只有冷档位置和编号，无法确认区间，禁止按顺序号猜日期；
- 写完 A、处理 B 前中断；中断期间值班员按事件时段发起实际重算，
  同时按 B 的旧编号核对原件；新查询立即能取到 A；
- 重启后从处置记录核对续跑，A 只形成一个有效引用，B 仅为未确认占位；
- 撤回新版后，旧查询仍能按已知编号定位两份原档。
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

DAY = "2026-11-24"
POINT = "SENS-COLD"
EVENT_START = f"{DAY}T10:00:00"
EVENT_END = f"{DAY}T10:10:00"


@pytest.fixture()
def workdir():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        with patch("app.services.base.DATA_ROOT", root):
            import app.services.series_index as si
            import app.services.range_replay as rr
            si.DATA_ROOT = root
            rr.DATA_ROOT = root
            yield root


def _write_artifact(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    base = datetime.fromisoformat(EVENT_START)
    with open(path, "w", encoding="utf-8") as f:
        for i in range(11):  # 10:00 至 10:10，每分钟一个事件
            rec = {
                "point": POINT,
                "event_time": (base + timedelta(minutes=i)).isoformat(),
                "value": 20.0 + i * 0.1,
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _register_archives(svc, workdir):
    artifact = workdir / "cold_source" / "ARCH-A_artifact.jsonl"
    _write_artifact(artifact)
    a = svc.register_cold_archive({
        "archive_id": "ARCH-A", "point": POINT, "legacy_no": "LEG-A",
        "cold_location": "/cold/archive/location-a",
        "clock_evidence": True, "artifact_file": str(artifact),
        "claimed_start": EVENT_START, "claimed_end": EVENT_END,
    })
    b = svc.register_cold_archive({
        "archive_id": "ARCH-B", "point": POINT, "legacy_no": "LEG-B",
        "cold_location": "/cold/archive/location-b",
        "clock_evidence": False, "artifact_file": "",
    })
    assert "error" not in a and "error" not in b
    svc.register_legacy_ref({
        "legacy_no": "LEG-A", "archive_id": "ARCH-A", "point": POINT,
        "cold_location": "/cold/archive/location-a",
        "artifact_file": str(artifact), "original_checksum": "ck-a",
    })
    svc.register_legacy_ref({
        "legacy_no": "LEG-B", "archive_id": "ARCH-B", "point": POINT,
        "cold_location": "/cold/archive/location-b",
        "artifact_file": "", "original_checksum": "ck-b",
    })
    return artifact


class TestColdArchiveMigration:
    def test_full_interrupt_resume_replay_rollback_scenario(self, workdir):
        from app.services.series_index import SeriesIndexService
        from app.store import store

        svc = SeriesIndexService()
        _register_archives(svc, workdir)

        # 1. 核验：A 的事件区间来自产物实际时刻；B 不可确认且无猜测日期
        va = svc.verify_archive_interval("ARCH-A")
        assert va["confirmed"] is True
        assert va["event_start"] == EVENT_START
        assert va["event_end"] == EVENT_END
        assert va["event_count"] == 11

        vb = svc.verify_archive_interval("ARCH-B")
        assert vb["confirmed"] is False
        assert vb["event_start"] is None and vb["event_end"] is None

        # 2. 迁移在写完 A 后、处理 B 前中断
        with pytest.raises(RuntimeError, match="模拟迁移中断"):
            svc.migrate_cold_references("RUN-1", ["ARCH-A", "ARCH-B"],
                                        crash_after="ARCH-A")

        checkpoint = svc._read_checkpoint("RUN-1")
        assert list(checkpoint["dispositions"]) == ["ARCH-A"]
        assert checkpoint["dispositions"]["ARCH-A"]["result"] == "confirmed"

        # 3. 中断当下：新查询立即能取到 A（按事件时刻，而非日期猜测）
        hits = svc.query_event_index(POINT, f"{DAY}T10:05:00")
        assert len(hits) == 1
        assert hits[0]["archive_id"] == "ARCH-A"
        assert hits[0]["confirmed"] is True
        # 区间外的时刻取不到
        assert svc.query_event_index(POINT, f"{DAY}T11:00:00") == []

        # 4. 值班员按事件时段发起一次实际重算（热区有效更正）
        replay = svc.operator_replay_by_event("RUN-1", POINT, EVENT_START, EVENT_END)
        assert "error" not in replay
        replay_row = next(r for r in store.rows("range_replay")
                          if r["重采编号"] == replay["replay_no"])
        # 重算任务携带的是核验过的实际起止时刻
        assert replay_row["起始时刻"] == EVENT_START
        assert replay_row["截止时刻"] == EVENT_END
        assert replay_row["status"] == "已完成"
        svc.accept_hot_correction("RUN-1", POINT, DAY, note="热区某日有效更正，老日分区仍由历史记录使用")

        # 5. 同时按 B 的旧编号核对原件（迁移期间旧引用保读）
        legacy_b = svc.lookup_legacy_ref("LEG-B")
        assert legacy_b is not None
        assert legacy_b["cold_location"] == "/cold/archive/location-b"
        assert svc.lookup_legacy_ref("LEG-A")["archive_id"] == "ARCH-A"

        # 6. 重启后续跑：新实例，从处置记录与实际索引核对，A 不重复处置
        restarted = SeriesIndexService()
        resume = restarted.migrate_cold_references("RUN-1", ["ARCH-A", "ARCH-B"])
        assert resume["resumed_skipped"] == ["ARCH-A"]
        assert resume["unconfirmed"] == ["ARCH-B"]
        assert resume["migrated"] == []

        # A 只能形成一个有效引用
        assert restarted.count_valid_refs("ARCH-A") == 1
        # B 是未确认占位，事件区间为空，任何日期查询都不会命中它
        b_refs = [r for r in restarted._list_event_refs() if r["archive_id"] == "ARCH-B"]
        assert len(b_refs) == 1 and b_refs[0]["confirmed"] is False
        assert b_refs[0]["event_start"] is None and b_refs[0]["event_end"] is None
        all_hits = restarted.query_event_index(POINT, f"{DAY}T10:05:00")
        assert all(r["archive_id"] != "ARCH-B" for r in all_hits)

        # 再次续跑保持幂等：A 仍只有一个有效引用
        again = restarted.migrate_cold_references("RUN-1", ["ARCH-A", "ARCH-B"])
        assert again["resumed_skipped"] == ["ARCH-A", "ARCH-B"]
        assert restarted.count_valid_refs("ARCH-A") == 1

        # 7. 撤回新版：新索引引用全部撤回，旧查询仍定位两份原档
        rollback = restarted.rollback_migration("RUN-1")
        assert rollback["withdrawn_refs"] == 2
        assert rollback["legacy_refs_kept"] == 2
        assert restarted._list_event_refs() == []
        assert restarted.lookup_legacy_ref("LEG-A")["archive_id"] == "ARCH-A"
        assert restarted.lookup_legacy_ref("LEG-B")["archive_id"] == "ARCH-B"

        # 已撤回的运行不得继续追加迁移
        err = restarted.migrate_cold_references("RUN-1", ["ARCH-A"])
        assert "error" in err and "已撤回" in err["error"]

        # 操作流水完整记录中断、重算、更正与撤回
        ops = [json.loads(line) for line in
               restarted._op_log_file("RUN-1").read_text(encoding="utf-8").splitlines()]
        events = {op["event"] for op in ops}
        assert {"crash_simulated", "operator_replay", "hot_correction",
                "rollback", "resume_skip"} <= events

    def test_claimed_interval_mismatch_is_not_confirmed(self, workdir):
        """声明区间与产物实际事件区间不一致时不得确认，更不能写入猜测日期。"""
        from app.services.series_index import SeriesIndexService

        svc = SeriesIndexService()
        artifact = workdir / "cold_source" / "ARCH-A2_artifact.jsonl"
        _write_artifact(artifact)
        svc.register_cold_archive({
            "archive_id": "ARCH-A2", "point": POINT, "legacy_no": "LEG-A2",
            "cold_location": "/cold/archive/location-a2",
            "clock_evidence": True, "artifact_file": str(artifact),
            # 故意声明成错误的日期
            "claimed_start": f"{DAY}T08:00:00",
            "claimed_end": f"{DAY}T08:10:00",
        })
        verdict = svc.verify_archive_interval("ARCH-A2")
        assert verdict["confirmed"] is False
        assert "不一致" in verdict["reason"]
        assert verdict["event_start"] is None

    def test_duplicate_archive_registration_rejected(self, workdir):
        from app.services.series_index import SeriesIndexService

        svc = SeriesIndexService()
        _register_archives(svc, workdir)
        again = svc.register_cold_archive({
            "archive_id": "ARCH-A", "point": POINT, "legacy_no": "LEG-A",
            "cold_location": "/cold/archive/location-a",
            "clock_evidence": False, "artifact_file": "",
        })
        assert "error" in again and "重复登记" in again["error"]

    def test_migrate_unknown_archive_rejected(self, workdir):
        from app.services.series_index import SeriesIndexService

        svc = SeriesIndexService()
        result = svc.migrate_cold_references("RUN-X", ["ARCH-NOPE"])
        assert "error" in result and "未登记" in result["error"]
