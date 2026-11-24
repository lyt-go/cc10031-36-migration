"""序列索引领域服务：维护时序数据按时间分区的点区间索引与存储位置登记。
按时间分区扫描归并结果数据构建序列索引分区并写入索引元数据文件。

冷档分区引用迁移（归档部署上线）：
- 新索引只关联“可确认”的冷档引用：能从产物复查出完整事件时段的老档，
  以实际扫描到的事件起止时间入索引；只有冷档位置和旧编号的老档，
  仅登记为未确认占位，绝不按历史顺序号猜测日期。
- 迁移进度以逐档处置记录（checkpoint）为准；中断后继续时核对处置记录
  与索引文件中的实际引用，而不是根据历史数据里的顺序号推断。
- 旧冷档引用（按已知编号复核原件）在迁移期间与撤回新版后始终保持可读。
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SeriesIndexService(BaseModuleService):
    MODULE = "series_index"
    ENTITY = "索引分区"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["索引编号", "测点编号", "时间分区", "点区间", "索引粒度", "索引条数", "存储位置", "索引版本"]
    REQUIRED_FIELDS = ["索引编号", "测点编号", "时间分区"]
    STATUS_ORDER = ["草稿", "待构建", "已构建", "已失效"]
    ACTION_RULES = {"启动构建": "待构建", "确认构建": "已构建", "标记失效": "已失效"}
    # 数据存储操作：扫描归并窗口结果按时间分区构建序列索引分区写入索引元数据
    NEGATIVE_ACTIONS = ["标记失效"]

    def build_index(self, index_id: int) -> dict[str, Any]:
        """按时间分区扫描归并窗口结果数据构建序列索引分区并写入索引存储元数据文件。"""
        entry = store.find(self.MODULE, index_id)
        if entry is None:
            return {"error": f"索引分区 {index_id} 不存在"}
        self.run_action(index_id, "启动构建")
        point_key = entry["测点编号"]
        partition = entry.get("时间分区", time.strftime("%Y-%m-%d", time.localtime()))

        # 扫描归并窗口数据文件
        rollup_dir = self._data_dir("rollup_windows")
        indexed_points = []
        if rollup_dir.exists():
            for f in rollup_dir.glob("*_rollup.jsonl"):
                records = self._read_jsonl(f)
                # 数据产物输出：将索引扫描结果写入序列索引数据JSONL文件
                for r in records:
                    indexed_points.append({
                        "index_id": entry["索引编号"],
                        "point": r.get("point", point_key),
                        "time_partition": partition,
                        "window_index": r.get("window_start", 0),
                        "record_count": r.get("record_count", 0),
                        "rollup_value": r.get("rollup_value", 0),
                    })
        # 也扫描降采样数据
        ds_dir = self._data_dir("downsampled")
        if ds_dir.exists():
            for f in ds_dir.glob("*_downsampled.jsonl"):
                records = self._read_jsonl(f)
                for r in records:
                    indexed_points.append({
                        "index_id": entry["索引编号"],
                        "point": r.get("point", point_key),
                        "time_partition": partition,
                        "window_index": r.get("window_index", 0),
                        "aggregated_value": r.get("aggregated_value", 0),
                    })
        # 将索引扫描结果写入序列索引数据文件
        output_file = self._data_dir("series_index") / f"{entry['索引编号']}_index.jsonl"
        written = self._write_jsonl(output_file, indexed_points)
        entry["索引条数"] = str(written)
        entry["点区间"] = f"1-{written}"
        entry["存储位置"] = str(output_file)
        entry["索引版本"] = "v1.0"
        self.run_action(index_id, "确认构建")
        return {"index_id": entry["索引编号"], "point": point_key,
                "partition": partition, "indexed_count": written}

    def build_all_partitions(self, point_key: str) -> dict[str, Any]:
        """为指定测点构建所有时间分区的序列索引数据集。"""
        partitions = ["2026-09-01", "2026-09-02", "2026-09-03"]
        results = []
        for part in partitions:
            tid = max((int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0) + 1
            self._auto_create_seed(self.MODULE, {
                "索引编号": f"IDX-{point_key}-{part}",
                "测点编号": point_key,
                "时间分区": part,
                "点区间": "0-0",
                "索引粒度": "1h",
                "索引条数": "0",
                "存储位置": "",
                "索引版本": "v1.0",
            })
            r = self.build_index(tid)
            results.append(r)
        return {"point": point_key, "partitions_built": len(results), "results": results}

    # ------------------------------------------------------------------
    # 冷档分区引用迁移：基于事件区间的新索引 + 旧冷档引用保读
    # ------------------------------------------------------------------

    def _cold_dir(self) -> Path:
        return self._data_dir("cold_archives")

    def _registry_file(self) -> Path:
        return self._cold_dir() / "registry.jsonl"

    def _legacy_refs_file(self) -> Path:
        return self._cold_dir() / "legacy_cold_refs.jsonl"

    def _event_index_file(self) -> Path:
        return self._data_dir("series_index") / "event_index.jsonl"

    def _run_dir(self, run_id: str) -> Path:
        return self._cold_dir() / "migration_runs" / run_id

    def _checkpoint_file(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "checkpoint.json"

    def _op_log_file(self, run_id: str) -> Path:
        return self._run_dir(run_id) / "operations.jsonl"

    def _overwrite_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        return len(records)

    def _append_op_log(self, run_id: str, event: str, detail: dict[str, Any]) -> None:
        """迁移运行操作流水：每个实际处置动作都落盘，供中断后核对。"""
        record = {"at": time.time(), "run_id": run_id, "event": event, **detail}
        self._write_jsonl(self._op_log_file(run_id), [record])

    # -- 老档登记 --

    def register_cold_archive(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记一份老冷档。

        clock_evidence=True 且给出 artifact_file 时，该档有时钟依据、可从产物
        复查完整事件时段；否则只保留冷档位置与旧编号，事件区间保持不可确认。
        """
        archive_id = str(values.get("archive_id", "")).strip()
        point = str(values.get("point", "")).strip()
        if not archive_id or not point:
            return {"error": "登记冷档缺少 archive_id 或 point"}
        archives = self._read_jsonl(self._registry_file())
        if any(a.get("archive_id") == archive_id for a in archives):
            return {"error": f"冷档 {archive_id} 已登记，不得重复登记"}
        entry = {
            "archive_id": archive_id,
            "point": point,
            "legacy_no": values.get("legacy_no", ""),
            "cold_location": values.get("cold_location", ""),
            "clock_evidence": bool(values.get("clock_evidence", False)),
            "artifact_file": values.get("artifact_file", ""),
            "claimed_start": values.get("claimed_start"),
            "claimed_end": values.get("claimed_end"),
            "checksum": values.get("checksum", ""),
        }
        self._write_jsonl(self._registry_file(), [entry])
        return entry

    def _load_registry(self) -> dict[str, dict[str, Any]]:
        return {a["archive_id"]: a for a in self._read_jsonl(self._registry_file())}

    def register_legacy_ref(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记历史取查依赖的旧冷档引用（按已知编号复核原件）。"""
        legacy_no = str(values.get("legacy_no", "")).strip()
        if not legacy_no:
            return {"error": "旧冷档引用缺少 legacy_no"}
        refs = self._read_jsonl(self._legacy_refs_file())
        if any(r.get("legacy_no") == legacy_no for r in refs):
            return {"error": f"旧编号 {legacy_no} 已存在"}
        entry = {
            "legacy_no": legacy_no,
            "archive_id": values.get("archive_id", ""),
            "point": values.get("point", ""),
            "cold_location": values.get("cold_location", ""),
            "artifact_file": values.get("artifact_file", ""),
            "original_checksum": values.get("original_checksum", ""),
        }
        self._write_jsonl(self._legacy_refs_file(), [entry])
        return entry

    def lookup_legacy_ref(self, legacy_no: str) -> dict[str, Any] | None:
        """旧查询：按已知编号定位原档（迁移与回滚均不破坏该入口）。"""
        for ref in self._read_jsonl(self._legacy_refs_file()):
            if ref.get("legacy_no") == legacy_no:
                return ref
        return None

    # -- 事件区间核验 --

    def verify_archive_interval(self, archive_id: str) -> dict[str, Any]:
        """核验老档的实际事件时间区间。

        有时钟依据且产物可读：扫描产物中的事件时刻，取实际最早/最晚时刻，
        并与登记的声明区间比对，全部吻合才确认。
        无时钟依据/无产物：返回 confirmed=False，不产生任何猜测日期。
        """
        archives = self._load_registry()
        archive = archives.get(archive_id)
        if archive is None:
            return {"error": f"冷档 {archive_id} 未登记"}
        if not archive.get("clock_evidence") or not archive.get("artifact_file"):
            return {
                "archive_id": archive_id,
                "confirmed": False,
                "reason": "无时钟依据且无产物可复查，仅保留冷档位置与旧编号",
                "event_start": None,
                "event_end": None,
            }
        artifact = Path(archive["artifact_file"])
        if not artifact.exists():
            return {"archive_id": archive_id, "confirmed": False,
                    "reason": f"产物文件不存在：{artifact}",
                    "event_start": None, "event_end": None}
        moments: list[datetime] = []
        for rec in self._read_jsonl(artifact):
            raw = rec.get("event_time") or rec.get("timestamp")
            if not raw:
                continue
            try:
                moments.append(datetime.fromisoformat(str(raw)))
            except ValueError:
                continue
        if not moments:
            return {"archive_id": archive_id, "confirmed": False,
                    "reason": "产物中没有可解析的事件时刻",
                    "event_start": None, "event_end": None}
        actual_start = min(moments).isoformat()
        actual_end = max(moments).isoformat()
        claimed_start = archive.get("claimed_start")
        claimed_end = archive.get("claimed_end")
        if claimed_start and claimed_end and (
                claimed_start != actual_start or claimed_end != actual_end):
            return {"archive_id": archive_id, "confirmed": False,
                    "reason": "声明区间与产物实际事件区间不一致",
                    "claimed_start": claimed_start, "claimed_end": claimed_end,
                    "actual_start": actual_start, "actual_end": actual_end,
                    "event_start": None, "event_end": None}
        content = artifact.read_text(encoding="utf-8")
        return {
            "archive_id": archive_id,
            "confirmed": True,
            "event_start": actual_start,
            "event_end": actual_end,
            "event_count": len(moments),
            "checksum": self._compute_checksum(content),
        }

    # -- 迁移与中断恢复 --

    def _read_checkpoint(self, run_id: str) -> dict[str, Any]:
        cp_file = self._checkpoint_file(run_id)
        if not cp_file.exists():
            return {"run_id": run_id, "status": "running", "dispositions": {}}
        with open(cp_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_checkpoint(self, run_id: str, checkpoint: dict[str, Any]) -> None:
        cp_file = self._checkpoint_file(run_id)
        cp_file.parent.mkdir(parents=True, exist_ok=True)
        checkpoint["_updated_at"] = time.time()
        with open(cp_file, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2, default=str)

    def _list_event_refs(self) -> list[dict[str, Any]]:
        return self._read_jsonl(self._event_index_file())

    def migrate_cold_references(
        self,
        run_id: str,
        archive_ids: list[str] | None = None,
        crash_after: str | None = None,
    ) -> dict[str, Any]:
        """执行（或中断后继续）冷档引用迁移。

        进度只以逐档处置记录 checkpoint 为准：继续时先核对处置记录与索引
        文件里的实际引用，已处置且引用存在的档跳过，绝不按顺序号补日期。
        crash_after 用于演练在指定档处置完成后模拟进程中断。
        """
        registry = self._load_registry()
        targets = archive_ids if archive_ids is not None else list(registry)
        unknown = [a for a in targets if a not in registry]
        if unknown:
            return {"error": f"存在未登记冷档：{', '.join(unknown)}"}

        checkpoint = self._read_checkpoint(run_id)
        if checkpoint.get("status") == "rolled_back":
            return {"error": f"迁移运行 {run_id} 已撤回，不能继续追加，请新建运行"}
        dispositions: dict[str, Any] = checkpoint.setdefault("dispositions", {})
        refs = self._list_event_refs()
        migrated, resumed_skipped, unresolved = [], [], []

        for archive_id in targets:
            archive = registry[archive_id]
            prior = dispositions.get(archive_id)
            has_ref = any(r.get("archive_id") == archive_id for r in refs)
            if prior and prior.get("result") in ("confirmed", "unconfirmed") and has_ref:
                # 从实际处置记录核对：该档已落引用，跳过且不产生重复有效引用
                resumed_skipped.append(archive_id)
                self._append_op_log(run_id, "resume_skip", {
                    "archive_id": archive_id,
                    "disposition_result": prior.get("result"),
                })
                continue

            verification = self.verify_archive_interval(archive_id)
            if "error" in verification:
                self._append_op_log(run_id, "verify_failed", {
                    "archive_id": archive_id, "reason": verification["error"]})
                continue

            if verification["confirmed"]:
                # 同一冷档只允许形成一个有效引用
                if any(r.get("archive_id") == archive_id and r.get("confirmed")
                       for r in self._list_event_refs()):
                    resumed_skipped.append(archive_id)
                    continue
                ref = {
                    "run_id": run_id,
                    "archive_id": archive_id,
                    "point": archive["point"],
                    "confirmed": True,
                    "event_start": verification["event_start"],
                    "event_end": verification["event_end"],
                    "event_count": verification["event_count"],
                    "cold_location": archive["cold_location"],
                    "legacy_no": archive.get("legacy_no", ""),
                    "checksum": verification["checksum"],
                }
                self._write_jsonl(self._event_index_file(), [ref])
                result = "confirmed"
                migrated.append(archive_id)
            else:
                # 无法确认区间：只登记位置与旧编号的占位，事件区间留空，禁止猜日期
                stub = {
                    "run_id": run_id,
                    "archive_id": archive_id,
                    "point": archive["point"],
                    "confirmed": False,
                    "event_start": None,
                    "event_end": None,
                    "cold_location": archive["cold_location"],
                    "legacy_no": archive.get("legacy_no", ""),
                    "note": verification.get("reason", "区间不可确认"),
                }
                self._write_jsonl(self._event_index_file(), [stub])
                result = "unconfirmed"
                unresolved.append(archive_id)

            # 每处置完一档立即落处置记录，保证中断后可从记录核对
            dispositions[archive_id] = {
                "archive_id": archive_id,
                "result": result,
                "event_start": verification.get("event_start"),
                "event_end": verification.get("event_end"),
                "cold_location": archive["cold_location"],
                "legacy_no": archive.get("legacy_no", ""),
                "at": time.time(),
            }
            self._write_checkpoint(run_id, checkpoint)
            self._append_op_log(run_id, "migrated", {
                "archive_id": archive_id, "result": result})

            if crash_after == archive_id:
                self._append_op_log(run_id, "crash_simulated",
                                    {"archive_id": archive_id})
                raise RuntimeError(f"模拟迁移中断：{archive_id} 处置完成后进程退出")

        return {
            "run_id": run_id,
            "migrated": migrated,
            "resumed_skipped": resumed_skipped,
            "unconfirmed": unresolved,
            "checkpoint": dispositions,
        }

    # -- 新查询：按事件时间区间取索引 --

    def query_event_index(self, point: str, event_time: str) -> list[dict[str, Any]]:
        """新查询：仅命中已确认事件区间且覆盖该事件时刻的有效引用。

        未确认占位没有事件区间，永远不会被猜成某个日期而命中。
        """
        try:
            moment = datetime.fromisoformat(event_time)
        except ValueError:
            return []
        hits = []
        for ref in self._list_event_refs():
            if not ref.get("confirmed") or ref.get("point") != point:
                continue
            start = datetime.fromisoformat(ref["event_start"])
            end = datetime.fromisoformat(ref["event_end"])
            if start <= moment <= end:
                hits.append(ref)
        return hits

    def count_valid_refs(self, archive_id: str) -> int:
        """统计某冷档当前的有效（已确认）引用数，用于唯一性核对。"""
        return sum(
            1 for r in self._list_event_refs()
            if r.get("archive_id") == archive_id and r.get("confirmed")
        )

    # -- 迁移期间的值班员操作 --

    def operator_replay_by_event(
        self, run_id: str, point: str, event_start: str, event_end: str
    ) -> dict[str, Any]:
        """值班员按事件时段发起一次实际重算（热区更正场景）。

        重算任务携带的是核验过的实际起止时刻，而不是顺序号推测的日期。
        """
        from app.services.range_replay import RangeReplayService

        for label, value in (("起始时刻", event_start), ("截止时刻", event_end)):
            try:
                datetime.fromisoformat(value)
            except (TypeError, ValueError):
                return {"error": f"重算{label}不是有效事件时间：{value}"}
        rows = store.rows("range_replay")
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        replay_no = f"REPLAY-EVENT-{new_id}"
        rows.append({
            "id": new_id,
            "重采编号": replay_no,
            "测点编号": point,
            "起始时刻": event_start,
            "截止时刻": event_end,
            "重采原因": "值班员按事件时段发起实际重算",
            "覆盖方式": "追加",
            "重采点数": "0",
            "重采水位": "0",
            "status": "待重采", "pending": True, "abnormal": False,
        })
        result = RangeReplayService().replay_range(new_id)
        self._append_op_log(run_id, "operator_replay", {
            "point": point, "event_start": event_start, "event_end": event_end,
            "replay_no": replay_no, "result": result,
        })
        return {"replay_no": replay_no, **result}

    def accept_hot_correction(
        self, run_id: str, point: str, partition_day: str, note: str = ""
    ) -> dict[str, Any]:
        """记录热区在迁移期间收到某日有效更正；老日分区仍由历史记录使用。"""
        record = {
            "at": time.time(), "run_id": run_id, "point": point,
            "partition_day": partition_day, "note": note,
        }
        self._write_jsonl(self._cold_dir() / "hot_corrections.jsonl", [record])
        self._append_op_log(run_id, "hot_correction", record)
        return record

    # -- 撤回新版 --

    def rollback_migration(self, run_id: str) -> dict[str, Any]:
        """撤回本次迁移生成的新版事件区间索引。

        只清理新索引与运行处置状态；旧冷档引用文件原样保留，旧查询仍可定位原件。
        """
        refs = self._list_event_refs()
        kept = [r for r in refs if r.get("run_id") != run_id]
        withdrawn = len(refs) - len(kept)
        self._overwrite_jsonl(self._event_index_file(), kept)

        checkpoint = self._read_checkpoint(run_id)
        checkpoint["status"] = "rolled_back"
        self._write_checkpoint(run_id, checkpoint)
        self._append_op_log(run_id, "rollback", {"withdrawn_refs": withdrawn})
        return {"run_id": run_id, "withdrawn_refs": withdrawn,
                "legacy_refs_kept": len(self._read_jsonl(self._legacy_refs_file()))}
