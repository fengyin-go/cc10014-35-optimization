"""查询编排执行引擎：断点续做、并行分支合并与编排步骤契约同步。

针对现有编排的两个老问题：
1. 失败重试会把已完成步骤再跑一遍 —— 续做只从断点步骤继续，已完成分支复用既有产物；
2. 并行分支的合并结果随完成顺序漂移 —— 合并顺序固定取步骤序列，与完成先后无关。

设计约定：
- 断点、合并顺序都是步骤执行记录与依赖产物的纯函数，因此数据库依赖变更、
  环境切换、上线部署之后，只要记录还在，断点就不会漂移；
- 步骤执行记录、依赖产物、合并结果三方对得上才允许合并，否则重试会把同一
  分支重复并入；合并按“记录+产物”签名幂等，同样的输入不会合并出第二份；
- 状态只能按顺序单向流转，不允许跳级；失败重试新开 attempt 记录，不改写旧记录；
- 合并结果同步写入编排步骤契约，转换模块按契约读取稳定顺序和完整字段。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.store import store

FLOW_MODULE = "flow"

RUN_TABLE = "flow_run"            # 执行实例
STEP_TABLE = "flow_step_record"   # 步骤执行记录
ARTIFACT_TABLE = "flow_artifact"  # 依赖产物
MERGE_TABLE = "flow_merge"        # 合并结果
CONTRACT_TABLE = "flow_contract"  # 编排步骤契约（同步给转换模块读取）

# 步骤状态只能按顺序单向流转，不跳级；执行中可转已失败（终态）。
STEP_STATUS_ORDER = ["待执行", "执行中", "已完成"]
STEP_FAILURE_STATUS = "已失败"
STEP_TRANSITIONS = {
    "待执行": ["执行中"],
    "执行中": ["已完成", "已失败"],
}

# 执行实例状态：执行中 → 待续做 → 已完成，只能向前，不允许回退；已失败为终态。
RUN_STATUS_ORDER = ["执行中", "待续做", "已完成"]
RUN_FAILURE_STATUS = "已失败"
RUN_TRANSITIONS = {
    "执行中": ["待续做", "已完成", "已失败"],
    "待续做": ["已完成", "已失败"],
}

# 不同口径并存时的优先级（靠前者优先）；判定过程必须留下依据。
CALIBER_PRIORITY = ["标准口径", "灰度口径", "临时口径"]
DEFAULT_CALIBER = "标准口径"
CALIBER_RULE = "口径并存时按优先级 " + " > ".join(CALIBER_PRIORITY) + " 选定，未选中的批次不并入"

# 多个恢复边界并存时，只形成一套断点与执行顺序。
BOUNDARY_RULE = "同一编排出现多个恢复边界时，按步骤序号取最早未完成步骤作为唯一断点，合并顺序固定为步骤序列"


class FlowRunError(ValueError):
    """编排执行规则被违反时抛出，消息要能让人直接看懂原因。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _rows(table: str) -> list[dict[str, Any]]:
    return store.rows(table)


def _next_id(table: str) -> int:
    return max((int(row.get("id", 0)) for row in _rows(table)), default=0) + 1


def _parse_steps(raw: Any) -> list[str]:
    """把步骤序列统一成有序列表：支持数组或逗号/箭头分隔的字符串。"""
    if isinstance(raw, (list, tuple)):
        steps = [str(item).strip() for item in raw]
    else:
        text = str(raw or "")
        for sep in ["->", "→", "，", "、", ",", ";", "；"]:
            text = text.replace(sep, "|")
        steps = [part.strip() for part in text.split("|")]
    return [step for step in steps if step]


def advance_step_status(record: dict[str, Any], target: str) -> None:
    """步骤状态只能按 待执行→执行中→已完成/已失败 单向流转，不允许跳级或回退。"""
    current = str(record.get("status") or "")
    allowed = STEP_TRANSITIONS.get(current, [])
    if target not in allowed:
        if not allowed:
            raise FlowRunError(
                f"步骤「{record.get('步骤名')}」第{record.get('attempt')}次执行已处于终态「{current}」，"
                f"状态只能单向流转，不能再转到「{target}」"
            )
        raise FlowRunError(
            f"步骤「{record.get('步骤名')}」第{record.get('attempt')}次执行的状态不能从「{current}」跳到「{target}」："
            f"只能按 待执行→执行中→已完成（执行中可转已失败）单向流转，不允许跳级或回退"
        )
    record["status"] = target


def advance_run_status(run: dict[str, Any], target: str) -> None:
    """执行实例状态只能按 执行中→待续做→已完成 单向流转，不允许回退。"""
    current = str(run.get("状态") or "")
    allowed = RUN_TRANSITIONS.get(current, [])
    if target not in allowed:
        if not allowed:
            raise FlowRunError(
                f"执行实例 {run.get('run_id')} 已处于终态「{current}」，状态只能单向流转，不能再转到「{target}」"
            )
        raise FlowRunError(
            f"执行实例 {run.get('run_id')} 的状态不能从「{current}」跳到「{target}」："
            f"只能按 执行中→待续做→已完成 单向流转，不允许跳级或回退"
        )
    run["状态"] = target


class FlowRunService:
    """编排执行、断点续做与结果合并。"""

    # ---------- 查询 ----------

    def list_runs(self, entry_id: int) -> list[dict[str, Any]]:
        flow = self._flow(entry_id)
        return [row for row in _rows(RUN_TABLE) if row.get("流程编号") == flow["流程编号"]]

    def get_run(self, entry_id: int, run_id: str) -> dict[str, Any]:
        flow = self._flow(entry_id)
        run = self._find_run(flow["流程编号"], run_id)
        return self._run_detail(run)

    def list_contracts(self, entry_id: int) -> list[dict[str, Any]]:
        flow = self._flow(entry_id)
        return [row for row in _rows(CONTRACT_TABLE) if row.get("流程编号") == flow["流程编号"]]

    # ---------- 发起执行 ----------

    def start_run(self, entry_id: int, values: dict[str, Any]) -> dict[str, Any]:
        flow = self._flow(entry_id)
        if flow.get("status") != "已启用":
            raise FlowRunError(f"编排流程当前状态「{flow.get('status')}」，只有「已启用」的流程才能发起执行")
        steps = _parse_steps(values.get("步骤序列")) or _parse_steps(flow.get("步骤序列"))
        if not steps:
            raise FlowRunError("步骤序列为空，无法发起执行")
        outcomes: dict[str, Any] = dict(values.get("步骤结果") or {})
        unknown = [name for name in outcomes if name not in steps]
        if unknown:
            raise FlowRunError(f"步骤结果里的 {'、'.join(unknown)} 不在步骤序列 {steps} 中")

        run_id = f"{flow['流程编号']}-RUN-{len(self.list_runs(entry_id)) + 1}"
        run = {
            "id": _next_id(RUN_TABLE),
            "run_id": run_id,
            "流程编号": flow["流程编号"],
            "步骤序列": steps,
            "状态": RUN_STATUS_ORDER[0],
            "断点步骤": None,
            "创建时间": _now(),
            "更新时间": _now(),
        }
        _rows(RUN_TABLE).append(run)
        for index, step in enumerate(steps):
            _rows(STEP_TABLE).append(self._new_record(run, step, index, attempt=1, 来源="本系统"))

        # 完成顺序只决定完成序号，合并顺序与它无关。
        completion = [s for s in _parse_steps(values.get("完成顺序")) if s in steps]
        completion += [s for s in steps if s not in completion]
        for step in completion:
            outcome = outcomes.get(step)
            if outcome is not None:
                self._apply_outcome(run, step, outcome)

        self._refresh_run(run)
        merge = self._merge_if_complete(run)
        return self._run_detail(run, extra={"合并结果": merge, "恢复边界判定依据": self._recovery_boundary(run)})

    # ---------- 断点续做 ----------

    def resume_run(self, entry_id: int, run_id: str, values: dict[str, Any]) -> dict[str, Any]:
        """接收已有步骤执行记录和依赖产物所关联的原始数据，从断点步骤续做，
        重新合并已完成分支并输出合并数据集。已完成分支一律不重跑。"""
        flow = self._flow(entry_id)
        run = self._find_run(flow["流程编号"], run_id)
        if run["状态"] == RUN_FAILURE_STATUS:
            raise FlowRunError(f"执行实例 {run_id} 已处于终态「已失败」，不能续做")

        # 1) 接收外部步骤执行记录与依赖产物（数据库依赖变更、环境切换、上线部署后带过来的）。
        conflicts = self._ingest_external(run, values)

        # 2) 一致性清理：先顺着数据流定位无效批次，再求唯一断点。
        self._locate_invalid_batches(run["run_id"])
        boundary = self._recovery_boundary(run)
        checkpoint = boundary["选定断点"]["步骤"] if boundary["选定断点"] else None

        # 3) 续做：只执行断点及之后仍未完成的分支；已完成分支有一致产物，跳过不重跑。
        outcomes: dict[str, Any] = dict(values.get("续做结果") or {})
        unknown = [name for name in outcomes if name not in run["步骤序列"]]
        if unknown:
            raise FlowRunError(f"续做结果里的 {'、'.join(unknown)} 不在步骤序列 {run['步骤序列']} 中")
        rerun: list[str] = []
        skipped: list[str] = []
        notices: list[str] = []
        for step in run["步骤序列"]:
            outcome = outcomes.get(step)
            if outcome is None:
                continue
            if self._consistent_success(run["run_id"], step) is not None:
                skipped.append(step)
                notices.append(f"步骤「{step}」已完成且产物一致，跳过不重跑")
                continue
            self._apply_outcome(run, step, outcome)
            rerun.append(step)

        # 4) 推进运行状态，重新合并已完成分支并输出合并数据集。
        self._refresh_run(run)
        merge = self._merge_if_complete(run, recorded_boundary=boundary)
        invalids = self._locate_invalid_batches(run["run_id"])
        return self._run_detail(run, extra={
            "断点步骤": checkpoint,
            "续做步骤": rerun,
            "跳过步骤": skipped,
            "提示": notices,
            "外部记录冲突": conflicts,
            "恢复边界判定依据": boundary,
            "口径判定依据": self._caliber_basis(run, merge),
            "合并结果": merge,
            "无效批次": invalids,
        })

    # ---------- 复核 ----------

    def audit_run(self, entry_id: int, run_id: str) -> dict[str, Any]:
        """复核视图：步骤执行记录、断点、合并顺序、无效批次与一致性校验。"""
        flow = self._flow(entry_id)
        run = self._find_run(flow["流程编号"], run_id)
        invalids = self._locate_invalid_batches(run["run_id"])
        boundary = self._recovery_boundary(run)
        merge = self._latest_merge(run["run_id"])
        return {
            "流程编号": flow["流程编号"],
            "run_id": run["run_id"],
            "运行状态": run["状态"],
            "断点步骤": run["断点步骤"],
            "步骤序列": run["步骤序列"],
            "步骤执行记录": self._run_records(run["run_id"]),
            "依赖产物": self._run_artifacts(run["run_id"]),
            "合并顺序": merge["合并顺序"] if merge else None,
            "合并结果": merge,
            "无效批次": invalids,
            "一致性校验": self._consistency_checks(run, merge),
            "口径判定依据": self._caliber_basis(run, merge),
            "恢复边界判定依据": boundary,
        }

    # ---------- 内部：记录与产物 ----------

    def _flow(self, entry_id: int) -> dict[str, Any]:
        flow = store.find(FLOW_MODULE, entry_id)
        if flow is None:
            raise FlowRunError(f"编排流程 {entry_id} 不存在或已归档")
        return flow

    def _find_run(self, flow_code: str, run_id: str) -> dict[str, Any]:
        for row in _rows(RUN_TABLE):
            if row.get("流程编号") == flow_code and row.get("run_id") == run_id:
                return row
        raise FlowRunError(f"执行实例 {run_id} 不存在或已归档")

    def _run_records(self, run_id: str) -> list[dict[str, Any]]:
        records = [row for row in _rows(STEP_TABLE) if row.get("run_id") == run_id]
        return sorted(records, key=lambda row: (int(row.get("步骤序号", 0)), int(row.get("attempt", 0))))

    def _run_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        return [row for row in _rows(ARTIFACT_TABLE) if row.get("run_id") == run_id]

    def _latest_record(self, run_id: str, step: str) -> dict[str, Any] | None:
        records = [row for row in self._run_records(run_id) if row.get("步骤名") == step]
        return max(records, key=lambda row: int(row.get("attempt", 0)), default=None)

    def _new_record(self, run: dict[str, Any], step: str, index: int, attempt: int, 来源: str) -> dict[str, Any]:
        return {
            "id": _next_id(STEP_TABLE),
            "记录编号": f"{run['run_id']}-{step}-第{attempt}次",
            "run_id": run["run_id"],
            "流程编号": run["流程编号"],
            "步骤名": step,
            "步骤序号": index,
            "attempt": attempt,
            "status": STEP_STATUS_ORDER[0],
            "产物编号": None,
            "批次号": None,
            "口径": None,
            "失败原因": None,
            "完成序号": None,
            "来源": 来源,
            "开始时间": None,
            "完成时间": None,
        }

    def _apply_outcome(self, run: dict[str, Any], step: str, outcome: dict[str, Any]) -> None:
        """把一次执行结果落到步骤记录上：在途记录继续推进，已终结的重开 attempt。"""
        record = self._latest_record(run["run_id"], step)
        if record is None or record["status"] in (STEP_STATUS_ORDER[-1], STEP_FAILURE_STATUS):
            attempt = int(record["attempt"]) + 1 if record else 1
            record = self._new_record(run, step, run["步骤序列"].index(step), attempt, 来源="本系统")
            _rows(STEP_TABLE).append(record)
        if record["status"] == STEP_STATUS_ORDER[0]:
            advance_step_status(record, "执行中")
            record["开始时间"] = _now()
        result = str(outcome.get("结果") or "").strip()
        if result == "已完成":
            artifact = self._write_artifact(run, record, outcome)
            record["产物编号"] = artifact["产物编号"]
            record["批次号"] = artifact["批次号"]
            record["口径"] = artifact["口径"]
            advance_step_status(record, "已完成")
        elif result == STEP_FAILURE_STATUS:
            record["失败原因"] = str(outcome.get("失败原因") or "未说明")
            advance_step_status(record, STEP_FAILURE_STATUS)
        else:
            raise FlowRunError(f"步骤「{step}」的结果「{result}」无法识别，只接受 已完成/已失败")
        record["完成序号"] = self._completion_seq(run["run_id"])
        record["完成时间"] = _now()

    def _write_artifact(self, run: dict[str, Any], record: dict[str, Any], outcome: dict[str, Any]) -> dict[str, Any]:
        artifact = {
            "id": _next_id(ARTIFACT_TABLE),
            "产物编号": f"ART-{run['run_id']}-{record['步骤名']}-第{record['attempt']}次",
            "run_id": run["run_id"],
            "流程编号": run["流程编号"],
            "步骤名": record["步骤名"],
            "attempt": record["attempt"],
            "批次号": str(outcome.get("批次号") or f"BATCH-{run['run_id']}-{record['步骤名']}-第{record['attempt']}次"),
            "口径": str(outcome.get("口径") or DEFAULT_CALIBER),
            "原始数据": list(outcome.get("原始数据") or []),
            "有效标识": True,
            "无效原因": None,
            "来源": "本系统",
            "生成时间": _now(),
        }
        _rows(ARTIFACT_TABLE).append(artifact)
        return artifact

    def _completion_seq(self, run_id: str) -> int:
        seqs = [int(row["完成序号"]) for row in self._run_records(run_id) if row.get("完成序号") is not None]
        return max(seqs, default=0) + 1

    # ---------- 内部：断点与状态 ----------

    def _consistent_success(self, run_id: str, step: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
        """步骤的一致成功：已完成记录 + 同 attempt 的有效产物。取 attempt 最小者，
        同一分支出现多个成功时只认第一个，重试不会重复合并同一分支。"""
        records = [row for row in self._run_records(run_id) if row.get("步骤名") == step and row.get("status") == "已完成"]
        for record in sorted(records, key=lambda row: int(row.get("attempt", 0))):
            artifact, _losers, _basis = self._select_artifact(run_id, step, int(record["attempt"]))
            if artifact is not None:
                return record, artifact
        return None

    def _select_artifact(
        self, run_id: str, step: str, attempt: int
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], dict[str, Any] | None]:
        """同一步骤同一 attempt 的有效产物可能并存多个口径：按口径优先级选定，
        返回（选定产物，未选中产物，判定依据）。纯查询，不改数据。"""
        candidates = [
            row for row in self._run_artifacts(run_id)
            if row.get("步骤名") == step and int(row.get("attempt", 0)) == attempt and row.get("有效标识")
        ]
        if not candidates:
            return None, [], None

        def rank(artifact: dict[str, Any]) -> tuple[int, str]:
            caliber = str(artifact.get("口径") or DEFAULT_CALIBER)
            order = CALIBER_PRIORITY.index(caliber) if caliber in CALIBER_PRIORITY else len(CALIBER_PRIORITY)
            return order, str(artifact.get("产物编号"))

        candidates.sort(key=rank)
        chosen, losers = candidates[0], candidates[1:]
        basis = None
        calibers = sorted({str(row.get("口径") or DEFAULT_CALIBER) for row in candidates})
        if len(calibers) > 1:
            basis = {
                "步骤": step,
                "并存口径": calibers,
                "选定口径": chosen.get("口径") or DEFAULT_CALIBER,
                "选定产物": chosen.get("产物编号"),
                "判定规则": CALIBER_RULE,
            }
        return chosen, losers, basis

    def _recovery_boundary(self, run: dict[str, Any]) -> dict[str, Any]:
        """唯一断点：候选恢复边界可能不止一个（失败回退、并行分支未完成同时存在），
        一律按步骤序号取最早未完成步骤，只形成一套执行顺序和断点。"""
        candidates = []
        for index, step in enumerate(run["步骤序列"]):
            if self._consistent_success(run["run_id"], step) is not None:
                continue
            latest = self._latest_record(run["run_id"], step)
            candidates.append({
                "步骤": step,
                "步骤序号": index,
                "边界类型": "失败回退" if latest and latest.get("status") == STEP_FAILURE_STATUS else "分支未完成",
                "当前状态": latest.get("status") if latest else "待执行",
            })
        return {
            "候选恢复边界": candidates,
            "选定断点": candidates[0] if candidates else None,
            "判定规则": BOUNDARY_RULE,
        }

    def _refresh_run(self, run: dict[str, Any]) -> None:
        boundary = self._recovery_boundary(run)
        run["断点步骤"] = boundary["选定断点"]["步骤"] if boundary["选定断点"] else None
        incomplete = [item["步骤"] for item in boundary["候选恢复边界"]]
        if not incomplete:
            target = "已完成"
        else:
            failed = any(
                (self._latest_record(run["run_id"], step) or {}).get("status") == STEP_FAILURE_STATUS
                for step in incomplete
            )
            target = "待续做" if failed else "执行中"
        if target != run["状态"]:
            advance_run_status(run, target)
        run["更新时间"] = _now()

    # ---------- 内部：合并与契约 ----------

    def _merge_if_complete(self, run: dict[str, Any], recorded_boundary: dict[str, Any] | None = None) -> dict[str, Any] | None:
        if self._recovery_boundary(run)["选定断点"] is not None:
            return None
        return self._merge(run, recorded_boundary or self._recovery_boundary(run))

    def _merge(self, run: dict[str, Any], boundary: dict[str, Any]) -> dict[str, Any]:
        """按步骤序列合并各分支产物，输出带溯源信息的合并数据集。

        幂等：合并签名 = 各分支（步骤、记录、产物）三元组；签名相同的合并结果
        直接复用，重试不会把同一分支重复并入。断点与恢复边界依据取续做前
        认定的那一套，保证合并结果里只有一套执行顺序和断点。"""
        self._locate_invalid_batches(run["run_id"])
        blocks: list[dict[str, Any]] = []
        fields: list[str] = []
        record_refs: list[str] = []
        artifact_refs: list[str] = []
        caliber_basis: list[dict[str, Any]] = []
        signature: list[list[str]] = []
        for index, step in enumerate(run["步骤序列"]):
            found = self._consistent_success(run["run_id"], step)
            if found is None:
                raise FlowRunError(f"步骤「{step}」缺少一致的成功记录与产物，不能合并")
            record, artifact = found
            _chosen, losers, basis = self._select_artifact(run["run_id"], step, int(record["attempt"]))
            for loser in losers:
                loser["有效标识"] = False
                loser["无效原因"] = (
                    f"口径并存：步骤「{step}」采用「{artifact.get('口径') or DEFAULT_CALIBER}」，"
                    f"本批次口径「{loser.get('口径') or DEFAULT_CALIBER}」未并入"
                )
            if basis:
                caliber_basis.append(basis)
            blocks.append({
                "顺序": index + 1,
                "来源步骤": step,
                "步骤序号": index,
                "attempt": record["attempt"],
                "记录编号": record["记录编号"],
                "产物编号": artifact["产物编号"],
                "批次号": artifact["批次号"],
                "口径": artifact.get("口径") or DEFAULT_CALIBER,
                "数据": artifact.get("原始数据") or [],
            })
            record_refs.append(str(record["记录编号"]))
            artifact_refs.append(str(artifact["产物编号"]))
            signature.append([step, str(record["记录编号"]), str(artifact["产物编号"])])
            for row in artifact.get("原始数据") or []:
                for key in row:
                    if key not in fields:
                        fields.append(key)

        for old in _rows(MERGE_TABLE):
            if old.get("run_id") == run["run_id"] and old.get("合并签名") == signature:
                return old

        merge = {
            "id": _next_id(MERGE_TABLE),
            "合并编号": f"MRG-{run['run_id']}-{len([m for m in _rows(MERGE_TABLE) if m.get('run_id') == run['run_id']]) + 1}",
            "流程编号": run["流程编号"],
            "run_id": run["run_id"],
            "断点步骤": boundary["选定断点"]["步骤"] if boundary.get("选定断点") else None,
            "合并顺序": list(run["步骤序列"]),
            "合并数据集": blocks,
            "完整字段": fields,
            "口径判定依据": caliber_basis,
            "恢复边界判定依据": boundary,
            "步骤记录引用": record_refs,
            "产物引用": artifact_refs,
            "合并签名": signature,
            "合并时间": _now(),
        }
        _rows(MERGE_TABLE).append(merge)
        self._sync_contract(run, merge)
        return merge

    def _sync_contract(self, run: dict[str, Any], merge: dict[str, Any]) -> dict[str, Any]:
        """合并结果同步写入编排步骤契约：转换模块按契约读取稳定顺序和完整字段。"""
        contract = next((row for row in _rows(CONTRACT_TABLE) if row.get("run_id") == run["run_id"]), None)
        if contract is None:
            contract = {
                "id": _next_id(CONTRACT_TABLE),
                "契约编号": f"CTT-{run['run_id']}",
                "流程编号": run["流程编号"],
                "run_id": run["run_id"],
            }
            _rows(CONTRACT_TABLE).append(contract)
        contract.update({
            "稳定顺序": list(merge["合并顺序"]),
            "完整字段": list(merge["完整字段"]),
            "合并编号": merge["合并编号"],
            "同步时间": _now(),
        })
        return contract

    def _latest_merge(self, run_id: str) -> dict[str, Any] | None:
        merges = [row for row in _rows(MERGE_TABLE) if row.get("run_id") == run_id]
        return max(merges, key=lambda row: int(row.get("id", 0)), default=None)

    # ---------- 内部：无效批次与一致性 ----------

    def _locate_invalid_batches(self, run_id: str) -> list[dict[str, Any]]:
        """顺着 步骤执行记录 → 依赖产物 → 合并结果 的数据流定位无效批次：
        孤儿批次、记录未成功的批次、同一分支的重复产物、同口径重复批次。"""
        records = self._run_records(run_id)
        artifacts = self._run_artifacts(run_id)
        for artifact in artifacts:
            if not artifact.get("有效标识"):
                continue
            reason = None
            record = next(
                (row for row in records
                 if row.get("步骤名") == artifact.get("步骤名") and int(row.get("attempt", 0)) == int(artifact.get("attempt", 0))),
                None,
            )
            if record is None:
                reason = "孤儿批次：找不到对应的步骤执行记录"
            elif record.get("status") != "已完成":
                reason = f"对应步骤执行记录未成功（状态：{record.get('status')}），批次不可并入"
            else:
                successes = [row for row in records if row.get("步骤名") == artifact.get("步骤名") and row.get("status") == "已完成"]
                first = min(successes, key=lambda row: int(row.get("attempt", 0)), default=None)
                if first is not None and record is not first:
                    reason = f"同一分支重复产物：已采用第{first['attempt']}次执行的产物，本批次不并入"
            if reason:
                artifact["有效标识"] = False
                artifact["无效原因"] = reason

        # 同一步骤同一 attempt 的同口径重复批次：保留产物编号最小者。
        groups: dict[tuple[str, int, str], list[dict[str, Any]]] = {}
        for artifact in artifacts:
            if not artifact.get("有效标识"):
                continue
            key = (str(artifact.get("步骤名")), int(artifact.get("attempt", 0)), str(artifact.get("口径") or DEFAULT_CALIBER))
            groups.setdefault(key, []).append(artifact)
        for group in groups.values():
            if len(group) <= 1:
                continue
            group.sort(key=lambda row: str(row.get("产物编号")))
            for duplicate in group[1:]:
                duplicate["有效标识"] = False
                duplicate["无效原因"] = f"同一步骤同一口径的重复批次：已保留 {group[0]['产物编号']}，本批次不并入"

        return [
            {
                "产物编号": artifact.get("产物编号"),
                "批次号": artifact.get("批次号"),
                "步骤名": artifact.get("步骤名"),
                "attempt": artifact.get("attempt"),
                "口径": artifact.get("口径"),
                "无效原因": artifact.get("无效原因"),
                "定位路径": "步骤执行记录 → 依赖产物 → 合并结果",
            }
            for artifact in artifacts
            if not artifact.get("有效标识")
        ]

    def _consistency_checks(self, run: dict[str, Any], merge: dict[str, Any] | None) -> list[dict[str, Any]]:
        """步骤执行记录、依赖产物、合并结果三方一致性校验。"""
        records = self._run_records(run["run_id"])
        artifacts = self._run_artifacts(run["run_id"])
        checks: list[dict[str, Any]] = []

        missing = [
            row["记录编号"] for row in records
            if row.get("status") == "已完成"
            and not any(
                art.get("有效标识") and art.get("步骤名") == row.get("步骤名") and int(art.get("attempt", 0)) == int(row.get("attempt", 0))
                for art in artifacts
            )
        ]
        checks.append({
            "检查项": "已完成步骤记录均有一致产物",
            "通过": not missing,
            "说明": "全部一致" if not missing else f"以下记录缺少有效产物：{'、'.join(missing)}",
        })

        orphan = [
            art["产物编号"] for art in artifacts
            if art.get("有效标识")
            and not any(
                rec.get("步骤名") == art.get("步骤名") and int(rec.get("attempt", 0)) == int(art.get("attempt", 0))
                for rec in records
            )
        ]
        checks.append({
            "检查项": "有效产物均有对应步骤执行记录",
            "通过": not orphan,
            "说明": "全部一致" if not orphan else f"以下产物找不到步骤执行记录：{'、'.join(orphan)}",
        })

        duplicated = []
        for step in run["步骤序列"]:
            successes = [
                row for row in records
                if row.get("步骤名") == step and row.get("status") == "已完成"
                and any(
                    art.get("有效标识") and art.get("步骤名") == step and int(art.get("attempt", 0)) == int(row.get("attempt", 0))
                    for art in artifacts
                )
            ]
            if len(successes) > 1:
                duplicated.append(step)
        checks.append({
            "检查项": "同一分支不重复合并",
            "通过": not duplicated,
            "说明": "每个分支至多一个一致成功" if not duplicated else f"以下分支存在多个成功产物：{'、'.join(duplicated)}",
        })

        if merge is None:
            checks.append({"检查项": "合并结果与记录、产物一致", "通过": True, "说明": "尚未形成合并结果"})
        else:
            artifact_ids = {str(art.get("产物编号")) for art in artifacts if art.get("有效标识")}
            record_ids = {str(rec.get("记录编号")) for rec in records}
            bad_artifacts = [ref for ref in merge["产物引用"] if ref not in artifact_ids]
            bad_records = [ref for ref in merge["步骤记录引用"] if ref not in record_ids]
            order_ok = merge["合并顺序"] == list(run["步骤序列"])
            ok = not bad_artifacts and not bad_records and order_ok
            说明 = "合并引用全部有效，合并顺序与步骤序列一致" if ok else (
                f"无效产物引用：{'、'.join(bad_artifacts) or '无'}；"
                f"无效记录引用：{'、'.join(bad_records) or '无'}；"
                f"合并顺序{'一致' if order_ok else '与步骤序列不一致'}"
            )
            checks.append({"检查项": "合并结果与记录、产物一致", "通过": ok, "说明": 说明})
        return checks

    def _caliber_basis(self, run: dict[str, Any], merge: dict[str, Any] | None) -> list[dict[str, Any]]:
        """复核口径判定：合并时已留下依据；合并被幂等复用时，按成功 attempt 的
        全部产物（含已被剔除的口径批次）重新推导，依据不随批次失效而丢失。"""
        if merge and merge.get("口径判定依据"):
            return list(merge["口径判定依据"])
        basis: list[dict[str, Any]] = []
        for step in run["步骤序列"]:
            found = self._consistent_success(run["run_id"], step)
            if found is None:
                continue
            record, chosen = found
            siblings = [
                row for row in self._run_artifacts(run["run_id"])
                if row.get("步骤名") == step and int(row.get("attempt", 0)) == int(record["attempt"])
            ]
            calibers = sorted({str(row.get("口径") or DEFAULT_CALIBER) for row in siblings})
            if len(calibers) > 1:
                basis.append({
                    "步骤": step,
                    "并存口径": calibers,
                    "选定口径": chosen.get("口径") or DEFAULT_CALIBER,
                    "选定产物": chosen.get("产物编号"),
                    "判定规则": CALIBER_RULE,
                })
        return basis

    # ---------- 内部：外部记录接收 ----------

    def _ingest_external(self, run: dict[str, Any], values: dict[str, Any]) -> list[str]:
        """接收外部带来的步骤执行记录与依赖产物（含原始数据）。

        环境切换、上线部署后从旧库恢复现场时，记录按状态机回放落库；
        与本地冲突时以本地为准并留下说明。"""
        conflicts: list[str] = []
        for raw in values.get("步骤执行记录") or []:
            code = str(raw.get("记录编号") or "").strip()
            if not code:
                conflicts.append("外部步骤执行记录缺少记录编号，已忽略")
                continue
            existing = next((row for row in self._run_records(run["run_id"]) if row.get("记录编号") == code), None)
            if existing is not None:
                for key in ("status", "产物编号", "批次号", "attempt"):
                    if raw.get(key) is not None and str(raw.get(key)) != str(existing.get(key)):
                        conflicts.append(
                            f"步骤执行记录 {code} 的 {key} 与本地不一致（外部：{raw.get(key)}，本地：{existing.get(key)}），以本地为准"
                        )
                continue
            step = str(raw.get("步骤名") or "").strip()
            if step not in run["步骤序列"]:
                conflicts.append(f"外部记录 {code} 的步骤「{step}」不在步骤序列中，已忽略")
                continue
            status = str(raw.get("status") or "").strip()
            if status and status not in STEP_STATUS_ORDER + [STEP_FAILURE_STATUS]:
                conflicts.append(f"外部记录 {code} 的状态「{status}」无法识别，已忽略")
                continue
            record = self._new_record(run, step, run["步骤序列"].index(step), attempt=int(raw.get("attempt") or 1), 来源="外部同步")
            record["记录编号"] = code
            # 恢复现场也按状态机回放，不允许直接跳到终态。
            if status in ("执行中", "已完成", STEP_FAILURE_STATUS):
                advance_step_status(record, "执行中")
                record["开始时间"] = _now()
            if status in ("已完成", STEP_FAILURE_STATUS):
                advance_step_status(record, status)
                record["完成时间"] = _now()
            record["产物编号"] = raw.get("产物编号")
            record["批次号"] = raw.get("批次号")
            record["口径"] = raw.get("口径")
            record["失败原因"] = raw.get("失败原因")
            _rows(STEP_TABLE).append(record)

        for raw in values.get("依赖产物") or []:
            code = str(raw.get("产物编号") or "").strip()
            if not code:
                conflicts.append("外部依赖产物缺少产物编号，已忽略")
                continue
            existing = next((row for row in _rows(ARTIFACT_TABLE) if row.get("产物编号") == code), None)
            if existing is not None:
                for key in ("批次号", "口径", "attempt"):
                    if raw.get(key) is not None and str(raw.get(key)) != str(existing.get(key)):
                        conflicts.append(
                            f"依赖产物 {code} 的 {key} 与本地不一致（外部：{raw.get(key)}，本地：{existing.get(key)}），以本地为准"
                        )
                continue
            step = str(raw.get("步骤名") or "").strip()
            if step not in run["步骤序列"]:
                conflicts.append(f"外部产物 {code} 的步骤「{step}」不在步骤序列中，已忽略")
                continue
            _rows(ARTIFACT_TABLE).append({
                "id": _next_id(ARTIFACT_TABLE),
                "产物编号": code,
                "run_id": run["run_id"],
                "流程编号": run["流程编号"],
                "步骤名": step,
                "attempt": int(raw.get("attempt") or 1),
                "批次号": str(raw.get("批次号") or f"BATCH-{code}"),
                "口径": str(raw.get("口径") or DEFAULT_CALIBER),
                "原始数据": list(raw.get("原始数据") or []),
                "有效标识": True,
                "无效原因": None,
                "来源": "外部同步",
                "生成时间": _now(),
            })
        return conflicts

    # ---------- 内部：视图 ----------

    def _run_detail(self, run: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
        detail = {
            "执行实例": dict(run),
            "断点步骤": run["断点步骤"],
            "步骤执行记录": self._run_records(run["run_id"]),
            "依赖产物": self._run_artifacts(run["run_id"]),
            "合并结果": self._latest_merge(run["run_id"]),
        }
        if extra:
            detail.update(extra)
        return detail
