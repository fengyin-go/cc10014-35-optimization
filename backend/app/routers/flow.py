"""查询编排接口：维护编排流程，覆盖提交校验、启用流程、停用流程等动作。

执行类接口（发起执行、断点续做、复核、编排步骤契约）只负责参数转述，
状态流转与合并规则都在 app/services/flowrun.py 里。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.flow import FlowService
from app.services.flowrun import FlowRunError, FlowRunService

router = APIRouter(prefix="/api/flow", tags=["查询编排"])

service = FlowService()
run_service = FlowRunService()

LIST_FIELDS = ["流程编号", "流程名称", "步骤序列", "数据依赖", "失败回退", "超时秒数", "并行度", "结果合并"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


def _run_or_raise(func, *args) -> Any:
    """把执行引擎的规则错误转成可读的状态码：找不到给 404，违反规则给 400。"""
    try:
        return func(*args)
    except FlowRunError as exc:
        status_code = 404 if "不存在" in str(exc) else 400
        raise HTTPException(status_code=status_code, detail=str(exc))


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按流程编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已启用、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按流程编号与状态过滤查询编排列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出查询编排清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "flow", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条编排流程明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"编排流程 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条编排流程，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="编排流程已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条编排流程执行提交校验、启用流程、停用流程；状态只能按顺序单向流转，跳级或回退会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/{entry_id}/runs")
def list_runs(entry_id: int) -> dict[str, Any]:
    """列出编排流程的执行实例。"""
    return {"items": _run_or_raise(run_service.list_runs, entry_id)}


@router.post("/{entry_id}/runs")
def start_run(entry_id: int, payload: EntryPayload) -> dict[str, Any]:
    """发起一次编排执行：登记步骤执行记录与依赖产物，全部完成时直接合并并同步契约。"""
    return _run_or_raise(run_service.start_run, entry_id, payload.values)


@router.get("/{entry_id}/runs/{run_id}")
def get_run(entry_id: int, run_id: str) -> dict[str, Any]:
    """读取执行实例详情：步骤执行记录、依赖产物、断点与最新合并结果。"""
    return _run_or_raise(run_service.get_run, entry_id, run_id)


@router.post("/{entry_id}/runs/{run_id}/resume")
def resume_run(entry_id: int, run_id: str, payload: EntryPayload) -> dict[str, Any]:
    """断点续做：接收已有步骤执行记录和依赖产物所关联的原始数据，从断点步骤续做，
    重新合并已完成分支并输出合并数据集；已完成分支不重跑。"""
    return _run_or_raise(run_service.resume_run, entry_id, run_id, payload.values)


@router.get("/{entry_id}/runs/{run_id}/audit")
def audit_run(entry_id: int, run_id: str) -> dict[str, Any]:
    """复核视图：步骤执行记录、断点、合并顺序、无效批次与一致性校验。"""
    return _run_or_raise(run_service.audit_run, entry_id, run_id)


@router.get("/{entry_id}/contracts")
def list_contracts(entry_id: int) -> dict[str, Any]:
    """编排步骤契约：合并结果同步的稳定顺序与完整字段，供转换模块读取。"""
    return {"items": _run_or_raise(run_service.list_contracts, entry_id)}
