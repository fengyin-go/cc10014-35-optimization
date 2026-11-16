"""FLOW-0003 断点续做复测 + 编排执行引擎规则校验。

场景：FLOW-0003 三步骤 A/B/C 并行执行，B 失败后重试复测，
A 和 C 不重跑，合并结果顺序与首次执行一致。

运行：cd backend && .venv/bin/python tests/test_flow_resume.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.flow import FlowService
from app.services.flowrun import (
    FlowRunError,
    FlowRunService,
    advance_run_status,
    advance_step_status,
)
from app.services.transform import TransformService
from app.store import store

FLOW_ID = 3  # FLOW-0003，已启用
runs = FlowRunService()
failures: list[str] = []


def check(name: str, fn) -> None:
    try:
        fn()
    except AssertionError as exc:
        failures.append(f"{name}: {exc}")
        print(f"  ✗ {name} — {exc}")
    else:
        print(f"  ✓ {name}")


def expect_error(fn, keyword: str = "") -> str:
    try:
        fn()
    except FlowRunError as exc:
        if keyword and keyword not in str(exc):
            raise AssertionError(f"错误消息里应包含「{keyword}」，实际：{exc}")
        return str(exc)
    raise AssertionError(f"应抛出 FlowRunError（{keyword}），但没有抛出")


print("== 1. FLOW-0003 首次执行：A/B/C 并行，B 失败 ==")
first = runs.start_run(FLOW_ID, {
    "步骤序列": ["A", "B", "C"],
    "完成顺序": ["C", "A", "B"],  # 完成顺序故意打乱，验证合并不随它漂移
    "步骤结果": {
        "A": {"结果": "已完成", "原始数据": [{"订单号": "O1", "金额": 100}], "批次号": "BATCH-A-1"},
        "B": {"结果": "已失败", "失败原因": "依赖库连接超时"},
        "C": {"结果": "已完成", "原始数据": [{"订单号": "O1", "渠道": "线上"}], "批次号": "BATCH-C-1"},
    },
})
run_id = first["执行实例"]["run_id"]


def t1():
    assert first["执行实例"]["状态"] == "待续做", first["执行实例"]["状态"]
    assert first["断点步骤"] == "B", first["断点步骤"]
    assert first["合并结果"] is None
    records = {r["步骤名"]: r for r in first["步骤执行记录"]}
    assert records["A"]["status"] == "已完成" and records["A"]["attempt"] == 1
    assert records["B"]["status"] == "已失败"
    assert records["C"]["status"] == "已完成"
    # 完成序号如实记录完成先后（C<A<B），但断点与合并不看它
    assert (records["C"]["完成序号"], records["A"]["完成序号"], records["B"]["完成序号"]) == (1, 2, 3)


check("首次执行：B 失败，断点落在 B，A/C 已完成", t1)

print("== 2. 断点续做：只重跑 B，A 和 C 不重跑 ==")
resumed = runs.resume_run(FLOW_ID, run_id, {"续做结果": {
    # 故意给 A、C 也带上新数据：已完成分支必须跳过，不生效
    "A": {"结果": "已完成", "原始数据": [{"订单号": "O1", "金额": 999}]},
    "B": {"结果": "已完成", "原始数据": [{"订单号": "O1", "金额": 100, "币种": "CNY"}], "批次号": "BATCH-B-2"},
    "C": {"结果": "已完成", "原始数据": [{"订单号": "O1", "渠道": "篡改"}]},
}})
merge = resumed["合并结果"]


def t2():
    assert resumed["断点步骤"] == "B"
    assert resumed["续做步骤"] == ["B"], resumed["续做步骤"]
    assert sorted(resumed["跳过步骤"]) == ["A", "C"]
    assert resumed["执行实例"]["状态"] == "已完成"
    records_a = [r for r in resumed["步骤执行记录"] if r["步骤名"] == "A"]
    records_b = [r for r in resumed["步骤执行记录"] if r["步骤名"] == "B"]
    records_c = [r for r in resumed["步骤执行记录"] if r["步骤名"] == "C"]
    # A、C 没有重跑：仍只有第 1 次记录，产物不变
    assert len(records_a) == 1 and records_a[0]["attempt"] == 1
    assert len(records_c) == 1 and records_c[0]["attempt"] == 1
    # B 重试产生第 2 次记录，旧失败记录保留可复核
    assert [r["status"] for r in records_b] == ["已失败", "已完成"]
    assert [r["attempt"] for r in records_b] == [1, 2]
    # A、C 的产物没有被续做结果篡改
    art_a = next(a for a in resumed["依赖产物"] if a["步骤名"] == "A")
    art_c = next(a for a in resumed["依赖产物"] if a["步骤名"] == "C")
    assert art_a["原始数据"] == [{"订单号": "O1", "金额": 100}]
    assert art_c["原始数据"] == [{"订单号": "O1", "渠道": "线上"}]
    # 合并结果：顺序固定为步骤序列
    assert merge["合并顺序"] == ["A", "B", "C"]
    assert [b["来源步骤"] for b in merge["合并数据集"]] == ["A", "B", "C"]
    assert [b["attempt"] for b in merge["合并数据集"]] == [1, 2, 1]


check("断点续做：只重跑 B，A/C 记录与产物原样保留", t2)


def t2b():
    # 可从合并数据集复核：每块都能反查步骤执行记录与依赖产物
    records = {r["记录编号"]: r for r in resumed["步骤执行记录"]}
    artifacts = {a["产物编号"]: a for a in resumed["依赖产物"]}
    for block in merge["合并数据集"]:
        rec = records.get(block["记录编号"])
        art = artifacts.get(block["产物编号"])
        assert rec is not None and rec["步骤名"] == block["来源步骤"]
        assert art is not None and art["批次号"] == block["批次号"]
        assert art["原始数据"] == block["数据"]
    assert merge["断点步骤"] == "B"  # 合并结果记录续做所依据的断点，可复核
    assert merge["恢复边界判定依据"]["选定断点"]["步骤"] == "B"
    assert merge["完整字段"] == ["订单号", "金额", "币种", "渠道"]


check("合并数据集可复核：记录编号/产物编号/批次号全部可反查", t2b)

print("== 3. 与首次执行一致：干净首跑（完成顺序不同）合并结果相同 ==")
clean = runs.start_run(FLOW_ID, {
    "步骤序列": ["A", "B", "C"],
    "完成顺序": ["B", "C", "A"],  # 另一种完成顺序
    "步骤结果": {
        "A": {"结果": "已完成", "原始数据": [{"订单号": "O1", "金额": 100}]},
        "B": {"结果": "已完成", "原始数据": [{"订单号": "O1", "金额": 100, "币种": "CNY"}]},
        "C": {"结果": "已完成", "原始数据": [{"订单号": "O1", "渠道": "线上"}]},
    },
})
clean_merge = clean["合并结果"]


def t3():
    assert clean["执行实例"]["状态"] == "已完成"
    assert clean_merge["合并顺序"] == ["A", "B", "C"]
    assert [b["来源步骤"] for b in clean_merge["合并数据集"]] == [b["来源步骤"] for b in merge["合并数据集"]]
    assert [b["数据"] for b in clean_merge["合并数据集"]] == [b["数据"] for b in merge["合并数据集"]]
    assert clean_merge["完整字段"] == merge["完整字段"]


check("合并结果顺序与首次执行一致，不随完成顺序漂移", t3)

print("== 4. 幂等：重复续做不会重复合并同一分支 ==")
again = runs.resume_run(FLOW_ID, run_id, {"续做结果": {
    "B": {"结果": "已完成", "原始数据": [{"订单号": "O1", "金额": 100, "币种": "CNY"}], "批次号": "BATCH-B-2"},
}})


def t4():
    assert again["续做步骤"] == [] and again["跳过步骤"] == ["B"]
    assert again["合并结果"]["合并编号"] == merge["合并编号"]
    assert again["合并结果"]["合并时间"] == merge["合并时间"]
    assert again["合并结果"]["合并数据集"] == merge["合并数据集"]
    assert len([r for r in again["步骤执行记录"] if r["步骤名"] == "B"]) == 2
    assert len([m for m in store.rows("flow_merge") if m["run_id"] == run_id]) == 1


check("重复续做：合并签名不变，同一分支不被重复合并", t4)

print("== 5. 状态只能按顺序单向流转，不能跳级 ==")


def t5():
    rec = {"步骤名": "X", "attempt": 1, "status": "待执行"}
    expect_error(lambda: advance_step_status(rec, "已完成"), "跳")
    advance_step_status(rec, "执行中")
    expect_error(lambda: advance_step_status(rec, "待执行"), "回退")
    advance_step_status(rec, "已失败")
    expect_error(lambda: advance_step_status(rec, "执行中"), "终态")

    run = {"run_id": "R-X", "状态": "待续做"}
    expect_error(lambda: advance_run_status(run, "执行中"), "回退")
    advance_run_status(run, "已完成")
    expect_error(lambda: advance_run_status(run, "待续做"), "终态")

    flow_service = FlowService()
    entry, missing = flow_service.create_entry({"流程编号": "FLOW-9001", "流程名称": "状态机验证", "步骤序列": "A"})
    assert not missing
    eid = entry["id"]
    e, msg = flow_service.run_action(eid, "启用流程")
    assert e is None and "不允许" in msg, msg  # 草稿→已启用：跳级
    e, _ = flow_service.run_action(eid, "提交校验")
    assert e["status"] == "待校验"
    e, msg = flow_service.run_action(eid, "提交校验")
    assert e is None and "不允许" in msg  # 原地不算下一态
    e, _ = flow_service.run_action(eid, "启用流程")
    assert e["status"] == "已启用"
    e, msg = flow_service.run_action(eid, "提交校验")
    assert e is None and "不允许" in msg  # 已启用→待校验：回退
    e, _ = flow_service.run_action(eid, "停用流程")
    assert e["status"] == "已停用"
    e, msg = flow_service.run_action(eid, "启用流程")
    assert e is None and "终态" in msg


check("步骤/实例/流程定义三级状态机：单向、不跳级、终态不可动", t5)

print("== 6. 两个恢复边界并存：只形成一套执行顺序和断点 ==")
two = runs.start_run(FLOW_ID, {
    "步骤序列": ["A", "B", "C"],
    "步骤结果": {
        "A": {"结果": "已完成", "原始数据": [{"订单号": "O2", "金额": 200}]},
        "B": {"结果": "已失败", "失败原因": "依赖库连接超时"},
        # C 不给结果：并行分支未完成，与 B 的失败回退同时存在
    },
})
run_id_two = two["执行实例"]["run_id"]


def t6():
    boundary = two["恢复边界判定依据"]
    candidates = boundary["候选恢复边界"]
    assert len(candidates) == 2, candidates
    assert candidates[0]["步骤"] == "B" and candidates[0]["边界类型"] == "失败回退"
    assert candidates[1]["步骤"] == "C" and candidates[1]["边界类型"] == "分支未完成"
    assert boundary["选定断点"]["步骤"] == "B"
    assert two["执行实例"]["状态"] == "待续做"


check("失败回退与并行分支未完成并存：唯一断点 B，判定有依据", t6)

done_two = runs.resume_run(FLOW_ID, run_id_two, {"续做结果": {
    "B": {"结果": "已完成", "原始数据": [{"订单号": "O2", "金额": 200, "币种": "CNY"}]},
    "C": {"结果": "已完成", "原始数据": [{"订单号": "O2", "渠道": "线下"}]},
}})


def t6b():
    merge_two = done_two["合并结果"]
    assert merge_two["合并顺序"] == ["A", "B", "C"]
    assert merge_two["恢复边界判定依据"]["选定断点"]["步骤"] == "B"
    # C 的在途记录直接推进完成，不重开 attempt
    records_c = [r for r in done_two["步骤执行记录"] if r["步骤名"] == "C"]
    assert len(records_c) == 1 and records_c[0]["status"] == "已完成"


check("两个恢复边界续做完成：一套执行顺序、一套断点写进合并结果", t6b)

print("== 7. 不同口径并存：优先级判定留下依据 ==")
runs.resume_run(FLOW_ID, run_id, {"依赖产物": [
    {"产物编号": "ART-EXT-A-GRAY", "步骤名": "A", "attempt": 1, "批次号": "BATCH-A-GRAY",
     "口径": "灰度口径", "原始数据": [{"订单号": "O1", "金额": 88}]},
]})
audit = runs.audit_run(FLOW_ID, run_id)


def t7():
    gray = next(a for a in audit["依赖产物"] if a["产物编号"] == "ART-EXT-A-GRAY")
    assert gray["有效标识"] is False and "口径并存" in gray["无效原因"]
    basis = next(b for b in audit["口径判定依据"] if b["步骤"] == "A")
    assert basis["并存口径"] == ["标准口径", "灰度口径"]
    assert basis["选定口径"] == "标准口径"
    assert "优先级" in basis["判定规则"]
    block_a = audit["合并结果"]["合并数据集"][0]
    assert block_a["口径"] == "标准口径" and block_a["数据"] == [{"订单号": "O1", "金额": 100}]
    assert any(b["产物编号"] == "ART-EXT-A-GRAY" for b in audit["无效批次"])


check("口径并存：标准口径优先，灰度批次未并入且判定依据可复核", t7)

print("== 8. 顺着数据流定位无效批次 ==")
runs.resume_run(FLOW_ID, run_id, {"依赖产物": [
    {"产物编号": "ART-ORPHAN-9", "步骤名": "B", "attempt": 9, "批次号": "BATCH-ORPHAN", "原始数据": []},
    {"产物编号": "ART-B-OLD", "步骤名": "B", "attempt": 1, "批次号": "BATCH-B-1", "原始数据": [{"订单号": "O1", "金额": 1}]},
]})
audit8 = runs.audit_run(FLOW_ID, run_id)


def t8():
    reasons = {b["产物编号"]: b["无效原因"] for b in audit8["无效批次"]}
    assert "孤儿批次" in reasons["ART-ORPHAN-9"], reasons.get("ART-ORPHAN-9")
    assert "未成功" in reasons["ART-B-OLD"], reasons.get("ART-B-OLD")  # B 第1次是已失败记录
    for batch in audit8["无效批次"]:
        assert batch["定位路径"] == "步骤执行记录 → 依赖产物 → 合并结果"
    # 无效批次被剔除后，记录/产物/合并结果三方保持一致
    assert all(item["通过"] for item in audit8["一致性校验"]), audit8["一致性校验"]


check("无效批次：孤儿批次与失败记录批次被定位，三方一致性校验通过", t8)

print("== 9. 合并结果同步编排步骤契约，转换模块读取稳定顺序和完整字段 ==")


def t9():
    contracts = runs.list_contracts(FLOW_ID)
    contract = next(c for c in contracts if c["run_id"] == run_id)
    assert contract["稳定顺序"] == ["A", "B", "C"]
    assert contract["完整字段"] == ["订单号", "金额", "币种", "渠道"]
    assert contract["合并编号"] == merge["合并编号"]
    read = TransformService().read_step_contract("FLOW-0003")
    assert read is not None and read["稳定顺序"] == ["A", "B", "C"]
    assert set(read["完整字段"]) >= {"订单号", "金额", "币种", "渠道"}


check("编排步骤契约同步，转换模块读到稳定顺序与完整字段", t9)

print("== 10. 环境切换/上线部署后：带记录回来仍从同一断点续做 ==")
ext = runs.start_run(FLOW_ID, {
    "步骤序列": ["A", "B", "C"],
    "步骤结果": {
        "A": {"结果": "已完成", "原始数据": [{"订单号": "O3", "金额": 300}]},
        "B": {"结果": "已失败", "失败原因": "依赖库连接超时"},
        "C": {"结果": "已完成", "原始数据": [{"订单号": "O3", "渠道": "线上"}]},
    },
})
run_id_ext = ext["执行实例"]["run_id"]
snapshot_records = [dict(r) for r in ext["步骤执行记录"]]
snapshot_artifacts = [dict(a) for a in ext["依赖产物"]]
resumed_ext = runs.resume_run(FLOW_ID, run_id_ext, {
    "步骤执行记录": snapshot_records,   # 模拟从另一个环境/旧库带回来的记录
    "依赖产物": snapshot_artifacts,
    "续做结果": {"B": {"结果": "已完成", "原始数据": [{"订单号": "O3", "金额": 300, "币种": "CNY"}]}},
})


def t10():
    assert resumed_ext["断点步骤"] == "B"
    assert resumed_ext["外部记录冲突"] == [], resumed_ext["外部记录冲突"]
    assert resumed_ext["续做步骤"] == ["B"]
    assert resumed_ext["合并结果"]["合并顺序"] == ["A", "B", "C"]
    # 外部同源记录不重复入库：A1、B1(失败)、B2、C1 共 4 条
    assert len(resumed_ext["步骤执行记录"]) == 4, len(resumed_ext["步骤执行记录"])


check("外部记录恢复现场：同一断点续做，记录不重复入库", t10)

print("== 11. 执行前置约束 ==")


def t11():
    expect_error(lambda: runs.start_run(1, {"步骤序列": ["A"]}), "已启用")  # FLOW-0001 还是草稿
    expect_error(lambda: runs.resume_run(FLOW_ID, run_id, {"续做结果": {"Z": {"结果": "已完成"}}}), "不在步骤序列")
    expect_error(lambda: runs.get_run(FLOW_ID, "FLOW-0003-RUN-999"), "不存在")


check("未启用不能执行、未知步骤被拒绝、不存在实例给可读错误", t11)

print()
if failures:
    print(f"共 {len(failures)} 项未通过：")
    for item in failures:
        print(f"  - {item}")
    sys.exit(1)
print(f"全部通过（FLOW-0003 三步骤并行、B 失败重试复测：A/C 不重跑，合并顺序与首次执行一致）")
