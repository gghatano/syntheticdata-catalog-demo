"""残業偏在検出プログラム（正常提出サンプル）

社員マスタ・プロジェクトアサイン・勤務時間データを読み込み、
残業時間の偏りを分析して結果をJSON形式で出力する。
"""

import argparse
import csv
import json
import sys
from collections import defaultdict


def load_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def analyze_overtime(employees, allocations, working_hours):
    """残業偏在を検出する。"""
    # 社員ごとの平均残業時間を算出
    overtime_by_emp = defaultdict(list)
    for row in working_hours:
        overtime_by_emp[row["employee_id"]].append(float(row["overtime_hours"]))

    avg_overtime = {
        emp_id: sum(hours) / len(hours)
        for emp_id, hours in overtime_by_emp.items()
    }

    # 全社平均
    all_values = list(avg_overtime.values())
    overall_avg = sum(all_values) / len(all_values) if all_values else 0

    # 部署ごとの平均残業
    emp_dept = {e["employee_id"]: e["department"] for e in employees}
    dept_overtime = defaultdict(list)
    for emp_id, avg in avg_overtime.items():
        dept = emp_dept.get(emp_id, "不明")
        dept_overtime[dept].append(avg)

    dept_avg = {
        dept: round(sum(vals) / len(vals), 1)
        for dept, vals in dept_overtime.items()
    }

    # 残業が全社平均の1.5倍以上の社員を「高残業者」として抽出
    threshold = overall_avg * 1.5
    high_overtime_employees = []
    emp_name = {e["employee_id"]: e["name"] for e in employees}
    for emp_id, avg in sorted(avg_overtime.items()):
        if avg >= threshold:
            high_overtime_employees.append({
                "employee_id": emp_id,
                "name": emp_name.get(emp_id, "不明"),
                "department": emp_dept.get(emp_id, "不明"),
                "avg_overtime_hours": round(avg, 1),
            })

    # プロジェクト別の関与社員の平均残業
    proj_employees = defaultdict(list)
    for a in allocations:
        proj_employees[a["project_name"]].append(a["employee_id"])

    project_overtime = {}
    for proj_name, emp_ids in proj_employees.items():
        vals = [avg_overtime[eid] for eid in set(emp_ids) if eid in avg_overtime]
        if vals:
            project_overtime[proj_name] = round(sum(vals) / len(vals), 1)

    return {
        "summary": {
            "overall_avg_overtime_hours": round(overall_avg, 1),
            "threshold_high_overtime": round(threshold, 1),
            "total_employees_analyzed": len(avg_overtime),
            "high_overtime_count": len(high_overtime_employees),
        },
        "department_avg_overtime": dept_avg,
        "high_overtime_employees": high_overtime_employees,
        "project_avg_overtime": project_overtime,
    }


def main():
    parser = argparse.ArgumentParser(description="残業偏在検出")
    parser.add_argument("--employee-master", required=True)
    parser.add_argument("--project-allocation", required=True)
    parser.add_argument("--working-hours", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    employees = load_csv(args.employee_master)
    allocations = load_csv(args.project_allocation)
    working_hours = load_csv(args.working_hours)

    result = analyze_overtime(employees, allocations, working_hours)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"分析完了: 高残業者 {result['summary']['high_overtime_count']}名検出")
    print(f"結果を {args.output} に出力しました")


if __name__ == "__main__":
    main()
