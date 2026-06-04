#!/usr/bin/env python3
"""Small local task tracker for the Repiq validation plan.

The repository JSON is the source of truth. A static HTML board is generated
after every write so the current plan can be inspected without any SaaS tool.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
import textwrap
import webbrowser
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TASKS_PATH = ROOT / "docs" / "tasks" / "repiq_14d_tasks.json"
DEFAULT_BOARD_PATH = ROOT / "artifacts" / "tasks" / "repiq_14d_board.html"
BOARD_COLUMNS = ["backlog", "ready", "in_progress", "review", "blocked", "done"]
STATUS_LABELS = {
    "backlog": "Backlog",
    "ready": "Ready",
    "in_progress": "In Progress",
    "review": "Review",
    "blocked": "Blocked",
    "done": "Done",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_data(path: Path = DEFAULT_TASKS_PATH) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Tasks file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def save_data(data: dict[str, Any], path: Path = DEFAULT_TASKS_PATH, *, render: bool = True) -> None:
    data["updated_at"] = utc_now()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp_path.replace(path)
    if render:
        render_board(data, DEFAULT_BOARD_PATH)


def find_task(data: dict[str, Any], task_id: str) -> dict[str, Any]:
    normalized = task_id.upper()
    for task in data.get("tasks", []):
        if str(task.get("id", "")).upper() == normalized:
            return task
    raise SystemExit(f"Task not found: {task_id}")


def validate_status(data: dict[str, Any], status: str) -> str:
    normalized = status.lower()
    allowed = set(data.get("statuses", BOARD_COLUMNS))
    if normalized not in allowed:
        raise SystemExit(f"Invalid status '{status}'. Allowed: {', '.join(sorted(allowed))}")
    return normalized


def append_history(task: dict[str, Any], action: str, **extra: Any) -> None:
    task.setdefault("history", []).append({"created_at": utc_now(), "action": action, **extra})


def append_note(task: dict[str, Any], text: str) -> None:
    task.setdefault("notes", []).append({"created_at": utc_now(), "text": text})
    append_history(task, "note", text=text)
    task["updated_at"] = utc_now()


def append_tests(task: dict[str, Any], tests: list[str] | None) -> None:
    for test in tests or []:
        if test and test not in task.setdefault("tests", []):
            task["tests"].append(test)


def set_status(
    data: dict[str, Any],
    task_id: str,
    status: str,
    *,
    note: str | None = None,
    tests: list[str] | None = None,
) -> dict[str, Any]:
    task = find_task(data, task_id)
    new_status = validate_status(data, status)
    previous = task.get("status")
    task["status"] = new_status
    task["updated_at"] = utc_now()
    append_tests(task, tests)
    if note:
        append_note(task, note)
    append_history(task, "status_changed", from_status=previous, to_status=new_status)
    return task


def filtered_tasks(data: dict[str, Any], args: argparse.Namespace) -> list[dict[str, Any]]:
    tasks = list(data.get("tasks", []))
    if getattr(args, "status", None):
        wanted = args.status.lower()
        tasks = [task for task in tasks if task.get("status") == wanted]
    elif not getattr(args, "all", False):
        tasks = [task for task in tasks if task.get("status") != "done"]
    if getattr(args, "area", None):
        wanted_area = args.area.lower()
        tasks = [task for task in tasks if str(task.get("area", "")).lower() == wanted_area]
    if getattr(args, "type", None):
        wanted_type = args.type.lower()
        tasks = [task for task in tasks if str(task.get("type", "")).lower() == wanted_type]
    status_order = {status: index for index, status in enumerate(BOARD_COLUMNS)}
    return sorted(
        tasks,
        key=lambda task: (
            status_order.get(str(task.get("status")), 99),
            str(task.get("priority", "P9")),
            str(task.get("id")),
        ),
    )


def print_tasks(tasks: list[dict[str, Any]]) -> None:
    if not tasks:
        print("No tasks matched.")
        return
    rows = [
        (
            str(task.get("id", "")),
            str(task.get("status", "")),
            str(task.get("priority", "")),
            str(task.get("area", "")),
            str(task.get("title", "")),
        )
        for task in tasks
    ]
    widths = [max(len(row[index]) for row in rows + [("ID", "STATUS", "PRI", "AREA", "TITLE")]) for index in range(5)]
    header = ("ID", "STATUS", "PRI", "AREA", "TITLE")
    print("  ".join(value.ljust(widths[index]) for index, value in enumerate(header)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def print_task(task: dict[str, Any]) -> None:
    print(f"{task.get('id')} - {task.get('title')}")
    print(f"Status: {task.get('status')} | Priority: {task.get('priority')} | Area: {task.get('area')} | Type: {task.get('type')}")
    if task.get("description"):
        print("\nDescription:")
        print(textwrap.fill(str(task["description"]), width=100))
    if task.get("plan_ref"):
        print(f"\nPlan: {task['plan_ref']}")
    if task.get("acceptance"):
        print("\nAcceptance:")
        for item in task["acceptance"]:
            print(f"- {item}")
    if task.get("tests"):
        print("\nTests:")
        for item in task["tests"]:
            print(f"- {item}")
    if task.get("notes"):
        print("\nNotes:")
        for note in task["notes"]:
            print(f"- {note.get('created_at')}: {note.get('text')}")
    if task.get("history"):
        print("\nHistory:")
        for item in task["history"][-8:]:
            action = item.get("action")
            if action == "status_changed":
                print(f"- {item.get('created_at')}: {item.get('from_status')} -> {item.get('to_status')}")
            elif action == "note":
                print(f"- {item.get('created_at')}: note")
            else:
                print(f"- {item.get('created_at')}: {action}")


def render_board(data: dict[str, Any], output_path: Path = DEFAULT_BOARD_PATH) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tasks_by_status: dict[str, list[dict[str, Any]]] = {status: [] for status in BOARD_COLUMNS}
    for task in data.get("tasks", []):
        tasks_by_status.setdefault(str(task.get("status", "backlog")), []).append(task)

    cards = []
    for status in BOARD_COLUMNS:
        task_cards = []
        for task in sorted(tasks_by_status.get(status, []), key=lambda t: (str(t.get("priority", "P9")), str(t.get("id")))):
            acceptance_count = len(task.get("acceptance", []))
            notes_count = len(task.get("notes", []))
            tests_count = len(task.get("tests", []))
            task_cards.append(
                f"""
                <article class="card">
                  <div class="card-top">
                    <strong>{html.escape(str(task.get("id", "")))}</strong>
                    <span>{html.escape(str(task.get("priority", "")))}</span>
                  </div>
                  <h3>{html.escape(str(task.get("title", "")))}</h3>
                  <p>{html.escape(str(task.get("description", "")))}</p>
                  <div class="meta">
                    <span>{html.escape(str(task.get("area", "")))}</span>
                    <span>{html.escape(str(task.get("type", "")))}</span>
                  </div>
                  <div class="counts">
                    <span>{acceptance_count} acceptance</span>
                    <span>{notes_count} notes</span>
                    <span>{tests_count} tests</span>
                  </div>
                </article>
                """
            )
        cards.append(
            f"""
            <section class="column column-{status}">
              <header>
                <h2>{STATUS_LABELS.get(status, status)}</h2>
                <span>{len(tasks_by_status.get(status, []))}</span>
              </header>
              <div class="cards">{''.join(task_cards) or '<div class="empty">No tasks</div>'}</div>
            </section>
            """
        )

    content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(str(data.get("title", "Task Board")))}</title>
  <style>
    :root {{
      --bg: #f6f3ee;
      --ink: #1d2321;
      --muted: #65706c;
      --line: #d8d1c7;
      --panel: #fffdf8;
      --accent: #23614f;
      --danger: #a33b2f;
      --review: #845b18;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    .page {{ padding: 24px; min-width: 1120px; }}
    .topbar {{
      display: flex;
      justify-content: space-between;
      align-items: end;
      gap: 24px;
      margin-bottom: 18px;
    }}
    h1 {{ margin: 0 0 4px; font-size: 24px; }}
    .subtitle {{ margin: 0; color: var(--muted); font-size: 13px; }}
    .command {{
      border: 1px solid var(--line);
      background: var(--panel);
      padding: 8px 10px;
      border-radius: 6px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 12px;
      color: var(--muted);
      white-space: nowrap;
    }}
    .board {{
      display: grid;
      grid-template-columns: repeat(6, minmax(176px, 1fr));
      gap: 12px;
      align-items: start;
    }}
    .column {{
      border: 1px solid var(--line);
      background: rgba(255, 253, 248, 0.72);
      border-radius: 8px;
      min-height: 72vh;
      overflow: hidden;
    }}
    .column header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }}
    .column h2 {{ margin: 0; font-size: 13px; text-transform: uppercase; letter-spacing: .04em; }}
    .column header span {{
      min-width: 24px;
      height: 24px;
      display: inline-grid;
      place-items: center;
      border-radius: 999px;
      background: #ece6dc;
      font-size: 12px;
      color: var(--muted);
    }}
    .cards {{ display: grid; gap: 8px; padding: 8px; }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      box-shadow: 0 1px 1px rgba(0, 0, 0, .03);
    }}
    .column-in_progress .card {{ border-left: 4px solid var(--accent); }}
    .column-blocked .card {{ border-left: 4px solid var(--danger); }}
    .column-review .card {{ border-left: 4px solid var(--review); }}
    .column-done .card {{ opacity: .72; }}
    .card-top, .meta, .counts {{
      display: flex;
      justify-content: space-between;
      gap: 8px;
      color: var(--muted);
      font-size: 11px;
    }}
    .card-top strong {{ color: var(--ink); }}
    .card h3 {{ margin: 8px 0 6px; font-size: 14px; line-height: 1.25; }}
    .card p {{ margin: 0 0 10px; color: var(--muted); font-size: 12px; line-height: 1.4; }}
    .meta {{ padding-top: 8px; border-top: 1px solid #ebe5da; }}
    .counts {{ margin-top: 6px; justify-content: start; flex-wrap: wrap; }}
    .empty {{ color: var(--muted); font-size: 13px; padding: 12px; }}
  </style>
</head>
<body>
  <main class="page">
    <div class="topbar">
      <div>
        <h1>{html.escape(str(data.get("title", "Task Board")))}</h1>
        <p class="subtitle">Source: {html.escape(str(data.get("source_plan", "")))} · Updated: {html.escape(str(data.get("updated_at", "")))}</p>
      </div>
      <div class="command">python scripts/tasks.py list</div>
    </div>
    <div class="board">{''.join(cards)}</div>
  </main>
</body>
</html>
"""
    output_path.write_text(content, encoding="utf-8")
    return output_path


def command_list(args: argparse.Namespace) -> None:
    data = load_data(args.file)
    print_tasks(filtered_tasks(data, args))


def command_show(args: argparse.Namespace) -> None:
    data = load_data(args.file)
    print_task(find_task(data, args.task_id))


def command_status(args: argparse.Namespace, status: str) -> None:
    data = load_data(args.file)
    task = set_status(data, args.task_id, status, note=getattr(args, "note", None), tests=getattr(args, "tests", None))
    save_data(data, args.file)
    print(f"{task['id']} -> {task['status']}")
    print(f"Board: {DEFAULT_BOARD_PATH}")


def command_note(args: argparse.Namespace) -> None:
    data = load_data(args.file)
    task = find_task(data, args.task_id)
    append_note(task, args.text)
    save_data(data, args.file)
    print(f"Note added to {task['id']}")


def command_add(args: argparse.Namespace) -> None:
    data = load_data(args.file)
    try:
        find_task(data, args.task_id)
    except SystemExit:
        pass
    else:
        raise SystemExit(f"Task already exists: {args.task_id}")
    status = validate_status(data, args.status)
    task = {
        "id": args.task_id.upper(),
        "type": args.type,
        "area": args.area,
        "priority": args.priority,
        "status": status,
        "title": args.title,
        "description": args.description or "",
        "plan_ref": args.plan_ref or "",
        "acceptance": args.acceptance or [],
        "notes": [],
        "tests": [],
        "history": [{"created_at": utc_now(), "action": "created"}],
        "updated_at": utc_now(),
    }
    data.setdefault("tasks", []).append(task)
    save_data(data, args.file)
    print(f"Added {task['id']} -> {task['status']}")


def command_summary(args: argparse.Namespace) -> None:
    data = load_data(args.file)
    counts = Counter(task.get("status") for task in data.get("tasks", []))
    for status in BOARD_COLUMNS:
        print(f"{status}: {counts.get(status, 0)}")


def command_board(args: argparse.Namespace) -> None:
    data = load_data(args.file)
    output = render_board(data, args.output)
    print(output)
    if args.open:
        webbrowser.open(output.resolve().as_uri())


def command_next(args: argparse.Namespace) -> None:
    data = load_data(args.file)
    for status in ("ready", "in_progress", "blocked"):
        tasks = [task for task in data.get("tasks", []) if task.get("status") == status]
        if tasks:
            print_tasks(sorted(tasks, key=lambda task: (str(task.get("priority", "P9")), str(task.get("id")))))
            return
    print("No ready, in_progress or blocked tasks.")


def add_common_file_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--file", type=Path, default=DEFAULT_TASKS_PATH, help="Tasks JSON file")


def add_status_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser], name: str, status: str) -> None:
    parser = subparsers.add_parser(name, help=f"Move task to {status}")
    add_common_file_arg(parser)
    parser.add_argument("task_id")
    parser.add_argument("--note")
    parser.add_argument("--tests", action="append", default=[])
    parser.set_defaults(func=lambda args, wanted=status: command_status(args, wanted))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local task tracker")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List tasks")
    add_common_file_arg(list_parser)
    list_parser.add_argument("--status")
    list_parser.add_argument("--area")
    list_parser.add_argument("--type")
    list_parser.add_argument("--all", action="store_true", help="Include done tasks")
    list_parser.set_defaults(func=command_list)

    show_parser = subparsers.add_parser("show", help="Show task details")
    add_common_file_arg(show_parser)
    show_parser.add_argument("task_id")
    show_parser.set_defaults(func=command_show)

    for command_name, status in (
        ("ready", "ready"),
        ("start", "in_progress"),
        ("review", "review"),
        ("done", "done"),
        ("block", "blocked"),
    ):
        add_status_parser(subparsers, command_name, status)

    set_parser = subparsers.add_parser("set", help="Set an explicit status")
    add_common_file_arg(set_parser)
    set_parser.add_argument("task_id")
    set_parser.add_argument("status")
    set_parser.add_argument("--note")
    set_parser.add_argument("--tests", action="append", default=[])
    set_parser.set_defaults(func=lambda args: command_status(args, args.status))

    note_parser = subparsers.add_parser("note", help="Add a note to a task")
    add_common_file_arg(note_parser)
    note_parser.add_argument("task_id")
    note_parser.add_argument("text")
    note_parser.set_defaults(func=command_note)

    add_parser = subparsers.add_parser("add", help="Add a task")
    add_common_file_arg(add_parser)
    add_parser.add_argument("task_id")
    add_parser.add_argument("title")
    add_parser.add_argument("--area", required=True)
    add_parser.add_argument("--type", default="technical")
    add_parser.add_argument("--priority", default="P2")
    add_parser.add_argument("--status", default="backlog")
    add_parser.add_argument("--description")
    add_parser.add_argument("--plan-ref")
    add_parser.add_argument("--acceptance", action="append", default=[])
    add_parser.set_defaults(func=command_add)

    summary_parser = subparsers.add_parser("summary", help="Count tasks by status")
    add_common_file_arg(summary_parser)
    summary_parser.set_defaults(func=command_summary)

    next_parser = subparsers.add_parser("next", help="Show the next actionable tasks")
    add_common_file_arg(next_parser)
    next_parser.set_defaults(func=command_next)

    board_parser = subparsers.add_parser("board", help="Render local HTML board")
    add_common_file_arg(board_parser)
    board_parser.add_argument("--output", type=Path, default=DEFAULT_BOARD_PATH)
    board_parser.add_argument("--open", action="store_true")
    board_parser.set_defaults(func=command_board)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
