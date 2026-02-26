from __future__ import annotations

import ast
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STATUS_NUEVA = "NUEVA"
STATUS_ACTIVA = "ACTIVA"
STATUS_INCONEXA = "INCONEXA"
STATUS_INEXISTENTE = "INEXISTENTE"

EXCLUDED_DIRS = {
    ".git",
    ".venv",
    ".pytest_cache",
    "__pycache__",
    ".mypy_cache",
    ".ruff_cache",
    ".idea",
    ".vscode",
}
EXCLUDED_GENERATED = {
    "docs/context/context_dictionary.md",
    "docs/context/architecture/scaffold_version.json",
    "docs/context/architecture/scaffold_context_input.md",
    "docs/context/architecture/scaffold_context.md",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_posix(path: Path) -> str:
    return path.as_posix()


def nid(kind: str, path: str, qual: str | None = None) -> str:
    return f"{kind}:{path}" if not qual else f"{kind}:{path}::{qual}"


def is_excluded_path(root: Path, path: Path, *, exclude_generated: bool) -> bool:
    rel = path.relative_to(root)
    if any(part in EXCLUDED_DIRS or part.startswith("playwright-data") for part in rel.parts[:-1]):
        return True
    if path.suffix.lower() in {".pyc", ".pyo"}:
        return True
    if exclude_generated and to_posix(rel) in EXCLUDED_GENERATED:
        return True
    return False


def iter_files(root: Path, *, exclude_generated: bool) -> list[Path]:
    files: list[Path] = []
    for p in root.rglob("*"):
        try:
            is_dir = p.is_dir()
        except OSError:
            continue
        if is_dir:
            continue
        if is_excluded_path(root, p, exclude_generated=exclude_generated):
            continue
        files.append(p)
    files.sort(key=lambda p: to_posix(p.relative_to(root)))
    return files


def module_from_rel(rel: str) -> str | None:
    if not rel.endswith(".py"):
        return None
    parts = list(Path(rel).with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts) if parts else None


class PyScanner(ast.NodeVisitor):
    def __init__(self, rel: str) -> None:
        self.rel = rel
        self.class_stack: list[str] = []
        self.funcs: list[dict[str, Any]] = []
        self.classes: list[dict[str, Any]] = []
        self.import_mods: list[str] = []
        self.import_froms: list[dict[str, Any]] = []
        self.refs_name: list[str] = []
        self.refs_attr: list[str] = []

    def visit_Import(self, node: ast.Import) -> Any:
        for a in node.names:
            self.import_mods.append(a.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
        self.import_mods.append(node.module or "")
        for a in node.names:
            self.import_froms.append({"module": node.module or "", "name": a.name, "level": int(node.level or 0)})
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> Any:
        self.refs_name.append(node.id)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> Any:
        self.refs_attr.append(node.attr)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> Any:
        qual = ".".join([*self.class_stack, node.name]) if self.class_stack else node.name
        item = {
            "id": nid("class", self.rel, qual),
            "kind": "class",
            "name": node.name,
            "qualname": qual,
            "path": self.rel,
            "lineno": getattr(node, "lineno", None),
            "end_lineno": getattr(node, "end_lineno", None),
            "signature": f"class {node.name}",
            "decorators": [self._dec_text(d) for d in node.decorator_list],
            "bases": [self._expr_text(b) for b in node.bases],
            "docstring": self._first(ast.get_docstring(node)),
            "methods": [],
        }
        if self.class_stack:
            parent = self._find_class(".".join(self.class_stack))
            if parent:
                parent["methods"].append(item)
        else:
            self.classes.append(item)
        self.class_stack.append(node.name)
        try:
            for child in node.body:
                self.visit(child)
        finally:
            self.class_stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        self._add_fn(node, is_async=False)
        for child in node.body:
            self.visit(child)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        self._add_fn(node, is_async=True)
        for child in node.body:
            self.visit(child)

    def _add_fn(self, node: ast.FunctionDef | ast.AsyncFunctionDef, *, is_async: bool) -> None:
        qual = ".".join([*self.class_stack, node.name]) if self.class_stack else node.name
        item = {
            "id": nid("method" if self.class_stack else "function", self.rel, qual),
            "kind": "method" if self.class_stack else "function",
            "name": node.name,
            "qualname": qual,
            "path": self.rel,
            "parent_class_qualname": ".".join(self.class_stack) if self.class_stack else None,
            "lineno": getattr(node, "lineno", None),
            "end_lineno": getattr(node, "end_lineno", None),
            "is_async": is_async,
            "signature": f"{'async ' if is_async else ''}def {node.name}{self._args(node.args)}",
            "decorators": [self._dec_text(d) for d in node.decorator_list],
            "docstring": self._first(ast.get_docstring(node)),
        }
        if self.class_stack:
            parent = self._find_class(".".join(self.class_stack))
            if parent:
                parent["methods"].append(item)
        else:
            self.funcs.append(item)

    def _find_class(self, qual: str) -> dict[str, Any] | None:
        for c in reversed(self.classes):
            if c["qualname"] == qual:
                return c
        return None

    def _args(self, args: ast.arguments) -> str:
        parts = [a.arg for a in [*args.posonlyargs, *args.args]]
        if args.vararg:
            parts.append(f"*{args.vararg.arg}")
        elif args.kwonlyargs:
            parts.append("*")
        parts.extend(a.arg for a in args.kwonlyargs)
        if args.kwarg:
            parts.append(f"**{args.kwarg.arg}")
        return f"({', '.join(parts)})"

    def _dec_text(self, node: ast.AST) -> str:
        try:
            return ast.unparse(node)
        except Exception:
            return node.__class__.__name__

    def _expr_text(self, node: ast.AST) -> str:
        try:
            return ast.unparse(node)
        except Exception:
            return node.__class__.__name__

    def _first(self, text: str | None) -> str | None:
        if not text:
            return None
        line = text.strip().splitlines()[0].strip()
        return line or None


def scan_python_file(root: Path, file_path: Path) -> dict[str, Any] | None:
    rel = to_posix(file_path.relative_to(root))
    try:
        src = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            src = file_path.read_text(encoding="latin-1")
        except Exception:
            return None
    except Exception:
        return None
    try:
        tree = ast.parse(src, filename=rel)
    except SyntaxError:
        return None
    s = PyScanner(rel)
    s.visit(tree)
    return {
        "path": rel,
        "module": module_from_rel(rel),
        "docstring": (ast.get_docstring(tree) or "").strip() or None,
        "imports_modules": sorted({m for m in s.import_mods if m is not None}),
        "imports_from": s.import_froms,
        "refs_name": s.refs_name,
        "refs_attr": s.refs_attr,
        "functions": s.funcs,
        "classes": s.classes,
    }


def resolve_from_import(module: str | None, level: int, current_module: str | None) -> str | None:
    base = current_module or ""
    if level <= 0:
        return module or None
    if not base:
        return None
    parts = base.split(".")
    prefix = parts[:-level] if level <= len(parts) else []
    suffix = [p for p in (module or "").split(".") if p]
    out = [*prefix, *suffix]
    return ".".join(out) if out else None


def default_short(kind: str, path: str, name: str | None = None) -> str:
    if kind == "directory":
        if path == ".":
            return "Raíz del repositorio."
        if path == "src":
            return "Código fuente principal."
        if path == "scripts":
            return "Scripts operativos y de prueba."
        if path == "docs":
            return "Documentación y fixtures."
        return f"Directorio `{Path(path).name}`."
    if kind == "file":
        p = Path(path)
        if path == "README.md":
            return "README principal del proyecto."
        if p.suffix == ".py":
            return f"Módulo Python `{p.stem}`."
        if p.suffix == ".md":
            return f"Documento Markdown `{p.name}`."
        return f"Archivo `{p.name}`."
    if kind == "class":
        return f"Clase `{name}`."
    if kind == "method":
        return f"Método `{name}`."
    return f"Función `{name}`."


def default_long(kind: str, path: str, name: str | None = None, doc: str | None = None) -> str:
    if doc:
        return doc
    if kind == "directory":
        return "Agrupa elementos relacionados del proyecto."
    if kind == "file":
        if path.startswith("src/routers/"):
            return "Endpoints FastAPI y capa HTTP."
        if path.startswith("src/services/"):
            return "Lógica de aplicación/orquestación y acceso a datos."
        if path.startswith("src/scraper/"):
            return "Scraping, navegación y extracción de datos."
        if path.startswith("src/pipeline/"):
            return "Procesado de reseñas y análisis con LLM."
        if path.startswith("src/workers/"):
            return "Workers de ejecución asíncrona."
        if path.startswith("scripts/"):
            return "Script auxiliar de operación, smoke test o bootstrap."
        return "Archivo del proyecto."
    if path.startswith("src/scraper/"):
        return "Callable del flujo de scraping/parsing."
    if path.startswith("src/services/"):
        return "Callable de lógica de negocio/aplicación."
    if path.startswith("src/pipeline/"):
        return "Callable de procesamiento/análisis."
    if path.startswith("src/routers/"):
        return "Callable relacionado con la capa HTTP."
    return f"Callable `{name or ''}` del proyecto.".strip()


def build_scaffold_tree(root: Path, files: list[Path]) -> tuple[dict[str, Any], dict[str, Any]]:
    py_scans: dict[str, dict[str, Any]] = {}
    module_to_file: dict[str, str] = {}
    for p in files:
        if p.suffix.lower() != ".py":
            continue
        scan = scan_python_file(root, p)
        if not scan:
            continue
        py_scans[scan["path"]] = scan
        if scan.get("module"):
            module_to_file[str(scan["module"])] = scan["path"]

    file_imported_by: dict[str, set[str]] = defaultdict(set)
    symbol_imported_by: dict[str, set[str]] = defaultdict(set)
    symbol_ref_count: dict[str, int] = defaultdict(int)
    symbol_to_file: dict[str, str] = {}
    name_to_ids: dict[str, set[str]] = defaultdict(set)

    for rel, scan in py_scans.items():
        for fn in scan["functions"]:
            name_to_ids[fn["name"]].add(fn["id"])
            symbol_to_file[fn["id"]] = rel
        for cls in scan["classes"]:
            name_to_ids[cls["name"]].add(cls["id"])
            symbol_to_file[cls["id"]] = rel
            for m in cls.get("methods", []):
                name_to_ids[m["name"]].add(m["id"])
                symbol_to_file[m["id"]] = rel

    for rel, scan in py_scans.items():
        for mod in scan["imports_modules"]:
            mod = (mod or "").strip(".")
            if mod in module_to_file and module_to_file[mod] != rel:
                file_imported_by[module_to_file[mod]].add(rel)
        for item in scan["imports_from"]:
            resolved = resolve_from_import(item.get("module"), int(item.get("level") or 0), scan.get("module"))
            if resolved and resolved in module_to_file and module_to_file[resolved] != rel:
                target_file = module_to_file[resolved]
                file_imported_by[target_file].add(rel)
                for sid in name_to_ids.get(str(item.get("name")), set()):
                    if symbol_to_file.get(sid) == target_file:
                        symbol_imported_by[sid].add(rel)
        for n in scan["refs_name"]:
            for sid in name_to_ids.get(n, set()):
                symbol_ref_count[sid] += 1
        for a in scan["refs_attr"]:
            for sid in name_to_ids.get(a, set()):
                symbol_ref_count[sid] += 1

    tree = {
        "id": nid("dir", "."),
        "kind": "directory",
        "name": ".",
        "path": ".",
        "short_description": default_short("directory", "."),
        "long_description": default_long("directory", "."),
        "children_dirs": [],
        "children_files": [],
    }
    dir_index: dict[str, dict[str, Any]] = {".": tree}

    def ensure_dir(rel_dir: str) -> dict[str, Any]:
        rel_dir = rel_dir or "."
        if rel_dir in dir_index:
            return dir_index[rel_dir]
        parent = to_posix(Path(rel_dir).parent)
        if parent == ".":
            parent = "."
        parent_node = ensure_dir(parent)
        node = {
            "id": nid("dir", rel_dir),
            "kind": "directory",
            "name": Path(rel_dir).name,
            "path": rel_dir,
            "short_description": default_short("directory", rel_dir),
            "long_description": default_long("directory", rel_dir),
            "children_dirs": [],
            "children_files": [],
        }
        parent_node["children_dirs"].append(node)
        dir_index[rel_dir] = node
        return node

    for p in files:
        rel = to_posix(p.relative_to(root))
        parent = to_posix(p.relative_to(root).parent)
        if parent == ".":
            parent = "."
        ensure_dir(parent)
        scan = py_scans.get(rel)
        file_node = {
            "id": nid("file", rel),
            "kind": "file",
            "name": p.name,
            "path": rel,
            "extension": p.suffix.lower(),
            "language": "python" if p.suffix.lower() == ".py" else "text",
            "module": scan.get("module") if scan else None,
            "imported_by_files": sorted(file_imported_by.get(rel, set())),
            "imports": {
                "modules": scan.get("imports_modules", []) if scan else [],
                "from_symbols": scan.get("imports_from", []) if scan else [],
            },
            "short_description": default_short("file", rel),
            "long_description": default_long("file", rel, doc=(scan.get("docstring") if scan else None)),
            "functions": [],
            "classes": [],
        }
        if scan:
            for fn in scan["functions"]:
                file_node["functions"].append(
                    {
                        **fn,
                        "imported_by_files": sorted(symbol_imported_by.get(fn["id"], set())),
                        "reference_count": int(symbol_ref_count.get(fn["id"], 0)),
                        "short_description": default_short("function", rel, fn["name"]),
                        "long_description": default_long("function", rel, fn["name"], fn.get("docstring")),
                    }
                )
            for cls in scan["classes"]:
                cls_node = {
                    **cls,
                    "imported_by_files": sorted(symbol_imported_by.get(cls["id"], set())),
                    "reference_count": int(symbol_ref_count.get(cls["id"], 0)),
                    "short_description": default_short("class", rel, cls["name"]),
                    "long_description": default_long("class", rel, cls["name"], cls.get("docstring")),
                    "methods": [],
                }
                for m in cls.get("methods", []):
                    cls_node["methods"].append(
                        {
                            **m,
                            "parent_class_name": cls["name"],
                            "parent_class_bases": cls.get("bases", []),
                            "imported_by_files": sorted(symbol_imported_by.get(m["id"], set())),
                            "reference_count": int(symbol_ref_count.get(m["id"], 0)),
                            "short_description": default_short("method", rel, m["name"]),
                            "long_description": default_long("method", rel, m["name"], m.get("docstring")),
                        }
                    )
                file_node["classes"].append(cls_node)
        dir_index[parent]["children_files"].append(file_node)

    def sort_dir(node: dict[str, Any]) -> None:
        node["children_dirs"].sort(key=lambda x: x["path"])
        node["children_files"].sort(key=lambda x: x["path"])
        for f in node["children_files"]:
            f["functions"].sort(key=lambda x: (x.get("lineno") or 0, x["name"]))
            f["classes"].sort(key=lambda x: (x.get("lineno") or 0, x["name"]))
            for c in f["classes"]:
                c["methods"].sort(key=lambda x: (x.get("lineno") or 0, x["name"]))
        for d in node["children_dirs"]:
            sort_dir(d)

    sort_dir(tree)
    return tree, py_scans


def flatten_tree(tree: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}

    def walk_dir(d: dict[str, Any]) -> None:
        out[d["id"]] = d
        for f in d.get("children_files", []):
            out[f["id"]] = f
            for fn in f.get("functions", []):
                out[fn["id"]] = fn
            for c in f.get("classes", []):
                out[c["id"]] = c
                for m in c.get("methods", []):
                    out[m["id"]] = m
        for child in d.get("children_dirs", []):
            walk_dir(child)

    walk_dir(tree)
    return out


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def prev_nodes_map(prev_json: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not prev_json:
        return out
    tree = prev_json.get("tree")
    if isinstance(tree, dict):
        out.update(flatten_tree(tree))
    for item in prev_json.get("deleted_nodes", []) or []:
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            out[item["id"]] = item
    return out


def parse_context_input_md(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    result: dict[str, dict[str, str]] = {}
    for m in re.finditer(r"```json\s*\n(\{.*?\})\s*\n```", text, re.DOTALL):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        item_id = str(data.get("id") or "").strip()
        if not item_id:
            continue
        result[item_id] = {
            "short_description": str(data.get("short_description") or "").strip(),
            "long_description": str(data.get("long_description") or "").strip(),
        }
    return result


def is_inconexa(item: dict[str, Any]) -> bool:
    if item.get("kind") not in {"function", "class", "method"}:
        return False
    name = str(item.get("name") or "")
    if name.startswith("__") and name.endswith("__"):
        return False
    if item.get("decorators"):
        return False
    if item.get("kind") == "method" and _is_dynamic_dispatch_method(item):
        return False
    return int(item.get("reference_count") or 0) <= 0 and not (item.get("imported_by_files") or [])


def _is_dynamic_dispatch_method(item: dict[str, Any]) -> bool:
    name = str(item.get("name") or "")
    if not name:
        return False

    parent_bases = [str(b or "") for b in (item.get("parent_class_bases") or [])]
    normalized_bases = [b.replace(" ", "") for b in parent_bases]

    is_node_visitor_subclass = any(
        base.endswith("NodeVisitor")
        or base.endswith(".NodeVisitor")
        or base == "ast.NodeVisitor"
        for base in normalized_bases
    )
    if is_node_visitor_subclass and (name == "visit" or name.startswith("visit_")):
        return True

    # Common callback names often connected by framework/runtime conventions.
    callback_like = (
        name.startswith("on_")
        or name.startswith("handle_")
    )
    if callback_like and item.get("decorators"):
        return True

    return False


def merge_desc(item: dict[str, Any], prev_item: dict[str, Any] | None, md_desc: dict[str, dict[str, str]]) -> None:
    if prev_item:
        s = str(prev_item.get("short_description") or "").strip()
        l = str(prev_item.get("long_description") or "").strip()
        if s:
            item["short_description"] = s
        if l:
            item["long_description"] = l
    d = md_desc.get(str(item.get("id")))
    if d:
        if d.get("short_description"):
            item["short_description"] = d["short_description"]
        if d.get("long_description"):
            item["long_description"] = d["long_description"]


def assign_status(item: dict[str, Any], prev_item: dict[str, Any] | None, run_version: int) -> None:
    item["last_seen_in"] = run_version
    item["introduced_in"] = prev_item.get("introduced_in", run_version) if prev_item else run_version
    if is_inconexa(item):
        item["status"] = STATUS_INCONEXA
        return
    item["status"] = STATUS_NUEVA if prev_item is None else STATUS_ACTIVA


def apply_versioning(tree: dict[str, Any], prev_map: dict[str, dict[str, Any]], md_desc: dict[str, dict[str, str]], run_version: int) -> list[dict[str, Any]]:
    def walk_dir(d: dict[str, Any]) -> None:
        merge_desc(d, prev_map.get(d["id"]), md_desc)
        assign_status(d, prev_map.get(d["id"]), run_version)
        for f in d.get("children_files", []):
            merge_desc(f, prev_map.get(f["id"]), md_desc)
            assign_status(f, prev_map.get(f["id"]), run_version)
            for fn in f.get("functions", []):
                merge_desc(fn, prev_map.get(fn["id"]), md_desc)
                assign_status(fn, prev_map.get(fn["id"]), run_version)
            for c in f.get("classes", []):
                merge_desc(c, prev_map.get(c["id"]), md_desc)
                assign_status(c, prev_map.get(c["id"]), run_version)
                for m in c.get("methods", []):
                    merge_desc(m, prev_map.get(m["id"]), md_desc)
                    assign_status(m, prev_map.get(m["id"]), run_version)
        for child in d.get("children_dirs", []):
            walk_dir(child)

    walk_dir(tree)
    current_ids = set(flatten_tree(tree).keys())
    deleted: list[dict[str, Any]] = []
    for item_id, prev in prev_map.items():
        if item_id in current_ids:
            continue
        ghost = json.loads(json.dumps(prev, ensure_ascii=False))
        ghost["status"] = STATUS_INEXISTENTE
        ghost["last_seen_in"] = run_version
        deleted.append(ghost)
    deleted.sort(key=lambda x: (str(x.get("kind")), str(x.get("path")), str(x.get("qualname") or x.get("name") or "")))
    return deleted


def stats_from_tree(tree: dict[str, Any], deleted: list[dict[str, Any]]) -> dict[str, int]:
    flat = flatten_tree(tree)
    stats: dict[str, int] = defaultdict(int)  # type: ignore[assignment]
    for item in flat.values():
        stats[f"kind:{item.get('kind')}"] += 1
        stats[f"status:{item.get('status')}"] += 1
    for _ in deleted:
        stats[f"status:{STATUS_INEXISTENTE}"] += 1
    return dict(sorted(stats.items()))


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def context_item_obj(item: dict[str, Any]) -> dict[str, Any]:
    out = {
        "id": item.get("id"),
        "kind": item.get("kind"),
        "path": item.get("path"),
        "name": item.get("name"),
        "status": item.get("status"),
        "short_description": item.get("short_description") or "",
        "long_description": item.get("long_description") or "",
    }
    for k in ("qualname", "signature", "lineno", "end_lineno"):
        if item.get(k) is not None:
            out[k] = item.get(k)
    return out


def iter_items_for_input(tree: dict[str, Any]) -> list[dict[str, Any]]:
    flat = flatten_tree(tree)
    return [flat[k] for k in sorted(flat.keys())]


def write_context_input_md(path: Path, tree: dict[str, Any], deleted: list[dict[str, Any]]) -> None:
    lines = [
        "# Scaffold Context Input (Strict)",
        "",
        "Edita solo `short_description` y `long_description` dentro de cada bloque JSON y mantén JSON válido.",
        "",
        f"## Actuales ({len(iter_items_for_input(tree))})",
        "",
    ]
    for item in iter_items_for_input(tree):
        lines += [f"### {item['id']}", "", "```json", json.dumps(context_item_obj(item), ensure_ascii=False, indent=2), "```", ""]
    lines += [f"## Inexistentes ({len(deleted)})", ""]
    for item in deleted:
        lines += [f"### {item['id']}", "", "```json", json.dumps(context_item_obj(item), ensure_ascii=False, indent=2), "```", ""]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def render_tree_md(tree: dict[str, Any]) -> list[str]:
    lines: list[str] = []

    def walk_dir(d: dict[str, Any], depth: int) -> None:
        ind = "  " * depth
        label = "Raíz" if d["path"] == "." else f"Directorio `{d['path']}`"
        lines.append(f"{ind}- [{d.get('status')}] {label}: {d.get('short_description')}")
        if d.get("long_description"):
            lines.append(f"{ind}  {d['long_description']}")
        for f in d.get("children_files", []):
            lines.append(f"{ind}  - [{f.get('status')}] `{f['path']}`: {f.get('short_description')}")
            for fn in f.get("functions", []):
                lines.append(f"{ind}    - [{fn.get('status')}] `{fn.get('signature') or fn.get('name')}`")
            for c in f.get("classes", []):
                lines.append(f"{ind}    - [{c.get('status')}] `{c.get('qualname')}`")
                for m in c.get("methods", []):
                    lines.append(f"{ind}      - [{m.get('status')}] `{m.get('signature') or m.get('name')}`")
        for child in d.get("children_dirs", []):
            walk_dir(child, depth + 1)

    walk_dir(tree, 0)
    return lines


def write_scaffold_context_md(path: Path, payload: dict[str, Any]) -> None:
    tree = payload["tree"]
    deleted = payload.get("deleted_nodes", [])
    flat = flatten_tree(tree)
    inconexas = [i for i in flat.values() if i.get("status") == STATUS_INCONEXA and i.get("kind") in {"function", "class", "method"}]
    lines = [
        "# Contexto de Scaffold del Proyecto",
        "",
        f"- `run_version`: `{payload.get('run_version')}`",
        f"- `generated_at`: `{payload.get('generated_at')}`",
        "",
        "## Leyenda",
        "",
        "- `NUEVA`: detectado por primera vez en esta versión.",
        "- `ACTIVA`: ya existía y sigue presente.",
        "- `INCONEXA`: callable sin conexión detectada (heurística AST/imports/referencias).",
        "- `INEXISTENTE`: existía en versión previa y ya no aparece en el análisis actual.",
        "",
        "## Resumen",
        "",
    ]
    for k, v in payload.get("stats", {}).items():
        lines.append(f"- `{k}`: `{v}`")
    lines += ["", "## Scaffold Recursivo", ""]
    lines.extend(render_tree_md(tree))
    lines += ["", "## Callables Inconexas", ""]
    if inconexas:
        for i in sorted(inconexas, key=lambda x: (x.get("path"), x.get("qualname") or x.get("name"))):
            lines.append(f"- `{i.get('path')}` -> `{i.get('qualname') or i.get('name')}`")
    else:
        lines.append("- Ninguna.")
    lines += ["", "## Elementos Inexistentes (Histórico)", ""]
    if deleted:
        for i in deleted:
            q = i.get("qualname") or i.get("name") or i.get("path")
            lines.append(f"- [{i.get('status')}] `{i.get('kind')}` `{i.get('path')}` :: `{q}`")
    else:
        lines.append("- Ninguno.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def ensure_file(path: Path, content: str) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def ensure_static_docs(root: Path) -> None:
    base = root / "docs" / "context"
    ensure_file(
        base / "README.md",
        """# Contexto del Proyecto

Documentación contextual para producto, arquitectura y scaffold del código.

## Entradas clave

- `context_dictionary.md`
- `project_objective.md`
- `phases/`
- `architecture/README.md`
- `architecture/scaffold_context.md` (generado)
- `architecture/scaffold_context_input.md` (intermedio, editable)
- `architecture/scaffold_version.json` (generado)

Regeneración:

```bash
uv run python scripts/generate_context_docs.py
```
""",
    )
    ensure_file(
        base / "project_objective.md",
        """# Objetivo del Proyecto

## Qué es

Sistema de inteligencia de reputación para negocios que recopila reseñas/menciones, las normaliza y genera análisis accionables.

## Qué hace hoy

- Scraping en Google Maps (Playwright)
- Persistencia en MongoDB
- Preprocesado y análisis con LLM (Gemini)
- API FastAPI con jobs y eventos de progreso (SSE)
- Reanálisis con reseñas almacenadas

## Fase actual

**Fase 1 (MVP Google Maps) avanzada**, con transición iniciada hacia una arquitectura multi-fuente.

## Fase objetivo

Plataforma multi-fuente con 3 workers:

1. `scrape-worker`
2. `analysis-worker`
3. `report-worker`

Con orquestación basada en mensajes (objetivo recomendado: RabbitMQ + workers propios).

## Mapa de fases

1. Fase 0: Descubrimiento y validación del problema
2. Fase 1: MVP Google Maps end-to-end
3. Fase 2: Núcleo multi-fuente + normalización
4. Fase 3: Workers especializados + orquestación
5. Fase 4: Informes y accionabilidad
6. Fase 5: Plataforma y escala
""",
    )
    ensure_file(
        base / "phases" / "README.md",
        """# Directorio de Fases

Cada archivo describe una fase desde producto + técnica + criterios de salida.
""",
    )
    phase_docs = {
        "fase_00_baseline_actual_google_maps.md": (
            "Fase 0 - Baseline actual Google Maps",
            "Documentar y estabilizar el sistema ya construido como base de no regresion.",
        ),
        "fase_01_arquitectura_modular_y_workers.md": (
            "Fase 1 - Arquitectura modular y workers",
            "Refactorizar la arquitectura para crecimiento multi-fuente con workers y colas.",
        ),
        "fase_02_modelo_canonico_negocio_y_fuentes.md": (
            "Fase 2 - Modelo canonico de negocio y fuentes",
            "Definir identidad de negocio, source profiles y mentions/reviews canonicas.",
        ),
        "fase_03_scraper_tripadvisor.md": (
            "Fase 3 - Scraper Tripadvisor",
            "Integrar Tripadvisor como nueva fuente de resenas usando el modelo canonico.",
        ),
        "fase_04_scraper_trustpilot.md": (
            "Fase 4 - Scraper Trustpilot",
            "Integrar Trustpilot como fuente de resenas empresariales.",
        ),
        "fase_05_scraper_reddit.md": (
            "Fase 5 - Scraper Reddit",
            "Integrar Reddit como fuente de menciones y contexto no estructurado.",
        ),
        "fase_06_refinamiento_analisis_llm_rag.md": (
            "Fase 6 - Refinamiento del analisis",
            "Mejorar prompts, modos, RAG/contexto, muestreo y calidad del output LLM.",
        ),
        "fase_07_informe_estructurado.md": (
            "Fase 7 - Informe estructurado",
            "Generar un informe profesional (PDF/Typst/HTML) desde analisis estructurados.",
        ),
        "fase_08_interfaz_mvp_analisis.md": (
            "Fase 8 - Interfaz MVP de analisis",
            "Formulario, progreso y visualizacion de informe para usuarios no tecnicos.",
        ),
        "fase_09_landing_demo_y_email.md": (
            "Fase 9 - Landing demo y email",
            "Captacion de leads y envio de mini resumen de analisis por correo.",
        ),
    }
    for filename, (title, desc) in phase_docs.items():
        ensure_file(
            base / "phases" / filename,
            f"""# {title}

## Objetivo

{desc}

## Producto

- Valor esperado para negocio en esta fase.
- Alcance funcional y límites.
- Métricas/KPI de validación.

## Técnico

- Arquitectura y componentes necesarios.
- Riesgos técnicos y de integración.
- Estrategia de pruebas/smoke tests.

## Criterios de salida

- Entregables mínimos completados.
- Riesgos críticos controlados.
- Documentación contextual actualizada.
""",
        )
    ensure_file(
        base / "architecture" / "README.md",
        """# Arquitectura del Proyecto

## Estado actual (MVP funcional)

Arquitectura pragmática por capas:

- `scraper` (Google Maps / Playwright)
- `pipeline` (preprocesado + LLM)
- `services` (orquestación + Mongo)
- `routers` (FastAPI)
- `workers` (jobs asíncronos)

## Evolución recomendada

Arquitectura híbrida:

- Vertical por fuente (`google_maps`, `trustpilot`, `tripadvisor`, `reddit`, ...)
- Horizontal en el core (normalización, matching, deduplicación, análisis, reporting)

## Workers objetivo

1. `scrape-worker`
2. `analysis-worker`
3. `report-worker`

## Orquestación recomendada

RabbitMQ + workers propios, con colas por etapa, reintentos, DLQ y progreso por eventos.

## Sistema de documentación contextual

El script `scripts/generate_context_docs.py` mantiene:

- `scaffold_version.json`
- `scaffold_context_input.md`
- `scaffold_context.md`
""",
    )


def markdown_files_for_dictionary(root: Path) -> list[Path]:
    files = [p for p in iter_files(root, exclude_generated=False) if p.suffix.lower() == ".md"]
    out = []
    for p in files:
        rel = to_posix(p.relative_to(root))
        if rel.startswith(".venv/") or rel.startswith(".pytest_cache/"):
            continue
        out.append(p)
    out.sort(key=lambda p: to_posix(p.relative_to(root)))
    return out


def md_category(rel: str) -> str:
    if rel == "README.md":
        return "README / Operación"
    if rel == "fase1_plan.docx.md":
        return "Plan original Fase 1"
    if rel.startswith("docs/context/phases/"):
        return "Roadmap por fases"
    if rel.startswith("docs/context/architecture/"):
        return "Arquitectura / Scaffold"
    if rel.startswith("docs/context/"):
        return "Contexto del proyecto"
    if rel.startswith("docs/"):
        return "Documento técnico/fixture de scraping"
    return "Markdown auxiliar"


def md_use(rel: str) -> str:
    if rel.endswith("scaffold_context.md"):
        return "Consultar mapa del código, estados y elementos inconexos/inexistentes."
    if rel.endswith("scaffold_context_input.md"):
        return "Editar descripciones estructuradas (short/long) por nodo."
    if rel.endswith("scaffold_version.json"):
        return "Usar como fuente machine-readable de estructura/versionado."
    if rel == "README.md":
        return "Entender ejecución local/docker, endpoints y scripts."
    if rel == "fase1_plan.docx.md":
        return "Comparar plan inicial vs estado actual."
    return "Contexto complementario del proyecto."


def write_context_dictionary(root: Path, path: Path) -> None:
    lines = [
        "# Diccionario Contextual de Markdown",
        "",
        "Índice de documentos Markdown relevantes para recuperar contexto de producto y técnico.",
        "",
        "## Orden recomendado de lectura",
        "",
        "1. `docs/context/context_dictionary.md`",
        "2. `docs/context/project_objective.md`",
        "3. `docs/context/architecture/README.md`",
        "4. `docs/context/architecture/scaffold_context.md`",
        "5. `fase1_plan.docx.md`",
        "6. `README.md`",
        "",
        "## Documentos",
        "",
    ]
    for p in markdown_files_for_dictionary(root):
        rel = to_posix(p.relative_to(root))
        if rel == "docs/context/context_dictionary.md":
            continue
        lines.append(f"- `{rel}`")
        lines.append(f"  Categoría: {md_category(rel)}")
        lines.append(f"  Uso: {md_use(rel)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def build_payload(root: Path, tree: dict[str, Any], deleted: list[dict[str, Any]], run_version: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "generated_at": now_iso(),
        "run_version": run_version,
        "project_name": root.name,
        "project_root": str(root),
        "scan": {
            "excluded_dirs": sorted(EXCLUDED_DIRS),
            "excluded_generated": sorted(EXCLUDED_GENERATED),
            "notes": [
                "AST Python para funciones/clases/métodos.",
                "INCONEXA se calcula por heurística de imports y referencias AST.",
                "Mover un archivo implica path nuevo => nodo NUEVA y nodo anterior INEXISTENTE.",
            ],
        },
        "stats": stats_from_tree(tree, deleted),
        "tree": tree,
        "deleted_nodes": deleted,
    }


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    ensure_static_docs(root)

    base = root / "docs" / "context" / "architecture"
    json_path = base / "scaffold_version.json"
    input_md_path = base / "scaffold_context_input.md"
    final_md_path = base / "scaffold_context.md"
    dict_md_path = root / "docs" / "context" / "context_dictionary.md"

    prev_json = load_json(json_path)
    prev_map = prev_nodes_map(prev_json)
    md_desc = parse_context_input_md(input_md_path)
    run_version = int(prev_json.get("run_version", 0)) + 1 if prev_json else 1

    files = iter_files(root, exclude_generated=True)
    tree, _py_scans = build_scaffold_tree(root, files)
    deleted = apply_versioning(tree, prev_map, md_desc, run_version)
    payload = build_payload(root, tree, deleted, run_version)

    write_json(json_path, payload)
    write_context_input_md(input_md_path, tree, deleted)
    write_scaffold_context_md(final_md_path, payload)
    write_context_dictionary(root, dict_md_path)

    print("Context docs generated")
    print(f"JSON: {json_path.relative_to(root)}")
    print(f"INPUT_MD: {input_md_path.relative_to(root)}")
    print(f"FINAL_MD: {final_md_path.relative_to(root)}")
    print(f"DICTIONARY_MD: {dict_md_path.relative_to(root)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
