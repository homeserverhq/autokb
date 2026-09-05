"""Dev Lab static source validation/surgery for the Manager.

The Dev Lab accepts user-authored plugin and Sink code. All validation here is
AST-based static analysis (no execution) so an operator can iterate on code in
the editor without any of it being imported or run on the host. Extracted from
the Manager monolith.
"""

from typing import Any, Dict, Optional, Type

from fastapi import HTTPException

from utils.constants import AUTOKB_RESERVED_NAMES
from utils.misc_utils import sanitize_name


# Maximum length of a plugin name in characters, and a display name /
# service name for the proof-of-schedule. Enforced at the Dev Lab endpoints
# to keep plugin grid cards from overflowing.
MAX_PLUGIN_NAME_LEN = 32
MAX_DISPLAY_NAME_LEN = 64

_SINK_ABSTRACT_METHODS = [
    "add_datafile",
    "update_datafile",
    "remove_datafile",
    "add_target",
    "remove_target",
    "clear_target",
]


def _find_plugin_class_in_module(module: Any) -> Optional[Type[Any]]:
    """Return the single BaseSubscription subclass defined in ``module``."""
    from utils.plugin_loading import find_plugin_subclass
    return find_plugin_subclass(module)


def _find_sink_class_in_module(module: Any) -> Optional[Type[Any]]:
    """Return the single BaseSink subclass defined in ``module``."""
    from utils.sink_base import BaseSink
    import inspect as _inspect
    found = None
    for _, obj in _inspect.getmembers(module, _inspect.isclass):
        if obj is BaseSink:
            continue
        if issubclass(obj, BaseSink) and obj.__module__ == module.__name__:
            found = obj
            break
    return found


def _require_display_name(body: Dict[str, Any]) -> str:
    dn = (body.get("display_name") or "").strip()
    if not dn:
        raise HTTPException(status_code=400, detail="Display name is required")
    if len(dn) > MAX_DISPLAY_NAME_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"Display name too long: {len(dn)} chars (max {MAX_DISPLAY_NAME_LEN})",
        )
    if any(ord(c) < 32 for c in dn):
        raise HTTPException(status_code=400, detail="Display name cannot contain control characters")
    return dn


def _set_metadata_display_name_in_source(code: str, display_name: str,
                                          base_class_marker: str = "BaseSubscription") -> str:
    """Set or add a ``display_name`` key in the class-level ``metadata`` dict."""
    import ast
    new_value_repr = f'"{display_name}"'
    try:
        tree = ast.parse(code, filename="<dev_lab>")
    except SyntaxError:
        return code
    target_class = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                bn = ast.unparse(base) if hasattr(ast, "unparse") else getattr(base, "id", "")
                if base_class_marker in bn:
                    target_class = node
                    break
        if target_class is not None:
            break
    if target_class is None:
        return code
    metadata_assign = None
    for node in target_class.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "metadata" for t in node.targets
        ):
            if isinstance(node.value, ast.Dict):
                metadata_assign = node
                break
    if metadata_assign is None:
        return code
    dict_node = metadata_assign.value
    # Try to replace an existing display_name value.
    for i, key in enumerate(dict_node.keys):
        if key is None:
            continue
        key_str = None
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            key_str = key.value
        elif hasattr(ast, "unparse"):
            try:
                key_str = ast.unparse(key)
            except Exception:
                pass
        if key_str == "display_name":
            val = dict_node.values[i]
            if isinstance(val, ast.Constant) and hasattr(val, "end_lineno") and val.end_lineno is not None:
                start_line = val.lineno - 1
                start_col = val.col_offset
                end_line = val.end_lineno - 1
                end_col = val.end_col_offset
                lines = code.splitlines(keepends=True)
                if start_line == end_line:
                    line = lines[start_line]
                    lines[start_line] = line[:start_col] + new_value_repr + line[end_col:]
                else:
                    first_part = lines[start_line][:start_col] + new_value_repr
                    last_part = lines[end_line][end_col:]
                    lines[start_line] = first_part + last_part
                    del lines[start_line + 1:end_line + 1]
                return "".join(lines)
            return code
    # Insert after the "name" entry.
    name_end = None
    for i, key in enumerate(dict_node.keys):
        if key is None:
            continue
        key_str = None
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            key_str = key.value
        elif hasattr(ast, "unparse"):
            try:
                key_str = ast.unparse(key)
            except Exception:
                pass
        if key_str == "name":
            val = dict_node.values[i]
            if isinstance(val, ast.Constant) and hasattr(val, "end_lineno") and val.end_lineno is not None:
                name_end = (val.end_lineno - 1, val.end_col_offset)
            break
    if name_end is None:
        return code
    line_no, col = name_end
    lines = code.splitlines(keepends=True)
    name_line = lines[line_no]
    indent = name_line[: len(name_line) - len(name_line.lstrip())]
    lines[line_no] = (
        lines[line_no][:col] + ",\n" + indent + f'"display_name": {new_value_repr}' + lines[line_no][col:]
    )
    return "".join(lines)


def _validate_plugin_code(code: str, plugin_name: str) -> Dict[str, Any]:
    """Run a static validation pass against the supplied code."""
    try:
        sanitized = sanitize_name(plugin_name)
    except ValueError as exc:
        return {"ok": False, "error": f"Invalid plugin name: {exc}"}
    if sanitized != plugin_name:
        return {"ok": False, "error": f"Plugin name must already be sanitized; got {plugin_name!r}"}

    import ast
    try:
        tree = ast.parse(code, filename=f"{plugin_name}.py")
    except SyntaxError as exc:
        return {"ok": False, "error": f"Syntax error: {exc}"}

    # Find a class with a BaseSubscription parent
    found_class = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                bn = ast.unparse(base) if hasattr(ast, "unparse") else getattr(base, "id", "")
                if "BaseSubscription" in bn:
                    found_class = node
                    break
        if found_class:
            break
    if found_class is None:
        return {"ok": False, "error": "No class inheriting from BaseSubscription found"}

    # Check required methods/attributes
    has_getData = any(isinstance(n, ast.FunctionDef) and n.name == "getData" for n in ast.walk(found_class))
    has_get_schema = any(isinstance(n, ast.FunctionDef) and n.name == "get_schema" for n in ast.walk(found_class))
    if not has_getData:
        return {"ok": False, "error": "Missing getData() method"}
    if not has_get_schema:
        return {"ok": False, "error": "Missing get_schema() method"}

    # metadata
    has_metadata = any(
        isinstance(n, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "metadata" for t in n.targets
        )
        for n in found_class.body
    )
    if not has_metadata:
        return {"ok": False, "error": "Missing class-level 'metadata' attribute"}

    if sanitized in AUTOKB_RESERVED_NAMES:
        return {"ok": False, "error": f"Plugin name '{plugin_name}' is reserved and cannot be used."}

    return {"ok": True, "plugin_id": sanitized}


def _validate_sink_code(code: str, service_name: str) -> Dict[str, Any]:
    """Run a static validation pass against Sink service code.

    Mirrors ``_validate_plugin_code`` but for ``BaseSink`` subclasses:
    the class must define the ``metadata`` dict (with a ``name`` key) and
    implement all six abstract remote-operation methods. The service name
    must already be sanitized (it becomes the ``*Sink.py`` file stem).
    """
    try:
        sanitized = sanitize_name(service_name)
    except ValueError as exc:
        return {"ok": False, "error": f"Invalid Sink service name: {exc}"}
    if sanitized != service_name:
        return {"ok": False, "error": f"Sink service name must already be sanitized; got {service_name!r}"}

    import ast
    try:
        tree = ast.parse(code, filename=f"{service_name}.py")
    except SyntaxError as exc:
        return {"ok": False, "error": f"Syntax error: {exc}"}

    found_class = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                bn = ast.unparse(base) if hasattr(ast, "unparse") else getattr(base, "id", "")
                if "BaseSink" in bn:
                    found_class = node
                    break
        if found_class:
            break
    if found_class is None:
        return {"ok": False, "error": "No class inheriting from BaseSink found"}

    for method in _SINK_ABSTRACT_METHODS:
        has_method = any(
            isinstance(n, ast.FunctionDef) and n.name == method for n in ast.walk(found_class)
        )
        if not has_method:
            return {"ok": False, "error": f"Missing {method}() method"}

    has_metadata = any(
        isinstance(n, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "metadata" for t in n.targets
        )
        for n in found_class.body
    )
    if not has_metadata:
        return {"ok": False, "error": "Missing class-level 'metadata' attribute"}

    # The registry requires sanitize_name(metadata["name"]) == file stem,
    # so the code's metadata name must sanitize to the provided name.
    meta_name = None
    for node in found_class.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "metadata" for t in node.targets
        ):
            if isinstance(node.value, ast.Dict):
                for i, key in enumerate(node.value.keys):
                    if key is None:
                        continue
                    key_str = None
                    if isinstance(key, ast.Constant) and isinstance(key.value, str):
                        key_str = key.value
                    elif hasattr(ast, "unparse"):
                        try:
                            key_str = ast.unparse(key)
                        except Exception:
                            pass
                    if key_str == "name":
                        val = node.value.values[i]
                        if isinstance(val, ast.Constant) and isinstance(val.value, str):
                            meta_name = val.value
                        break
            break
    if not meta_name:
        return {"ok": False, "error": "Missing metadata['name'] string in Sink service class"}
    try:
        if sanitize_name(meta_name) != sanitized:
            return {
                "ok": False,
                "error": (
                    f"metadata['name'] ({meta_name!r}) must sanitize to the service name "
                    f"{sanitized!r}"
                ),
            }
    except ValueError:
        return {"ok": False, "error": f"metadata['name'] {meta_name!r} is not a valid name"}

    return {"ok": True, "service_name": sanitized}