"""Shared plugin-class loading helpers.

The "import a plugin file and find its ``BaseSubscription`` subclass" dance
was previously copy-pasted across the registry, the worker execution engine,
the child process entry point, and the Manager's Dev Lab. This module is the
single implementation, so the load semantics stay consistent everywhere.
"""

import importlib.util
import inspect
import os
import sys
from typing import Any, Optional, Type

from .plugin_base import BaseSubscription


def load_plugin_module(file_path: str, name: Optional[str] = None, *, register: bool = False) -> Any:
    """Import and execute a plugin file, returning its module.

    ``register=True`` adds the module to ``sys.modules`` (needed for
    relative imports inside plugins), but caches it for the process lifetime.
    The execution path passes ``register=False`` so every run loads a fresh
    class (no stale cached schema/state).
    """
    module_name = name or f"_plugin_{os.path.splitext(os.path.basename(file_path))[0]}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load spec for {file_path}")
    module = importlib.util.module_from_spec(spec)
    if register:
        sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def find_plugin_subclass(module: Any) -> Optional[Type[BaseSubscription]]:
    """Return the ``BaseSubscription`` subclass defined in ``module``, or None."""
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if obj is BaseSubscription:
            continue
        if issubclass(obj, BaseSubscription) and obj.__module__ == module.__name__:
            return obj
    return None


def load_plugin_class(file_path: str, *, register: bool = False) -> Type[BaseSubscription]:
    """Import ``file_path`` and return its ``BaseSubscription`` subclass.

    Raises ``RuntimeError`` when the file cannot be imported or defines no
    usable subclass.
    """
    module = load_plugin_module(file_path, register=register)
    cls = find_plugin_subclass(module)
    if cls is None:
        raise RuntimeError(f"No BaseSubscription subclass found in {file_path}")
    return cls


__all__ = ["load_plugin_module", "find_plugin_subclass", "load_plugin_class"]