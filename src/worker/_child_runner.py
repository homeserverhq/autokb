"""Standalone child entry-point for ``execution_engine._child_main``.

This script is invoked by ``subprocess.Popen`` from the worker process
when running a single subscription. It is *not* a multiprocessing
target — multiprocessing's spawn would have sys.path issues importing
``worker.execution_engine`` since the worker entry point is a script
run with ``python /src/worker/worker.py``.

Args are passed as JSON on the command line (the only argv[1] we accept
is a base64-encoded JSON blob containing the call arguments).
"""
import base64
import json
import os
import sys

_BOOTSTRAP_DONE = False


def _bootstrap() -> None:
    global _BOOTSTRAP_DONE
    if _BOOTSTRAP_DONE:
        return
    _BOOTSTRAP_DONE = True
    # Ensure /src is on sys.path so 'utils' and 'worker' packages import
    here = os.path.dirname(os.path.abspath(__file__))
    parent = os.path.dirname(here)
    for p in (parent, here):
        if p not in sys.path:
            sys.path.insert(0, p)


_bootstrap()

from worker.execution_engine import _child_main  # noqa: E402


def main() -> None:
    raw = sys.argv[1]
    args = json.loads(base64.b64decode(raw).decode("utf-8"))
    _child_main(
        file_path=args["file_path"],
        config=args["config"],
        sub_id=args["sub_id"],
        sub_name=args["sub_name"],
        db_url=args["db_url"],
        password_field_names=args["password_field_names"],
        hb_path=args.get("hb_path"),
        err_path=args.get("err_path"),
    )


if __name__ == "__main__":
    main()
