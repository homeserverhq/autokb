"""Standalone child entry-point for ``execution_engine._child_main``.

This script is invoked by ``subprocess.Popen`` from the worker process
when running a single subscription. It is *not* a multiprocessing
target — multiprocessing's spawn would have sys.path issues importing
``worker.execution_engine`` since the worker entry point is a script
run with ``python /src/worker/worker.py``.

The run payload (the base64-free JSON blob with the decrypted config) is
delivered over an inherited pipe file descriptor named by the
``AUTOKB_CFG_FD`` environment variable — never on the command line, so
credentials never appear in ``/proc/<pid>/cmdline`` or ``ps``.
"""
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


def _read_fd_blob(fd: int, max_bytes: int = 16 * 1024 * 1024) -> bytes:
    """Read all bytes from ``fd`` until EOF, enforcing a size cap."""
    chunks = []
    total = 0
    while True:
        chunk = os.read(fd, 65536)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ValueError("Run payload exceeds the maximum allowed size")
        chunks.append(chunk)
    return b"".join(chunks)


def _prune_on_parent_death() -> None:
    """Best-effort orphan prevention: die if the supervising worker dies.

    Installs Linux ``prctl(PR_SET_PDEATHSIG, SIGKILL)`` so the kernel kills
    this child the moment its parent (the worker L1 process) exits — even if
    the parent was hard-killed (SIGKILL) with no chance to clean up. Without
    this, a dead worker leaves a runaway child that can race a legitimate
    later run once the safety lock lapses.
    """
    try:
        import ctypes
        import signal
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        libc.prctl(1, signal.SIGKILL)
    except Exception:
        pass


def main() -> None:
    _prune_on_parent_death()
    fd = int(os.environ["AUTOKB_CFG_FD"])
    try:
        raw = _read_fd_blob(fd)
    finally:
        os.close(fd)
    args = json.loads(raw.decode("utf-8"))
    del raw
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