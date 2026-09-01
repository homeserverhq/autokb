"""Seed built-in plugins/sinks into the live (mounted) directories.

At runtime the container's ``/src/plugins`` and ``/src/sinks`` are host
bind mounts, so the built-in files baked into the image at
``/src/builtin_plugins`` and ``/src/builtin_sinks`` are invisible there.
On startup this module copies any built-in file that is *missing* from the
live directory so new releases can ship additional plugins/sinks without
clobbering user customisations. Files that already exist (same name) are
never overwritten.
"""

import os
import shutil
import tempfile


def _seed_dir(target_dir: str, source_dir: str, log) -> int:
    """Copy built-in files missing from ``target_dir`` into it.

    Never overwrites existing entries. Returns the number of files copied.
    """
    if not os.path.isdir(source_dir):
        log.info("builtin_source_missing", source_dir=source_dir, target_dir=target_dir)
        return 0

    os.makedirs(target_dir, exist_ok=True)
    copied = 0
    for fname in sorted(os.listdir(source_dir)):
        if fname.startswith(".") or not fname.endswith(".py"):
            continue
        src_path = os.path.join(source_dir, fname)
        if not os.path.isfile(src_path):
            continue
        dst_path = os.path.join(target_dir, fname)
        if os.path.exists(dst_path):
            continue

        fd, tmp_path = tempfile.mkstemp(dir=target_dir, prefix=f".{fname}.", suffix=".tmp")
        os.close(fd)
        try:
            shutil.copyfile(src_path, tmp_path)
            os.replace(tmp_path, dst_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        copied += 1
        log.info("builtin_seeded", file=fname, target_dir=target_dir)

    if copied:
        log.info("builtin_seed_done", target_dir=target_dir, copied=copied)
    return copied


def seed_builtins(log) -> None:
    """Seed built-in plugins and sinks into the live directories."""
    _seed_dir("/src/plugins", "/src/builtin_plugins", log)
    _seed_dir("/src/sinks", "/src/builtin_sinks", log)


__all__ = ["seed_builtins"]