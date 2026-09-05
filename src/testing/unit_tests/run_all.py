"""Run every unit test script in this directory, stop on first failure.

Usage: ``python /src/testing/unit_tests/run_all.py`` (requires
``ENCRYPTION_KEY`` for the cipher tests).
"""

import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = ["test_crons.py", "test_cipher.py", "test_plugin_loading.py"]


def main() -> int:
    failed = []
    for name in SCRIPTS:
        path = os.path.join(HERE, name)
        print(f"=== {name} ===", flush=True)
        rc = subprocess.call([sys.executable, "-u", path], cwd="/tmp")
        if rc != 0:
            failed.append(name)
    if failed:
        print(f"FAILED: {failed}", flush=True)
        return 1
    print("RUN_ALL OK", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())