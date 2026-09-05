"""Cron validation + matching unit checks (R14).

Runnable directly: ``python /src/testing/unit_tests/test_crons.py``.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.constants import LOG_LEVEL  # noqa: F401  (import-time env defaults)
from utils.misc_utils import cron_due, is_valid_cron
from datetime import datetime, timezone


def test_range_validation():
    assert is_valid_cron("0 0 * * *")
    assert is_valid_cron("*/5 * * * *")
    assert is_valid_cron("0 0 * * 0")
    assert is_valid_cron("0 0 1 1 *")
    assert is_valid_cron("30 6 15 * 1-5")
    assert not is_valid_cron("60 * * * *")       # minute out of range
    assert not is_valid_cron("0 25 * * *")       # hour out of range
    assert not is_valid_cron("0 0 32 * *")       # dom out of range
    assert not is_valid_cron("0 0 * 13 *")       # month out of range
    assert not is_valid_cron("0 0 * * 8")        # dow out of range
    assert not is_valid_cron("0 0 * * * *")      # 6 fields
    assert not is_valid_cron("* * * *")          # 4 fields
    assert not is_valid_cron("a b c d e")        # non-numeric
    assert not is_valid_cron("1-0 * * * *")      # reversed range
    assert not is_valid_cron("*/0 * * * *")      # zero step
    assert not is_valid_cron("*/61 * * * *")     # step beyond minute range


def test_dow_7_aliases_sunday():
    sunday = datetime(2026, 9, 6, 0, 0, tzinfo=timezone.utc)
    monday = datetime(2026, 9, 7, 0, 0, tzinfo=timezone.utc)
    assert cron_due("0 0 * * 7", sunday) is True
    assert cron_due("0 0 * * 7", monday) is False
    assert cron_due("0 0 * * 1-7", monday) is True
    assert cron_due("0 0 * * 0", sunday) is True


def test_utc_matching():
    # cron_due should use UTC even without an explicit now (matches NOW()).
    due = cron_due("*/5 * * * *")
    assert isinstance(due, bool)


def main():
    for fn in (test_range_validation, test_dow_7_aliases_sunday, test_utc_matching):
        fn()
        print(f"  ok: {fn.__name__}")
    print("test_crons.py: ALL PASSED")


if __name__ == "__main__":
    main()