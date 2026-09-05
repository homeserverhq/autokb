"""Shared Docling (OCR / chunk-hybrid) HTTP helpers.

The long-running Docling task-polling loop and the auth error type were
duplicated verbatim across the crawl4AI and ePaperless plugins. They live
here now; each plugin keeps its own submit/result flow and its own
``_DOCLING_OPTIONS`` payload.
"""


HEARTBEAT_INTERVAL = 20  # seconds between progress heartbeats while polling


class DoclingAuthError(RuntimeError):
    """Fatal Docling authentication/authorization failure."""


def poll_docling_task(base_url, headers, task_id, progress_callback, current_pct, log):
    """Poll a Docling ``/v1/status/poll/{task_id}`` job until it terminates.

    Pulses ``progress_callback(current_pct)`` every ``HEARTBEAT_INTERVAL``
    seconds so the enclosing run's heartbeat stays fresh during what can be a
    long synchronous wait. Returns the final JSON body on ``success`` and
    raises ``RuntimeError`` on ``failure``.
    """
    import requests
    import time

    poll_url = f"{base_url}/v1/status/poll/{task_id}"
    last_heartbeat = time.time()

    while True:
        try:
            resp = requests.get(
                poll_url, params={"wait": 30}, headers=headers, timeout=35
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            log.error("docling_poll_error", task_id=task_id, error=str(exc))
            raise

        status = data["task_status"]

        now = time.time()
        if now - last_heartbeat >= HEARTBEAT_INTERVAL:
            progress_callback(current_pct)
            last_heartbeat = now

        if status == "success":
            return data
        elif status == "failure":
            err_msg = data.get("error_message", "Unknown error")
            log.error("docling_job_failed", task_id=task_id, error=err_msg)
            raise RuntimeError(f"Docling failed: {err_msg}")

        time.sleep(5)


__all__ = ["DoclingAuthError", "HEARTBEAT_INTERVAL", "poll_docling_task"]