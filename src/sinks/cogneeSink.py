"""Cognee Sink service stub."""
import os

from utils.sink_base import BaseSink


class CogneeSink(BaseSink):
    metadata = {
        "name": "cognee",
        "description": (
            "Synchronizes AutoKB output files into a Cognee Memory Dataset "
            "so they can be processed into cognee's graph memory. Each Data "
            "Target maps to a dedicated dataset."
        ),
        "icon": "cognee.png",
    }
    default_api_url = "http://cognee-app:8000"
    api_key_env_var = "COGNEE_API_KEY"

    def __init__(self, target_row, db):
        super().__init__(target_row, db)
        self.api_url = (self.api_url or self.default_api_url).rstrip("/")
        self.api_key = self.api_key or (os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else "")

    def add_datafile(self, path: str) -> str:
        raise NotImplementedError("Cognee add_datafile not implemented")

    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        raise NotImplementedError("Cognee update_datafile not implemented")

    def remove_datafile(self, remote_datafile_id: str) -> None:
        raise NotImplementedError("Cognee remove_datafile not implemented")

    def add_target(self) -> str:
        raise NotImplementedError("Cognee add_target not implemented")

    def remove_target(self) -> None:
        raise NotImplementedError("Cognee remove_target not implemented")

    def clear_target(self) -> None:
        raise NotImplementedError("Cognee clear_target not implemented")


__all__ = ["CogneeSink"]
