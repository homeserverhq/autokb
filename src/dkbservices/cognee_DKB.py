"""Cognee DKB service stub."""
import os

from utils.dkb_service_base import BaseDKBService


class CogneeDKB(BaseDKBService):
    metadata = {
        "name": "cognee",
        "description": "Cognee Memory Dataset",
        "icon": "cognee.png",
    }
    default_api_url = "http://cognee-app:8000"
    api_key_env_var = "COGNEE_API_KEY"

    def __init__(self, datastore_row, db):
        super().__init__(datastore_row, db)
        self.api_url = (self.api_url or self.default_api_url).rstrip("/")
        self.api_key = self.api_key or (os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else "")

    def add_datafile(self, path: str) -> str:
        raise NotImplementedError("Cognee add_datafile not implemented")

    def update_datafile(self, remote_datafile_id: str, path: str) -> str:
        raise NotImplementedError("Cognee update_datafile not implemented")

    def remove_datafile(self, remote_datafile_id: str) -> None:
        raise NotImplementedError("Cognee remove_datafile not implemented")

    def add_datastore(self) -> str:
        raise NotImplementedError("Cognee add_datastore not implemented")

    def remove_datastore(self) -> None:
        raise NotImplementedError("Cognee remove_datastore not implemented")

    def clear_datastore(self) -> None:
        raise NotImplementedError("Cognee clear_datastore not implemented")


__all__ = ["CogneeDKB"]
