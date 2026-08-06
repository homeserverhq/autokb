"""Cognee DKB service stub."""
from utils.dkb_service_base import BaseDKBService


class CogneeDKB(BaseDKBService):
    metadata = {
        "name": "cognee",
        "description": "Cognee Memory Dataset",
        "icon": "cognee.png",
    }

    def add_datafile(self, path: str) -> str:
        raise NotImplementedError("Cognee add_datafile not implemented")

    def update_datafile(self, remote_datafile_id: str, path: str) -> None:
        raise NotImplementedError("Cognee update_datafile not implemented")

    def remove_datafile(self, remote_datafile_id: str) -> None:
        raise NotImplementedError("Cognee remove_datafile not implemented")

    def add_datastore(self) -> str:
        raise NotImplementedError("Cognee add_datastore not implemented")

    def remove_datastore(self) -> None:
        raise NotImplementedError("Cognee remove_datastore not implemented")

    def clear_datastore(self) -> None:
        raise NotImplementedError("Cognee clear_datastore not implemented")
