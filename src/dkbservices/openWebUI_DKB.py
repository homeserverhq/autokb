"""OpenWebUI DKB service stub."""
from utils.dkb_service_base import BaseDKBService


class OpenWebUIDKB(BaseDKBService):
    metadata = {
        "name": "OpenWebUI",
        "description": "Open WebUI Knowledge Base",
        "icon": "openwebui.png",
    }

    def add_datafile(self, path: str) -> str:
        raise NotImplementedError("OpenWebUI add_datafile not implemented")

    def update_datafile(self, remote_datafile_id: str, path: str) -> None:
        raise NotImplementedError("OpenWebUI update_datafile not implemented")

    def remove_datafile(self, remote_datafile_id: str) -> None:
        raise NotImplementedError("OpenWebUI remove_datafile not implemented")

    def add_datastore(self) -> str:
        raise NotImplementedError("OpenWebUI add_datastore not implemented")

    def remove_datastore(self) -> None:
        raise NotImplementedError("OpenWebUI remove_datastore not implemented")

    def clear_datastore(self) -> None:
        raise NotImplementedError("OpenWebUI clear_datastore not implemented")
