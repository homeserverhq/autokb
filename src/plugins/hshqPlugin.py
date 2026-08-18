"""HSHQ help plugin: reads *.json from a local dir and emits one markdown
help file per entry (# Function Name / Description)."""

import glob
import hashlib
import json
import os

from utils.plugin_base import BaseSubscription


def _file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()


class hshqPlugin(BaseSubscription):
    metadata = {
        "name": "hshqPlugin",
        "display_name": "HSHQ Help",
        "description": (
            "Reads HSHQ *.json files from a local directory and writes one "
            "markdown help file per entry (Function Name and Description)."
        ),
        "sub_type": "SCHEDULED",
    }

    def get_schema(self):
        return {
            "type": "object",
            "properties": {
                "source_dir": {
                    "type": "string",
                    "default": "/scriptserver_hshq",
                    "minLength": 1,
                    "description": "Directory containing the *.json files to extract from",
                },
            },
            "required": ["source_dir"],
        }

    def getData(self, config, progress_callback):
        progress_callback(0, message="Starting...")
        source_dir = config["source_dir"]
        if not os.path.isdir(source_dir):
            progress_callback(100, message=f"Source dir missing: {source_dir}")
            return

        files = sorted(glob.glob(os.path.join(source_dir, "*.json")))
        api_index = {}
        for path in files:
            stem = os.path.splitext(os.path.basename(path))[0]
            try:
                with open(path, "r") as f:
                    data = json.load(f)
            except (OSError, ValueError) as e:
                self.log.warning("json_skipped", file=os.path.basename(path), error=str(e))
                continue
            group = data.get("group", "")
            name = data.get("name", "")
            desc = data.get("description", "")
            util_name = (group + " -> " + name) if group else name
            tmp = f"/tmp/{stem}.md"
            with open(tmp, "w") as f:
                f.write(f"# Function Name: {util_name}\n")
                f.write(f"\nDescription: {desc}\n")
            api_index[stem] = {"tmp_path": tmp, "hash": _file_hash(tmp)}

        output_dir = self.get_destination_path()
        disk_index = {}
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                stem = os.path.splitext(fname)[0]
                fpath = os.path.join(output_dir, fname)
                try:
                    disk_index[stem] = _file_hash(fpath)
                except OSError:
                    pass

        api_keys = set(api_index)
        disk_keys = set(disk_index)
        to_add = api_keys - disk_keys
        to_update = {k for k in api_keys & disk_keys
                     if api_index[k]["hash"] != disk_index[k]}
        to_delete = disk_keys - api_keys
        to_skip = (api_keys & disk_keys) - to_update

        progress_callback(10, message=(
            f"{len(api_keys)} entries, {len(to_add)} new, {len(to_update)} updated, "
            f"{len(to_delete)} stale"))
        done = 0
        total = len(to_add) + len(to_update) + len(to_delete) or 1

        for stem in sorted(to_add | to_update):
            self.move_to_destination(api_index[stem]["tmp_path"])
            done += 1
            progress_callback(10 + int(85 * done / total))
        for stem in to_skip:
            os.remove(api_index[stem]["tmp_path"])
        for stem in to_delete:
            for fname in os.listdir(output_dir):
                if os.path.splitext(fname)[0] == stem:
                    os.remove(os.path.join(output_dir, fname))
            done += 1
            progress_callback(10 + int(85 * done / total))
        if os.path.isdir(output_dir):
            for fname in os.listdir(output_dir):
                if os.path.splitext(fname)[0] not in api_keys:
                    os.remove(os.path.join(output_dir, fname))

        progress_callback(100, message=(
            f"Done: {len(to_add)} added, {len(to_update)} updated, "
            f"{len(to_delete)} removed, {len(to_skip)} unchanged"))
