import json
from importlib.resources import as_file, files


def load_json_resource(name):
    resources = files("ctapipe_io_zfits") / "resources"

    with as_file(resources / name) as path:
        with path.open("r") as f:
            return json.load(f)


def load_subarrays():
    return load_json_resource("subarray-ids.json")


def load_array_elements():
    return load_json_resource("array-element-ids.json")
