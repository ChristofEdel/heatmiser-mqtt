import json
from dataclasses import asdict, is_dataclass

def _json_default(obj):
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if is_dataclass(obj):
        return asdict(obj)
    raise TypeError(f"{type(obj).__name__} is not JSON serializable")

def json_dumps(obj, indent=4):
    return json.dumps(obj, default=_json_default, indent=indent)