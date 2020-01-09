import json
from datetime import datetime


def json_loads(json_str):
    def object_hook(obj):
        if "_t_" not in obj:
            return obj
        if obj["_t_"] == "datetime":
            return datetime.fromisoformat(obj["_v_"])
        return obj

    return json.loads(json_str, object_hook=object_hook)


def json_dumps(data):
    def default(obj):
        if isinstance(obj, datetime):
            return {"_t_": "datetime", "_v_": obj.isoformat()}
        return super().default(obj)

    return json.dumps(data, default=default)


utcnow = datetime.utcnow
