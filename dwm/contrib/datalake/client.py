from collections import OrderedDict
import requests


class DatalakeClient:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")
        self._datasets = None

    def _get(self, path):
        return requests.get("{}{}".format(self.base_url, path)).json()

    def datasets(self):
        if not self._datasets:
            self._datasets = self._get("/v2/datasets/")
        return self._datasets

    def ds_profile(self, ds_id, full=False):
        resp = self._get("/v2/datasets/{}/".format(ds_id))

        if full:
            return resp

        profile = {
            key: resp[key] for key in ["id", "name", "dataset_type", "ingest_type"]
        }
        s3_dest = next(
            dest for dest in resp["destination"] if dest["name"] == "S3_MANIFEST"
        )
        profile.update(
            {k: s3_dest[k] for k in ["partition_field", "sync_id", "last_sync_time"]}
        )
        profile["fields"] = sorted(
            (f["name"], f["data_type"]) for f in resp["fields"]
        )
        return profile

    def ds_partitions(self, ds_id):
        partitions = self._get("/v2/datasets/{}/manifest".format(ds_id))["partitions"]
        return sorted(partitions.items(), reverse=True)
