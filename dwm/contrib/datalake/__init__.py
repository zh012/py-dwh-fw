from .client import DatalakeClient
from .lookup import get_dataset_id


prod = DatalakeClient(
    "http://dataset-service-prod.mm7pbnhhzr.ap-south-1.elasticbeanstalk.com"
)

