from datetime import datetime

from .data import DataClass, Extra


class Event(DataClass, polymorph=True, concrete_type_key='event_name'):
    domain: str
    domain_id: str
    timestamp: datetime

    class Config:
        allow_mutation = False
