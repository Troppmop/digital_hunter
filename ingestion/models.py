from pydantic import BaseModel
import datetime
import uuid
from typing import Literal

class IntelSignal(BaseModel):
    timestamp: datetime.datetime
    signal_id: uuid.UUID
    entity_id: str
    reported_lat: float
    reported_lon: float
    signal_type: Literal['SIGINT', 'VISINT', 'HUMINT']
    priority_level: int

class AirAttack(BaseModel):
    timestamp: datetime.datetime
    attack_id: uuid.UUID
    entity_id: str
    weapon_type: str

class DamageAlert(BaseModel):
    timestamp: datetime.datetime
    attack_id: uuid.UUID
    entity_id: str
    result: str
