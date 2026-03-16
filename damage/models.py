from pydantic import BaseModel

class DamageAlert(BaseModel):
    timestamp: str
    attack_id: str
    entity_id: str
    result: str