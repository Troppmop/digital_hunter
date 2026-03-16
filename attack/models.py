from pydantic import BaseModel

class AttackAlert(BaseModel):
    timestamp: str
    attack_id: str
    entity_id: str
    weapon_type: str