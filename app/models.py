from pydantic import BaseModel
from typing import Dict, Union, Optional

class DataRequest(BaseModel):
    operation: str
    table: str
    fields: Dict[str, Union[str, int]]
    primary_key: Optional[Dict[str, Union[str, int]]] = None
