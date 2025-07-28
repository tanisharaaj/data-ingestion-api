from pydantic import BaseModel
from typing import Dict, Union

class DataRequest(BaseModel):
    operation: str
    table: str
    fields: Dict[str, Union[str, int]]
    primary_key: Dict[str, Union[str, int]]
