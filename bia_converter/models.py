from pydantic import BaseModel
from typing import Dict, Tuple


class StructuredFileset(BaseModel):
    fileref_map: Dict[str, Tuple[int, int, int]]
    attributes: Dict = {}