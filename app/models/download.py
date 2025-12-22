from pydantic import BaseModel
from typing import Optional


class Download(BaseModel):
    id: str
    url: str
    file_path: str
    downloaded: int = 0
    total: Optional[int] = None
    status: str = "queued"  
