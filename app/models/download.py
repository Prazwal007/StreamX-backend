from pydantic import BaseModel
from typing import Optional


class Download(BaseModel):
    id: str
    filename:str
    url: str
    file_path: str
    downloaded: int = 0
    total: Optional[int] = None
    status: str = "queued"  
    eta_seconds:Optional[int]=None

    is_pinned:Optional[bool]=False
    pinned_at:Optional[int]=None
