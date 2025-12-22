# from pydantic import BaseModel, HttpUrl

# class DownloadRequest(BaseModel):
#     url: HttpUrl


from pydantic import BaseModel, HttpUrl
from typing import Optional
from enum import Enum


class FileCategory(str, Enum):
    document = "document"
    video = "video"
    audio = "audio"
    compressed = "compressed"
    program = "program"
    other = "other"


class DownloadRequest(BaseModel):
    url: HttpUrl
    filename: Optional[str] = None
    category: FileCategory = FileCategory.other


class DownloadStatusResponse(BaseModel):
    download_id: str
    status: str
    downloaded_bytes: int
    file_path: Optional[str] = None
