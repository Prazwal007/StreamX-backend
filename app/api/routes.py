from fastapi import APIRouter,Query
from app.models.download import Download
from pydantic import BaseModel
import time
from app.services.manager import DownloadManager
from fastapi import WebSocket, WebSocketDisconnect
from app.ws import ws_manager
from app.utils.download_utils import get_total_size
from app.utils.url_validator import validate_download_url

router = APIRouter()
manager = DownloadManager()

@router.websocket("/ws/downloads")
async def downloads_ws(ws: WebSocket):
    await ws_manager.connect(ws)

    try:
        while True:
            await ws.receive_text() 
    except WebSocketDisconnect:
        await ws_manager.disconnect(ws)


@router.post("/download")
async def add_download(url:str):
    url=validate_download_url(url) #line added
    d = await manager.create_download(url)
    d.total=await get_total_size(url)
    await manager.start(d.id)   # starts async task
    return d


@router.post("/pause/{download_id}")
async def pause(download_id: str):
    await manager.pause(download_id)
    return {"status": "paused"}


@router.post("/resume/{download_id}")
async def resume(download_id: str):
    await manager.resume(download_id)
    return {"status": "resumed"}


@router.post("/pause-all")
async def pause_all():
    await manager.pause_all()
    return {"status": "paused all"}


@router.post("/resume-all")
async def resume_all():
    await manager.resume_all()
    return {"status": "resumed all"}


@router.get("/downloads")
def get_downloads():
    return manager.get_all()

@router.get("/files")
def get_files(category: str | None = Query(None, description="Filter by category")):
    """
    Get all downloaded files. Optionally filter by category like 'Images', 'Videos', etc.
    """
    return manager.list_files(category)

@router.post("/downloads/{download_id}/pin")
async def pin_download(download_id: str):
    d = manager.downloads.get(download_id)
    if not d:
        return {"error": "Download not found"}

    d.is_pinned = True
    d.pinned_at = time.monotonic()

    await manager._enforce_priority()
    await manager._emit(d)

    return {"status": "pinned"}

@router.post("/downloads/{download_id}/unpin")
async def unpin_download(download_id: str):
    d = manager.downloads.get(download_id)
    if not d:
        return {"error": "Download not found"}

    d.is_pinned = False
    d.pinned_at = None

    await manager._enforce_priority()
    await manager._emit(d)

    return {"status": "unpinned"}
