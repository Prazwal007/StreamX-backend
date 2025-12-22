from fastapi import APIRouter,Query
from app.services.manager import DownloadManager
from fastapi import WebSocket, WebSocketDisconnect
from app.ws import ws_manager
from app.utils.download_utils import get_total_size

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
async def add_download(url: str):
    d = manager.create_download(url)
    d.total=await get_total_size(url)
    await manager.start(d.id)   # starts async task
    return d


@router.post("/pause/{download_id}")
def pause(download_id: str):
    manager.pause(download_id)
    return {"status": "paused"}


@router.post("/resume/{download_id}")
def resume(download_id: str):
    manager.resume(download_id)
    return {"status": "resumed"}


@router.post("/pause-all")
def pause_all():
    manager.pause_all()
    return {"status": "paused all"}


@router.post("/resume-all")
def resume_all():
    manager.resume_all()
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