
import asyncio
import os
import uuid
from urllib.parse import urlparse

from tqdm import tqdm
from app.ws import ws_manager
from app.core.downloder import download_file
from app.models.download import Download
from app.utils.download_utils import get_download_path,DOWNLOADS_PATH


class DownloadManager:
    def __init__(self):
        self.downloads = {}
        self.tasks = {}
        self.pause_events = {}

    async def create_download(self, url: str) -> Download:
        download_id = str(uuid.uuid4())

        parsed = urlparse(url)
        filename = os.path.basename(parsed.path) or download_id
        file_path = get_download_path(filename)

        d = Download(
            id=download_id,
            url=url,
            file_path=str(file_path),
            downloaded=0,
            total=None,
            status="queued"
        )

        self.downloads[download_id] = d
        self.pause_events[download_id] = asyncio.Event()
        self.pause_events[download_id].set()

        await self._emit(d)

        return d


    async def start(self, download_id: str):
        d = self.downloads[download_id]
        pause_event = self.pause_events[download_id]

        os.makedirs(os.path.dirname(d.file_path), exist_ok=True)

        start_byte = os.path.getsize(d.file_path) if os.path.exists(d.file_path) else 0
        d.downloaded = start_byte
        d.status = "downloading"
        await self._emit(d)

     
        bar = tqdm(
            total=d.total,
            initial=start_byte,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=d.id[:8],
            leave=True
        )

        def progress(downloaded_so_far: int, total: int | None):
            d.downloaded = downloaded_so_far

            if total is not None and d.total != total:
                d.total = total
                bar.total = total

            bar.n = downloaded_so_far
            bar.refresh()

            asyncio.create_task(self._emit(d))


        async def runner():
            try:
                await download_file(
                    d.url,
                    d.file_path,
                    start_byte,
                    progress,
                    pause_event
                )

                if d.total is None:
                    d.total = d.downloaded
                else:
                    d.downloaded = d.total

                d.status = "completed"
                self._emit(d)

            except Exception as e:
                print("Download failed:", e)
                d.status = "failed"
                self._emit(d)

            finally:
                bar.close()

        self.tasks[download_id] = asyncio.create_task(runner())


    async def _emit(self, d):
            await ws_manager.broadcast({
                "id": d.id,
                "url": d.url,
                "downloaded": d.downloaded,
                "total": d.total,
                "status": d.status,
                "file_path": d.file_path,
            })
    

    async def pause(self, download_id: str):
        if download_id in self.downloads:
            d = self.downloads[download_id]
            d.status = "paused"
            self.pause_events[download_id].clear()
            await self._emit(d)


    async def resume(self, download_id: str):
        if download_id in self.downloads:
            d = self.downloads[download_id]
            d.status = "downloading"
            self.pause_events[download_id].set()
            await self._emit(d)


    async def pause_all(self):
        for download_id in self.downloads:
            await self.pause(download_id)

    async def resume_all(self):
        for download_id in self.downloads:
            await self.resume(download_id)

    def get_all(self):
        return list(self.downloads.values())

    
    def list_files(self,category: str | None = None):
        """
        List all files in Downloads/StreamX, optionally filtered by category.
        """
        result = []

        if category:
            folder = DOWNLOADS_PATH / category
            if folder.exists():
                for file_path in folder.iterdir():
                    if file_path.is_file():
                        result.append({
                            "name": file_path.name,
                            "path": str(file_path),
                            "category": category,
                            "size": file_path.stat().st_size
                        })
        else:
            # List all categories
            for folder in DOWNLOADS_PATH.iterdir():
                if folder.is_dir():
                    for file_path in folder.iterdir():
                        if file_path.is_file():
                            result.append({
                                "name": file_path.name,
                                "path": str(file_path),
                                "category": folder.name,
                                "size": file_path.stat().st_size
                            })
        return result
    
    async def cancel_and_remove(self, download_id: str):
        """Stop the download if running and remove it from manager."""
        task = self.downloads.get(download_id)
        if task:
            
            await task.pause()

        # 2. Remove from internal tracking
        self.downloads.pop(download_id, None)