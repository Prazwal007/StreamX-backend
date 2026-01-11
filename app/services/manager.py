

import asyncio
import os
import uuid
import time
from urllib.parse import urlparse

from tqdm import tqdm

from app.ws import ws_manager
from app.core.downloder import download_file
from app.models.download import Download
from app.utils.download_utils import get_download_path, DOWNLOADS_PATH


class DownloadManager:
    def __init__(self):
        self.downloads: dict[str, Download] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.pause_events: dict[str, asyncio.Event] = {}

        # ETA helpers
        self._last_bytes: dict[str, int] = {}
        self._last_time: dict[str, float] = {}
        self._speed: dict[str, float] = {}  # bytes/sec 

    def _get_active_priority_download(self) -> Download | None:
        pinned = [
            d for d in self.downloads.values()
            if d.is_pinned and d.status not in ("completed", "failed")
        ]

        if not pinned:
            return None

        # Latest pin wins
        return max(pinned, key=lambda d: d.pinned_at or 0)

    async def create_download(self, url: str) -> Download:
        download_id = str(uuid.uuid4())

        parsed = urlparse(url)
        filename = os.path.basename(parsed.path) or download_id
        file_path = get_download_path(filename)

        d = Download(
            id=download_id,
            filename=filename,
            url=url,
            file_path=str(file_path),
            downloaded=0,
            total=None,
            status="queued",
            eta_seconds=None,
        )

        self.downloads[download_id] = d
        self.pause_events[download_id] = asyncio.Event()
        self.pause_events[download_id].set()

        self._last_bytes[download_id] = 0
        self._last_time[download_id] = time.monotonic()
        self._speed[download_id] = 0.0

        await self._emit(d)
        return d

    async def start(self, download_id: str):
        d = self.downloads[download_id]
        pause_event = self.pause_events[download_id]

        os.makedirs(os.path.dirname(d.file_path), exist_ok=True)

        start_byte = os.path.getsize(d.file_path) if os.path.exists(d.file_path) else 0
        d.downloaded = start_byte
        d.status = "downloading"

        self._last_bytes[download_id] = start_byte
        self._last_time[download_id] = time.monotonic()

        await self._emit(d)

        bar = tqdm(
            total=d.total,
            initial=start_byte,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=d.id[:8],
            leave=True,
        )

        def progress(downloaded_so_far: int, total: int | None, chunk_size: int, avg_speed: float):
            now = time.monotonic()
            last_t = self._last_time[d.id]
            last_b = self._last_bytes[d.id]

            dt = now - last_t
            db = downloaded_so_far - last_b

            if dt > 0 and db > 0:
                instant_speed = db / dt

                alpha = 0.3
                prev = self._speed[d.id]
                self._speed[d.id] = (
                    instant_speed if prev == 0 else prev * (1 - alpha) + instant_speed * alpha
                )

            self._last_time[d.id] = now
            self._last_bytes[d.id] = downloaded_so_far

            d.downloaded = downloaded_so_far

            if total is not None and d.total != total:
                d.total = total
                bar.total = total

            # ETA
            if d.total and self._speed[d.id] > 0:
                remaining = d.total - d.downloaded
                d.eta_seconds = int(remaining / self._speed[d.id])
            else:
                d.eta_seconds = None

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
                    pause_event,
                )

                if d.total is None:
                    d.total = d.downloaded
                else:
                    d.downloaded = d.total

                d.status = "completed"
                d.eta_seconds = 0
                await self._emit(d)

                await self._enforce_priority()

            except Exception as e:
                print("Download failed:", e)
                d.status = "failed"
                d.eta_seconds = None
                await self._emit(d)

            finally:
                bar.close()

        self.tasks[download_id] = asyncio.create_task(runner())
        await self._enforce_priority()


    async def _emit(self, d: Download):
        await ws_manager.broadcast(
            {
                "id": d.id,
                "filename": d.filename,
                "url": d.url,
                "downloaded": d.downloaded,
                "total": d.total,
                "status": d.status,
                "file_path": d.file_path,
                "eta_seconds": d.eta_seconds,
                "is_pinned": d.is_pinned,

            }
        )

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
        for download_id in list(self.downloads):
            await self.pause(download_id)

    async def resume_all(self):
        for download_id in list(self.downloads):
            await self.resume(download_id)


    def get_all(self):
        return list(self.downloads.values())

    # def list_files(self, category: str | None = None):
    #     result = []

    #     if category:
    #         folder = DOWNLOADS_PATH / category
    #         if folder.exists():
    #             for file_path in folder.iterdir():
    #                 if file_path.is_file():
    #                     result.append(
    #                         {
    #                             "name": file_path.name,
    #                             "path": str(file_path),
    #                             "category": category,
    #                             "size": file_path.stat().st_size,
    #                         }
    #                     )
    #     else:
    #         for folder in DOWNLOADS_PATH.iterdir():
    #             if folder.is_dir():
    #                 for file_path in folder.iterdir():
    #                     if file_path.is_file():
    #                         result.append(
    #                             {
    #                                 "name": file_path.name,
    #                                 "path": str(file_path),
    #                                 "category": folder.name,
    #                                 "size": file_path.stat().st_size,
    #                             }
    #                         )

    #     return result
    def list_files(self, category: str | None = None):
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
                            "size": file_path.stat().st_size,
                            "_mtime": file_path.stat().st_mtime,  # internal only
                        })
        else:
            for folder in DOWNLOADS_PATH.iterdir():
                if folder.is_dir():
                    for file_path in folder.iterdir():
                        if file_path.is_file():
                            result.append({
                                "name": file_path.name,
                                "path": str(file_path),
                                "category": folder.name,
                                "size": file_path.stat().st_size,
                                "_mtime": file_path.stat().st_mtime,  # internal only
                            })

        # Sort by modified time (latest first)
        result.sort(key=lambda x: x["_mtime"], reverse=True)

        # Remove internal field before returning
        for item in result:
            item.pop("_mtime", None)

        return result


    async def cancel_and_remove(self, download_id: str):
        task = self.tasks.get(download_id)
        if task:
            task.cancel()

        self.pause_events.pop(download_id, None)
        self._last_bytes.pop(download_id, None)
        self._last_time.pop(download_id, None)
        self._speed.pop(download_id, None)
        self.downloads.pop(download_id, None)

    async def _enforce_priority(self):
        active = self._get_active_priority_download()

        if active is None:
            for d in self.downloads.values():
                if d.status == "queued":
                    self.pause_events[d.id].set()
                    d.status = "downloading"
                    await self._emit(d)
            return

        for d in self.downloads.values():
            if d.status in ("completed", "failed"):
                continue

            if active and d.id == active.id:
                self.pause_events[d.id].set()
                if d.status == "queued":
                    d.status = "downloading"
                    await self._emit(d)
            else:
                self.pause_events[d.id].clear()
                if d.status == "downloading":
                    d.status = "queued"
                    await self._emit(d)
