

import aiohttp
import asyncio
import os

from app.utils.download_utils import get_total_size
from app.ws import ws_manager

CHUNK_SIZE = 1024 * 8       
SPEED_LIMIT_KBPS = 20        # artificial throttle


async def _download(
    session: aiohttp.ClientSession,
    url: str,
    file_path: str,
    headers: dict,
    start_byte: int,
    progress_cb,
    pause_event: asyncio.Event
):
    async with session.get(url, headers=headers, allow_redirects=True) as resp:
        if resp.status not in (200, 206):
            raise Exception(f"HTTP {resp.status}")

    
        total = None

        if resp.status == 206:
            # Content-Range: bytes start-end/total
            content_range = resp.headers.get("Content-Range")
            if content_range:
                total = int(content_range.split("/")[-1])

        elif resp.status == 200:
            content_length = resp.headers.get("Content-Length")
            if content_length:
                total = int(content_length)

        # Fallback without Range headers
        if total is None:
            total = await get_total_size(url)

        
        # File mode
  
        if resp.status == 206 and start_byte > 0:
            mode = "ab"
        else:
            start_byte = 0
            mode = "wb"

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        downloaded_so_far = start_byte

        with open(file_path, mode) as f:
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                await pause_event.wait()

                f.write(chunk)
                downloaded_so_far += len(chunk)

                progress_cb(downloaded_so_far, total)
                
                # Artificial throttle
                await asyncio.sleep(len(chunk) / (SPEED_LIMIT_KBPS * 1024))


async def download_file(
    url: str,
    file_path: str,
    start_byte: int,
    progress_cb,
    pause_event: asyncio.Event
):
    timeout = aiohttp.ClientTimeout(total=None)

    base_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }

    async with aiohttp.ClientSession(timeout=timeout, headers=base_headers) as session:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status not in (200, 206):
                raise Exception(f"HTTP {resp.status}")

        # Attempt resume
        if start_byte > 0:
            try:
                range_headers = base_headers | {"Range": f"bytes={start_byte}-"}
                await _download(
                    session,
                    url,
                    file_path,
                    range_headers,
                    start_byte,
                    progress_cb,
                    pause_event
                )
                return
            except Exception as e:
                print("Range resume failed, restarting:", e)

        # Full download
        await _download(
            session,
            url,
            file_path,
            base_headers,
            start_byte,
            progress_cb,
            pause_event
        )
