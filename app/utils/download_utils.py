# from pathlib import Path
# import os

# downloads_path = Path.home() / "Downloads"

# def get_download_path(filename: str) -> Path:
#     """
#     Returns the full path in the Downloads folder for the given filename.
#     """
#     return downloads_path / filename

# async def get_total_size(session, url, headers):
#     try:
#         range_headers = headers | {"Range": "bytes=0-0"}
#         async with session.get(url, headers=range_headers) as resp:
#             cr = resp.headers.get("Content-Range")
#             if cr and "/" in cr:
#                 return int(cr.split("/")[-1])
#     except:
#         pass
#     return None


from pathlib import Path
import mimetypes
import aiohttp

DOWNLOADS_PATH = Path.home() / "Downloads"/"StreamX"

EXTENSION_MAP = {
    "Images": {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"},
    "Documents": {".pdf", ".doc", ".docx", ".txt", ".rtf", ".odt"},
    "Spreadsheets": {".xls", ".xlsx", ".csv"},
    "Presentations": {".ppt", ".pptx"},
    "Archives": {".zip", ".rar", ".7z", ".tar", ".gz"},
    "Videos": {".mp4", ".mkv", ".avi", ".mov"},
    "Audio": {".mp3", ".wav", ".flac", ".aac"},
}

MIME_MAP = {
    "image": "Images",
    "video": "Videos",
    "audio": "Audio",
    "text": "Documents",
    "application/pdf": "Documents",
    "application/zip": "Archives",
}

def get_download_path(filename: str) -> Path:
    """
    Returns an organized download path based on file extension or MIME type.
    """
    path = Path(filename)
    ext = path.suffix.lower()

    folder_name = None

    if ext:
        for category, extensions in EXTENSION_MAP.items():
            if ext in extensions:
                folder_name = category
                break

    if folder_name is None:
        mime_type, _ = mimetypes.guess_type(filename)
        if mime_type:
            main_type = mime_type.split("/")[0]
            folder_name = MIME_MAP.get(mime_type) or MIME_MAP.get(main_type)


    if folder_name is None:
        folder_name = "NoExtension" if not ext else "Others"

    target_dir = DOWNLOADS_PATH / folder_name
    target_dir.mkdir(parents=True, exist_ok=True)

    return target_dir / filename



async def get_total_size(url: str) -> int | None:
    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.head(url, allow_redirects=True) as resp:
                cl = resp.headers.get("Content-Length")
                if cl:
                    return int(cl)
        except Exception:
            pass

        try:
            headers = {"Range": "bytes=0-0"}
            async with session.get(url, headers=headers, allow_redirects=True) as resp:
                cr = resp.headers.get("Content-Range")
                if cr and "/" in cr:
                    return int(cr.split("/")[-1])
        except Exception:
            pass

    return None
