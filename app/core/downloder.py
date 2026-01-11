

# import aiohttp
# import asyncio
# import os
# import math
# import time
# from collections import deque

# from app.utils.download_utils import get_total_size
# from app.ws import ws_manager
# from app.ml.controller import MLController


# #  Download Parameters 
# INITIAL_CHUNK_SIZE = 1024 * 8       # 8 KB
# MIN_CHUNK_SIZE = 1024 * 4           # 4 KB
# MAX_CHUNK_SIZE = 1024 * 512         # 512 KB

# SPEED_LIMIT_KBPS = 20                # artificial throttle


# async def _download(
#     session: aiohttp.ClientSession,
#     url: str,
#     file_path: str,
#     headers: dict,
#     start_byte: int,
#     progress_cb,
#     pause_event: asyncio.Event,
#     use_ml:bool=True
# ):
#     async with session.get(url, headers=headers, allow_redirects=True) as resp:
#         if resp.status not in (200, 206):
#             raise Exception(f"HTTP {resp.status}")

#         total = None

#         if resp.status == 206:
#             content_range = resp.headers.get("Content-Range")
#             if content_range:
#                 total = int(content_range.split("/")[-1])
#         elif resp.status == 200:
#             content_length = resp.headers.get("Content-Length")
#             if content_length:
#                 total = int(content_length)

#         if total is None:
#             total = await get_total_size(url)

#         # ---------- File mode ----------
#         if resp.status == 206 and start_byte > 0:
#             mode = "ab"
#         else:
#             start_byte = 0
#             mode = "wb"

#         # os.makedirs(os.path.dirname(file_path), exist_ok=True)
#         dir_path = os.path.dirname(file_path)
#         if dir_path:
#             os.makedirs(dir_path, exist_ok=True)


#         downloaded_so_far = start_byte
#         chunk_size = INITIAL_CHUNK_SIZE

#         ml = MLController()
#         decision_interval = 3.0 
#         last_decision_time = time.monotonic()

#         speed_window = deque(maxlen=10)
#         retry_count = 0
#         rtt_estimate_ms = 0.0  # placeholder for future RTT tracking

#         with open(file_path, mode) as f:
#             while True:
#                 await pause_event.wait()

#                 start_time = time.perf_counter()
#                 chunk = await resp.content.read(chunk_size)

#                 if not chunk:
#                     break

#                 f.write(chunk)
#                 downloaded_so_far += len(chunk)

#                 elapsed = time.perf_counter() - start_time
#                 speed_kbps = (len(chunk) / 1024) / max(elapsed, 0.001)

#                 speed_window.append(speed_kbps)
#                 avg_speed = sum(speed_window) / len(speed_window)
#                 speed_variance = (
#                     sum((s - avg_speed) ** 2 for s in speed_window)
#                     / len(speed_window)
#                 )

#                 now = time.monotonic()
#                 if now - last_decision_time >= decision_interval:
#                     metrics = {
#                         "avg_speed": avg_speed,
#                         "speed_variance": speed_variance,
#                         "retries": retry_count,
#                         "connections": 1,  
#                         "chunk_kb": chunk_size // 1024,
#                         "rtt_ms": rtt_estimate_ms,
#                     }

#                     action = ml.decide(metrics)

             
#                     if action == 0:      # keep
#                         chunk_size = chunk_size

#                     elif action == 1:    # increase slightly
#                         chunk_size = min(int(chunk_size * 1.25), MAX_CHUNK_SIZE)

#                     elif action == 2:    # decrease slightly
#                         chunk_size = max(int(chunk_size * 0.8), MIN_CHUNK_SIZE)

#                     elif action == 3:    # increase aggressively
#                         chunk_size = min(chunk_size * 2, MAX_CHUNK_SIZE)

#                     elif action == 4:    # decrease aggressively
#                         chunk_size = max(chunk_size // 2, MIN_CHUNK_SIZE)

                  

#                     # reward = (
#                     #     avg_speed / 1000
#                     #     - 0.2 * speed_variance
#                     #     - 0.5 * retry_count
#                     # )
#                     speed_term = math.log1p(avg_speed)
#                     variance_penalty = min(speed_variance, 1.0)
#                     retry_penalty = min(retry_count, 3)

#                     reward = (
#                         speed_term
#                         - 0.3 * variance_penalty
#                         - 0.7 * retry_penalty
#                     )

#                     reward = max(-5.0, min(reward, 5.0))


#                     ml.feedback(reward)
#                     last_decision_time = now

#                 # #  Throttling 
#                 # throttle_delay = len(chunk) / (SPEED_LIMIT_KBPS * 1024)
#                 # await asyncio.sleep(throttle_delay)

#                 progress_cb(downloaded_so_far, total,chunk_size,avg_speed)


# async def download_file(
#     url: str,
#     file_path: str,
#     start_byte: int,
#     progress_cb,
#     pause_event: asyncio.Event,
#     use_ml:bool=True
# ):
#     timeout = aiohttp.ClientTimeout(total=None)

#     base_headers = {
#         "User-Agent": (
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#             "AppleWebKit/537.36 (KHTML, like Gecko) "
#             "Chrome/143.0.0.0 Safari/537.36"
#         ),
#         "Accept": "*/*",
#     }

#     async with aiohttp.ClientSession(timeout=timeout, headers=base_headers) as session:
#         #Pre-flight request 
#         async with session.get(url, allow_redirects=True) as resp:
#             if resp.status not in (200, 206):
#                 raise Exception(f"HTTP {resp.status}")

#         #  Attempt resume 
#         if start_byte > 0:
#             try:
#                 range_headers = base_headers | {
#                     "Range": f"bytes={start_byte}-"
#                 }
#                 await _download(
#                     session,
#                     url,
#                     file_path,
#                     range_headers,
#                     start_byte,
#                     progress_cb,
#                     pause_event,
#                     use_ml
#                 )
#                 return
#             except Exception as e:
#                 print("Range resume failed, restarting:", e)

       
#         await _download(
#             session,
#             url,
#             file_path,
#             base_headers,
#             start_byte,
#             progress_cb,
#             pause_event,
#             use_ml
#         )


import aiohttp
import asyncio
import math
from collections import deque
import os
import time

from app.utils.download_utils import get_total_size
from app.ml.controller import MLController
from app.ws import ws_manager


#  Download Parameters 
INITIAL_CHUNK_SIZE = 1024 * 8       # 8 KB
MIN_CHUNK_SIZE = 1024 * 4           # 4 KB
MAX_CHUNK_SIZE = 1024 * 512         # 512 KB

# INITIAL_CHUNK_SIZE = 128 * 1024   # 128 KB
# MIN_CHUNK_SIZE = 32 * 1024        # 32 KB
# MAX_CHUNK_SIZE = 1024 * 1024      # 1 MB

SPEED_LIMIT_KBPS = 1                # artificial throttle


async def _download(
    session,
    url: str,
    file_path: str,
    headers: dict,
    start_byte: int,
    progress_cb,
    pause_event: asyncio.Event,
    use_ml: bool = True
):
    async with session.get(url, headers=headers, allow_redirects=True) as resp:
        if resp.status not in (200, 206):
            raise Exception(f"HTTP {resp.status}")

        # ---------- Determine total size ----------
        total = None
        if resp.status == 206:
            content_range = resp.headers.get("Content-Range")
            if content_range:
                total = int(content_range.split("/")[-1])
        elif resp.status == 200:
            content_length = resp.headers.get("Content-Length")
            if content_length:
                total = int(content_length)
        if total is None:
            total = await get_total_size(url)

        # ---------- File mode ----------
        if resp.status == 206 and start_byte > 0:
            mode = "ab"
        else:
            start_byte = 0
            mode = "wb"

        # Ensure directory exists
        dir_path = os.path.dirname(file_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

        downloaded_so_far = start_byte
        chunk_size = INITIAL_CHUNK_SIZE

        # ---------- ML Setup ----------
        if use_ml:
            ml = MLController()
            decision_interval = 3.0
            last_decision_time = time.monotonic()
            speed_window = deque(maxlen=10)
            retry_count = 0
            rtt_estimate_ms = 0.0  # placeholder
        else:
            # dummy variables to avoid NameError
            speed_window = deque([0])
            last_decision_time = time.monotonic()
            decision_interval = 1.0
            retry_count = 0
            rtt_estimate_ms = 0.0

        with open(file_path, mode) as f:
            while True:
                await pause_event.wait()

                start_time = time.perf_counter()
                chunk = await resp.content.read(chunk_size)

                if not chunk:
                    break

                f.write(chunk)
                downloaded_so_far += len(chunk)

                # ---------- Speed calculation ----------
                elapsed = time.perf_counter() - start_time
                speed_kbps = (len(chunk) / 1024) / max(elapsed, 0.001)

                # ---------- ML adaptive logic ----------
                if use_ml:
                    speed_window.append(speed_kbps)
                    avg_speed = sum(speed_window) / len(speed_window)
                    speed_variance = sum((s - avg_speed) ** 2 for s in speed_window) / len(speed_window)

                    now = time.monotonic()
                    if now - last_decision_time >= decision_interval:
                        metrics = {
                            "avg_speed": avg_speed,
                            "speed_variance": speed_variance,
                            "retries": retry_count,
                            "connections": 1,
                            "chunk_kb": chunk_size // 1024,
                            "rtt_ms": rtt_estimate_ms,
                        }

                        action = ml.decide(metrics)

                        #  Apply action 
                        if action == 0:  # keep
                            pass
                        elif action == 1:  # +5%
                            chunk_size = min(int(chunk_size * 1.05), MAX_CHUNK_SIZE)
                        elif action == 2:  # +10%
                            chunk_size = min(int(chunk_size * 1.1), MAX_CHUNK_SIZE)
                        elif action == 3:  # +15%
                            chunk_size = min(int(chunk_size * 1.15), MAX_CHUNK_SIZE)
                        elif action == 4:  # -5%
                            chunk_size = max(int(chunk_size * 0.95), MIN_CHUNK_SIZE)
                        elif action == 5:  # -10%
                            chunk_size = max(int(chunk_size * 0.9), MIN_CHUNK_SIZE)
                        elif action == 6:  # -15%
                            chunk_size = max(int(chunk_size * 0.85), MIN_CHUNK_SIZE)
                        elif action == 7:  # +25% aggressive
                            chunk_size = min(int(chunk_size * 1.25), MAX_CHUNK_SIZE)
                        elif action == 8:  # -25% aggressive
                            chunk_size = max(int(chunk_size * 0.75), MIN_CHUNK_SIZE)

                        # Reward calculation
                        speed_term = math.log1p(avg_speed)
                        variance_penalty = min(speed_variance, 1.0)
                        retry_penalty = min(retry_count, 3)
                        reward = speed_term - 0.3 * variance_penalty - 0.7 * retry_penalty
                        reward = max(-5.0, min(reward, 5.0))

                        ml.feedback(reward)
                        last_decision_time = now
                else:
                    avg_speed = speed_kbps  # for logging
                    # chunk_size remains constant

                # ---------- Progress callback ----------
                progress_cb(downloaded_so_far, total, chunk_size, avg_speed)
                # Throttling 
                throttle_delay = len(chunk) / (SPEED_LIMIT_KBPS * 1024)
                await asyncio.sleep(throttle_delay)



async def download_file(
    url: str,
    file_path: str,
    start_byte: int,
    progress_cb,
    pause_event: asyncio.Event,
    use_ml:bool=True
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
        #Pre-flight request 
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status not in (200, 206):
                raise Exception(f"HTTP {resp.status}")

        #  Attempt resume 
        if start_byte > 0:
            try:
                range_headers = base_headers | {
                    "Range": f"bytes={start_byte}-"
                }
                await _download(
                    session,
                    url,
                    file_path,
                    range_headers,
                    start_byte,
                    progress_cb,
                    pause_event,
                    use_ml
                )
                return
            except Exception as e:
                print("Range resume failed, restarting:", e)

       
        await _download(
            session,
            url,
            file_path,
            base_headers,
            start_byte,
            progress_cb,
            pause_event,
            use_ml
        )