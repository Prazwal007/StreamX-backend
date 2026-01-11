import asyncio
import time

class DownloadScheduler:
    def __init__(self):
        self.queue = []          
        self.active = set()      
        self.lock = asyncio.Lock()
        self.max_parallel = 3    
