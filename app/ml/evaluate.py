import asyncio
import matplotlib.pyplot as plt
import pandas as pd
from app.core.downloder import download_file
from app.ml.logger import MetricsLogger

async def run_download(url, file_path, logger, use_ml=True):
    pause_event = asyncio.Event()
    pause_event.set()  

    last_chunk_size = 0
    last_speed = 0

    def progress_cb(downloaded, total, chunk_size=None, speed_kbps=None):
        nonlocal last_chunk_size, last_speed
        if chunk_size is not None:
            last_chunk_size = chunk_size
        if speed_kbps is not None:
            last_speed = speed_kbps
        logger.log(downloaded, total, last_chunk_size, last_speed)


    await download_file(url, file_path, start_byte=0, progress_cb=progress_cb, pause_event=pause_event,use_ml=use_ml)


def plot_comparison(csv1, label1, csv2, label2):
    df1 = pd.read_csv(csv1)
    df2 = pd.read_csv(csv2)

    df1["time"] = df1["time"] - df1["time"].iloc[0]
    df2["time"] = df2["time"] - df2["time"].iloc[0]

    plt.figure(figsize=(12,6))
    plt.plot(df1["time"], df1["downloaded"]/1024, label=label1)
    plt.plot(df2["time"], df2["downloaded"]/1024, label=label2, linestyle='--')
    plt.xlabel("Time (s)")
    plt.ylabel("Downloaded (KB)")
    plt.title("Download Progress Comparison")
    plt.legend()
    plt.grid(True)
    plt.show()

    plt.figure(figsize=(12,6))
    plt.plot(df1["time"], df1["chunk_size"], label=label1)
    plt.plot(df2["time"], df2["chunk_size"], label=label2, linestyle='--')
    plt.xlabel("Time (s)")
    plt.ylabel("Chunk Size (B)")
    plt.title("Chunk Size Over Time")
    plt.legend()
    plt.grid(True)
    plt.show()

    plt.figure(figsize=(12,6))
    plt.plot(df1["time"], df1["speed_kbps"], label=label1)
    plt.plot(df2["time"], df2["speed_kbps"], label=label2, linestyle='--')
    plt.xlabel("Time (s)")
    plt.ylabel("Speed (KB/s)")
    plt.title("Download Speed Over Time")
    plt.legend()
    plt.grid(True)
    plt.show()



async def main():
    url = "https://getsamplefiles.com/download/mp4/sample-2.mp4" 
    logger_heuristic = MetricsLogger()
    await run_download(url, "file_heuristic.mp4", logger_heuristic,use_ml=False)
    logger_heuristic.save_csv("non-ml.csv")

    logger_ml = MetricsLogger()
    await run_download(url, "file_ml.mp4", logger_ml,use_ml=True)
    logger_ml.save_csv("ml.csv")

    plot_comparison("non-ml.csv", "non-ml download", "ml.csv", "ML Bandit")


if __name__ == "__main__":
    asyncio.run(main())
