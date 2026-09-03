import time
import threading
import psutil


class SystemMonitor:
    def __init__(self, interval=5):
        self.interval = interval
        self.running = False
        self.thread = None

    def _monitor(self):
        while self.running:
            cpu = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()

            mem_used = mem.used / (1024 ** 3)
            mem_total = mem.total / (1024 ** 3)

            print(
                f"  ◈ SYSTEM STATUS  |  "
                f"CPU {cpu:5.1f}%  |  "
                f"MEM {mem.percent:5.1f}%  "
                f"[{mem_used:.2f}/{mem_total:.2f} GB]"
            )

            time.sleep(self.interval)

    def start(self):
        self.running = True
        self.thread = threading.Thread(
            target=self._monitor,
            daemon=True
        )
        self.thread.start()

    def stop(self):
        self.running = False

        if self.thread:
            self.thread.join(timeout=1)