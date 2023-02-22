import logging
import psutil
import time

from prometheus_client import start_http_server, Gauge

from teuthology.beanstalk import connect, stats_tube, watch_tube
from teuthology.config import config
from teuthology.dispatcher import find_dispatcher_processes


log = logging.getLogger(__name__)


class TeuthologyMetrics:
    port = 61764  # int(''.join([str((ord(c) - 100) % 10) for c in "teuth"]))

    def __init__(self, interval=60):
        self.interval: int = int(interval)
        self.dispatcher_count = Gauge(
            "dispatcher_count", "Dispatcher Count", ["machine_type"]
        )
        self.beanstalk_queue_length = Gauge(
            "beanstalk_queue_length", "Beanstalk Queue Length", ["machine_type"]
        )
        self.beanstalk_queue_paused = Gauge(
            "beanstalk_queue_paused", "Beanstalk Queue is Paused", ["machine_type"]
        )
        self.job_process_count = Gauge(
            "job_process_count", "Job Process Count",
        )

    def update(self):
        log.info("Updating...")
        machine_types = list(config.active_machine_types)
        dispatcher_procs = find_dispatcher_processes()
        for machine_type in machine_types:
            self.dispatcher_count.labels(machine_type).set(
                len(dispatcher_procs.get(machine_type, []))
            )
            queue_stats = stats_tube(connect(), machine_type)
            self.beanstalk_queue_length.labels(machine_type).set(queue_stats["count"])
            self.beanstalk_queue_paused.labels(machine_type).set(
                1 if queue_stats["paused"] else 0
            )
        self.job_process_count.set(self.job_processes())
        log.info("Update finished.")

    def loop(self):
        log.info("Starting teuthology-exporter...")
        while True:
            try:
                before = time.perf_counter()
                try:
                    self.update()
                except Exception:
                    log.exception("Failed to update metrics")
                interval = self.interval
                # try to deliver metrics _at_ $interval, as opposed to sleeping for
                # $interval between updates
                elapsed: float = time.perf_counter() - before
                if elapsed < 0:
                    interval *= 2
                interval -= elapsed
                time.sleep(interval)
            except KeyboardInterrupt:
                log.info("Stopping.")
                raise SystemExit

    def job_processes(self):
        def match(proc):
            cmdline = proc.cmdline()
            if not cmdline[0].endswith("teuthology"):
                return False
            if not '--archive' in cmdline:
                return False
            if not '--name' in cmdline:
                return False
            try:
                owner_index = cmdline.index("--owner") + 1
                if not cmdline[owner_index].startswith("scheduled_"):
                    return False
            except ValueError:
                return False
            return True

        attrs = ["pid", "cmdline"]
        total = 0
        for proc in psutil.process_iter(attrs=attrs):
            if match(proc):
                total += 1
        return total

def main(args):
    interval = args["--interval"]
    metrics = TeuthologyMetrics(interval=interval)
    start_http_server(metrics.port)
    metrics.loop()
