import logging
import time

from prometheus_client import start_http_server, Gauge

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

    def update(self):
        for machine_type, procs in find_dispatcher_processes().items():
            self.dispatcher_count.labels(machine_type).set(len(procs))

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


def main(args):
    interval = args["--interval"]
    metrics = TeuthologyMetrics(interval=interval)
    start_http_server(metrics.port)
    metrics.loop()
