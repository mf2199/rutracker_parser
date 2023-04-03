#!/usr/bin/env python3
import json
import logging
import os
import queue  # for exceptions
import random
import signal
import time
from multiprocessing import Process, Queue, freeze_support

import parse
from settings import Settings


def worker(input_, output, logger_=None):
    try:
        # A random logger name
        logger_ = logger_ or logging.getLogger(
            f"thread({random.randrange(1, 999):3})"
        )
        logger_.info("Starting thread...")

        for new_input in iter(input_.get, ("STOP", {})):
            # log.debug('thread iteration')
            new_input[1]["logger"] = logger_
            if new_input[0] == "COOKIE":
                status_, details_ = parse.get_cookie(new_input[1])
                output.put((new_input[0], status_, details_))
            elif new_input[0] == "GET_PAGE":
                time.sleep(3)
                status_, details_ = parse.get_page(new_input[1])
                output.put((new_input[0], status_, details_))
            else:
                logger_.warning(f"Unknown task: {new_input[0]}")

    except KeyboardInterrupt:
        pass


class Loader:
    def __init__(self):
        self.settings = Settings()
        self.options = self.settings.options

        self.task_queue = Queue()
        self.done_queue = Queue()

        self.settings.prepare_lists()

        self.counters = {
            "finished_all": 0,
            "error_all": 0,
            "nohash_all": 0,
            "finished_last": 0,
            "error_last": 0,
            "nohash_last": 0,
        }

        self._nexttime = time.time()
        self._exit_counter = 0
        self._status_nexttime = time.time()
        self._ids_pointer = 0

        self._processes = []

    def _start_threads(self):
        logger.info(f"Numbers of allocated threads: {self.options.threads}")
        for _ in range(self.options.threads):
            p = Process(
                target=worker, args=(self.task_queue, self.done_queue, logger)
            )
            p.start()
            self._processes.append(p)

    def _stop_all_threads(self):
        logger.debug("Stopping all threads and exiting")
        for _ in range(self.options.threads):
            self.task_queue.put(("STOP", {}))

    def _setup_timing(self):
        if time.time() > self._status_nexttime:
            self._status_nexttime = time.time() + 10
            speed = (
                self.counters["finished_last"] + self.counters["nohash_last"]
            ) * 0.1
            if speed != 0:
                time_remaining = (
                    len(self.settings.ids) - self._ids_pointer
                ) / speed
            else:
                time_remaining = 0

            m, s = divmod(time_remaining, 60)
            h, m = divmod(m, 60)
            logger.info(
                f"Last 10 sec: {self.counters['finished_last']} - OK, "
                f"{self.counters['nohash_last']} - NOHASH, "
                f"{self.counters['error_last']} - ERROR, Remaining: "
                f"{(len(self.settings.ids) - self._ids_pointer)}, {h}:{m}"
            )

            self.counters["finished_all"] += self.counters["finished_last"]
            self.counters["error_all"] += self.counters["error_last"]
            self.counters["nohash_all"] += self.counters["nohash_last"]
            self.counters["finished_last"] = 0
            self.counters["error_last"] = 0
            self.counters["nohash_last"] = 0

    def _set_cookie(self, status, details):
        logger.info("Setting a new cookie")
        if status == "OK":
            logger.debug("processing loop. cookie - ok")
            self.settings.set_free_proxy(
                details["proxy_ip"], details["proxy_port"]
            )
            self.settings.set_cookie(details["username"], details["cookie"])
        elif status == "ERROR":
            logger.error(f"Processing loop - cookie error: {details['text']}")
            self.settings.set_free_proxy(
                details["proxy_ip"], details["proxy_port"]
            )
            # settings.set_cookie_error(details['username'])
        else:
            logger.warning(
                f"Processing loop - unknown cookie status: {status}"
            )

    def _get_page(self, status, details):
        logger.info("Getting a new page")
        id_ = details["id"]

        logger.info(f"Processing status: {status}")
        if status == "OK":
            self.counters["finished_last"] += 1
            logger.debug(f"Processing loop, get page - OK, ID: {id_}")
            self.settings.set_free_proxy(
                details["proxy_ip"], details["proxy_port"]
            )
            self.settings.set_free_cookie(details["cookie"])

            if not os.path.exists(self.options.desc_folder):
                os.mkdir(self.options.desc_folder)
            path = os.path.join(
                self.options.desc_folder, f"{id_ // 100000:03}/"
            )
            if not os.path.exists(path):
                os.mkdir(path)

            filename = os.path.join(path, f"{id_:08}")
            with open(filename, "w", encoding="utf8") as f:
                f.write(details["description"])

            logger.info(f"LINE: {details['line']}")
            self.settings.handle_table_file.write(f"{details['line']}\n")
            self.settings.handle_finished_file.write(f"{id_}\n")

        elif status == "NO_HASH":
            self.counters["nohash_last"] += 1
            logger.info(f"Processing loop, get page - NO HASH, ID: {id_}")
            self.settings.set_free_proxy(
                details["proxy_ip"], details["proxy_port"]
            )
            self.settings.set_free_cookie(details["cookie"])
            self.settings.handle_finished_file.write(f"{id_}\n")

        elif status == "ERROR":
            self.counters["error_last"] += 1
            logger.error(
                f"Processing loop. get page - error: {details['text']}"
            )

            if details["text"] == "not logined":
                self.settings.set_error_cookie(details["cookie"])
            self.settings.set_free_cookie(details["cookie"])

            if (
                "request exception" in details["text"]
                or "request timeout exception" in details["text"]
            ):
                self.settings.set_error_proxy(
                    details["proxy_ip"], details["proxy_port"]
                )

            self.settings.set_free_proxy(
                details["proxy_ip"], details["proxy_port"]
            )
            self.settings.ids.append(int(id_))

        else:
            logger.warning(f"Unknown status encountered: {status}, id: {id_}")

    def _run_cycle(self):
        ...

    def _add_new_tasks(self):
        self._exit_counter = 0
        id_max = min(
            self._ids_pointer + self.settings.qsize,
            len(self.settings.ids),
        )
        for i in range(self._ids_pointer, id_max):
            proxy = self.settings.get_free_proxy()
            logger.info(f"Proxy obtained from settings: {proxy}")
            if not proxy:
                if time.time() > self._nexttime:
                    logger.info("No free proxies are available")
                    logger.debug(f"Proxies: {self.settings.proxy_list}")
                    self._nexttime = time.time() + 60
                break

            cookie = self.settings.get_free_cookie()
            if not cookie:
                if time.time() > self._nexttime:
                    logger.info("No free cookies are available")
                    logger.debug(f"Cookies: {self.settings.login_list}")
                    self._nexttime = time.time() + 60
                break

            work = (
                "GET_PAGE",
                {
                    "id": int(self.settings.ids[i]),
                    "cookie": cookie,
                    "headers": self.settings.headers,
                    "proxy_ip": proxy["ip"],
                    "proxy_port": int(proxy["port"]),
                },
            )
            self.task_queue.put(work)
            self._ids_pointer += 1

    def run(self):
        self._start_threads()
        self.settings.open_files()

        if self.options.print:
            self._stop_all_threads()
            return

        if not self.settings.ids:
            logger.info("Empty input/left list. Terminated")
            self._stop_all_threads()
            return

        self.settings.load_cookies()
        for login in self.settings.login_list:
            if not login.get("cookie"):
                proxy = self.settings.get_free_proxy()
                work = (
                    "COOKIE",
                    {
                        "username": login["username"],
                        "password": login["password"],
                        "proxy_ip": proxy["ip"],
                        "proxy_port": proxy["port"],
                    },
                )
                self.task_queue.put(work)

        while True:
            self._setup_timing()

            logger.info(F"Task queue size: {self.task_queue.qsize()}")

            # Adding new tasks
            if (
                self.task_queue.qsize() < self.settings.qsize
                and self._ids_pointer < len(self.settings.ids)
            ):
                logger.info("Adding new tasks")
                self._add_new_tasks()

            if self.task_queue.empty() and self.done_queue.empty():
                logger.info("The queues might be empty")
                # common part
                if self._exit_counter > 1:
                    logger.info("The queues are empty!")
                time.sleep(1)
                self._exit_counter += 1
                if self._exit_counter > 5:
                    logger.info("Retry count exceeded, exiting")
                    self._stop_all_threads()
                    return
            else:
                self._exit_counter = 0

            try:
                task, status, _details = self.done_queue.get(timeout=1)
                # logger.info(f"TASK: {task}")
                logger.info(f"STATUS: {status}")
                logger.info(f"DETAILS: {json.dumps(_details, indent=4)}")
                # logger.info(f"TEXT: {_details.get('text')}")
            except queue.Empty:
                if any(_.is_alive() for _ in self._processes):
                    logger.info("Some threads are still alive, continuing.")
                    continue
                else:
                    logger.info("All threads are dead, exiting.")
                    return

            s = signal.signal(signal.SIGINT, signal.SIG_IGN)

            if task == "COOKIE":
                self._set_cookie(status, _details)

            elif task == "GET_PAGE":
                self._get_page(status, _details)

            else:
                logger.warning(f"Processing loop - unknown task: {task}")

            signal.signal(signal.SIGINT, s)

        self.settings.close_files()


class CustomFormatter(logging.Formatter):
    # def __init__(self, fmt=None, datefmt=None, style='%', validate=True):
    #     super().__init__(fmt, datefmt, style, validate)

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"  # noqa: E501

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


if __name__ == "__main__":
    freeze_support()

    f_str = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s"
    logging.basicConfig(level=logging.INFO, format=f_str, filename="log.txt")

    # logging.basicConfig(level=logging.DEBUG, format=f_str, filename='log.txt')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(CustomFormatter(f_str))
    logging.getLogger().addHandler(console_handler)

    logger = logging.getLogger(__name__)
    # logger.addHandler(console_handler)

    # Disabling debug logging for urllib3 in requests
    # urllib3_logger = logging.getLogger("urllib3")
    # urllib3_logger.setLevel(logging.ERROR)

    logger.info("\n\n\n========== Program started ==========")

    loader = Loader()

    try:
        loader.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt (Ctrl+^C), exiting the application...")
