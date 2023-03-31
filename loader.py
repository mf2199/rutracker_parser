#!/usr/bin/env python3

import logging
import os
import queue  # for exceptions
import random
# import requests
import signal
import time
from multiprocessing import Process, Queue, freeze_support

import parse
from settings import Settings


def worker(input_, output):
    try:
        logger_ = logging.getLogger(
            "thread(%3i)" % random.randrange(1, 999)
        )  # random name
        logger_.debug("starting thread")

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
                logger_.warning("unknown task: %s" % new_input[0])

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    freeze_support()

    f_str = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s"
    logging.basicConfig(level=logging.INFO, format=f_str, filename="log.txt")

    # logging.basicConfig(level=logging.DEBUG, format=f_str, filename='log.txt')

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logging.Formatter(f_str))
    logging.getLogger().addHandler(consoleHandler)
    logger = logging.getLogger(__name__)

    # disable debug logging for urllib3 in requests
    urllib3_logger = logging.getLogger("urllib3")
    urllib3_logger.setLevel(logging.CRITICAL)

    logger.info("\n\n\n========== Program started ==========")

    try:
        settings = Settings()

        task_queue = Queue()
        done_queue = Queue()

        processes = []
        logger.info("numbers of threads: %i" % settings.options.threads)
        for _ in range(settings.options.threads):
            p = Process(target=worker, args=(task_queue, done_queue))
            p.start()
            processes.append(p)

        settings.prepare_lists()
        settings.open_files()

        def stop_threads_and_exit():
            logger.debug("Stopping all threads and exiting")
            for _ in range(settings.options.threads_num):
                task_queue.put(("STOP", {}))
            exit()

        if settings.options.print:
            stop_threads_and_exit()

        if not settings.ids:
            logger.info("Empty input/left list. Terminated")
            stop_threads_and_exit()

        settings.load_cookies()
        for login in settings.login_list:
            if not login.get("cookie"):
                proxy = settings.get_free_proxy()
                work = (
                    "COOKIE",
                    {
                        "username": login["username"],
                        "password": login["password"],
                        "proxy_ip": proxy["ip"],
                        "proxy_port": int(proxy["port"]),
                    },
                )
                task_queue.put(work)

        ids_pointer = 0
        # bulk = 30

        nexttime = time.time()
        exit_counter = 0
        status_nexttime = time.time()
        ids_status = {
            "finished_all": 0,
            "error_all": 0,
            "nohash_all": 0,
            "finished_last": 0,
            "error_last": 0,
            "nohash_last": 0,
        }

        while True:
            if time.time() > status_nexttime:
                status_nexttime = time.time() + 10
                speed = (
                    ids_status["finished_last"] + ids_status["nohash_last"]
                ) / 10.0
                if speed != 0:
                    time_remaining = (len(settings.ids) - ids_pointer) / speed
                else:
                    time_remaining = 0
                m, s = divmod(time_remaining, 60)
                h, m = divmod(m, 60)
                logger.info(
                    f'Last 10 sec: {ids_status["finished_last"]} - OK, '
                    f'{ids_status["nohash_last"]} - NOHASH, '
                    f'{ids_status["error_last"]} - ERROR, Remaining: '
                    f'{(len(settings.ids) - ids_pointer) // 1000,}, {h}:{m}"'
                )

                ids_status["finished_all"] += ids_status["finished_last"]
                ids_status["error_all"] += ids_status["error_last"]
                ids_status["nohash_all"] += ids_status["nohash_last"]
                ids_status["finished_last"] = 0
                ids_status["error_last"] = 0
                ids_status["nohash_last"] = 0

            # Adding new tasks
            if task_queue.qsize() < settings.qsize and ids_pointer < len(settings.ids):  # noqa : E501
                exit_counter = 0
                id_max = min(ids_pointer + settings.qsize, len(settings.ids))
                for i in range(ids_pointer, id_max):
                    proxy = settings.get_free_proxy()
                    if not proxy:
                        if time.time() > nexttime:
                            logger.info("No free proxies are available")
                            logger.debug(f"Proxies: {settings.proxy_list}")
                            nexttime = time.time() + 60
                        break

                    cookie = settings.get_free_cookie()
                    if not cookie:
                        if time.time() > nexttime:
                            logger.info("No free cookies are available")
                            logger.debug(f"Cookies: {settings.login_list}")
                            nexttime = time.time() + 60
                        break

                    work = (
                        "GET_PAGE",
                        {
                            "id": int(settings.ids[i]),
                            "cookie": cookie,
                            "headers": settings.headers,
                            "proxy_ip": proxy["ip"],
                            "proxy_port": int(proxy["port"]),
                        },
                    )
                    task_queue.put(work)
                    ids_pointer += 1

            if task_queue.empty() and done_queue.empty():
                # common part
                if exit_counter > 1:
                    logger.info("Queues are empty.")
                time.sleep(1)
                exit_counter += 1
                if exit_counter > 5:
                    stop_threads_and_exit()
            else:
                exit_counter = 0

            task = None
            status = None
            details = None

            try:
                task, status, details = done_queue.get(timeout=1)
            except queue.Empty:
                anybody_alive = False
                for process in processes:
                    if process.is_alive():
                        anybody_alive = True
                        break
                if anybody_alive:
                    continue
                else:
                    logger.info("All threads are dead, exiting.")
                    exit()

            s = signal.signal(signal.SIGINT, signal.SIG_IGN)

            if task == "COOKIE":
                if status == "OK":
                    logger.debug("processing loop. cookie - ok")
                    settings.set_free_proxy(
                        details["proxy_ip"], details["proxy_port"]
                    )
                    settings.set_cookie(details["username"], details["cookie"])
                elif status == "ERROR":
                    logger.error(
                        f"Processing loop - cookie error: {details['text']}"
                    )
                    settings.set_free_proxy(
                        details["proxy_ip"], details["proxy_port"]
                    )
                    # settings.set_cookie_error(details['username'])
                else:
                    logger.warning(
                        f"Processing loop - unknown cookie status: {status}"
                    )

            elif task == "GET_PAGE":
                if status == "OK":
                    ids_status["finished_last"] += 1
                    logger.debug(
                        f"Processing loop, get page - OK, id: {details['id']}"
                    )
                    settings.set_free_proxy(
                        details["proxy_ip"], details["proxy_port"]
                    )
                    settings.set_free_cookie(details["cookie"])
                    id_, line, description = (
                        details["id"],
                        details["line"],
                        details["description"],
                    )

                    if not os.path.exists(settings.options.descr_folder):
                        os.mkdir(settings.options.descr_folder)
                    path = os.path.join(
                        settings.options.descr_folder,
                        "%03i/" % (id_ // 100000),
                    )
                    if not os.path.exists(path):
                        os.mkdir(path)

                    filename = os.path.join(path, "%08i" % id_)
                    with open(filename, "w", encoding="utf8") as f:
                        f.write(description)

                    settings.handle_table_file.write(line + "\n")
                    settings.handle_finished_file.write(str(id_) + "\n")

                elif status == "NO_HASH":
                    ids_status["nohash_last"] += 1
                    logger.debug(
                        "Processing loop, get page - NO HASH, id: "
                        f"{details['id']}"
                    )
                    settings.set_free_proxy(
                        details["proxy_ip"], details["proxy_port"]
                    )
                    settings.set_free_cookie(details["cookie"])
                    id_ = details["id"]
                    settings.handle_finished_file.write(str(id_) + "\n")

                elif status == "ERROR":
                    ids_status["error_last"] += 1
                    logger.error(
                        f"Processing loop. get page - error: {details['text']}"
                    )
                    if details["text"] == "not logined":
                        settings.set_error_cookie(details["cookie"])
                    settings.set_free_cookie(details["cookie"])
                    if "request exception" in details["text"] or "request timeout exception" in details["text"]:  # noqa: E501
                        settings.set_error_proxy(
                            details["proxy_ip"], details["proxy_port"]
                        )
                    settings.set_free_proxy(
                        details["proxy_ip"], details["proxy_port"]
                    )
                    settings.ids.append(int(details["id"]))

                else:
                    logger.warning(
                        f"Processing loop, get page - unknown status: {status}, id: {details['id']}"  # noqa: E501
                    )
            else:
                logger.warning(f"Processing loop - unknown task: {task}")

            signal.signal(signal.SIGINT, s)

        settings.close_files()

    except KeyboardInterrupt:
        logger.info("Ctrl+^C, exitting...")
        exit()
