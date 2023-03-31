#!/usr/bin/env python3

import argparse
import json
import logging
import os
import random
from typing import Any, Optional

DEFAULTS_FILE = "defaults.json"
SECRETS_FILE = "secrets.json"


class CLITool:
    def __init__(self):
        ...

    def _parse_cli_args(self) -> None:
        parser = argparse.ArgumentParser()

        ids_group = parser.add_mutually_exclusive_group(required=True)
        ids_group.add_argument("--ids_file", "-if", default="")
        ids_group.add_argument("--ids", nargs=2, type=int, default=0)

        parser.add_argument("--folder", "-f", default="descr")
        parser.add_argument("--ids_finished", default="finished.txt")
        parser.add_argument("--ids_ignore", "-old", default="")
        parser.add_argument("--login_file", "-lf", default="login.txt")
        parser.add_argument(
            "--noproxy",
            "--direct",
            "-d",
            action="store_true",
            default=False,
        )
        parser.add_argument("--password", "-pw", default="")
        parser.add_argument("--port", "-p", type=int, default=9150)
        parser.add_argument(
            "--print", action="store_true", default=False
        )
        parser.add_argument("--proxy_file", "-pf", "-pr", default="proxy.txt")
        parser.add_argument("--qsize", "-q", type=int, default=0)
        parser.add_argument(
            "--random", action="store_true", default=False
        )
        parser.add_argument(
            "--restore",
            "--resume",
            action="store_true",
            default=False,
        )
        parser.add_argument("--threads", "-tr", type=int, default=1)
        parser.add_argument("--table_file", "-tf", default="table.txt")
        parser.add_argument("--user", "-u", default="")

        # parser.add_argument('--html')
        # parser.add_argument('--cookie')
        # parser.add_argument('--cookies_list')

        self.options = parser.parse_args()

        self.proxy_port = self.options.port
        self.table_file = self.options.table_file
        self.ids_finished = self.options.ids_finished

        # for attr_name, attr_value in self.options.__dict__.items():
        #     setattr(self, attr_name, attr_value)

        self.qsize = self.options.qsize or min((self.options.threads + 2), 30)


class Settings(CLITool):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Loading settings")

        self._parse_cli_args()

        self._load_defaults()
        self._load_secrets()

        self.useragents = self.defaults["useragents"]

        self.headers = self.defaults["headers"]
        # self.headers["User-Agent"] = self.useragents[
        #     random.randrange(0, len(self.useragents))
        # ]
        # self.headers["Cookie"] = cookie

        self.proxy_list = []
        self.login_list = []
        self.ids = set()

        self.handle_table_file = None
        self.handle_finished_file = None

        self.temp_cookies_filename = "temp_cookies.txt"

        self.logger.debug("Finished loading settings")

        self.threads_per_proxy = 1
        self.threads_per_cookie = 1

    def _load_defaults(self) -> None:
        with open(DEFAULTS_FILE, "r") as f:
            self.defaults = json.load(f)

    def _load_secrets(self) -> None:
        with open(SECRETS_FILE, "r") as f:
            self.secrets = json.load(f)

    # Separate def, because ids lists eats much RAM => each thread eats much RAM
    def prepare_lists(self) -> None:
        self.logger.info("Preparing lists")

        # Setting up proxy
        if self.options.noproxy:
            self.logger.info("Not using proxy")
            # self.proxy_list.append(options.port)

        elif os.path.exists(self.options.proxy_file):
            proxies = list(open(self.options.proxy_file))
            for line in proxies:
                if not line.strip():
                    continue
                ip, port = line.split()
                self.proxy_list.append(
                    {"ip": ip, "port": int(port), "in_use": 0, "fails": 0}
                )
            random.shuffle(self.proxy_list)
            self.logger.info(f"loaded {len(self.proxy_list)} proxies from file")

        else:  # len(self.proxy_list) == 0:
            self.proxy_list = [
                {
                    "ip": "127.0.0.1",
                    "port": self.proxy_port,
                    "in_use": 0,
                    "fails": 0,
                }
            ]
            self.logger.info(
                f"Loaded single proxy - 127.0.0.1:{self.proxy_port}"
            )
            self.logger.info(f"Proxy list obtained: {self.proxy_list}")

        # Setting up credentials
        self.login_list = [
            {
                "username": self.options.user or self.secrets["username"],
                "password": self.options.password or self.secrets["password"],
                "in_use": 0,
                "fails": 0,
            }
        ]

        if self.options.ids_file:
            self.logger.debug("Loading ids from file")
            self.ids = set(map(int, open(self.options.ids_file)))
        else:
            self.ids = set(range(self.options.ids[0], self.options.ids[1]))

        if self.options.ids_ignore and os.path.isfile(self.options.ids_ignore):
            self.logger.debug("ignore part of ids from file")
            max_id = 0
            # ignoring blank lines
            old_ids = set(
                map(
                    int,
                    filter(
                        lambda s: s != "",
                        map(lambda s: s.strip(), open(self.options.ids_ignore)),
                    ),
                )
            )
            # old_ids = set(map(int, open(options.old)))

            max_id = max(max_id, max(old_ids))

            for id_ in range(1, max_id):
                if (id_ not in old_ids) and (id_ in self.ids):
                    self.ids.remove(id_)

        if self.options.restore and os.path.isfile("finished.txt"):
            self.logger.debug(
                "ignoring ids from finished file (restore option)"
            )
            ids_finished = set(map(int, open("finished.txt")))
            ids_new = []
            for id_ in self.ids:
                if id_ not in ids_finished:
                    ids_new.append(id_)

            self.logger.info("input:   \t%i" % len(self.ids))
            self.logger.info("finished:\t%i" % len(ids_finished))
            self.logger.info("left:    \t%i" % len(ids_new))
            self.ids = ids_new

        if self.options.random:
            self.logger.debug("shuffle ids")
            random.shuffle(self.ids)

        self.ids = list(self.ids)

        self.logger.debug("end preparing lists")

    def open_files(self) -> None:
        self.logger.debug("opening files to write results")
        self.handle_table_file = open(self.table_file, "a", encoding="utf8")
        self.handle_finished_file = open(
            self.ids_finished, "a", encoding="utf8"
        )
        # log_file = open('log.txt', 'a', encoding='utf8')

    def close_files(self) -> None:
        self.logger.debug("closing files with results")
        self.handle_table_file.close()
        # log_file.close()
        self.handle_finished_file.close()

    def load_cookies(self) -> None:
        if not os.path.isfile(self.temp_cookies_filename):
            return

        self.logger.debug("Loading cookies")

        for item in json.load(open(self.temp_cookies_filename)):
            cookie = item.get("cookie")
            if cookie:
                for i, login in enumerate(self.login_list):
                    if login["username"] == item["username"]:
                        self.login_list[i]["cookie"] = cookie
                        break

        self.logger.debug("load_cookies done")
        self.logger.debug(f"Cookies: {self.login_list}")

    def save_cookies(self) -> None:
        self.logger.debug("Saving cookies")
        with open(self.temp_cookies_filename, "w") as f:
            json.dump(self.login_list, f)

    def set_cookie(self, username: str, cookie: Any) -> None:
        for i, login in enumerate(self.login_list):
            if login["username"] == username:
                self.login_list[i]["cookie"] = cookie
                break

        self.save_cookies()

    def get_free_cookie(self) -> Any:
        t = self.threads_per_cookie
        not_using_logins = [
            _ for _ in self.login_list if _.get("cookie") and _["in_use"] < t
        ]
        if not not_using_logins:
            return

        random.shuffle(not_using_logins)
        selected_login = min(
            not_using_logins, key=lambda login_: login_["fails"]
        )
        # if selected_login['fails'] > 10:
        #     return None
        cookie = selected_login["cookie"]

        for i, login in enumerate(self.login_list):
            if login.get("cookie") == cookie:
                self.login_list[i]["in_use"] += 1

        return cookie

    def set_free_cookie(self, cookie: Any) -> None:
        for i, login in enumerate(self.login_list):
            if login.get("cookie") == cookie:
                self.login_list[i]["in_use"] -= 1

    def set_error_cookie(self, cookie: Any) -> None:
        self.logger.debug(f"Setting error cookie: {cookie}")
        self.logger.debug(self.login_list)

        for i, login in enumerate(self.login_list):
            if login.get("cookie") == cookie:
                self.login_list[i]["fails"] += 1

                if self.login_list[i]["fails"] > 5:
                    self.login_list[i]["cookie"] = ""
                    self.save_cookies()
                    self.logger.warning(
                        "cookie removed from pool (too many fails)"
                    )

    def get_free_proxy(self) -> Optional[Any]:
        if self.options.noproxy:
            return {"ip": "", "port": -1}
        self.logger.info(f"Proxy list: {self.proxy_list}")
        unused_proxies = [
            proxy
            for proxy in self.proxy_list
            if proxy.get("in_use", 0) < self.threads_per_proxy
        ]
        if not unused_proxies:
            self.logger.info(
                f"No unused proxies found in the list: {self.proxy_list}"
            )
            return

        random.shuffle(unused_proxies)

        selected_proxy = min(unused_proxies, key=lambda p: p["fails"])
        if selected_proxy["fails"] > 1000:
            self.logger.info("No free proxies, all have excessive fails")
            return

        ip, port = selected_proxy["ip"], selected_proxy["port"]
        for i, proxy in enumerate(self.proxy_list):
            if proxy["ip"] == ip and proxy["port"] == port:
                self.proxy_list[i]["in_use"] += 1
                return selected_proxy

    def _set_proxy(self, key: str, ip: str, port: int) -> None:
        if port >= 0:
            self.logger.debug(f"Setting `{key}` proxy: IP={ip}, PORT={port}")
            for i, proxy in enumerate(self.proxy_list):
                if proxy["ip"] == ip and proxy["port"] == port:
                    self.proxy_list[i][key] -= 1

    def set_free_proxy(self, proxy_ip: str, proxy_port: int) -> None:
        self._set_proxy("in_use", proxy_ip, proxy_port)

    def set_error_proxy(self, proxy_ip: str, proxy_port: int) -> None:
        self._set_proxy("fails", proxy_ip, proxy_port)
