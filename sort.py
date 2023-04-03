#!/usr/bin/env python3
import json
import logging
import tarfile


def read():
    logging.info("Reading...")
    with open("table.txt", "r", encoding="utf8") as f:
        for line in f:
            item = line.strip().split(sep="\t")
            items.append(item)
            # if item[0]:
            #     items.append(item)


def sort():
    logger.info("Sorting...")
    for item in items:
        logger.info(f"ITEM: {item}")
    items.sort(key=lambda x: int(x[3]), reverse=True)


def write():
    logger.info("Writing...")
    with open("table_sorted.txt", "w", encoding="utf8") as f:
        for item in items:
            line = ""
            for subitem in item:
                line += str(subitem) + "\t"
            f.write(line + "\n")
    with open("data.json", "w") as f:
        json.dump(items, f, indent=4)


def compres():
    logger.info("Compressing...")
    with tarfile.open("table_sorted.tar.bz2", "w:bz2") as tar:
        for name in ["table_sorted.txt"]:
            tar.add(name)


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    items = []

    read()
    # sort()
    write()
    compres()
    logger.info("Done")
