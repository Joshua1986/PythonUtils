# coding=utf-8
import json
import logging
import requests
import time

logger = logging.getLogger("settlement")


def send_request(method, url, params=None, retry_time_max=3):
    retry_times = 0

    r = requests.request(method, url, params=params)
    logger.info(r.url)
    logger.info(r.status_code)
    try:
        s = r.json()
        logger.info(s)
    except ValueError as e:
        print e.message
        while retry_times < retry_time_max:
            try:
                time.sleep(2)
                retry_times += 1
                s = requests.request(method, url, params=params).json()
            except ValueError as e:
                logger.error(e.message)
                continue
            return s
        return {}
    return s


def send_post_request(url, params=None, retry_time_max=2):
    return send_request("POST", url, params, retry_time_max)


def send_get_request(url, params=None, retry_time_max=2):
    return send_request("GET", url, params, retry_time_max)


def send_post_json_request(url, params):
    r = requests.post(url, data=json.dumps(params))
    logger.info(r.url)
    logger.info(r.status_code)
    s = r.json()
    logger.info(s)
    return s


def del_tag(del_column, iterator):
    if isinstance(iterator, dict):
        for tag in del_column:
            del iterator[tag]
    elif isinstance(iterator, list):
        for l in iterator:
            for tag in del_column:
                del l[tag]
    else:
        raise RuntimeError("The type of iterator is incorrect!")
    return iterator


def show_tag(show_column, iterator):
    if isinstance(iterator, dict):
        for tag in iterator.keys():
            if tag not in show_column:
                del iterator[tag]
    elif isinstance(iterator, list):
        for l in iterator:
            for tag in l.keys():
                if tag not in show_column:
                    del l[tag]
    else:
        raise RuntimeError("The type of iterator is incorrect!")
    return iterator


if __name__ == "__main__":
    from itertools import *
    for vec in permutations(range(8)):
        if (8 == len(set(vec[i] + i for i in range(8))) == len(set(vec[i] - i for i in range(8)))): print vec
