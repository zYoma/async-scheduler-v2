import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from typing import Generator

import requests

from job import Job, coroutine
from scheduler import Scheduler


@coroutine
def print_log() -> Generator:
    # будим использовать данную корутину для вывода логов
    # лог можно отправить в корутину методом send
    while True:
        message = yield
        logging.info(message)


def create_dir() -> Generator:
    print_log().send("Start create_dir")
    yield
    os.makedirs("tmp", exist_ok=True)
    print_log().send("Success create_dir")


def create_file() -> Generator:
    print_log().send("Start create_file")
    yield
    print_log().send("Get response")
    response = requests.get("https://ya.ru")
    yield
    print_log().send("Save to file")
    with open('tmp/response.txt', 'w') as f:
        f.write(response.text)
    print_log().send("Success create_file")


def delete_dir() -> Generator:
    print_log().send("Start delete_dir")
    yield
    shutil.rmtree("tmp/", ignore_errors=True)
    print_log().send("Success delete_dir")


def long_time_job() -> Generator:
    # долгая задача, должна быть отменена
    print_log().send("Start long_time_job")
    time.sleep(3)
    yield
    print_log().send("i alive")
    yield
    print_log().send("Success long_time_job")


def job_with_error():
    print_log().send("Start job_with_error")
    raise ValueError


def main(scheduler: Scheduler):
    job1 = Job(target=long_time_job, start_at=datetime.now() + timedelta(seconds=5))
    job2 = Job(target=create_dir)
    job3 = Job(target=create_file, dependencies=["create_dir"])
    job4 = Job(target=delete_dir, dependencies=["create_dir", "create_file"])
    job5 = Job(target=job_with_error, retries=2)
    # восстанавливаем ранее не выполненные задачи
    scheduler.recovery_queue()

    scheduler.add_job(job4)
    scheduler.add_job(job3)
    scheduler.add_job(job1)
    # задача от которой зависят другие, добавлена в очередь последней,
    # остальные должны ждать ее выполнения
    scheduler.add_job(job2)
    scheduler.add_job(job5)

    scheduler.run()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
    )

    scheduler = Scheduler()
    try:
        main(scheduler)
    except KeyboardInterrupt:
        scheduler.stop()
