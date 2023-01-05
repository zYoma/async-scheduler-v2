import logging
import os
import pickle
from collections import deque
from datetime import datetime

from job import Job, PrematureStartError

QUEUE_FILE = 'queue.lock'


class Scheduler:
    queue: deque = deque([])
    completed_job_list: list = []
    is_running: bool = True
    not_completed_jobs: list = []

    def add_job(self, job: Job):
        self.queue.appendleft(job)

    def run(self):
        while True:
            try:
                # берем из очереди задачу
                if job := self.queue.pop():
                    gen = job.generator
                    name = job.target.__name__

                    # если у задачи нет генератора, то это первый запуск
                    if not gen:
                        # если планировщик был остановлен
                        if not self.is_running:
                            # собираем задачи, которые еще не начали выполняться
                            self.not_completed_jobs.append(job)
                            continue

                        # если у задачи есть зависимости и не все они выполнены, не выполняем задачу
                        if job.dependencies and not self._check_all_dep_complete(job):
                            continue

                        # получаем генератор
                        gen = job.run()
                        # присваиваем его задаче
                        job.generator = gen

                    # проверяем время работы задачи
                    self._check_working_time(job)
                    # прокручиваем генератор
                    next(gen)

                    # если не упало StopIteration, значит задача не полностью выполнена
                    # вставляем ее в конец очереди
                    self.add_job(job)
            except StopIteration:
                # задача полностью выполнена
                self.completed_job_list.append(name)
            except PrematureStartError:
                # если задача запущена раньше запланированного времени, отправляем ее обратно в очередь
                self.add_job(job)
            except IndexError:
                # очередь пустая, можно завершать работу
                # очищаем файл с сохраненными задачами
                os.remove(QUEUE_FILE) if os.path.exists(QUEUE_FILE) else None
                break
            except Exception:
                logging.info(f"job {name} is fail, attempts retries {job.retries}")
                # в случае ошибки при выполнении задачи, смотрим на количество ретраев
                if job.retries > 0:
                    # если еще остались попытки, снова добавляем задачу в очередь
                    self.add_job(job)
                    job.retries -= 1

    def _check_all_dep_complete(self, job: Job) -> bool:
        all_dep_complete = True
        # бежим по всем зависимостям
        for depend in job.dependencies:
            # если зависимости еще нет в списке завершенных задач
            if depend not in self.completed_job_list:
                # добавляем обратно задачу в конец очереди
                self.add_job(job)
                all_dep_complete = False
                break

        return all_dep_complete

    def recovery_queue(self):
        # читаем таски из файла
        jobs = self.recovery_tasks(QUEUE_FILE)
        # наполняем очередь не выполненными ранее тасками
        [self.add_job(job) for job in jobs]

    def stop(self):
        self.is_running = False
        # еще раз запускаем планировщик с флагом is_running = False,
        # чтобы уже запущенные задачи отработали, а не запущенные были собраны в отдельный список
        self.run()
        # сохраняем таски, которые нужно будет выполнить при следующем старте
        self._save_tasks(QUEUE_FILE, self.not_completed_jobs)

    @staticmethod
    def _save_tasks(filename, jobs: list[Job]):
        with open(filename, 'wb') as f:
            pickle.dump(jobs, f, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def recovery_tasks(filename: str):
        try:
            with open(filename, 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            return []

    @staticmethod
    def _check_working_time(job: Job):
        # получаем время, прошедшее с момента запуска задачи
        working_time = (datetime.now() - job.started_at).total_seconds()
        # отменяем задачу, если время превышено
        if working_time > job.max_working_time:
            raise StopIteration
