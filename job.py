from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Generator


def coroutine(func):
    # декоратор для инициализации генератора
    def inner(*args, **kwargs):
        g = func(*args, **kwargs)
        g.send(None)
        return g
    return inner


class PrematureStartError(Exception):
    ...


@dataclass
class Job:
    target: Callable
    generator: Generator | None = None
    dependencies: list[str] | None = None
    retries: int = 0
    max_working_time: int = 2
    started_at: datetime | None = None
    start_at: datetime = datetime.now()

    @coroutine
    def run(self):
        if self.start_at > datetime.now():
            # если задача запущена раньше указанного времени
            raise PrematureStartError

        # фиксируем время старта
        self.started_at = datetime.now()
        yield from self.target()
