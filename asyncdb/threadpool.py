"""
Thread pool with support of passing asyncio context to the threads.
"""

from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor

# noinspection PyPackageRequirements
from contextvars import copy_context
from functools import wraps
from typing import Any, Callable


class ContextAwareThreadPoolExecutor(ThreadPoolExecutor):
    """
    ThreadPoolExecutor that maintains context for executed methods. This was refused to implement to standard
    executor due to not being able to support this in process pool executors, but we don't use that, so we can
    do this.
    """

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future[Any]:
        """
        Submit a function to the executor with current context.
        :param fn:
        :param args:
        :param kwargs:
        :return:
        """
        ctx = copy_context()

        @wraps(fn)
        def wrapper(*a: Any, **k: Any) -> Future[Any]:
            return ctx.run(fn, *a, **k)

        return super().submit(wrapper, *args, **kwargs)
