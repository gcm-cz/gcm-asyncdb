"""
Thread pool with support of passing asyncio context to the threads.
"""

# noinspection PyPackageRequirements
from contextvars import copy_context

from concurrent.futures.thread import ThreadPoolExecutor
from functools import wraps


class ContextAwareThreadPoolExecutor(ThreadPoolExecutor):
    """
    ThreadPoolExecutor that maintains context for executed methods. This was refused to implement to standard
    executor due to not being able to support this in process pool executors, but we don't use that, so we can
    do this.
    """

    def submit(self, fn, /, *args, **kwargs):
        ctx = copy_context()

        @wraps(fn)
        def wrapper(*a, **k):
            return ctx.run(fn, *a, **k)

        return super().submit(wrapper, *args, **kwargs)
