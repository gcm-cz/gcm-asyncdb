"""
sqlfactory (https://gitlab.com/gcm-cz/sqlfactory) transaction implementation for asyncdb, to be able to use sqlfactory's
statements directly.
"""

from .transaction import Transaction

# Ensure sqlfactory is present when using this module.
try:
    import sqlfactory  # noqa: F401
except ImportError as exc:
    raise ImportError("sqlfactory is not installed. Please install it with `pip install sqlfactory`") from exc


__all__ = ["Transaction"]
