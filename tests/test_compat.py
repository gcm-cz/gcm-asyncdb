import pytest

from asyncdb.asyncdb import observer as new_observer
from asyncdb.asyncdb.observers.logging_observer import LoggingObserver as NewLoggingObserver


def test_import_observers_warns() -> None:
    with pytest.warns(DeprecationWarning):
        from asyncdb.aiomysql.observer import GenericQueryObserver

        assert new_observer.GenericQueryObserver is GenericQueryObserver

    with pytest.warns(DeprecationWarning):
        from asyncdb.aiomysql.observer import QueryObserver

        assert new_observer.QueryObserver is QueryObserver

    with pytest.warns(DeprecationWarning):
        from asyncdb.aiomysql import QueryObserver

        assert new_observer.QueryObserver is QueryObserver

    with pytest.warns(DeprecationWarning):
        from asyncdb.aiomysql.observer import GenericTransactionObserver

        assert new_observer.GenericTransactionObserver is GenericTransactionObserver

    with pytest.warns(DeprecationWarning):
        from asyncdb.aiomysql.observer import TransactionObserver

        assert new_observer.TransactionObserver is TransactionObserver

    with pytest.warns(DeprecationWarning):
        from asyncdb.aiomysql import TransactionObserver

        assert new_observer.TransactionObserver is TransactionObserver

    with pytest.warns(DeprecationWarning):
        from asyncdb.aiomysql.observer import ObserverContext

        assert new_observer.ObserverContext is ObserverContext

    with pytest.warns(DeprecationWarning):
        from asyncdb.aiomysql.observers.logging_observer import LoggingObserver

        assert NewLoggingObserver is LoggingObserver

    with pytest.raises(ImportError):
        from asyncdb.aiomysql.observer import NonExistentObserver
