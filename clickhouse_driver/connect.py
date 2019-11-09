from itertools import islice

from clickhouse_driver.client import Client
from clickhouse_driver.errors import Error as DriverError

apilevel = '2.0'

threadsafety = 2

paramstyle = 'pyformat'


class Error(StandardError):
    pass


class Warning(StandardError):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class Connection(object):
    def __init__(self, dsn=None, user=None, password=None, host=None,
                 database=None, **kwargs):

        self.cursors = []

        self.dsn = dsn
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.connection_kwargs = kwargs
        super(Connection, self).__init__()

    def _make_client(self):
        if self.dsn is not None:
            return Client.from_url(self.dsn)

        return Client(self.host, user=self.user, password=self.password,
                      database=self.database, **self.connection_kwargs)

    def close(self):
        for cursor in self.cursors:
            cursor.disconnect()

    def commit(self):
        # ClickHouse has no transactions
        pass

    def rollback(self):
        # ClickHouse has no transactions
        pass

    def cursor(self):
        client = self._make_client()
        cursor = Cursor(client)
        self.cursors.append(cursor)
        return cursor


class Cursor(object):

    class States(object):
        (
            NONE,
            RUNNING,
            FINISHED,
            CURSOR_CLOSED
        ) = range(4)

    _states = States()

    def __init__(self, client):
        self._client = client
        self._reset_state()

        self.arraysize = -1

        super(Cursor, self).__init__()

    @property
    def description(self):
        columns = self._columns or []
        types = self._types or []

        return [
            (name, type_code, None, None, None, None, True)
            for name, type_code in zip(columns, types)
        ]

    @property
    def rowcount(self):
        return -1

    def close(self):
        self._client.disconnect()
        self._state = self._states.CURSOR_CLOSED

    def execute(self, operation, parameters):
        self._check_cursor_closed()
        self._begin_query()

        try:
            execute, execute_kwargs = self._prepare()

            response = execute(
                operation, params=parameters, with_column_types=True,
                **execute_kwargs
            )

        except DriverError as orig:
            # TODO: check if original error exists in repr.
            raise OperationalError(orig)

        self._process_response(response)
        self._end_query()

    def executemany(self, operation, seq_of_parameters):
        self._check_cursor_closed()
        self._begin_query()

        try:
            execute, execute_kwargs = self._prepare()

            response = execute(
                operation, params=seq_of_parameters, **execute_kwargs
            )

        except DriverError as orig:
            raise OperationalError(orig)

        self._process_response(response, executemany=True)
        self._end_query()

    def fetchone(self):
        self._check_query_started()

        if self._stream_results:
            return next(self._rows, None)

        else:
            if not self._rows:
                return None

            return self._rows.pop(0)

    def fetchmany(self, size=None):
        self._check_query_started()

        if size is None:
            size = self.arraysize

        if self._stream_results:
            return list(islice(self._rows, size))

        rv = self._rows[:size]
        self._rows = self._rows[size:]
        return rv

    def fetchall(self):
        self._check_query_started()

        if self._stream_results:
            return list(self._rows)

        rv = self._rows
        self._rows = []
        return rv

    def setinputsizes(self, sizes):
        # Do nothing.
        pass

    def setoutputsize(self, size, column):
        # Do nothing.
        pass

    # Begin non-PEP methods
    def set_stream_results(self, stream_results, max_row_buffer):
        self._stream_results = stream_results
        self._max_row_buffer = max_row_buffer

    def set_settings(self, settings):
        self._settings = settings

    def set_types_check(self, types_check):
        self._types_check = types_check

    def set_external_table(self, name, structure, data):
        """
        Adds external table to cursor context.

        If the same table is specified more than once the last one is used.

        :param name: Name of external table
        :param structure: List of tuples (name, type) that defines table
                          structure. Example [(x, Int32)].
        :param data: Sequence of rows of tuples or dicts for transmission.
        :return: None
        """
        self._external_tables[name] = (structure, data)
    # End non-PEP methods

    # Private methods.
    def _prepare(self):
        external_tables = [
            {'name': name, 'structure': structure, 'data': data}
            for name, (structure, data) in self._external_tables
        ] or None

        execute = self._client.execute

        if self._stream_results:
            execute = self._client.execute_iter
            self._settings = self._settings or {}
            self._settings['max_block_size'] = self._max_row_buffer

        execute_kwargs = {
            'settings': self._settings,
            'external_tables': external_tables,
            'types_check': self._types_check
        }

        return execute, execute_kwargs

    def _process_response(self, response, executemany=False):
        if executemany:
            self._rowcount = response
            response = None

        if not response:
            self._columns = self._types = self._rows = []
            return

        if self._stream_results:
            columns_with_types = next(response)
            rows = response

        else:
            rows, columns_with_types = response

        if columns_with_types:
            self._columns, self._types = zip(*columns_with_types)
        else:
            self._columns = self._types = []

        self._rows = rows

    def _reset_state(self):
        """
        Resets query state and get ready for another query.
        """
        self._state = self._states.NONE

        self._columns = None
        self._types = None
        self._rows = None
        self._rowcount = -1

        self._stream_results = False
        self._max_row_buffer = 0
        self._settings = None
        self._external_tables = {}
        self._types_check = False

    def _begin_query(self):
        self._state = self._states.RUNNING

    def _end_query(self):
        self._state = self._states.FINISHED

    def _check_cursor_closed(self):
        if self._state == self._states.CURSOR_CLOSED:
            raise Error('Cursor is closed')

    def _check_query_started(self):
        if self._state == self._states.NONE:
            raise Error('No query yet')


def connect(dsn=None, user=None, password=None, host=None, database=None,
            **kwargs):
    return Connection(dsn=dsn, user=user, password=password, host=host,
                      database=database, **kwargs)
