# -*- coding: utf-8 -*-
from contextlib import contextmanager
from html import escape
from threading import Timer
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Type, Union
from types import TracebackType

import cx_Oracle
import sqlparse
from robot.api import logger
from robot.running.context import EXECUTION_CONTEXTS
from robot.running.timeouts import KeywordTimeout, TestTimeout
from robot.utils import ConnectionCache
from robot.libraries.BuiltIn import BuiltIn


class OracleDB(object):
    """
    Robot Framework library for working with Oracle DB.

    == Dependencies ==
    | cx_Oracle | http://cx-oracle.sourceforge.net | version >= 5.3 |
    | robot framework | http://robotframework.org |
    """

    DEFAULT_TIMEOUT = 900.0  # The default timeout for executing an SQL query is 15 minutes
    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
    last_executed_statement: Optional[str] = None
    last_executed_statement_params: Optional[Dict[str, Any]] = None
    last_used_connection_index: Optional[int] = None

    def __init__(self) -> None:
        """Library initialization.
        Robot Framework ConnectionCache() class is prepared for working with concurrent connections."""
        self._connection: Optional[cx_Oracle.Connection] = None
        self._cache = ConnectionCache()

    @property
    def connection(self) -> cx_Oracle.Connection:
        """Get current connection to Oracle database.

        *Raises:*\n
            RuntimeError: if there isn't any open connection.

        *Returns:*\n
            Current connection to the database.
        """
        if self._connection is None:
            raise RuntimeError('There is no open connection to Oracle database.')
        return self._connection

    def make_dsn(self, host: str, port: str, sid: str, service_name: str = '') -> str:
        """
        Build dsn string for use in connection.

        *Args:*\n
            host - database host;\n
            port - database port;\n
            sid - database sid;\n
            service_name - database service name;\n

        *Returns:*\n
            Returns dsn string.
        """
        return cx_Oracle.makedsn(host=host, port=port, sid=sid, service_name=service_name)

    def connect_to_oracle(self, dbname: str, dbusername: str, dbpassword: str = None, alias: str = None) -> int:
        """
        Connection to Oracle DB.

        *Args:*\n
            _dbname_ - database name;\n
            _dbusername_ - username for db connection;\n
            _dbpassword_ - password for db connection;\n
            _alias_ - connection alias, used for switching between open connections;\n

        *Returns:*\n
            Returns ID of the new connection. The connection is set as active.

        *Example:*\n
            | Connect To Oracle  |  rb60db  |  bis  |  password |
        """

        try:
            logger.debug(f'Connecting using : dbname={dbname}, dbusername={dbusername}, dbpassword={dbpassword}')
            connection_string = f'{dbusername}/{dbpassword}@{dbname}'
            self._connection = cx_Oracle.connect(connection_string)
            return self._cache.register(self.connection, alias)
        except cx_Oracle.DatabaseError as err:
            raise Exception("Logon to oracle  Error:", str(err))

    def disconnect_from_oracle(self) -> None:
        """
        Close active Oracle connection.

        *Example:*\n
            | Connect To Oracle  |  rb60db  |  bis  |  password |
            | Disconnect From Oracle |
        """

        self.connection.close()
        self._cache.empty_cache()

    def close_all_oracle_connections(self) -> None:
        """
        Close all Oracle connections that were opened.
        You should not use [#Disconnect From Oracle|Disconnect From Oracle] and [#Close All Oracle Connections|Close All Oracle Connections]
        together.
        After calling this keyword connection IDs returned by opening new connections [#Connect To Oracle|Connect To Oracle],
        will start from 1.

        *Example:*\n
            | Connect To Oracle  |  rb60db  |  bis |   password  |  alias=bis |
            | Connect To Oracle  |  rb60db  |  bis_dcs  |  password  |  alias=bis_dsc |
            | Switch Oracle Connection  |  bis |
            | @{sql_out_bis}=  |  Execute Sql String  |  select SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') from dual |
            | Switch Oracle Connection  |  bis_dsc |
            | @{sql_out_bis_dsc}=  |  Execute Sql String  |  select SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') from dual |
            | Close All Oracle Connections |
        """

        self._connection = self._cache.close_all()

    def switch_oracle_connection(self, index_or_alias: Union[int, str]) -> int:
        """
        Switch between existing Oracle connections using their connection IDs or aliases.
        The connection ID is obtained on creating connection.
        Connection alias is optional and can be set at connecting to DB [#Connect To Oracle|Connect To Oracle].

        *Args:*\n
            _index_or_alias_ - connection ID or alias assigned to connection;

        *Returns:*\n
            ID of the previous connection.

        *Example:* (switch by alias)\n
            | Connect To Oracle  |  rb60db  |  bis |   password  |  alias=bis |
            | Connect To Oracle  |  rb60db  |  bis_dcs  |  password  |  alias=bis_dsc |
            | Switch Oracle Connection  |  bis |
            | @{sql_out_bis}=  |  Execute Sql String  |  select SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') from dual |
            | Switch Oracle Connection  |  bis_dsc |
            | @{sql_out_bis_dsc}=  |  Execute Sql String  |  select SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') from dual |
            | Close All Oracle Connections |
            =>\n
            @{sql_out_bis} = BIS\n
            @{sql_out_bis_dcs}= BIS_DCS

        *Example:* (switch by index)\n
            | ${bis_index}=  |  Connect To Oracle  |  rb60db  |  bis  |  password  |
            | ${bis_dcs_index}=  |  Connect To Oracle  |  rb60db  |  bis_dcs  |  password |
            | @{sql_out_bis_dcs_1}=  |  Execute Sql String  |  select SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') from dual |
            | ${previous_index}=  |  Switch Oracle Connection  |  ${bis_index} |
            | @{sql_out_bis}=  |  Execute Sql String  |  select SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') from dual |
            | Switch Oracle Connection  |  ${previous_index} |
            | @{sql_out_bis_dcs_2}=  |  Execute Sql String  |  select SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') from dual |
            | Close All Oracle Connections |
            =>\n
            ${bis_index}= 1\n
            ${bis_dcs_index}= 2\n
            @{sql_out_bis_dcs_1} = BIS_DCS\n
            ${previous_index}= 2\n
            @{sql_out_bis} = BIS\n
            @{sql_out_bis_dcs_2}= BIS_DCS
        """

        old_index = self._cache.current_index
        self._connection = self._cache.switch(index_or_alias)
        return old_index

    @staticmethod
    def wrap_into_html_details(statement: str, summary: str) -> str:
        """Format statement for html logging.

        *Args:*\n
            _statement_: statement to log.
            _summary_: summary for details tag.

        *Returns:*\n
            Formatted statement.
        """
        statement = sqlparse.format(statement, reindent=True, indent_width=4, keyword_case='upper')
        statement_html = escape(statement)
        data = f'<details><summary>{summary}</summary><p>{statement_html}</p></details>'
        return data

    def _execute_sql(self, cursor: cx_Oracle.Cursor, statement: str, params: Dict[str, Any]) -> cx_Oracle.Cursor:
        """ Execute SQL query on Oracle DB using active connection.

        *Args*:\n
            _cursor_: cursor object.\n
            _statement_: SQL query to be executed.\n
            _params_: SQL query parameters.\n

        *Returns:*\n
            Query results.
        """
        statement_with_params = self._replace_parameters_in_statement(statement, params)
        _connection_info = '@'.join((cursor.connection.username, cursor.connection.dsn))
        data = self.wrap_into_html_details(statement=statement_with_params,
                                           summary=f'Executed PL/SQL statement on {_connection_info}')
        logger.info(data, html=True)
        cursor.prepare(statement)
        self.last_executed_statement = self._replace_parameters_in_statement(statement, params)
        self.last_used_connection_index = self._cache.current_index
        cursor.execute(None, params)

    @staticmethod
    def _get_timeout_from_execution_context() -> float:
        """Get timeout from Robot Framework execution context.

        Returns:
            Current timeout value in seconds or None if timeout is not set.
        """
        timeouts = {}
        default_timeout = OracleDB.DEFAULT_TIMEOUT
        for timeout in EXECUTION_CONTEXTS.current.timeouts:
            if timeout.active:
                timeouts[timeout.type] = timeout.time_left()

        if timeouts.get(KeywordTimeout.type, None):
            return timeouts[KeywordTimeout.type]
        test_timeout = timeouts.get(TestTimeout.type, None)
        return test_timeout if test_timeout and test_timeout < default_timeout else default_timeout

    def _replace_parameters_in_statement(self, statement: str, params: Dict[str, Any]) -> str:
        """Update SQL query parameters, if any exist, with their values for logging purposes.

        *Args*:\n
            _statement_: SQL query to be updated.\n
            _params_: SQL query parameters.\n

        *Returns:*\n
            SQL query with parameter names replaced with their values.
        """
        params_keys = sorted(params.keys(), reverse=True)
        for key in params_keys:
            if isinstance(params[key], (int, float)):
                statement = statement.replace(f':{key}', str(params[key]))
            elif params[key] is None:
                statement = statement.replace(f':{key}', 'NULL')
            else:
                statement = statement.replace(f':{key}', f"'{params[key]}'")
        return statement

    def execute_plsql_block(self, plsqlstatement: str, **params: Any) -> None:
        """
        PL/SQL block execution.

        *Args:*\n
            _plsqlstatement_ - PL/SQL block;\n
            _params_ - PL/SQL block parameters;\n

        *Raises:*\n
            PLSQL Error: Error message encoded according to DB where the code was run

        *Returns:*\n
            PL/SQL block execution result.

        *Example:*\n
            | *Settings* | *Value* |
            | Library    |       OracleDB |

            | *Variables* | *Value* |
            | ${var_failed}    |       3 |

            | *Test Cases* | *Action* | *Argument* | *Argument* | *Argument* |
            | Simple |
            |    | ${statement}=  |  catenate   |   SEPARATOR=\\r\\n  |    DECLARE  |
            |    | ...            |             |                     |       a NUMBER := ${var_failed}; |
            |    | ...            |             |                     |    BEGIN |
            |    | ...            |             |                     |       a := a + 1; |
            |    | ...            |             |                     |       if a = 4 then |
            |    | ...            |             |                     |         raise_application_error ( -20001, 'This is a custom error' ); |
            |    | ...            |             |                     |       end if; |
            |    | ...            |             |                     |    END; |
            |    | Execute Plsql Block   |  plsqlstatement=${statement} |
            =>\n
            DatabaseError: ORA-20001: This is a custom error

            |    | ${statement}=  |  catenate   |   SEPARATOR=\\r\\n  |    DECLARE  |
            |    | ...            |             |                     |       a NUMBER := :var; |
            |    | ...            |             |                     |    BEGIN |
            |    | ...            |             |                     |       a := a + 1; |
            |    | ...            |             |                     |       if a = 4 then |
            |    | ...            |             |                     |         raise_application_error ( -20001, 'This is a custom error' ); |
            |    | ...            |             |                     |       end if; |
            |    | ...            |             |                     |    END; |
            |    | Execute Plsql Block   |  plsqlstatement=${statement} | var=${var_failed} |
            =>\n
            DatabaseError: ORA-20001: This is a custom error
        """
        cursor = self.connection.cursor()
        with sql_timeout(timeout=self._get_timeout_from_execution_context(), connection=cursor.connection):
            try:
                self._execute_sql(cursor, plsqlstatement, params)
                self.connection.commit()
            finally:
                self.connection.rollback()

    def execute_plsql_block_with_dbms_output(self, plsqlstatement: str, **params: Any) -> List[str]:
        """
        Execute PL/SQL block with dbms_output().

        *Args:*\n
            _plsqlstatement_ - PL/SQL block;\n
            _params_ - PL/SQL block parameters;\n

        *Raises:*\n
            PLSQL Error: Error message encoded according to DB where the code was run.

        *Returns:*\n
            List of values returned by Oracle dbms_output.put_line().

        *Example:*\n
            | *Settings* | *Value* |
            | Library    |       OracleDB |

            | *Variables* | *Value* |
            | ${var}    |       4 |

            | *Test Cases* | *Action* | *Argument* | *Argument* | *Argument* |
            | Simple |
            |    | ${statement}=  |  catenate   |   SEPARATOR=\\r\\n  |    DECLARE  |
            |    | ...            |             |                     |       a NUMBER := ${var}; |
            |    | ...            |             |                     |    BEGIN |
            |    | ...            |             |                     |       a := a + 1; |
            |    | ...            |             |                     |       if a = 4 then |
            |    | ...            |             |                     |         raise_application_error ( -20001, 'This is a custom error' ); |
            |    | ...            |             |                     |       end if; |
            |    | ...            |             |                     |       dbms_output.put_line ('text '||a||', e-mail text'); |
            |    | ...            |             |                     |       dbms_output.put_line ('string 2 '); |
            |    | ...            |             |                     |    END; |
            |    | @{dbms}=       | Execute Plsql Block With Dbms Output   |  plsqlstatement=${statement} |
            =>\n
            | @{dbms} | text 5, e-mail text |
            | | string 2 |

            |    | ${statement}=  |  catenate   |   SEPARATOR=\\r\\n  |    DECLARE  |
            |    | ...            |             |                     |       a NUMBER := :var; |
            |    | ...            |             |                     |    BEGIN |
            |    | ...            |             |                     |       a := a + 1; |
            |    | ...            |             |                     |       if a = 4 then |
            |    | ...            |             |                     |         raise_application_error ( -20001, 'This is a custom error' ); |
            |    | ...            |             |                     |       end if; |
            |    | ...            |             |                     |       dbms_output.put_line ('text '||a||', e-mail text'); |
            |    | ...            |             |                     |       dbms_output.put_line ('string 2 '); |
            |    | ...            |             |                     |    END; |
            |    | @{dbms}=       | Execute Plsql Block With Dbms Output   |  plsqlstatement=${statement} |  var=${var} |
            =>\n
            | @{dbms} | text 5, e-mail text |
            | | string 2 |
        """
        dbms_output = []
        cursor = self.connection.cursor()
        with sql_timeout(timeout=self._get_timeout_from_execution_context(), connection=cursor.connection):
            try:
                cursor.callproc("dbms_output.enable")
                self._execute_sql(cursor, plsqlstatement, params)
                self.connection.commit()
                statusvar = cursor.var(cx_Oracle.NUMBER)
                linevar = cursor.var(cx_Oracle.STRING)
                while True:
                    cursor.callproc("dbms_output.get_line", (linevar, statusvar))
                    if statusvar.getvalue() != 0:
                        break
                    dbms_output.append(linevar.getvalue())
                return dbms_output
            finally:
                self.connection.rollback()

    def execute_plsql_script(self, file_path: str, **params: Any) -> None:
        """
         Execution of PL/SQL code from file.

        *Args:*\n
            _file_path_ - path to PL/SQL script file;\n
            _params_ - PL/SQL code parameters;\n

        *Raises:*\n
            PLSQL Error: Error message encoded according to DB where the code was run.

        *Example:*\n
            |  Execute Plsql Script  |  ${CURDIR}${/}plsql_script.sql |
            |  Execute Plsql Script  |  ${CURDIR}${/}plsql_script.sql | first_param=1 | second_param=2 |
        """

        with open(file_path, "r") as script:
            data = script.read()
            self.execute_plsql_block(data, **params)

    def execute_sql_string(self, plsqlstatement: str, **params: Any) -> List[Tuple[Any, ...]]:
        """
        Execute PL/SQL string.

        *Args:*\n
            _plsqlstatement_ - PL/SQL string;\n
            _params_ - PL/SQL string parameters;\n

        *Raises:*\n
            PLSQL Error: Error message encoded according to DB where the code was run.

        *Returns:*\n
            PL/SQL string execution result.

        *Example:*\n
            | @{query}= | Execute Sql String | select sysdate, sysdate+1 from dual |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][0]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][1]} |

            | @{query}= | Execute Sql String | select sysdate, sysdate+:d from dual | d=1 |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][0]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][1]} |
        """
        cursor = self.connection.cursor()
        with sql_timeout(timeout=self._get_timeout_from_execution_context(), connection=cursor.connection):
            try:
                self._execute_sql(cursor, plsqlstatement, params)
                query_result = cursor.fetchall()
                self.result_logger(query_result)
                return query_result
            finally:
                self.connection.rollback()

    def execute_sql_string_mapped(self, sql_statement: str, **params: Any) -> List[Dict[str, Any]]:
        """SQL query execution where each result row is mapped as a dict with column names as keys.

        *Args:*\n
            _sql_statement_ - PL/SQL string;\n
            _params_ - PL/SQL string parameters;\n

        *Returns:*\n
            A list of dictionaries where column names are mapped as keys.

        *Example:*\n
            | @{query}= | Execute Sql String Mapped| select sysdate, sysdate+1 from dual |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][sysdate]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][sysdate1]} |

            | @{query}= | Execute Sql String Mapped| select sysdate, sysdate+:d from dual | d=1 |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][sysdate]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][sysdate1]} |
        """
        cursor = self.connection.cursor()
        with sql_timeout(timeout=self._get_timeout_from_execution_context(), connection=cursor.connection):
            try:
                self._execute_sql(cursor, sql_statement, params)
                col_name = tuple(i[0] for i in cursor.description)
                query_result = [dict(zip(col_name, row)) for row in cursor]
                self.result_logger(query_result)
                return query_result
            finally:
                self.connection.rollback()

    def execute_sql_string_generator(self, sql_statement: str, **params: Any) -> Iterable[Dict[str, Any]]:
        """Generator that yields each result row mapped as a dict with column names as keys.\n
        Intended for use mainly in code for other keywords.
        *If used, the generator must be explicitly closed before closing DB connection*

        *Args:*\n
            _sql_statement_ - PL/SQL string;\n
            _params_ - PL/SQL string parameters;\n

        Yields:*\n
            results dict.
        """
        self.last_executed_statement = sql_statement
        self.last_executed_statement_params = params
        cursor = self.connection.cursor()
        with sql_timeout(timeout=self._get_timeout_from_execution_context(), connection=cursor.connection):
            try:
                self._execute_sql(cursor, sql_statement, params)
                col_name = tuple(i[0] for i in cursor.description)
                for row in cursor:
                    yield dict(zip(col_name, row))
            finally:
                self.connection.rollback()

    def result_logger(self, query_result: List[Any], result_amount: int = 10) -> None:
        """Log first n rows from the query results

        *Args:*\n
            _query_result_ - query result to log, must be greater than 0
            _result_amount_ - amount of entries to display from result
        """
        if len(query_result) > result_amount > 0:
            query_result = query_result[:result_amount]
        logged_result = self.wrap_into_html_details(str(query_result), "SQL Query Result")
        logger.info(logged_result, html=True)

    @contextmanager
    def use_connection(self, conn_index: Union[int, str]) -> Iterator[None]:
        """Context manager for switching connection.

        Args:
            conn_index: Connection index or alias to switch.

        Yields: generator.
        """
        _old_con_index = self.switch_oracle_connection(conn_index)
        yield
        self.switch_oracle_connection(_old_con_index)


class sql_timeout(object):
    """Context manager to set SQL execution timeout."""

    def __init__(self, timeout: Optional[float], connection: cx_Oracle.Connection) -> None:
        """Initialisation.

        Args:
            timeout: timeout in seconds.
            connection: Oracle database connection.
        """
        self.timer = Timer(timeout, connection.cancel) if timeout else None
        self.builtin = BuiltIn()

    def __enter__(self) -> 'sql_timeout':
        """Enter the sql_timeout context manager.

        Returns:
            Instance of sql_timeout context manager.
        """
        if self.timer:
            self.timer.start()
            logger.debug(f'SQL execution started with timeout {self.timer.interval} seconds')  # type: ignore
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        """Exit the sql_timeout context manager.
        The parameters describe the exception that caused the context to be exited.
        If the context was exited without an exception, all three arguments will be None.

        Args:
            exc_type: exception type;
            exc_val: exception value;
            exc_tb: exception traceback.
        """
        if self.timer:
            if self.timer.is_alive():
                self.timer.cancel()
            else:
                logger.debug(f'SQL execution timeout {self.timer.interval} seconds exceeded.')  # type: ignore
                self.builtin.fail(msg=f'Timeout is ended and equal as {self.timer.interval}.')
