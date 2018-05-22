# -*- coding: utf-8 -*-

from robot.api import logger
from robot.utils import ConnectionCache

try:
    import cx_Oracle
except ImportError as info:
    logger.warn("Import cx_Oracle Error:", info)
if cx_Oracle.version < '3.0':
    logger.warn("Very old version of cx_Oracle :", cx_Oracle.version)


class OracleDB(object):
    """
    Robot Framework library for working with Oracle DB.

    == Dependencies ==
    | cx_Oracle | http://cx-oracle.sourceforge.net | version > 3.0 |
    | robot framework | http://robotframework.org |
    """

    ROBOT_LIBRARY_SCOPE = 'GLOBAL'
    last_executed_statement = None
    last_executed_statement_params = None

    def __init__(self):
        """Library initialization.
        Robot Framework ConnectionCache() class is prepared for working with concurrent connections."""
        self._connection = None
        self._cache = ConnectionCache()

    def connect_to_oracle(self, dbname, dbusername, dbpassword, alias=None):
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
            logger.debug(
                'Connecting using : dbname=%s, dbusername=%s, dbpassword=%s ' % (dbname, dbusername, dbpassword))
            connection_string = '%s/%s@%s' % (dbusername, dbpassword, dbname)
            self._connection = cx_Oracle.connect(connection_string)
            return self._cache.register(self._connection, alias)
        except cx_Oracle.DatabaseError as err:
            raise Exception("Logon to oracle  Error:", str(err))

    def disconnect_from_oracle(self):
        """
        Close active Oracle connection.

        *Example:*\n
            | Connect To Oracle  |  rb60db  |  bis  |  password |
            | Disconnect From Oracle |
        """

        self._connection.close()
        self._cache.empty_cache()

    def close_all_oracle_connections(self):
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

    def switch_oracle_connection(self, index_or_alias):
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

    def _execute_sql(self, cursor, statement, params):
        """ Execute SQL query on Oracle DB using active connection.

        *Args*:\n
            _cursor_: cursor object.\n
            _statement_: SQL query to be executed.\n
            _params_: SQL query parameters.\n

        *Returns:*\n
            Query results.
        """
        statement_with_params = self._replace_parameters_in_statement(statement, params)
        logger.info(statement_with_params, html=True)
        cursor.prepare(statement)
        self.last_executed_statement = self._replace_parameters_in_statement(statement, params)
        return cursor.execute(None, params)

    def _replace_parameters_in_statement(self, statement, params):
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
                statement = statement.replace(':{}'.format(key), str(params[key]))
            else:
                statement = statement.replace(':{}'.format(key), "'{}'".format(params[key]))
        return statement

    def execute_plsql_block(self, plsqlstatement, **params):
        """
        PL\SQL block execution.

        *Args:*\n
            _plsqlstatement_ - PL\SQL block;\n
            _params_ - PL\SQL block parameters;\n

        *Raises:*\n
            PLSQL Error: Error message encoded according to DB where the code was run

        *Returns:*\n
            PL\SQL block execution result.

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

        cursor = None
        try:
            cursor = self._connection.cursor()
            self._execute_sql(cursor, plsqlstatement, params)
            self._connection.commit()
        finally:
            if cursor:
                self._connection.rollback()

    def execute_plsql_block_with_dbms_output(self, plsqlstatement, **params):
        """
        Execute PL\SQL block with dbms_output().

        *Args:*\n
            _plsqlstatement_ - PL\SQL block;\n
            _params_ - PL\SQL block parameters;\n

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

        cursor = None
        dbms_output = []
        try:
            cursor = self._connection.cursor()
            cursor.callproc("dbms_output.enable")
            self._execute_sql(cursor, plsqlstatement, params)
            self._connection.commit()
            statusvar = cursor.var(cx_Oracle.NUMBER)
            linevar = cursor.var(cx_Oracle.STRING)
            while True:
                cursor.callproc("dbms_output.get_line", (linevar, statusvar))
                if statusvar.getvalue() != 0:
                    break
                dbms_output.append(linevar.getvalue())
            return dbms_output
        finally:
            if cursor:
                self._connection.rollback()

    def execute_plsql_script(self, file_path, **params):
        """
         Execution of PL\SQL code from file.

        *Args:*\n
            _file_path_ - path to PL\SQL script file;\n
            _params_ - PL\SQL code parameters;\n

        *Raises:*\n
            PLSQL Error: Error message encoded according to DB where the code was run.

        *Example:*\n
            |  Execute Plsql Script  |  ${CURDIR}${/}plsql_script.sql |
            |  Execute Plsql Script  |  ${CURDIR}${/}plsql_script.sql | first_param=1 | second_param=2 |
        """

        with open(file_path, "r") as script:
            data = script.read()
            self.execute_plsql_block(data, **params)

    def execute_sql_string(self, plsqlstatement, **params):
        """
        Execute PL\SQL string.

        *Args:*\n
            _plsqlstatement_ - PL\SQL string;\n
            _params_ - PL\SQL string parameters;\n

        *Raises:*\n
            PLSQL Error: Error message encoded according to DB where the code was run.

        *Returns:*\n
            PL\SQL string execution result.

        *Example:*\n
            | @{query}= | Execute Sql String | select sysdate, sysdate+1 from dual |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][0]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][1]} |

            | @{query}= | Execute Sql String | select sysdate, sysdate+:d from dual | d=1 |
            | Set Test Variable  |  ${sys_date}  |  ${query[0][0]} |
            | Set Test Variable  |  ${next_date}  |  ${query[0][1]} |
        """

        cursor = None
        try:
            cursor = self._connection.cursor()
            self._execute_sql(cursor, plsqlstatement, params)
            query_result = cursor.fetchall()
            self.result_logger(query_result)
            return query_result
        finally:
            if cursor:
                self._connection.rollback()

    def execute_sql_string_mapped(self, sql_statement, **params):
        """SQL query execution where each result row is mapped as a dict with column names as keys.

        *Args:*\n
            _sql_statement_ - PL\SQL string;\n
            _params_ - PL\SQL string parameters;\n

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
        cursor = None
        try:
            cursor = self._connection.cursor()
            self._execute_sql(cursor, sql_statement, params)
            col_name = tuple(i[0] for i in cursor.description)
            query_result = [dict(zip(col_name, row)) for row in cursor]
            self.result_logger(query_result)
            return query_result
        finally:
            if cursor:
                self._connection.rollback()

    def execute_sql_string_generator(self, sql_statement, **params):
        """Generator that yields each result row mapped as a dict with column names as keys.\n
        Intended for use mainly in code for other keywords.
        *If used, the generator must be explicitly closed before closing DB connection*

        *Args:*\n
            _sql_statement_ - PL\SQL string;\n
            _params_ - PL\SQL string parameters;\n

        Yields:*\n
            results dict.
        """
        cursor = None
        self.last_executed_statement = sql_statement
        self.last_executed_statement_params = params
        try:
            cursor = self._connection.cursor()
            self._execute_sql(cursor, sql_statement, params)
            col_name = tuple(i[0] for i in cursor.description)
            for row in cursor:
                yield dict(zip(col_name, row))
        finally:
            if cursor:
                self._connection.rollback()

    def result_logger(self, query_result, result_amount=10):
        """Log first n rows from the query results

        *Args:*\n
            _query_result_ - query result to log, must be greater than 0
            _result_amount_ - amount of entries to display from result
        """
        if len(query_result) > result_amount > 0:
            query_result = query_result[:result_amount]
        logger.info(query_result, html=True)
