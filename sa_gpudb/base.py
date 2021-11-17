# mssql/base.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

 
import datetime
import operator
import re

from sqlalchemy import sql, schema as sa_schema, exc, util
from sqlalchemy.sql import compiler, expression, util as sql_util
from sqlalchemy import engine
from sqlalchemy.engine import reflection, default
from sqlalchemy import types as sqltypes
from sqlalchemy.types import (
    INTEGER,
    BIGINT,
    SMALLINT,
    DECIMAL,
    NUMERIC,
    FLOAT,
    TIMESTAMP,
    DATETIME,
    DATE,
    BINARY,
    TEXT,
    VARCHAR,
    NVARCHAR,
    CHAR,
    NCHAR,
)

from sqlalchemy.util import update_wrapper
#from . import information_schema as ischema

# Column types from ODBC get columns response
ODBC_TYPE_BYTES = "BYTES"
ODBC_TYPE_DOUBLE = "DOUBLE"
ODBC_TYPE_FLOAT = "FLOAT"
ODBC_TYPE_INT = "INTEGER"
ODBC_TYPE_BIGINT = "BIGINT"
ODBC_TYPE_SMALLINT = "SMALLINT"
ODBC_TYPE_TINYINT = "TINYINT"
ODBC_TYPE_LONG = "LONG"
ODBC_TYPE_REAL = "REAL"
ODBC_TYPE_TIMESTAMP = "TIMESTAMP"
ODBC_TYPE_TYPE_TIMESTAMP = "TYPE_TIMESTAMP"
ODBC_TYPE_VARCHAR = "VARCHAR"
ODBC_TYPE_DECIMAL = "DECIMAL"
ODBC_TYPE_DATE = "DATE"
ODBC_TYPE_TYPE_DATE = "TYPE_DATE"
ODBC_TYPE_TYPE_TIME = "TYPE_TIME"
ODBC_TYPE_DATETIME = "DATETIME"
ODBC_TYPE_GEOMETRY = "GEOMETRY"
ODBC_TYPE_IPV4 = "IPV4"

# http://sqlserverbuilds.blogspot.com/
MS_2012_VERSION = (11,)
MS_2008_VERSION = (10,)
MS_2005_VERSION = (9,)
MS_2000_VERSION = (8,)

RESERVED_WORDS = set(
    [
        "add",
        "all",
        "alter",
        "and",
        "any",
        "as",
        "asc",
        "authorization",
        "backup",
        "begin",
        "between",
        "break",
        "browse",
        "bulk",
        "by",
        "cascade",
        "case",
        "check",
        "checkpoint",
        "close",
        "clustered",
        "coalesce",
        "collate",
        "column",
        "commit",
        "compute",
        "constraint",
        "contains",
        "containstable",
        "continue",
        "convert",
        "create",
        "cross",
        "current",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "cursor",
        "database",
        "dbcc",
        "deallocate",
        "declare",
        "default",
        "delete",
        "deny",
        "desc",
        "disk",
        "distinct",
        "distributed",
        "double",
        "drop",
        "dump",
        "else",
        "end",
        "errlvl",
        "escape",
        "except",
        "exec",
        "execute",
        "exists",
        "exit",
        "external",
        "fetch",
        "file",
        "fillfactor",
        "for",
        "foreign",
        "freetext",
        "freetexttable",
        "from",
        "full",
        "function",
        "goto",
        "grant",
        "group",
        "having",
        "holdlock",
        "identity",
        "identity_insert",
        "identitycol",
        "if",
        "in",
        "index",
        "inner",
        "insert",
        "intersect",
        "into",
        "is",
        "join",
        "key",
        "kill",
        "left",
        "like",
        "lineno",
        "load",
        "merge",
        "national",
        "nocheck",
        "nonclustered",
        "not",
        "null",
        "nullif",
        "of",
        "off",
        "offsets",
        "on",
        "open",
        "opendatasource",
        "openquery",
        "openrowset",
        "openxml",
        "option",
        "or",
        "order",
        "outer",
        "over",
        "percent",
        "pivot",
        "plan",
        "precision",
        "primary",
        "print",
        "proc",
        "procedure",
        "public",
        "raiserror",
        "read",
        "readtext",
        "reconfigure",
        "references",
        "replication",
        "restore",
        "restrict",
        "return",
        "revert",
        "revoke",
        "right",
        "rollback",
        "rowcount",
        "rowguidcol",
        "rule",
        "save",
        "schema",
        "securityaudit",
        "select",
        "session_user",
        "set",
        "setuser",
        "shutdown",
        "some",
        "statistics",
        "system_user",
        "table",
        "tablesample",
        "textsize",
        "then",
        "to",
        "top",
        "tran",
        "transaction",
        "trigger",
        "truncate",
        "tsequal",
        "union",
        "unique",
        "unpivot",
        "update",
        "updatetext",
        "use",
        "user",
        "values",
        "varying",
        "view",
        "waitfor",
        "when",
        "where",
        "while",
        "with",
        "writetext",
    ]
)


class REAL(sqltypes.REAL):
    __visit_name__ = "REAL"

    def __init__(self, **kw):
        # REAL is a synonym for FLOAT(24) on SQL server
        kw["precision"] = 24
        super(REAL, self).__init__(**kw)


class TINYINT(sqltypes.Integer):
    __visit_name__ = "TINYINT"


# MSSQL DATE/TIME types have varied behavior, sometimes returning
# strings.  MSDate/TIME check for everything, and always
# filter bind parameters into datetime objects (required by pyodbc,
# not sure about other dialects).


class _MSDate(sqltypes.Date):
    def bind_processor(self, dialect):
        def process(value):
            if type(value) == datetime.date:
                return datetime.datetime(value.year, value.month, value.day)
            else:
                return value

        return process

    _reg = re.compile(r"(\d+)-(\d+)-(\d+)")

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, datetime.datetime):
                return value.date()
            elif isinstance(value, util.string_types):
                m = self._reg.match(value)
                if not m:
                    raise ValueError("could not parse %r as a date value" % (value,))
                return datetime.date(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


class TIME(sqltypes.TIME):
    def __init__(self, precision=None, **kwargs):
        self.precision = precision
        super(TIME, self).__init__()

    __zero_date = datetime.date(1900, 1, 1)

    def bind_processor(self, dialect):
        def process(value):
            if isinstance(value, datetime.datetime):
                value = datetime.datetime.combine(self.__zero_date, value.time())
            elif isinstance(value, datetime.time):
                value = datetime.datetime.combine(self.__zero_date, value)
            return value

        return process

    _reg = re.compile(r"(\d+):(\d+):(\d+)(?:\.(\d{0,6}))?")

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, datetime.datetime):
                return value.time()
            elif isinstance(value, util.string_types):
                m = self._reg.match(value)
                if not m:
                    raise ValueError("could not parse %r as a time value" % (value,))
                return datetime.time(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


_MSTime = TIME


class _DateTimeBase(object):
    def bind_processor(self, dialect):
        def process(value):
            if type(value) == datetime.date:
                return datetime.datetime(value.year, value.month, value.day)
            else:
                return value

        return process


class _MSDateTime(_DateTimeBase, sqltypes.DateTime):
    pass


class SMALLDATETIME(_DateTimeBase, sqltypes.DateTime):
    __visit_name__ = "SMALLDATETIME"


class DATETIME2(_DateTimeBase, sqltypes.DateTime):
    __visit_name__ = "DATETIME2"

    def __init__(self, precision=None, **kw):
        super(DATETIME2, self).__init__(**kw)
        self.precision = precision


# TODO: is this not an Interval ?
class DATETIMEOFFSET(sqltypes.TypeEngine):
    __visit_name__ = "DATETIMEOFFSET"

    def __init__(self, precision=None, **kwargs):
        self.precision = precision


class _StringType(object):

    """Base for MSSQL string types."""

    def __init__(self, collation=None):
        super(_StringType, self).__init__(collation=collation)


class NTEXT(sqltypes.UnicodeText):

    """MSSQL NTEXT type, for variable-length unicode text up to 2^30
    characters."""

    __visit_name__ = "NTEXT"


class VARBINARY(sqltypes.VARBINARY, sqltypes.LargeBinary):
    """The MSSQL VARBINARY type.

    This type extends both :class:`.types.VARBINARY` and
    :class:`.types.LargeBinary`.   In "deprecate_large_types" mode,
    the :class:`.types.LargeBinary` type will produce ``VARBINARY(max)``
    on SQL Server.

    .. versionadded:: 1.0.0

    .. seealso::

        :ref:`mssql_large_type_deprecation`



    """

    __visit_name__ = "VARBINARY"


class IMAGE(sqltypes.LargeBinary):
    __visit_name__ = "IMAGE"


class BIT(sqltypes.TypeEngine):
    __visit_name__ = "BIT"


class MONEY(sqltypes.TypeEngine):
    __visit_name__ = "MONEY"


class SMALLMONEY(sqltypes.TypeEngine):
    __visit_name__ = "SMALLMONEY"


class UNIQUEIDENTIFIER(sqltypes.TypeEngine):
    __visit_name__ = "UNIQUEIDENTIFIER"


class SQL_VARIANT(sqltypes.TypeEngine):
    __visit_name__ = "SQL_VARIANT"


# old names.
MSDateTime = _MSDateTime
MSDate = _MSDate
MSReal = REAL
MSTinyInteger = TINYINT
MSTime = TIME
MSSmallDateTime = SMALLDATETIME
MSDateTime2 = DATETIME2
MSDateTimeOffset = DATETIMEOFFSET
MSText = TEXT
MSNText = NTEXT
MSString = VARCHAR
MSNVarchar = NVARCHAR
MSChar = CHAR
MSNChar = NCHAR
MSBinary = BINARY
MSVarBinary = VARBINARY
MSImage = IMAGE
MSBit = BIT
MSMoney = MONEY
MSSmallMoney = SMALLMONEY
MSUniqueIdentifier = UNIQUEIDENTIFIER
MSVariant = SQL_VARIANT

ischema_names = {
    "int": INTEGER,
    "bigint": BIGINT,
    "smallint": SMALLINT,
    "tinyint": TINYINT,
    "varchar": VARCHAR,
    "nvarchar": NVARCHAR,
    "char": CHAR,
    "nchar": NCHAR,
    "text": TEXT,
    "ntext": NTEXT,
    "decimal": DECIMAL,
    "numeric": NUMERIC,
    "float": FLOAT,
    "datetime": DATETIME,
    "datetime2": DATETIME2,
    "datetimeoffset": DATETIMEOFFSET,
    "date": DATE,
    "time": TIME,
    "smalldatetime": SMALLDATETIME,
    "binary": BINARY,
    "varbinary": VARBINARY,
    "bit": BIT,
    "real": REAL,
    "image": IMAGE,
    "timestamp": TIMESTAMP,
    "money": MONEY,
    "smallmoney": SMALLMONEY,
    "uniqueidentifier": UNIQUEIDENTIFIER,
    "sql_variant": SQL_VARIANT,
}


class MSTypeCompiler(compiler.GenericTypeCompiler):
    def _extend(self, spec, type_, length=None):
        """Extend a string-type declaration with standard SQL
        COLLATE annotations.

        """

        if getattr(type_, "collation", None):
            collation = "COLLATE %s" % type_.collation
        else:
            collation = None

        if not length:
            length = type_.length

        if length:
            spec = spec + "(%s)" % length

        return " ".join([c for c in (spec, collation) if c is not None])

    def visit_FLOAT(self, type_, **kw):
        precision = getattr(type_, "precision", None)
        if precision is None:
            return "FLOAT"
        else:
            return "FLOAT(%(precision)s)" % {"precision": precision}

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_DATETIMEOFFSET(self, type_, **kw):
        if type_.precision is not None:
            return "DATETIMEOFFSET(%s)" % type_.precision
        else:
            return "DATETIMEOFFSET"

    def visit_TIME(self, type_, **kw):
        precision = getattr(type_, "precision", None)
        if precision is not None:
            return "TIME(%s)" % precision
        else:
            return "TIME"

    def visit_DATETIME2(self, type_, **kw):
        precision = getattr(type_, "precision", None)
        if precision is not None:
            return "DATETIME2(%s)" % precision
        else:
            return "DATETIME2"

    def visit_SMALLDATETIME(self, type_, **kw):
        return "SMALLDATETIME"

    def visit_unicode(self, type_, **kw):
        return self.visit_NVARCHAR(type_, **kw)

    def visit_text(self, type_, **kw):
        if self.dialect.deprecate_large_types:
            return self.visit_VARCHAR(type_, **kw)
        else:
            return self.visit_TEXT(type_, **kw)

    def visit_unicode_text(self, type_, **kw):
        if self.dialect.deprecate_large_types:
            return self.visit_NVARCHAR(type_, **kw)
        else:
            return self.visit_NTEXT(type_, **kw)

    def visit_NTEXT(self, type_, **kw):
        return self._extend("NTEXT", type_)

    def visit_TEXT(self, type_, **kw):
        return self._extend("TEXT", type_)

    def visit_VARCHAR(self, type_, **kw):
        return self._extend("VARCHAR", type_, length=type_.length or "max")

    def visit_CHAR(self, type_, **kw):
        return self._extend("CHAR", type_)

    def visit_NCHAR(self, type_, **kw):
        return self._extend("NCHAR", type_)

    def visit_NVARCHAR(self, type_, **kw):
        return self._extend("NVARCHAR", type_, length=type_.length or "max")

    def visit_date(self, type_, **kw):
        if self.dialect.server_version_info < MS_2008_VERSION:
            return self.visit_DATETIME(type_, **kw)
        else:
            return self.visit_DATE(type_, **kw)

    def visit_time(self, type_, **kw):
        if self.dialect.server_version_info < MS_2008_VERSION:
            return self.visit_DATETIME(type_, **kw)
        else:
            return self.visit_TIME(type_, **kw)

    def visit_large_binary(self, type_, **kw):
        if self.dialect.deprecate_large_types:
            return self.visit_VARBINARY(type_, **kw)
        else:
            return self.visit_IMAGE(type_, **kw)

    def visit_IMAGE(self, type_, **kw):
        return "IMAGE"

    def visit_VARBINARY(self, type_, **kw):
        return self._extend("VARBINARY", type_, length=type_.length or "max")

    def visit_boolean(self, type_, **kw):
        return self.visit_BIT(type_)

    def visit_BIT(self, type_, **kw):
        return "BIT"

    def visit_MONEY(self, type_, **kw):
        return "MONEY"

    def visit_SMALLMONEY(self, type_, **kw):
        return "SMALLMONEY"

    def visit_UNIQUEIDENTIFIER(self, type_, **kw):
        return "UNIQUEIDENTIFIER"

    def visit_SQL_VARIANT(self, type_, **kw):
        return "SQL_VARIANT"


class MSExecutionContext(default.DefaultExecutionContext):
    _enable_identity_insert = False
    _select_lastrowid = False
    _result_proxy = None
    _lastrowid = None

    def _opt_encode(self, statement):
        if not self.dialect.supports_unicode_statements:
            return self.dialect._encoder(statement)[0]
        else:
            return statement

    def pre_exec(self):
        """Activate IDENTITY_INSERT if needed."""

        if self.isinsert:
            tbl = self.compiled.statement.table
            seq_column = tbl._autoincrement_column
            insert_has_sequence = seq_column is not None

            if insert_has_sequence:
                self._enable_identity_insert = seq_column.key in self.compiled_parameters[0] or (
                    self.compiled.statement.parameters
                    and (
                        (
                            self.compiled.statement._has_multi_parameters
                            and seq_column.key in self.compiled.statement.parameters[0]
                        )
                        or (
                            not self.compiled.statement._has_multi_parameters
                            and seq_column.key in self.compiled.statement.parameters
                        )
                    )
                )
            else:
                self._enable_identity_insert = False

            self._select_lastrowid = (
                insert_has_sequence
                and not self.compiled.returning
                and not self._enable_identity_insert
                and not self.executemany
            )

            if self._enable_identity_insert:
                self.root_connection._cursor_execute(
                    self.cursor,
                    self._opt_encode("SET IDENTITY_INSERT %s ON" % self.dialect.identifier_preparer.format_table(tbl)),
                    (),
                    self,
                )

    def post_exec(self):
        """Disable IDENTITY_INSERT if enabled."""

        conn = self.root_connection
        if self._select_lastrowid:
            if self.dialect.use_scope_identity:
                conn._cursor_execute(self.cursor, "SELECT scope_identity() AS lastrowid", (), self)
            else:
                conn._cursor_execute(self.cursor, "SELECT @@identity AS lastrowid", (), self)
            # fetchall() ensures the cursor is consumed without closing it
            row = self.cursor.fetchall()[0]
            self._lastrowid = int(row[0])

        if (self.isinsert or self.isupdate or self.isdelete) and self.compiled.returning:
            self._result_proxy = engine.FullyBufferedResultProxy(self)

        if self._enable_identity_insert:
            conn._cursor_execute(
                self.cursor,
                self._opt_encode(
                    "SET IDENTITY_INSERT %s OFF"
                    % self.dialect.identifier_preparer.format_table(self.compiled.statement.table)
                ),
                (),
                self,
            )

    def get_lastrowid(self):
        return self._lastrowid

    def handle_dbapi_exception(self, e):
        if self._enable_identity_insert:
            try:
                self.cursor.execute(
                    self._opt_encode(
                        "SET IDENTITY_INSERT %s OFF"
                        % self.dialect.identifier_preparer.format_table(self.compiled.statement.table)
                    )
                )
            except Exception:
                pass

    def get_result_proxy(self):
        if self._result_proxy:
            return self._result_proxy
        else:
            return engine.ResultProxy(self)


class MSSQLCompiler(compiler.SQLCompiler):
    returning_precedes_values = True

    extract_map = util.update_copy(
        compiler.SQLCompiler.extract_map,
        {"doy": "dayofyear", "dow": "weekday", "milliseconds": "millisecond", "microseconds": "microsecond"},
    )

    def __init__(self, *args, **kwargs):
        self.tablealiases = {}
        super(MSSQLCompiler, self).__init__(*args, **kwargs)

    def _with_legacy_schema_aliasing(fn):
        def decorate(self, *arg, **kw):
            if self.dialect.legacy_schema_aliasing:
                return fn(self, *arg, **kw)
            else:
                super_ = getattr(super(MSSQLCompiler, self), fn.__name__)
                return super_(*arg, **kw)

        return decorate

    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_current_date_func(self, fn, **kw):
        return "GETDATE()"

    def visit_length_func(self, fn, **kw):
        return "LEN%s" % self.function_argspec(fn, **kw)

    def visit_char_length_func(self, fn, **kw):
        return "LEN%s" % self.function_argspec(fn, **kw)

    def visit_concat_op_binary(self, binary, operator, **kw):
        return "%s + %s" % (self.process(binary.left, **kw), self.process(binary.right, **kw))

    def visit_true(self, expr, **kw):
        return "1"

    def visit_false(self, expr, **kw):
        return "0"

    def visit_match_op_binary(self, binary, operator, **kw):
        return "CONTAINS (%s, %s)" % (self.process(binary.left, **kw), self.process(binary.right, **kw))

    def get_select_precolumns(self, select, **kw):
        """MS-SQL puts TOP, it's version of LIMIT here"""

        s = ""
        if select._distinct:
            s += "DISTINCT "

        if select._simple_int_limit and not select._offset:
            # ODBC drivers and possibly others
            # don't support bind params in the SELECT clause on SQL Server.
            # so have to use literal here.
            s += "TOP %d " % select._limit

        if s:
            return s
        else:
            return compiler.SQLCompiler.get_select_precolumns(self, select, **kw)

    def get_from_hint_text(self, table, text):
        return text

    def get_crud_hint_text(self, table, text):
        return text

    def limit_clause(self, select, **kw):
        # Limit in mssql is after the select keyword
        return ""

    def visit_select(self, select, **kwargs):
        """Look for ``LIMIT`` and OFFSET in a select statement, and if
        so tries to wrap it in a subquery with ``row_number()`` criterion.

        """
        if (
            (not select._simple_int_limit and select._limit_clause is not None)
            or (select._offset_clause is not None and not select._simple_int_offset or select._offset)
        ) and not getattr(select, "_mssql_visit", None):

            # to use ROW_NUMBER(), an ORDER BY is required.
            if not select._order_by_clause.clauses:
                raise exc.CompileError(
                    "MSSQL requires an order_by when " "using an OFFSET or a non-simple " "LIMIT clause"
                )

            _order_by_clauses = select._order_by_clause.clauses
            limit_clause = select._limit_clause
            offset_clause = select._offset_clause
            kwargs["select_wraps_for"] = select
            select = select._generate()
            select._mssql_visit = True
            select = (
                select.column(sql.func.ROW_NUMBER().over(order_by=_order_by_clauses).label("mssql_rn"))
                .order_by(None)
                .alias()
            )

            mssql_rn = sql.column("mssql_rn")
            limitselect = sql.select([c for c in select.c if c.key != "mssql_rn"])
            if offset_clause is not None:
                limitselect.append_whereclause(mssql_rn > offset_clause)
                if limit_clause is not None:
                    limitselect.append_whereclause(mssql_rn <= (limit_clause + offset_clause))
            else:
                limitselect.append_whereclause(mssql_rn <= (limit_clause))
            return self.process(limitselect, **kwargs)
        else:
            return compiler.SQLCompiler.visit_select(self, select, **kwargs)

    @_with_legacy_schema_aliasing
    def visit_table(self, table, mssql_aliased=False, iscrud=False, **kwargs):
        if mssql_aliased is table or iscrud:
            return super(MSSQLCompiler, self).visit_table(table, **kwargs)

        # alias schema-qualified tables
        alias = self._schema_aliased_table(table)
        if alias is not None:
            return self.process(alias, mssql_aliased=table, **kwargs)
        else:
            return super(MSSQLCompiler, self).visit_table(table, **kwargs)

    @_with_legacy_schema_aliasing
    def visit_alias(self, alias, **kw):
        # translate for schema-qualified table aliases
        kw["mssql_aliased"] = alias.original
        return super(MSSQLCompiler, self).visit_alias(alias, **kw)

    @_with_legacy_schema_aliasing
    def visit_column(self, column, add_to_result_map=None, **kw):
        if column.table is not None and (not self.isupdate and not self.isdelete) or self.is_subquery():
            # translate for schema-qualified table aliases
            t = self._schema_aliased_table(column.table)
            if t is not None:
                converted = expression._corresponding_column_or_error(t, column)
                if add_to_result_map is not None:
                    add_to_result_map(column.name, column.name, (column, column.name, column.key), column.type)

                return super(MSSQLCompiler, self).visit_column(converted, **kw)

        return super(MSSQLCompiler, self).visit_column(column, add_to_result_map=add_to_result_map, **kw)

    def _schema_aliased_table(self, table):
        if getattr(table, "schema", None) is not None:
            if self.dialect._warn_schema_aliasing and table.schema.lower() != "information_schema":
                util.warn(
                    "legacy_schema_aliasing flag is defaulted to True; "
                    "some schema-qualified queries may not function "
                    "correctly. Consider setting this flag to False for "
                    "modern SQL Server versions; this flag will default to "
                    "False in version 1.1"
                )

            if table not in self.tablealiases:
                self.tablealiases[table] = table.alias()
            return self.tablealiases[table]
        else:
            return None

    def visit_extract(self, extract, **kw):
        field = self.extract_map.get(extract.field, extract.field)
        return "DATEPART(%s, %s)" % (field, self.process(extract.expr, **kw))

    def visit_savepoint(self, savepoint_stmt):
        return "SAVE TRANSACTION %s" % self.preparer.format_savepoint(savepoint_stmt)

    def visit_rollback_to_savepoint(self, savepoint_stmt):
        return "ROLLBACK TRANSACTION %s" % self.preparer.format_savepoint(savepoint_stmt)

    def visit_binary(self, binary, **kwargs):
        """Move bind parameters to the right-hand side of an operator, where
        possible.

        """
        if (
            isinstance(binary.left, expression.BindParameter)
            and binary.operator == operator.eq
            and not isinstance(binary.right, expression.BindParameter)
        ):
            return self.process(expression.BinaryExpression(binary.right, binary.left, binary.operator), **kwargs)
        return super(MSSQLCompiler, self).visit_binary(binary, **kwargs)

    def returning_clause(self, stmt, returning_cols):

        if self.isinsert or self.isupdate:
            target = stmt.table.alias("inserted")
        else:
            target = stmt.table.alias("deleted")

        adapter = sql_util.ClauseAdapter(target)

        columns = [
            self._label_select_column(None, adapter.traverse(c), True, False, {})
            for c in expression._select_iterables(returning_cols)
        ]

        return "OUTPUT " + ", ".join(columns)

    def get_cte_preamble(self, recursive):
        # SQL Server finds it too inconvenient to accept
        # an entirely optional, SQL standard specified,
        # "RECURSIVE" word with their "WITH",
        # so here we go
        return "WITH"

    def label_select_column(self, select, column, asfrom):
        if isinstance(column, expression.Function):
            return column.label(None)
        else:
            return super(MSSQLCompiler, self).label_select_column(select, column, asfrom)

    def for_update_clause(self, select):
        # "FOR UPDATE" is only allowed on "DECLARE CURSOR" which
        # SQLAlchemy doesn't use
        return ""

    def order_by_clause(self, select, **kw):
        order_by = self.process(select._order_by_clause, **kw)

        # MSSQL only allows ORDER BY in subqueries if there is a LIMIT
        if order_by and (not self.is_subquery() or select._limit):
            return " ORDER BY " + order_by
        else:
            return ""

    def update_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        """Render the UPDATE..FROM clause specific to MSSQL.

        In MSSQL, if the UPDATE statement involves an alias of the table to
        be updated, then the table itself must be added to the FROM list as
        well. Otherwise, it is optional. Here, we add it regardless.

        """
        return "FROM " + ", ".join(
            t._compiler_dispatch(self, asfrom=True, fromhints=from_hints, **kw) for t in [from_table] + extra_froms
        )


class MSSQLStrictCompiler(MSSQLCompiler):

    """A subclass of MSSQLCompiler which disables the usage of bind
    parameters where not allowed natively by MS-SQL.

    A dialect may use this compiler on a platform where native
    binds are used.

    """

    ansi_bind_rules = True

    def visit_in_op_binary(self, binary, operator, **kw):
        kw["literal_binds"] = True
        return "%s IN %s" % (self.process(binary.left, **kw), self.process(binary.right, **kw))

    def visit_notin_op_binary(self, binary, operator, **kw):
        kw["literal_binds"] = True
        return "%s NOT IN %s" % (self.process(binary.left, **kw), self.process(binary.right, **kw))

    def render_literal_value(self, value, type_):
        """
        For date and datetime values, convert to a string
        format acceptable to MSSQL. That seems to be the
        so-called ODBC canonical date format which looks
        like this:

            yyyy-mm-dd hh:mi:ss.mmm(24h)

        For other data types, call the base class implementation.
        """
        # datetime and date are both subclasses of datetime.date
        if issubclass(type(value), datetime.date):
            # SQL Server wants single quotes around the date string.
            return "'" + str(value) + "'"
        else:
            return super(MSSQLStrictCompiler, self).render_literal_value(value, type_)


class MSDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):
        colspec = (
            self.preparer.format_column(column)
            + " "
            + self.dialect.type_compiler.process(column.type, type_expression=column)
        )

        if column.nullable is not None:
            if not column.nullable or column.primary_key or isinstance(column.default, sa_schema.Sequence):
                colspec += " NOT NULL"
            else:
                colspec += " NULL"

        if column.table is None:
            raise exc.CompileError("mssql requires Table-bound columns " "in order to generate DDL")

        # install an IDENTITY Sequence if we either a sequence or an implicit
        # IDENTITY column
        if isinstance(column.default, sa_schema.Sequence):
            if column.default.start == 0:
                start = 0
            else:
                start = column.default.start or 1

            colspec += " IDENTITY(%s,%s)" % (start, column.default.increment or 1)
        elif column is column.table._autoincrement_column:
            colspec += " IDENTITY(1,1)"
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec

    def visit_create_index(self, create, include_schema=False):
        index = create.element
        self._verify_index_table(index)
        preparer = self.preparer
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "

        # handle clustering option
        if index.dialect_options["mssql"]["clustered"]:
            text += "CLUSTERED "

        text += "INDEX %s ON %s (%s)" % (
            self._prepared_index_name(index, include_schema=include_schema),
            preparer.format_table(index.table),
            ", ".join(
                self.sql_compiler.process(expr, include_table=False, literal_binds=True) for expr in index.expressions
            ),
        )

        # handle other included columns
        if index.dialect_options["mssql"]["include"]:
            inclusions = [
                index.table.c[col] if isinstance(col, util.string_types) else col
                for col in index.dialect_options["mssql"]["include"]
            ]

            text += " INCLUDE (%s)" % ", ".join([preparer.quote(c.name) for c in inclusions])

        return text

    def visit_drop_index(self, drop):
        return "\nDROP INDEX %s ON %s" % (
            self._prepared_index_name(drop.element, include_schema=False),
            self.preparer.format_table(drop.element.table),
        )

    def visit_primary_key_constraint(self, constraint):
        if len(constraint) == 0:
            return ""
        text = ""
        if constraint.name is not None:
            text += "CONSTRAINT %s " % self.preparer.format_constraint(constraint)
        text += "PRIMARY KEY "

        if constraint.dialect_options["mssql"]["clustered"]:
            text += "CLUSTERED "

        text += "(%s)" % ", ".join(self.preparer.quote(c.name) for c in constraint)
        text += self.define_constraint_deferrability(constraint)
        return text

    def visit_unique_constraint(self, constraint):
        if len(constraint) == 0:
            return ""
        text = ""
        if constraint.name is not None:
            text += "CONSTRAINT %s " % self.preparer.format_constraint(constraint)
        text += "UNIQUE "

        if constraint.dialect_options["mssql"]["clustered"]:
            text += "CLUSTERED "

        text += "(%s)" % ", ".join(self.preparer.quote(c.name) for c in constraint)
        text += self.define_constraint_deferrability(constraint)
        return text


class MSIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

    def __init__(self, dialect):
        super(MSIdentifierPreparer, self).__init__(dialect, initial_quote='"', final_quote='"')

    def _escape_identifier(self, value):
        return value

    def quote_schema(self, schema, force=None):
        """Prepare a quoted table and schema name."""
        result = ".".join([self.quote(x, force) for x in schema.split(".")])
        return result


def _db_plus_owner_listing(fn):
    def wrap(dialect, connection, schema=None, **kw):
        dbname, owner = _owner_plus_db(dialect, schema)
        return _switch_db(dbname, connection, fn, dialect, connection, dbname, owner, schema, **kw)

    return update_wrapper(wrap, fn)


def _db_plus_owner(fn):
    def wrap(dialect, connection, tablename, schema=None, **kw):
        dbname, owner = _owner_plus_db(dialect, schema)
        return _switch_db(dbname, connection, fn, dialect, connection, tablename, dbname, owner, schema, **kw)

    return update_wrapper(wrap, fn)


def _switch_db(dbname, connection, fn, *arg, **kw):
    if dbname:
        current_db = connection.scalar("select db_name()")
        connection.execute("use %s" % dbname)
    try:
        return fn(*arg, **kw)
    finally:
        if dbname:
            connection.execute("use %s" % current_db)


def _owner_plus_db(dialect, schema):
    if not schema:
        return None, dialect.default_schema_name
    elif "." in schema:
        return schema.split(".", 1)
    else:
        return None, schema


class KineticaBaseDialect(default.DefaultDialect):
    name = "kinetica" #test change
    supports_default_values = True
    supports_empty_insert = False
    execution_ctx_cls = MSExecutionContext
    use_scope_identity = True
    max_identifier_length = 128
    schema_name = ""

    colspecs = {
        sqltypes.DateTime: _MSDateTime,
        sqltypes.Date: _MSDate,
        sqltypes.Time: TIME,
    }

    engine_config_types = default.DefaultDialect.engine_config_types.union(
        [
            ("legacy_schema_aliasing", util.asbool),
        ]
    )

    ischema_names = ischema_names

    supports_native_boolean = False
    supports_unicode_binds = True
    postfetch_lastrowid = True

    server_version_info = ()

    statement_compiler = MSSQLCompiler
    ddl_compiler = MSDDLCompiler
    type_compiler = MSTypeCompiler
    preparer = MSIdentifierPreparer

    construct_arguments = [
        (sa_schema.PrimaryKeyConstraint, {"clustered": False}),
        (sa_schema.UniqueConstraint, {"clustered": False}),
        (sa_schema.Index, {"clustered": False, "include": None}),
    ]

    def __init__(
        self,
        query_timeout=None,
        use_scope_identity=True,
        max_identifier_length=None,
        schema_name="",
        deprecate_large_types=None,
        legacy_schema_aliasing=None,
        **opts
    ):
        self.query_timeout = int(query_timeout or 0)
        self.schema_name = schema_name

        self.use_scope_identity = use_scope_identity
        self.max_identifier_length = int(max_identifier_length or 0) or self.max_identifier_length
        self.deprecate_large_types = deprecate_large_types

        if legacy_schema_aliasing is None:
            self.legacy_schema_aliasing = True
            self._warn_schema_aliasing = True
        else:
            self.legacy_schema_aliasing = legacy_schema_aliasing
            self._warn_schema_aliasing = False

        #super(MSDialect, self).__init__(**opts) #test change

    def do_savepoint(self, connection, name):
        # give the DBAPI a push
        #connection.execute("IF @@TRANCOUNT = 0 BEGIN TRANSACTION")
        #super(MSDialect, self).do_savepoint(connection, name)

    def do_release_savepoint(self, connection, name):
        # SQL Server does not support RELEASE SAVEPOINT
        pass

    def initialize(self, connection):
        #super(MSDialect, self).initialize(connection)
        self._setup_version_attributes()

    def _setup_version_attributes(self): 
        self.implicit_returning = True
        self.supports_multivalues_insert = True
 
    def _get_default_schema_name(self, connection):
        return self.schema_name

       

    @_db_plus_owner
    def has_table(self, connection, tablename, dbname, owner, schema):
        if not hasattr(connection, "connection"):
            connection = connection.contextual_connect()

        cursor = connection.connection.cursor()

        # Use ODBC to get table with matching name
        if cursor.tables(table=tablename).fetchone():
            return True

        return False

    @reflection.cache
    @_db_plus_owner_listing
    def get_schema_names(self, connection, **kw):
        if not hasattr(connection, "connection"):
            connection = connection.contextual_connect()

        cursor = connection.connection.cursor()

        # Array to store extracted table names
        schema_names = []

        # Use ODBC to get list of tables and extract schemas
        for table in cursor.tables():
            if not table.table_cat in schema_names:
                schema_names.append(table.table_schem)

        schema_names.sort()
        return schema_names

    @reflection.cache
    @_db_plus_owner_listing
    def get_table_names(self, connection, dbname, owner, schema, **kw):
        if not hasattr(connection, "connection"):
            connection = connection.contextual_connect()

        cursor = connection.connection.cursor()

        # Array to store extracted table names
        table_names = []

        # Use ODBC to get list of tables and extract names
        for table in cursor.tables():
            # Table name already returned as fully qualified
            table_names.append(table.table_schem + "." + table.table_name)

        table_names.sort()
        return table_names

    @reflection.cache
    @_db_plus_owner_listing
    def get_view_names(self, connection, dbname, owner, schema, **kw):
        return []

    @reflection.cache
    @_db_plus_owner
    def get_indexes(self, connection, tablename, dbname, owner, schema, **kw):
        return []

        

    @reflection.cache
    @_db_plus_owner
    def get_view_definition(self, connection, viewname, dbname, owner, schema, **kw):
        return ""
      

    @reflection.cache
    @_db_plus_owner
    def get_columns(self, connection, tablename, dbname, owner, schema, **kw):
        if not hasattr(connection, "connection"):
            connection = connection.contextual_connect()

        cursor = connection.connection.cursor()

        # Array to store column data
        columns = []

        # Use ODBC to get list of columns for table
        for column in cursor.columns(table=tablename, schema=schema):
            name = column.column_name
            type = column.type_name
            size = column.column_size
            nullable = column.nullable

            if type.startswith(ODBC_TYPE_BYTES):
                type = VARBINARY(length="max")
            elif type.startswith(ODBC_TYPE_DOUBLE):
                type = FLOAT(precision=53)
            elif type.startswith(ODBC_TYPE_DECIMAL):
                type = DECIMAL()
            elif type.startswith(ODBC_TYPE_FLOAT):
                type = FLOAT(precision=24)
            elif type.startswith(ODBC_TYPE_INT):
                type = INTEGER()
            elif type.startswith(ODBC_TYPE_BIGINT):
                type = BIGINT()
            elif type.startswith(ODBC_TYPE_SMALLINT):
                type = SMALLINT()
            elif type.startswith(ODBC_TYPE_TINYINT):
                type = SMALLINT()
            elif type.startswith(ODBC_TYPE_LONG):
                type = BIGINT()
            elif type.startswith(ODBC_TYPE_REAL):
                type = FLOAT(precision=24)
            elif type.startswith(ODBC_TYPE_TYPE_TIMESTAMP):
                type = DATETIME()
            elif type.startswith(ODBC_TYPE_TIMESTAMP):
                type = DATETIME()
            elif type.startswith(ODBC_TYPE_DATETIME):
                type = DATETIME()
            elif type.startswith(ODBC_TYPE_TYPE_DATE):
                type = DATE()
            elif type.startswith(ODBC_TYPE_TYPE_TIME):
                type = TIME()
            elif type.startswith(ODBC_TYPE_DATE):
                type = DATE()
            elif type.startswith(ODBC_TYPE_IPV4):
                type = VARCHAR(collation="SQL_Latin1_General_CP1_CI_AS")
            elif type.startswith(ODBC_TYPE_GEOMETRY):
                type = VARCHAR(collation="SQL_Latin1_General_CP1_CI_AS")
            elif type.startswith(ODBC_TYPE_VARCHAR):
                if size == 1:
                    length = 1
                elif size == 2:
                    length = 2
                elif size == 4:
                    length = 4
                elif size == 8:
                    length = 8
                elif size == 16:
                    length = 16
                elif size == 32:
                    length = 32
                elif size == 64:
                    length = 64
                elif size == 128:
                    length = 128
                elif size == 255:
                    length = 256
                elif size == 256:
                    length = 256
                else:
                    length = -1

                if length != -1:
                    type = VARCHAR(length=length, collation="SQL_Latin1_General_CP1_CI_AS")
                else:
                    type = VARCHAR(collation="SQL_Latin1_General_CP1_CI_AS")
            else:
                util.warn("Did not recognize type '%s' [%s] of column '%s'" % (type, size, name))
                type = sqltypes.NULLTYPE

            if type != sqltypes.NULLTYPE:
                isNullable = False
                if nullable == 1:
                    isNullable = True

                columns.append(
                    {"name": name, "type": type, "nullable": isNullable, "default": None, "autoincrement": False}
                )

        return columns

    @reflection.cache
    @_db_plus_owner
    def get_pk_constraint(self, connection, tablename, dbname, owner, schema, **kw):
        if not hasattr(connection, "connection"):
            connection = connection.contextual_connect()

        cursor = connection.connection.cursor()

        # Array to store pk data
        pkeys = []

        # Use ODBC to get list of primary keys by table
        # for keys in cursor.primaryKeys(tablename):

        return {"constrained_columns": pkeys, "name": "pk"}

    @reflection.cache
    @_db_plus_owner
    def get_foreign_keys(self, connection, tablename, dbname, owner, schema, **kw):
        # TODO: once FK info is exposed via /show/table, read it here
        return []

        