import re
import contextlib
from . import sqlread


PRIM_KEY_SELECT = """
select array_agg(kc.column_name::text)
from  
    information_schema.table_constraints tc,  
    information_schema.key_column_usage kc  
where 
    tc.constraint_type = 'PRIMARY KEY' 
    and kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
    and kc.constraint_name = tc.constraint_name
    and tc.table_schema = %(sname)s
    and tc.table_name = %(tname)s
"""

COL_TYPE_SELECT = """
select tables.table_name, columns.column_name, columns.is_nullable, 
	columns.data_type, columns.character_maximum_length, 
	columns.numeric_precision, columns.numeric_precision_radix, columns.numeric_scale
from information_schema.tables
join information_schema.columns on columns.table_name=tables.table_name
where tables.table_schema=%(sname)s and tables.table_name=%(tname)s and tables.table_type='BASE TABLE'"""


class WriteChunk:
    def __init__(self, conn):
        self.conn = conn

    @staticmethod
    def _split_table_name(tname):
        if tname.find(".") >= 0:
            sx, tx = tname.split(".")
        else:
            sx, tx = "public", tname

        if None == re.match("[a-zA-Z_][a-z0-9A-Z_]*", sx):
            raise RuntimeError(
                f'invalid schema name "{sx}" (as determined by regex only)'
            )
        if None == re.match("[a-zA-Z_][a-z0-9A-Z_]*", tx):
            raise RuntimeError(
                f'invalid table name "{tx}" (as determined by regex only)'
            )

        return sx, tx

    def upsert_rows(self, tname, table):
        sx, tx = WriteChunk._split_table_name(tname)

        if not hasattr(table, "deleted_keys"):
            table.deleted_keys = []

        tosave = set(table.DataRow.__slots__)

        cols = sqlread.sql_rows(self.conn, COL_TYPE_SELECT, {"sname": sx, "tname": tx})
        coltypes = {}
        for row in cols:
            if row.column_name in tosave:
                if row.data_type in ("character", "character varying", "text"):
                    # no casting necessary
                    pass
                elif row.data_type == "numeric":
                    coltypes[row.column_name] = "numeric({}, {})".format(
                        row.numeric_precision, row.numeric_scale
                    )
                elif row.data_type in (
                    "date",
                    "boolean",
                    "json",
                    "integer",
                    "smallint",
                    "uuid",
                ):
                    # bit of a catch-all
                    coltypes[row.column_name] = row.data_type
                else:
                    # untracked for now
                    pass

        mog = TableSaveMogrification()
        mog.primary_key = sqlread.sql_1row(
            self.conn, PRIM_KEY_SELECT, {"sname": sx, "tname": tx}
        )
        mog.table = tname
        mog.column_types = coltypes
        mog.persist(self.conn, table)

    def delete_rows(self, tname, table):
        sx, tx = WriteChunk._split_table_name(tname)

        keys = sqlread.sql_1row(self.conn, PRIM_KEY_SELECT, {"sname": sx, "tname": tx})
        if list(sorted(keys)) != list(sorted(table.DataRow.__slots__)):
            raise RuntimeError("primary key must be exactly represented")

        mog = TableSaveMogrification()
        values = mog.as_values(self.conn, table, table.DataRow.__slots__)
        c = ", ".join(table.DataRow.__slots__)

        delete_sql = """delete from {t} where ({columns}) in ({v})"""
        with self.conn.cursor() as cursor:
            cursor.execute(delete_sql.format(t=tname, columns=c, v=values))

    def insert_rows(self, tname, table):
        sx, tx = WriteChunk._split_table_name(tname)

        insert_sql = """insert into {t} ({columns}) {v}"""

        mog = TableSaveMogrification()
        values = mog.as_values(self.conn, table, table.DataRow.__slots__)
        c = ", ".join(table.DataRow.__slots__)

        with self.conn.cursor() as cursor:
            cursor.execute(insert_sql.format(t=tname, columns=c, v=values))


@contextlib.contextmanager
def writeblock(conn):
    yield WriteChunk(conn)


def _mogrify_values(cursor, rows, row2dict, columns, types):
    if isinstance(types, dict):
        types = [types.get(cname, None) for cname in columns]
    elif types == None:
        types = [None] * len(columns)
    assert len(types) == len(columns)

    qualnames = [
        f"%({cname})s::{t}" if t != None else f"%({cname})s"
        for cname, t in zip(columns, types)
    ]
    fragment = f"({', '.join(qualnames)})"
    mogrifications = [cursor.mogrify(fragment, row2dict(r)) for r in rows]

    return ",\n\t".join([x.decode(cursor.connection.encoding) for x in mogrifications])


def mogrify_values(cursor, rows, columns, types=None):
    return _mogrify_values(cursor, rows, lambda r: r._as_dict(), columns, types)


def mogrify_values_anon(cursor, rows, columns, types=None):
    return _mogrify_values(
        cursor, rows, lambda r: dict(zip(columns, r)), columns, types
    )


class TableSaveMogrification:
    """
    Consider starting the PG transaction block with::

        set transaction isolation level serializable;
        set constraints all deferred;
    """

    def __init__(self):
        self.table = None
        self.primary_key = None
        self.column_types = None

    def as_values(self, conn, table, columns):
        result_template = """\
values/*REPRESENTED*/
"""

        with conn.cursor() as cursor:
            mogrifications = mogrify_values(
                cursor, table.rows, columns, self.column_types
            )

        return result_template.replace("/*REPRESENTED*/", mogrifications)

    def persist(self, conn, table):
        collist = table.DataRow.__slots__

        if isinstance(self.primary_key, str):
            pkey = [self.primary_key]
        else:
            pkey = self.primary_key

        cols_no_pk = [c for c in collist if c not in pkey]

        colnames = ", ".join(['"{0}"'.format(c) for c in collist])
        colnames_no_pk = ", ".join(['"{0}"'.format(c) for c in cols_no_pk])
        staging_no_pk = ", ".join(['staging."{0}"'.format(c) for c in cols_no_pk])
        colassign = ", ".join(['"{0}"=staging."{0}"'.format(c) for c in cols_no_pk])

        interpolations = {
            "fqtn": self.table,
            "tn": self.table.rsplit(".", 1)[-1],
            "colnames": colnames,
            "colassign": colassign,
            "colnames_no_pk": colnames_no_pk,
            "staging_no_pk": staging_no_pk,
        }

        m1 = ["{tn}.{pk}=staging.{pk}".format(pk=pk, **interpolations) for pk in pkey]
        pkey_match = " and ".join(m1)
        m1 = ["{tn}.{pk} is null".format(pk=pk, **interpolations) for pk in pkey]
        pkey_null = " and ".join(m1)

        interpolations["pkm"] = pkey_match
        interpolations["pkn"] = pkey_null

        insert = """
with staging({colnames}) as (
    values/*REPRESENTED*/
)
insert into {fqtn} ({colnames})
(
    select staging.*
    from staging
    left outer join {fqtn} on {pkm}
    where {pkn}
)""".format(
            **interpolations
        )

        insert2 = """
with staging({colnames}) as (
    values/*REPRESENTED*/
)
insert into {fqtn} ({colnames_no_pk})
(
    select {staging_no_pk}
    from staging
    left outer join {fqtn} on {pkm}
    where {pkn}
)""".format(
            **interpolations
        )

        update = """
with staging({colnames}) as (
    values/*REPRESENTED*/
)
update {fqtn} set {colassign} 
from staging
where {pkm}""".format(
            **interpolations
        )

        delete = """
with staging({pknames}) as (
    values/*REPRESENTED*/
)
delete from {fqtn} where ({pknames}) in (select * from staging)""".format(
            pknames=",".join(pkey), **interpolations
        )

        with conn.cursor() as cursor:
            # Delete first since other rows may induce duplicates ... although
            # this raises questions about whether this just moves problems
            # around.  Hence we recommend "set constraints all deferred".
            if len(table.deleted_keys) > 0:
                mogrifications = mogrify_values_anon(
                    cursor, table.deleted_keys, pkey, self.column_types
                )
                my_delete = delete.replace("/*REPRESENTED*/", mogrifications)
                cursor.execute(my_delete, {"keys": tuple(table.deleted_keys)})

            if len(pkey) == 1:
                need_defaulting = lambda row: None in [getattr(row, p) for p in pkey]
                rows1 = [row for row in table.rows if not need_defaulting(row)]
                rows2 = [row for row in table.rows if need_defaulting(row)]
            else:
                # defaulting is not supported on composite primary key
                rows1 = table.rows
                rows2 = []
            if len(rows1) > 0:
                mogrifications = mogrify_values(
                    cursor, rows1, collist, self.column_types
                )

                # TODO: consider using upsert
                if len(cols_no_pk) > 0:
                    # update
                    my_update = update.replace("/*REPRESENTED*/", mogrifications)
                    cursor.execute(my_update)
                # insert
                my_insert = insert.replace("/*REPRESENTED*/", mogrifications)
                cursor.execute(my_insert)

            if len(rows2) > 0:
                mogrifications = mogrify_values(
                    cursor, rows2, collist, self.column_types
                )
                # insert
                my_insert = insert2.replace("/*REPRESENTED*/", mogrifications)
                cursor.execute(my_insert)
