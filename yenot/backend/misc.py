import json
import collections
import rtlib
from bottle import request
import psycopg2.extras as extras
from . import sqlwrite


class UserError(Exception):
    def __init__(self, key, msg):
        super(UserError, self).__init__(msg)
        self.key = key


def write_event_entry(conn, ltype, ldescr, ldata):
    ins = """
insert into yenotsys.eventlog (logtype, logtime, descr, logdata)
values (%(lt)s, current_timestamp, %(ld)s, %(lj)s)
returning id, logtype, logtime;"""
    dumps = lambda x: json.dumps(x, cls=rtlib.DateTimeEncoder)
    with conn.cursor(cursor_factory=extras.NamedTupleCursor) as cursor:
        cursor.execute(
            ins, {"lt": ltype, "ld": ldescr, "lj": extras.Json(ldata, dumps=dumps)}
        )
        row = list(cursor.fetchall())[0]
    return row.id, row.logtype, row.logtime


def tab2_columns_transform(columns, insert=None, remove=None, column_map=None):
    """
    This function transforms a standard tab2 column list by inserting a
    removing specified columns.
    """

    if insert == None:
        inserts = {}
    else:
        inserts = {x: y for x, *y in insert}
    remove = [] if remove == None else remove
    column_map = {} if column_map == None else column_map

    # Make some sanity checks about inserts and removes.
    assert (
        len(set(inserts.keys()).intersection(remove)) == 0
    ), "insertion points cannot overlap with removals"
    # TODO not sure if I want this next assert
    assert (
        len(set(remove).difference([c for c, _ in columns])) == 0
    ), "some removals do not exist"
    assert (
        len(set(inserts.keys()).difference([c for c, _ in columns])) == 0
    ), "insertion points not all found"

    newcols = []
    for attr, meta in columns:
        if attr in remove:
            continue
        newcols.append((attr, meta))
        if attr in inserts:
            for a2 in inserts[attr]:
                newcols.append((a2, column_map.get(a2, None)))
    return newcols


def tab2_rows_transform(colrows, columns_target, transform):
    """
    Follow up a call to :meth:`tab2_columns_transform` with this
    function to iterate through the rows a standard tab2 table and
    assign new columns.

    :param colrows: initial tab2 2-tuple of columns & rows
    :param columns_target: new column structure -- like a result of
        :meth:`tab2_columns_transform`
    :param transform: callable taking two parameters -- (oldrow, row);
        this is called once per row in the `colrows` parameter.
    """
    source_attrs = [a for a, _ in colrows[0]]
    target_attrs = [a for a, _ in columns_target]
    overlap = set(source_attrs).intersection(target_attrs)

    RecordType = rtlib.fixedrecord("RecordType", target_attrs)
    RowType = collections.namedtuple("RowType", target_attrs)

    rows = []
    for oldrow in colrows[1]:
        assign = {a: getattr(oldrow, a) for a in overlap}
        row = RecordType(**assign)
        transform(oldrow, row)
        rows.append(RowType(**row._as_dict()))
    return rows


def tab2_rows_default(columns, indices, default):
    """
    This is similar to :meth:`tab2_rows_transform`, but this function is
    used to construct brand new rows from a column structure and some
    indices (which have arbitrary structure -- or none at all).

    :param columns: column structure in standard tab2 table format
    :param indices: list of identifiers or indices to the rows to construct
    :param transform: callable taking two parameters -- (index, row);
        this is called once per element of the `indices` parameter
    """
    target_attrs = [a for a, _ in columns]
    RecordType = rtlib.fixedrecord("RecordType", target_attrs)
    RowType = collections.namedtuple("RowType", target_attrs)

    rows = []
    for index in indices:
        row = RecordType()
        default(index, row)
        rows.append(RowType(**row._as_dict()))
    return rows


# rtlib server incoming utils


def table_from_tab2(
    name, required=None, amendments=None, options=None, allow_extra=False
):
    try:
        return InboundTable.from_file(
            request.files[name].file,
            encoding="utf8",
            required=required,
            amendments=amendments,
            options=options,
            allow_extra=allow_extra,
        )
    except RuntimeError as e:
        raise UserError(
            "invalid-collection",
            f'Post file "{name}" contains incorrect data.  {str(e)}',
        )


class InboundTable:
    def __init__(self, columns, rows):
        self.rows = rows[:]
        self.columns = columns

    @classmethod
    def from_file(
        cls,
        file,
        encoding="utf8",
        required=None,
        amendments=None,
        options=None,
        allow_extra=False,
    ):
        payload = json.loads(file.read().decode(encoding))
        keys, fields, rows = payload
        clfields = list(fields)
        allowed = set(options) if options != None else set()
        if required == None:
            required = []
        if required != None:
            allowed = allowed.union(required)
        if amendments != None:
            allowed = allowed.union(amendments)

        if not allow_extra and not set(fields).issubset(allowed):
            raise RuntimeError(
                f"Extra fields given:  {' '.join(set(fields).difference(allowed))}"
            )
        if not set(required).issubset(fields):
            raise RuntimeError(
                "Required fields not given:  {}".format(
                    " ".join(set(required).difference(fields))
                )
            )
        if amendments != None:
            clfields += set(amendments).difference(fields)

        dr = rtlib.fixedrecord("DataRow", clfields)
        rows = [dr(**dict(zip(fields, r))) for r in rows]
        self = cls([(c, None) for c in clfields], rows)
        self.DataRow = dr
        self.deleted_keys = keys.get("deleted", [])

        return self

    def as_cte(self, conn, cte, columns=None, column_types=None):
        if not columns:
            columns = self.DataRow.__slots__

        result_template = """\
/*NAME*/(/*COLUMNS*/) as (
    values/*REPRESENTED*/
)"""

        with conn.cursor() as cursor:
            mogrifications = sqlwrite.mogrify_values(
                cursor, self.rows, columns, column_types
            )

        return (
            result_template.replace("/*REPRESENTED*/", mogrifications)
            .replace("/*COLUMNS*/", ", ".join(columns))
            .replace("/*NAME*/", cte)
        )
