import re
import psycopg2.extensions as psyext
import psycopg2.extras as extras


def sql_rows(conn, select, params=None):
    # The presence of non-none params in the call to execute causes psycopg2
    # interpolation.   This may or may not be desirable in general.
    if params == None:
        params = []

    with conn.cursor(cursor_factory=extras.NamedTupleCursor) as cursor:
        cursor.execute(select, params)
        rows = list(cursor.fetchall())
    return rows


def sql_1row(conn, select, params=None):
    """
    Note that this function is designed to be always used with tuple unpacking
    for multiple columns and the single value is unpacked in the function.
    Thus the function returns the actual single column value if a single column
    and a tuple otherwise.  While this decision looks awkward at this level, it
    is convenient on the outside.
    """
    # The presence of non-none params in the call to execute causes psycopg2
    # interpolation.   This may or may not be desirable in general.
    if params == None:
        params = []
    # use simple tuple cursor no matter what the connection cursor_factory is.
    cursor = conn.cursor(cursor_factory=psyext.cursor)

    cursor.execute(select, params)
    results = list(cursor.fetchall())
    if len(results) == 0:
        row = (None,) * len(cursor.description)
    elif len(results) == 1:
        row = results[0]
    else:
        raise RuntimeError("Multiple row result in sql_1row")

    cursor.close()
    # This is moderately ugly semantic decision here.  If you don't like it,
    # don't use this function :).
    return row[0] if len(row) == 1 else row


def sql_1object(conn, select, params=None):
    """
    Similarly to :meth:`sql_1row` this function executes an SQL select that is
    expected to return exactly one row.   It returns an object whose
    members are the row attributes named accordingly.
    """
    # The presence of non-none params in the call to execute causes psycopg2
    # interpolation.   This may or may not be desirable in general.
    if params == None:
        params = []

    with conn.cursor(cursor_factory=extras.NamedTupleCursor) as cursor:
        cursor.execute(select, params)
        results = list(cursor.fetchall())
        if len(results) == 0:
            row = None
        elif len(results) == 1:
            row = results[0]
        else:
            raise RuntimeError("Multiple row result in sql_1row")

    return row


def sql_void(conn, sql, params=None):
    """
    Execute an SQL statement.  You must call `conn.commit()` after this
    function for the change to be committed.
    """
    # The presence of non-none params in the call to execute causes psycopg2
    # interpolation.   This may or may not be desirable in general.
    if params == None:
        params = []
    with conn.cursor() as cursor:
        cursor.execute(sql, params)


def sql_tab2(conn, stmt, mogrify_params=None, column_map=None):
    """
    This convenience function executes an SQL statement and returns a standard
    (columns, rows) tuple prepared to be returned from a Yenot REST end-point.
    The column list is prepared from the SQL columns in the result set.  The
    types are deduced from the SQL result types and the columns are refined by
    the column_map.

    :param connection conn: a database connection object
    :param str stmt: SQL statement to be executed (likely with placeholders for substitution)
    :param dict/tuple mogrify_params: tuple or dictionary to substitute in stmt
    :param dict column_map: a dictionary of column names to rtlib column declaration dictionaries
    """
    cursor = conn.cursor(cursor_factory=extras.NamedTupleCursor)
    if mogrify_params != None:
        cursor.execute(stmt, mogrify_params)
    else:
        cursor.execute(stmt)
    columns, rows = _sql_tab2_cursor(cursor, column_map)
    cursor.close()
    return columns, rows


def _sql_tab2_cursor(cursor, column_map=None):
    """
    This function returns the rows from the (presumably psycopg2) cursor in the
    format expected by Yenot clients.  Briefly, this format is a 2-tuple with
    the first element a list of columns and the second element a list of rows
    with values (no attribute names).  The index of the column in the first
    element maps to the index of the value in each row.
    """
    if column_map == None:
        column_map = {}

    rows = cursor.fetchall()
    columns = []
    for pgcol in cursor.description:
        rt = column_map.get(pgcol[0], {})
        pgtype = pgcol.type_code
        if "type" not in rt:
            if pgtype in psyext.DATE.values:
                rt["type"] = "date"
            elif pgtype in psyext.TIME.values + psyext.PYDATETIME.values:
                # Uncertain if this also contains a time-only value
                rt["type"] = "datetime"
            elif pgtype in psyext.INTEGER.values + psyext.LONGINTEGER.values:
                rt["type"] = "integer"
            elif pgtype in psyext.FLOAT.values + psyext.DECIMAL.values:
                rt["type"] = "numeric"
            elif pgtype in psyext.BOOLEAN.values:
                rt["type"] = "boolean"
        if pgtype in psyext.UNICODE.values and pgcol.internal_size > 0:
            rt["max_length"] = pgcol.internal_size
        columns.append((pgcol[0], rt))
    return (columns, rows)


def sanitize_fragment(text):
    """
    >>> sanitize_fragment('asdf')
    '%asdf%'
    >>> sanitize_fragment('a%a')
    '%a%%a%'
    """
    return f"%{text.replace('%', '%%')}%"


def sanitize_prefix(text):
    """
    >>> sanitize_prefix('asdf')
    'asdf%'
    >>> sanitize_prefix('a%a')
    'a%%a%'
    """
    return f"{text.replace('%', '%%')}%"


# See http://blog.lostpropertyhq.com/postgres-full-text-search-is-good-enough/

SENTENCE_PUNCTUATION_RE = re.compile("[.,?!]*$")
ALPHABETIC_RE = re.compile("^[a-zA-Z]*$")
NON_WORDY_RE = re.compile("^[^a-zA-Z0-9]*$")


def sanitize_fts(text):
    """
    This function removes spurious spaces and escapes & characters for
    PostgreSQL full text search.  The concepts should be portable to other SQL.

    >>> sanitize_fts('big ox')
    'big&ox'
    >>> sanitize_fts('joel  mohler')
    'joel&mohler'
    >>> sanitize_fts('at&t phone')
    "'at&t'&phone"
    >>> sanitize_fts("Siobhan O'Henry")
    "Siobhan&'O''Henry'"
    >>> sanitize_fts('PT&C | LWG FORENSIC CONSULTING')
    "'PT&C'&LWG&FORENSIC&CONSULTING"
    >>> sanitize_fts('ON THE ROAD AGAIN!!')
    'ON&THE&ROAD&AGAIN'
    """
    words = text.split(" ")
    # strip trailing punctuation
    words = [SENTENCE_PUNCTUATION_RE.sub("", w) for w in words]
    # omit empty symbols
    words = [w for w in words if w != ""]
    # omit pure punctuation symbols
    words = [w for w in words if not NON_WORDY_RE.match(w)]
    # quote non-alphabetic
    words = [
        (w if ALPHABETIC_RE.match(w) else "'{}'".format(w.replace("'", "''")))
        for w in words
    ]
    # join
    return "&".join(words)
