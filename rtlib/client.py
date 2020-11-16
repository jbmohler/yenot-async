import contextlib
from . import reportcore
from . import serialization


def simple_table(columns, column_map=None):
    if column_map == None:
        column_map = {}
    return ClientTable([(c, column_map.get(c, None)) for c in columns], [])


class ClientTable:
    """
    Tabular API from a Yenot serialized table structure with rich type
    information.
    """

    def __init__(self, columns, rows, mixin=None, to_localtime=True):
        self.to_localtime = to_localtime
        f = self.row_factory(columns, mixin=mixin)
        self.rows = [f(x) for x in rows]

        # initialize pkey for deletion
        self.columns = reportcore.parse_columns(columns)
        self.columns_full = reportcore.parse_columns_full(columns)
        self.DataRow.model_columns = {c.attr: c for c in self.columns}

        self.deleted_rows = []

    def converter(self, row_field_list):
        return reportcore.as_python(row_field_list, to_localtime=self.to_localtime)

    def row_factory(self, row_field_list, mixin):
        self.DataRow = reportcore.fixedrecord(
            "DataRow", [r[0] for r in row_field_list], mixin=mixin
        )
        to_python = self.converter(row_field_list)

        def init_bare(r):
            nonlocal to_python, self
            return self.DataRow(*to_python(r))

        def init_custom(r):
            nonlocal to_python, self
            x = self.DataRow(*to_python(r))
            x._rtlib_init_()
            return x

        return init_custom if hasattr(self.DataRow, "_rtlib_init_") else init_bare

    @contextlib.contextmanager
    def adding_row(self):
        row = self.candidate_row()
        yield row
        self.rows.append(row)
        if hasattr(row, "_row_added_"):
            row._row_added_()

    def candidate_row(self):
        newself = self.DataRow.__new__(self.DataRow)
        try:
            newself._init_block = True
            newself.__init__(**{a: None for a in self.DataRow.__slots__})
            if hasattr(newself, "_init_candidate_"):
                newself._init_candidate_()
            return newself
        finally:
            newself._init_block = False

    def as_writable(
        self, exclusions=None, inclusions=None, extensions=None, getter=None
    ):
        assert exclusions == None or inclusions == None

        if (
            exclusions == None
            and inclusions == None
            and extensions == None
            and getter == None
        ):
            attrs = self.DataRow.__slots__
            slimrows = [r._as_tuple() for r in self.rows]
        else:
            if inclusions != None:
                attrs = list(inclusions)
            elif exclusions != None:
                attrs = [a for a in self.DataRow.__slots__ if a not in exclusions]
            else:
                attrs = list(self.DataRow.__slots__)
            if extensions != None:
                attrs += list(extensions)

            getter = getter if getter != None else getattr
            slimrows = []
            for r in self.rows:
                slim = [getter(r, a) for a in attrs]
                slimrows.append(slim)

        keys = {}
        if len(self.deleted_rows):
            if len(self.pkey) == 0:
                raise RuntimeError(
                    "no primary key declared; needed for deleted row set"
                )
            pfunc = lambda row: [getattr(row, p1) for p1 in self.pkey]
            keys["deleted"] = [pfunc(row) for row in self.deleted_rows]
        return (keys, attrs, slimrows)

    def as_http_post_file(self, *args, **kwargs):
        tab3 = self.as_writable(*args, **kwargs)
        return serialization.to_json(tab3)

    def as_tab2(self, column_map=None):
        """
        This function is serializing function somewhat like as_http_post_file.
        Perhaps they should be more related.
        """
        # TODO: return meta constructed from columns?
        if column_map == None:
            column_map = {}
        columns = [(c, column_map.get(c, None)) for c in self.DataRow.__slots__]
        rows = [r._as_tuple() for r in self.rows]
        return columns, rows


class UnparsingClientTable(ClientTable):
    """
    This class reconstructs the ClientTable object model from rtlib table
    2-tuple which has not passed through JSON serialization.  A principle
    difference is that dates handled as their python native datetime types and
    not reconstructed from strings.  The general issue is that JSON has less
    native types than Python.
    """

    def converter(self, row_field_list):
        return reportcore.as_client(row_field_list, to_localtime=self.to_localtime)
