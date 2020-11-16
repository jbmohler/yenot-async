import re
import copy
import functools
import datetime
import keyword
import base64

IDENTIFIER_RE = re.compile(r"^[^\d\W]\w*\Z", re.UNICODE)
KEYWORD_SET = set(keyword.kwlist)

# This roughly models a Qt QAbstractItemModel, but it has no Qt dependency.
# See apputils.models for the rest of that.


class Unassigned:
    def __repr__(self):
        return "unassigned"


unassigned = Unassigned()


class SlottedRow:
    def __init__(self, *args, **kwargs):
        for k, v in zip(self.__class__.__slots__, args):
            setattr(self, k, v)
        for k, v in kwargs.items():
            setattr(self, k, v)

    def _as_tuple(self):
        return tuple(getattr(self, k, None) for k in self.__class__.__slots__)

    def _as_dict(self):
        return {k: getattr(self, k, None) for k in self.__class__.__slots__}

    def __repr__(self):
        values = [
            f"{k}={repr(getattr(self, k, unassigned))}"
            for k in self.__class__.__slots__
        ]
        return f"{self.__class__.__name__}({', '.join(values)})"


def fixedrecord(name, members, mixin=None):
    """
    This is a namedtuple only better.
    """
    kw_clash = KEYWORD_SET.intersection(members)
    if len(kw_clash) > 0:
        raise RuntimeError(
            "refused identifiers ({}):  fixedrecord identifiers must not be keywords".format(
                ", ".join(kw_clash)
            )
        )
    junk = [m for m in members if None == IDENTIFIER_RE.match(m)]
    if len(junk) > 0:
        raise RuntimeError(
            "refused identifiers ({}):  fixedrecord identifiers must be valid Python variable identifier".format(
                ", ".join(junk)
            )
        )

    Kls1 = type(name, (SlottedRow,), {"__slots__": members})
    if mixin == None:
        return Kls1
    elif isinstance(mixin, (list, tuple)):
        return type(name, (Kls1,) + tuple(mixin), {})
    else:
        return type(name, (Kls1, mixin), {})


class ColumnAction:
    def __init__(self, label, callback, scope="global", defaulted=False, reloads=False):
        self.label = label
        self.callback = callback
        self.scope = scope
        self.defaulted = defaulted
        self.reloads = reloads

    def matches_scope(self, column):
        return self.scope == "global" or column.represents

    def interpolated_label(self, column):
        return self.label.format(header=column.label)


class Column:
    def __init__(
        self,
        attr,
        label,
        checkbox=False,
        check_attr=None,
        editable=False,
        alignment="left",
        formatter=None,
        coerce_edit=None,
        url_factory=None,
        url_key=None,
        url_new_window=False,
        max_length=None,
        is_numeric=False,
        char_width=None,
        represents=False,
        primary_key=False,
        hidden=False,
        widget_factory=None,
        widget_kwargs=None,
        background_attr=None,
        foreground_attr=None,
        sort_proxy=None,
        sort_key=None,
        sort_null=None,
        actions=None,
        add_actions=None,
    ):
        self.attr = attr
        self.label = label
        # TODO:  figure out the difference between represents and primary_key; there is a subtlety with regards to the autoid and human readable name
        self.represents = bool(represents)
        self.primary_key = bool(primary_key)
        self.hidden = bool(hidden)
        self.editable = editable
        self.max_length = max_length
        self.widget_factory = widget_factory
        self.widget_kwargs = {} if widget_kwargs == None else widget_kwargs
        if coerce_edit == None:
            coerce_edit = lambda x: str(x) if x != None else ""
        self.coerce_edit = coerce_edit
        self.checkbox = checkbox
        self.char_width = char_width
        self.check_attr = check_attr
        self.alignment = alignment
        if formatter == None:
            formatter = lambda x: str(x) if x != None else ""
        self.formatter = formatter
        self.is_numeric = is_numeric
        self.sort_proxy = sort_proxy
        self.sort_null = sort_null
        if sort_key == None:
            nkey = "c" if self.sort_null == "last" else "a"
            # null items sort high
            sort_key = lambda x: (nkey, "") if x == None else ("b", x)
        self.sort_key = sort_key
        if actions == None:
            # callable?, templated string, (global, represents)
            actions = [ColumnAction("View &{header}", "__url__", defaulted=True)]
        if add_actions != None:
            actions = list(actions) + add_actions
        self.actions = list(actions)
        self.url_factory = url_factory
        self.url_key = url_key
        self.url_new_window = url_new_window
        self.background_attr = background_attr
        self.foreground_attr = foreground_attr

    def mutate(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        return self


TYPE_DEFINITION_PLUGINS = []


def add_type_definition_plugin(tplug):
    global TYPE_DEFINITION_PLUGINS
    TYPE_DEFINITION_PLUGINS.append(tplug)


def attr_to_label(attr):
    return attr.replace("_", " ").title()


def api_to_model(attr, meta):
    if meta == None:
        meta = {}

    if "label" not in meta:
        meta["label"] = attr_to_label(attr)
    type_ = meta.pop("type", None)
    return field(attr, meta.pop("label"), type_=type_, **meta)


def field(attr, label, editable=False, type_=None, **kwargs):
    global TYPE_DEFINITION_PLUGINS
    meta = {"label": label, "editable": editable}

    meta.update(kwargs)
    for tplug in TYPE_DEFINITION_PLUGINS:
        tplug.polish(attr, type_, meta)

    # TODO:  figure out what this is about
    if (
        type_ == "options"
        and "formatter" not in meta
        and "widget_kwargs" in kwargs
        and "options" in kwargs["widget_kwargs"]
    ):
        d = {v: k for k, v in kwargs["widget_kwargs"]["options"]}
        meta["formatter"] = lambda v, d=d: d.get(v, "")

    c = Column(attr, **meta)
    c.type_ = type_
    return c


def type_included(type_):
    if type_ == None:
        return True
    elif type_ in ["text_color"]:
        return False
    elif "." in type_ and type_.split(".", 1)[1] == "surrogate":
        return False
    return True


def parse_columns(column_list):
    # wish to mutate -- work on a copy
    column_list = copy.deepcopy(column_list)

    def column_included(attr, meta):
        if meta == None:
            return True
        return type_included(meta.get("type", None))

    return [api_to_model(*x) for x in column_list if column_included(*x)]


def parse_columns_full(column_list):
    # TODO:  this is an obnoxious minor variant of parse_columns
    # wish to mutate -- work on a copy
    column_list = copy.deepcopy(column_list)
    return [api_to_model(*x) for x in column_list]


def parse_datetime(v):
    if v == None:
        return v
    try:
        return datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        pass
    try:
        return datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        pass
    raise ValueError(f"could not parse {v} as datetime")


def parse_date(s):
    """
    >>> parse_date('2014-12-13')
    datetime.date(2014, 12, 13)
    >>> parse_date(None) == None
    True
    """
    if s == None:
        return None
    if isinstance(s, datetime.date):
        return s
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise ValueError(f"invalid date string {s}")
    return datetime.date(int(s[:4]), int(s[5:7]), int(s[8:10]))


def parse_bool(v):
    if isinstance(v, str):
        v = v.lower()
    if v in [True, 1, "true", "yes"]:
        return True
    if v in [False, 0, "false", "no"]:
        return False
    raise ValueError(f"unacceptable bool import:  {v}")


def as_python(columns, to_localtime=True):
    def coerce(converters, _tuple):
        return tuple(t(v) for t, v in zip(converters, _tuple))

    identity = lambda v: v

    def column_converter(attr, meta):
        if meta == None or meta.get("type", None) == None:
            return identity
        elif meta["type"] == "boolean":
            return lambda v: False if v == None else v
        elif meta["type"] == "binary":
            return lambda v: None if v == None else base64.b64decode(v.encode("ascii"))
        elif meta["type"] == "date":
            return lambda v: parse_date(v) if v != None else None
        elif meta["type"] == "datetime":
            if to_localtime and not meta.get("widget_kwargs", {}).get(
                "localtime", False
            ):
                offset = (
                    datetime.datetime.utcnow() - datetime.datetime.now()
                ).total_seconds() / 3600
                return (
                    lambda v, offset=offset: parse_datetime(v)
                    - datetime.timedelta(hours=offset)
                    if v != None
                    else v
                )
            else:
                return parse_datetime
        else:
            return identity

    converters = [column_converter(*x) for x in columns]
    return functools.partial(coerce, converters)


def as_client(columns, to_localtime=True):
    def coerce(converters, _tuple):
        return tuple(t(v) for t, v in zip(converters, _tuple))

    identity = lambda v: v

    def column_converter(attr, meta):
        if meta == None or meta.get("type", None) == None:
            return identity
        elif meta["type"] == "datetime":
            if to_localtime and not meta.get("widget_kwargs", {}).get(
                "localtime", False
            ):
                offset = (
                    datetime.datetime.utcnow() - datetime.datetime.now()
                ).total_seconds() / 3600
                return (
                    lambda v, offset=offset: v - datetime.timedelta(hours=offset)
                    if v != None
                    else v
                )
            else:
                return identity
        else:
            return identity

    converters = [column_converter(*x) for x in columns]
    return functools.partial(coerce, converters)
