import io
import json
import datetime
import decimal

# other places as well, but this is canonical and the others should be swallowed


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        if isinstance(o, datetime.date):
            return o.isoformat()
        if isinstance(o, datetime.time):
            return o.isoformat()
        if isinstance(o, decimal.Decimal):
            return float(o)

        return json.JSONEncoder.default(self, o)


def serialize(thing, pprint=False):
    if pprint:
        return json.dumps(thing, cls=DateTimeEncoder, indent=4)
    else:
        return json.dumps(thing, cls=DateTimeEncoder)


def to_json(thing):
    return io.BytesIO(serialize(thing).encode("utf8"))
