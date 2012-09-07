import datetime
import decimal
import math
from time import localtime

from psycopg2ct._impl import libpq

from ctypes import *
import struct
from pytz import utc

string_types = {}

binary_types = {}

# XXX How do we determine this?
integer_datetimes = False
integer_datetimes = True

class Type(object):
    def __init__(self, name, values, caster=None, py_caster=None):
        self.name = name
        self.values = values
        self.caster = caster
        self.py_caster = py_caster

    def __eq__(self, other):
        return other in self.values

    def cast(self, value, cursor, length=None):
        if self.py_caster is not None:
            return self.py_caster(value, cursor)
        return self.caster(value, length, cursor)


def register_type(type_obj, scope=None):
    typecasts = binary_types
    if scope:
        from psycopg2ct._impl.connection import Connection
        from psycopg2ct._impl.cursor import Cursor

        if isinstance(scope, Connection):
            typecasts = scope._typecasts
        elif isinstance(scope, Cursor):
            typecasts = scope._typecasts
        else:
            typecasts = None

    for value in type_obj.values:
        typecasts[value] = type_obj


def new_type(values, name, castobj):
    return Type(name, values, py_caster=castobj)


def new_array_type(values, name, baseobj):
    caster = parse_array(baseobj)
    return Type(name, values, caster=caster)


def typecast(caster, value, length, cursor):
    return caster.cast(value, cursor, length)


def parse_unknown(value, length, cursor):
    # XXX
    return None

def parse_string(value, length, cursor):
    return cast(value, c_char_p).value

def parse_integer(value, length, cursor):
    if length == -1:
        return None
    if length == 2:
        return struct.unpack('!h', value[:length])[0]
    if length == 4:
        return struct.unpack('!i', value[:length])[0]
    if length == 8:
        return struct.unpack("!q", value[:length])[0]
    raise ValueError('Unexpected length for INT type: %r' % length)

def parse_float(value, length, cursor):
    if length == -1:
        return None
    if length == 4:
        return struct.unpack('!q', value[:length])[0]
    if length == 8:
        return struct.unpack('!d', value[:length])[0]
    raise ValueError('Unexpected length for FLOAT type: %r' % length)

def parse_decimal(value, length, cursor):
    if length == -1:
        return None
    num_digits, weight, sign, dscale = struct.unpack("!hhhh", value[:8])
    digits = struct.unpack("!" + ("h" * num_digits), value[8:length])
    weight = decimal.Decimal(weight)
    retval = decimal.Decimal(0)
    for d in digits:
        d = decimal.Decimal(d)
        retval += d * (10000 ** weight)
        weight -= 1
    if sign:
        retval *= -1
    return retval.quantize(decimal.Decimal(10) ** -dscale)

def parse_binary(value, length, cursor):
    return buffer(value[:length])

def parse_boolean(value, length, cursor):
    return value[0] == '\x01'

def parse_unicode(value, length, cursor):
    """Decode the given value with the connection encoding"""
    return cast(value, c_char_p).value.decode(cursor._conn._py_enc)

def parse_date(value, length, cursor):
    # Stored as days since 2000-1-1
    val = struct.unpack('!i', value[:length])[0]
    return datetime.date(2000,1,1) + datetime.timedelta(days=val)

def parse_timestamp(value, length, cursor):
    assert length == 8, 'Invalid timestamp length: %d (%r)' % (length, value[:length])
    if integer_datetimes:
        # data is 64-bit integer representing milliseconds since 2000-01-01
        val = struct.unpack('!q', value[:length])[0]
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(microseconds = val)
    else:
        # data is double-precision float representing seconds since 2000-01-01
        val = struct.unpack('!d', value[:length])[0]
        return datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds = val)

def parse_timestamptz(value, length, cursor):
    # XXX For backward compatibility, we return a naive value
    return parse_timestamp(value, length, cursor)
    #return parse_timestamp(value, length, cursor).replace(tzinfo=utc)

def parse_time(value, length, cursor):
    return parse_debug(value, length, cursor)

def parse_interval(value, length, cursor):
    if integer_datetimes:
        microseconds, days, months = struct.unpack("!qii", value[:length])
        seconds=0
    else:
        seconds, days, months = struct.unpack("!dii", value[:length])
        microseconds = 0
    return datetime.timedelta(days=(months*30)+days, seconds=seconds, microseconds=microseconds)

def _default_type(name, oids, caster):
    """Shortcut to register internal types"""
    type_obj = Type(name, oids, caster)
    register_type(type_obj)
    return type_obj


def parse_record(value, length, cursor):
    # This is why we're here, folks...
    data = value[:length]
    result = []
    # XXX Field names?
    nfields = struct.unpack('!i', data[:4])
    data = data[4:]
    while len(data):
        # Get the OID and length
        oid, olen = struct.unpack('!ii', data[:8])
        data = data[8:]
        # Remove value date
        val = data[:olen]
        data = data[olen:]
        # Convert
        result.append(
            typecast(binary_types[oid], val, olen, cursor)
        )
    return tuple(result)

def parse_inet(value, length, cursor):
    ip_family, ip_bits, is_cidr, dlen = struct.unpack('bbb', value[:3])
    addr = struct.unpack('b' * dlen, value[3:length])
    return ''

def parse_array(obj):

    def inner(value, length, cursor):
        # Flags only contains 'has null' flag
        ndim, flags, oid = struct.unpack('!iii', value[:12])
        # Dimension offset
        offset = 12
        # Data offset
        doffset = offset + ndim * 8
        data = []
        for x in range(ndim):
            dim = struct.unpack('!ii', value[offset:offset+8])
            offset += 8
            vals = []
            for y in range(dim[1]+1):
                l = struct.unpack('!i', value[doffset:doffset+4])[0]
                doffset += 4
                vals.append(
                    obj.cast(value[doffset:doffset+l], cursor, l)
                )
                doffset += l
            data.append(vals)

        if ndim == 1:
            return data[0]
        return data
    return inner

def parse_void(value, length, cursor):
    return None

# XXX
def parse_debug(value, length, cursor):
    return None

# DB API 2.0 types
BINARY = _default_type('BINARY', [17], parse_binary)
#DATETIME = _default_type('DATETIME',  [1114, 1184, 704, 1186], parse_datetime)
DATETIME = _default_type('DATETIME', [1115], parse_timestamp)
# XXX This overlaps with INTEGER, FLOAT, and DECIMAL -- also, there is no OID 33
NUMBER = _default_type('NUMBER', [20, 33, 21, 701, 700, 1700], parse_float)
ROWID = _default_type('ROWID', [26], parse_integer)
STRING = _default_type('STRING', [19, 18, 25, 1042, 1043], parse_string)

# Register the basic typecasters
BOOLEAN = _default_type('BOOLEAN', [16], parse_boolean)
DATE = _default_type('DATE', [1082], parse_date)
DECIMAL = _default_type('DECIMAL', [1700], parse_decimal)
FLOAT = _default_type('FLOAT', [701, 700], parse_float)
INTEGER = _default_type('INTEGER', [20, 21, 23], parse_integer)
INTERVAL = _default_type('INTERVAL', [1186], parse_interval)
LONGINTEGER = INTEGER
#TIME = _default_type('TIME', [1083, 1266], parse_time)
TIME = _default_type('TIME', [], parse_time)
UNKNOWN = _default_type('UNKNOWN', [705], parse_unknown)


TIMESTAMPTZ = _default_type('TIMESTAMPTZ', [1184], parse_timestamptz)
RECORD = _default_type('RECORD', [2249], parse_record)
INET = _default_type('INET', [869], parse_inet)
VOID = _default_type('VOID', [2278], parse_void)

# Array types
BINARYARRAY = _default_type(
    'BINARYARRAY', [1001], parse_array(BINARY))
BOOLEANARRAY = _default_type(
    'BOOLEANARRAY', [1000], parse_array(BOOLEAN))
DATEARRAY = _default_type(
    'DATEARRAY', [1182], parse_array(DATE))
DATETIMEARRAY = _default_type(
    'DATETIMEARRAY', [1115, 1185], parse_array(DATETIME))
DECIMALARRAY = _default_type(
    'DECIMALARRAY', [1231], parse_array(DECIMAL))
FLOATARRAY = _default_type(
    'FLOATARRAY', [1017, 1021, 1022], parse_array(FLOAT))
INTEGERARRAY = _default_type(
    'INTEGERARRAY', [1005, 1006, 1007], parse_array(INTEGER))
INTERVALARRAY = _default_type(
    'INTERVALARRAY', [1187], parse_array(INTERVAL))
LONGINTEGERARRAY = _default_type(
    'LONGINTEGERARRAY', [1016], parse_array(LONGINTEGER))
ROWIDARRAY = _default_type(
    'ROWIDARRAY', [1013, 1028], parse_array(ROWID))
STRINGARRAY = _default_type(
    'STRINGARRAY', [1002, 1003, 1009, 1014, 1015], parse_array(STRING))
TIMEARRAY = _default_type(
    'TIMEARRAY', [1183, 1270], parse_array(TIME))

UNICODE = Type('UNICODE', [19, 18, 25, 1042, 1043], parse_unicode)
UNICODEARRAY = Type('UNICODEARRAY', [1002, 1003, 1009, 1014, 1015],
    parse_array(UNICODE))
