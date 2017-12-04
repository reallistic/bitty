import pytz
import time

from datetime import datetime


def get_usec_timestamp():
    return int(time.time() * 1e6)


def get_iso8601():
    usec = get_usec_timestamp()
    return iso8601_from_usec(usec)


def iso8601_from_usec(usec):
    """Return a microsecond timestamp as an ISO8601 string
    Note that we prefer the UTC format using 'Z' defined by W3 over the
    equally valid +00:00 used by default in Python.
    """
    return iso8601_from_secs(usec / 1e6)


def iso8601_from_secs(secs):
    """Return a ISO8601 string from a millisecond timestamp
    Note that we prefer the UTC format using 'Z' defined by W3 over the
    equally valid +00:00 used by default in Python.
    """
    return datetime.fromtimestamp(secs, pytz.utc).isoformat()[:-6] + 'Z'
