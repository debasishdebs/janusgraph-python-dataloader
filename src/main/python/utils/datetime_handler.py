import re
from dateutil.parser import parse
import datetime


def extract_datetime(line):
    pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
    regex = re.compile(pattern)
    try:
        times = regex.findall(line)[0]
        dt = parse(times)
        return dt
    except IndexError:
        try:
            dt_str = line.split("SSTECH_")[0].strip()
            dt = parse(dt_str)
            return dt
        except:
            print(line)
            return False


def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return str(x)
    raise TypeError("Unknown type")
