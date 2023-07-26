from datetime import datetime
from dateutil import tz

def datetime_to_str(input, to_tzlocal=False):
    if not isinstance(input, datetime):
        raise ValueError(f"Invalid type: expect 'datetime', got '{type(input).__name__}'")
    if to_tzlocal:
        input = convert_to_tzlocal(input)
    output = input.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return output


def str_to_datetime(input: str, to_tzlocal=False):
    output = datetime.strptime(input, '%Y-%m-%dT%H:%M:%S.%fZ')
    if to_tzlocal:
        output = convert_to_tzlocal(output)
    return output


def convert_to_tzlocal(input):
    if not isinstance(input, datetime):
        raise ValueError(f"Invalid type: expect 'datetime', got '{type(input).__name__}'")
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    return input.replace(tzinfo=from_zone).astimezone(to_zone)
