# coding=utf-8
import platform
from datetime import datetime, timedelta, timezone


def str2datetime(time_str: str, fmt: str = "%Y-%m-%d-%H") -> datetime:
    return datetime.strptime(time_str, fmt)


def fmt_time_tick(tick_now):
    system = platform.system().lower()
    if system == "linux":
        tick_now = tick_now.strftime("%Y-%m-%d-%-H")  # Linux
    elif system == "windows":
        tick_now = tick_now.strftime("%Y-%m-%d-%#H")  # Windows
    else:
        tick_now = "{dt.year}-{dt.month:02d}-{dt.day:02d}-{dt.hour}".format(dt=tick_now)
    return tick_now


def utc_time2local_time(text: str, fmt: str) -> datetime:
    time_data = datetime.strptime(text, fmt)
    time_data = time_data.replace(tzinfo=timezone.utc)
    time_data = time_data.astimezone()
    return time_data
