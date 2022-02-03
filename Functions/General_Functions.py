import datetime
import math


def convtimestamp(UNIXstring):
    timestamp = datetime.datetime.fromtimestamp(UNIXstring)
    date = timestamp.strftime('%Y-%m-%d')
    day = timestamp.strftime('%A')
    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return [date, day, timestamp]


class TimeDifference:

    def __init__(self, start, end):
        start_unix = start
        end_unix = end
        begin = start_unix / 1000
        end = end_unix / 1000
        difference = end - begin
        self.minutes_raw = round(difference / 60, 4)
        floor_min = math.floor(self.minutes_raw)
        seconds = (self.minutes_raw - floor_min) * 60
        self.minute = round(floor_min, 2)
        self.seconds = round(seconds)
