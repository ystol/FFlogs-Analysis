import datetime

def convtimestamp(UNIXstring):
    timestamp = datetime.datetime.fromtimestamp(UNIXstring)
    date = timestamp.strftime('%Y-%m-%d')
    day = timestamp.strftime('%A')
    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return [date, day, timestamp]