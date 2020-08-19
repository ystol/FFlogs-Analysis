import json
import requests
import datetime
import calendar
import pandas as pd
import csv

pd.options.display.width = None

config_file = 'Key.csv'

# open the csv, using the csv reader, and then convert into a list (for single column csv)
with open(config_file) as config_data:
    reader = csv.reader(config_data)
    logskey = dict(reader)


def convtimestamp(UNIXstring):
    timestamp = datetime.datetime.fromtimestamp(UNIXstring)
    date = timestamp.strftime('%Y-%m-%d')
    day = timestamp.strftime('%A')
    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return [date, day, timestamp]


key = logskey['Key']
keysyntax = '?api_key=' + key
endpoint = 'https://www.fflogs.com:443/v1/'
myreports = 'reports/user/Defai'
getreport = 'report/fights/'

endurl = endpoint + myreports + keysyntax
print(endurl)

r = requests.get(endurl).json()
#print(r)
# zone 33 is edens verse
mydata = pd.DataFrame()
reports = []
for eachentry in r:
    eachentry['date'], eachentry['day'], eachentry['start'] = convtimestamp(eachentry['start']/1000)
    ignore, ignore, eachentry['end'] = convtimestamp(eachentry['end']/1000)
    reports.append(eachentry)

print(reports)
keys = list(reports[0].keys())
test = pd.DataFrame.from_dict(reports)

print(test)

getid = reports[0]['id']
# reportgeturl = endpoint + getreport + getid + keysyntax
# r = requests.get(reportgeturl).json()
# print(r)