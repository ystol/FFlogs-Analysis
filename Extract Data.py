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


keysyntax = '?api_key=' + logskey['Key']
endpoint = 'https://www.fflogs.com:443/v1/'
myreports = 'reports/user/Defai'
getreport = 'report/fights/'
zonescall = 'zones'

reports_url = endpoint + myreports + keysyntax
print(reports_url)
zones_url = endpoint + zonescall + keysyntax

get_reports_call = requests.get(reports_url).json()
get_zones_call = requests.get(zones_url).json()
#print(r)
# zone 33 is edens verse
mydata = pd.DataFrame()
reports = []
for eachentry in get_reports_call:
    eachentry['date'], eachentry['day'], eachentry['start_time'] = convtimestamp(eachentry['start']/1000)
    ignore, ignore, eachentry['end_time'] = convtimestamp(eachentry['end']/1000)
    reports.append(eachentry)

print(reports)
keys = list(reports[0].keys())
allreports = pd.DataFrame.from_dict(reports)

# print(allreports)
print(get_zones_call)

getid = reports[0]['id']
# reportgeturl = endpoint + getreport + getid + keysyntax
# r = requests.get(reportgeturl).json()
# print(r)