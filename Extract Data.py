import json
import requests
from Functions import General_Functions as gFunc
from Functions import Functions_Configs as configFunc
from Functions import Functions_SQL_Interfacing as save
import calendar
import pandas as pd
import csv
import mysql.connector

pd.options.display.width = None

config_file = 'Key.csv'

# open the csv, using the csv reader, and then convert into a list (for single column csv)
with open(config_file) as config_data:
    reader = csv.reader(config_data)
    logskey = dict(reader)

owner = 'Defai'
keysyntax = '?api_key=' + logskey['Key']
endpoint = 'https://www.fflogs.com:443/v1/'
myreports = 'reports/user/' + owner
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
    eachentry['date'], eachentry['day'], eachentry['start_time'] = gFunc.convtimestamp(eachentry['start']/1000)
    ignore, ignore, eachentry['end_time'] = gFunc.convtimestamp(eachentry['end']/1000)
    reports.append(eachentry)

#print(reports)
keys = list(reports[0].keys())
allreports = pd.DataFrame.from_dict(reports)

print(allreports)
#print(get_zones_call)


with pd.ExcelWriter('testdata.xlsx') as writer:
    allreports.to_excel(writer, sheet_name='test data')

config, found = configFunc.check_for_config('SQL_Config.csv')
# get SQL connection working. Add database=config['Database_Name'] if not using localhost
connection = mysql.connector.connect(host=config['Host'],
                                     user=config['User'],
                                     passwd=config['Password'],
                                     database=config['Database_Name'])

report_columns = list(allreports.columns)
table = 'reportdata'
delete_syntax = 'DELETE FROM ' + table + " WHERE owner = '" + owner + "'"
print(delete_syntax)
save.save_to_SQL(connection, table, report_columns, allreports, delete_syntax)