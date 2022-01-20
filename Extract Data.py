import json
import requests
from Functions import General_Functions as gFunc
from Functions import Functions_Configs as configFunc
from Functions import Functions_SQL_Interfacing as sqlf
from Functions import Functions_FFlogsAPI as fflogs
import calendar
import pandas as pd
import csv
import mysql.connector

pd.options.display.width = None

config, found = configFunc.check_for_config('SQL_Config.csv')
# get SQL connection working. Add database=config['Database_Name'] if not using localhost
connection = mysql.connector.connect(host=config['Host'],
                                     user=config['User'],
                                     passwd=config['Password'],
                                     database=config['Database_Name'])

config_file = 'Key.csv'
# open the csv, using the csv reader, and then convert into a list (for single column csv)
with open(config_file) as config_data:
    reader = csv.reader(config_data)
    logskey = dict(reader)

owner = 'Defai'
keysyntax = '?api_key=' + logskey['Key']
endpoint = 'https://www.fflogs.com:443/v1/'
myreports = 'reports/user/' + owner
fightreports = 'report/fights/'

reports_url = endpoint + myreports + keysyntax
print(reports_url)

# get reports and their data (at some point include check to cut list down if it was extracted
print('Getting Report Data')
allreports = fflogs.fflogs_getReports(reports_url)
report_list = allreports['id']
print(allreports)

# get fight data from each report
print('Getting Fight data (wipes, kills, etc)')
reportfight = fflogs.fflogs_getfightdata(report_list, endpoint + fightreports, keysyntax, owner)

# get character data from each report
print('Getting Characters from each report')
# currently does not consider same name, but different servers (find kuku waz without a duplicate drop)
characters = fflogs.fflogs_getcharacters(report_list, endpoint + fightreports, keysyntax, owner)
table_col_name = 'charname'
target_table = 'characters'
char_dupe_data = sqlf.get_from_mySQL(connection, table_col_name, target_table)
# this line checks the dataframe.column_name.isin(list of values to match against) and then keeps the false outputs
# check if this is optimal and if you can do this inplace somehow?
characters = characters[~characters.charname.isin(char_dupe_data)]
print('Characters not in database:')
print(characters)

# get participants per fight
print('Getting participants per fight and attempt')
participants_data = fflogs.fflogs_getparticipants(report_list, endpoint + fightreports, keysyntax)

with pd.ExcelWriter('testdata.xlsx') as writer:
    allreports.to_excel(writer, sheet_name='reports')
    reportfight.to_excel(writer, sheet_name='fightdata')
    characters.to_excel(writer, sheet_name='characters')
    participants_data.to_excel(writer, sheet_name='participant_data')

print('Saving Report Data')
report_columns = list(allreports.columns)
rtable = 'reportdata'
delete_syntax = 'DELETE FROM ' + rtable + " WHERE owner = '" + owner + "'"
print(delete_syntax)
sqlf.save_to_SQL(connection, rtable, report_columns, allreports, delete_syntax)

print('Saving Fight Data')
fights_columns = list(reportfight.columns)
ftable = 'fights'
# remove this once duplicate checks are completed
delete_syntax = 'DELETE FROM ' + ftable + " WHERE owner = '" + owner + "'"
print(delete_syntax)
sqlf.save_to_SQL(connection, ftable, fights_columns, reportfight, delete_syntax)

# check if there are any new characters against the database of characters
if len(characters) > 0:
    print('Saving Character Data')
    char_columns = list(characters.columns)
    ctable = 'characters'
    delete_syntax = ''
    print(delete_syntax)
    sqlf.save_to_SQL(connection, ctable, char_columns, characters, delete_syntax)

print('Saving Participant Data')
participants_columns = list(participants_data.columns)
ptable = 'fight_participants'
# remove this once duplicate checks are complete
delete_syntax = 'TRUNCATE ' + ptable
print(delete_syntax)
print(participants_data)
sqlf.save_to_SQL(connection, ptable, participants_columns, participants_data, delete_syntax)
