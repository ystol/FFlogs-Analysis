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
report_column = 'reportid'
report_list = allreports[report_column]
print(allreports)

# get fight data from each report
print('Getting Fight data (wipes, kills, etc)')
ftable = 'fights'
reports_extracted = sqlf.get_from_MYSQL_to_df(connection, ftable, report_column)[report_column]
new_reports = report_list[~report_list.isin(reports_extracted)]
num_fight_reports = len(new_reports)
if num_fight_reports > 0:
    reportfight = fflogs.fflogs_getfightdata(new_reports, endpoint + fightreports, keysyntax, owner)
else:
    print('No new reports found')

# get participants per fight
print('Getting participants per fight and attempt')
fptable = 'fight_participants'
participants_extracted = sqlf.get_from_MYSQL_to_df(connection, fptable, report_column)[report_column]
new_participants = report_list[~report_list.isin(participants_extracted)]
num_participants_reports = len(new_participants)
if num_participants_reports > 0:
    participants_data = fflogs.fflogs_getparticipants(new_participants, endpoint + fightreports, keysyntax)
else:
    print('No new reports found')

# get character data from each report
print('Getting Characters from each report')
if num_participants_reports > 0:
    # currently does not consider same name, but different servers (find kuku waz without a duplicate drop)
    characters = fflogs.fflogs_getcharacters(new_participants, endpoint + fightreports, keysyntax, owner)
    table_col_name = 'charname'
    target_table = 'characters'
    char_dupe_data = sqlf.get_from_MYSQL_to_df(connection, target_table, table_col_name)[table_col_name]
    # this line checks the dataframe.column_name.isin(list of values to match against) and then keeps the false outputs
    # check if this is optimal and if you can do this inplace somehow?
    characters = characters[~characters['charname'].isin(char_dupe_data.tolist())]
    if len(characters) > 0:
        print('Characters not in database:')
        print(characters)
else:
    characters = []
    print('No new Characters found')

# for local debugging and checking
excel_saver = pd.ExcelWriter('testdata.xlsx')

print('Saving Report Data')
# for local debugging and checking
allreports.to_excel(excel_saver, sheet_name='reports')
report_columns = list(allreports.columns)
rtable = 'reportdata'
delete_syntax = 'DELETE FROM ' + rtable + " WHERE owner = '" + owner + "'"
print(delete_syntax)
sqlf.save_to_SQL(connection, rtable, report_columns, allreports, delete_syntax)

# checked earlier, in case there are no new fights
if num_fight_reports > 0 and len(reportfight) > 0:
    print('Saving Fight Data')
    # for local debugging and checking
    reportfight.to_excel(excel_saver, sheet_name='fightdata')
    fights_columns = list(reportfight.columns)
    print(reportfight)
    sqlf.save_to_SQL(connection, ftable, fights_columns, reportfight, delete_syntax)

# check if there are any new characters against the database of characters
if len(characters) > 0:
    print('Saving Character Data')
    # for local debugging and checking
    characters.to_excel(excel_saver, sheet_name='characters')
    char_columns = list(characters.columns)
    ctable = 'characters'
    sqlf.save_to_SQL(connection, ctable, char_columns, characters, delete_syntax)

if num_participants_reports > 0:
    print('Saving Participant Data')
    # for local debugging and checking
    participants_data.to_excel(excel_saver, sheet_name='participant_data')
    participants_columns = list(participants_data.columns)
    sqlf.save_to_SQL(connection, fptable, participants_columns, participants_data, delete_syntax)