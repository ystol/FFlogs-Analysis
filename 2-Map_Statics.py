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
from collections import Counter

pd.options.display.width = None

config, found = configFunc.check_for_config('SQL_Config.csv')
# get SQL connection working. Add database=config['Database_Name'] if not using localhost
connection = mysql.connector.connect(host=config['Host'],
                                     user=config['User'],
                                     passwd=config['Password'],
                                     database=config['Database_Name'])

table = 'fights'
parttable = 'fight_participants'
fightsdf = sqlf.get_from_MYSQL_to_df(connection, table)
fightparticipants = sqlf.get_from_MYSQL_to_df(connection, parttable)
characterlist = sqlf.get_from_MYSQL_to_df(connection, 'characters')

fight_static = []
for index, eachentry in fightsdf.iterrows():
    report = eachentry['reportid']
    attempt = eachentry['run_num']
    partic_reports = fightparticipants[fightparticipants['reportid'] == report]
    participants = partic_reports[partic_reports['run_num'] == attempt]['charname']
    charactersfound = characterlist[characterlist['charname'].isin(participants)]['static_name']
    staticoccurences = Counter(charactersfound).most_common()
    static = staticoccurences[0]
    if static[1] > 5 and static[0] != 'N/A':
        fight_static.append([report, attempt, static[0]])
for eachentry in fight_static:
    print(sqlf.update_MYSQL(connection, table, ['reportid', 'run_num'], eachentry[:2], ['static'], [eachentry[2]]))




