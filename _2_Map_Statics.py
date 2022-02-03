import math
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


def map_statics(sqlconnection):
    table = 'fights'
    parttable = 'fight_participants'
    fightsdf = sqlf.get_from_MYSQL_to_df(sqlconnection, table)
    fightparticipants = sqlf.get_from_MYSQL_to_df(sqlconnection, parttable)
    characterlist = sqlf.get_from_MYSQL_to_df(sqlconnection, 'characters')

    fight_static = []
    total_fights = len(fightsdf)
    progress = 1
    print('Calculating Statics Participating. Total to scan: ' + str(total_fights))
    for index, eachentry in fightsdf.iterrows():
        report = eachentry['reportid']
        attempt = eachentry['run_num']

        if math.ceil(total_fights/100) == total_fights/100:
            print(str(progress) + "/" + str(total_fights))

        partic_reports = fightparticipants[fightparticipants['reportid'] == report]
        participants = partic_reports[partic_reports['run_num'] == attempt]['charname']
        charactersfound = characterlist[characterlist['charname'].isin(participants)]['static_name']
        staticoccurences = Counter(charactersfound).most_common()
        static = staticoccurences[0]
        if static[1] >= 5 and static[0] != 'N/A':
            fight_static.append([report, attempt, static[0]])
    print('Statics Participating complete')

    total_updates = len(fight_static)
    progress = 1
    print('Updating database. Total to update: ' + str(total_updates))
    for eachentry in fight_static:
        if math.ceil(total_updates/10) == total_updates/10:
            print(str(progress) + "/" + str(total_updates))
        # set below line to assign to a variable for debugging outputs
        sqlf.update_MYSQL(sqlconnection, table, ['reportid', 'run_num'], eachentry[:2], ['static'], [eachentry[2]])

    print('Update Complete')


if __name__ == '__main__':
    config, found = configFunc.check_for_config('SQL_Config.csv')
    # get SQL connection working. Add database=config['Database_Name'] if not using localhost
    connection = mysql.connector.connect(host=config['Host'],
                                         user=config['User'],
                                         passwd=config['Password'],
                                         database=config['Database_Name'])
    map_statics(connection)


