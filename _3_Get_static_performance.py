from Functions import Functions_Configs as configFunc
from Functions import Functions_SQL_Interfacing as sqlf
from Functions import Functions_FFlogsAPI as fflogs
import pandas as pd
import csv
import time
import os
import datetime
import mysql.connector
import _1_Extract_Data as extract
import _2_Map_Statics as mapstatic

pd.options.display.width = None


def get_Static_Performance(sqlconnection):
    reporttable = 'reportdata'
    reportdf = sqlf.get_from_MYSQL_to_df(sqlconnection, reporttable).set_index('reportid')
    fighttable = 'fights'
    fightdf = sqlf.get_from_MYSQL_to_df(sqlconnection, fighttable)
    fightdf['date'] = [reportdf.loc[report, 'date'] for report in fightdf['reportid']]
    fightdf.sort_values('date', inplace=True)
    # print(reportdf)
    statictable = 'static_data'
    staticdf = sqlf.get_from_MYSQL_to_df(sqlconnection, statictable)
    # print(fightdf)

    bosscolumn = 'boss_zone'
    all_statics = fightdf['static'].drop_duplicates()
    # all_statics = all_statics[all_statics != 'N/A']
    static_data = []
    static_columns = ['static', 'boss_zone', 'first_attempt', 'first_clear', 'total_wipes', 'total_clears']
    for eachstatic in all_statics:
        print('Analyzing fights for ' + str(eachstatic))
        static_fights = fightdf[fightdf['static'] == eachstatic]
        bosses = static_fights[bosscolumn].drop_duplicates()
        clearedbosses = static_fights[static_fights['defeated'] == 'True']
        wipedbosses = static_fights[static_fights['defeated'] == 'False']
        for eachboss in bosses:
            first_wipe = datetime.datetime(2000, 1, 1)
            total_wipes = 0
            first_clear = datetime.datetime(2000, 1, 1)
            total_clears = 0
            if any(wipedbosses[bosscolumn] == eachboss):
                bosswipes = wipedbosses[wipedbosses[bosscolumn] == eachboss]
                first_wipe = min(bosswipes['date'].tolist())
                total_wipes = len(bosswipes)
            if any(clearedbosses[bosscolumn] == eachboss):
                bossclears = clearedbosses[clearedbosses[bosscolumn] == eachboss]
                cleardates = bossclears['date'].tolist()
                if len(cleardates) > 0:
                    first_clear = min(cleardates)
                    total_clears = len(cleardates)
            values = [eachstatic, eachboss, first_wipe, first_clear, total_wipes, total_clears]
            static_data.append(dict(zip(static_columns, values)))
            # print('First wipe: '+str(first_wipe)+". Total wipes: "+str(total_wipes))
            # print('First clear: '+str(first_clear)+". Total clears: "+str(total_clears))

        print('Saving static date for: ' + str(eachstatic))
        static_df = pd.DataFrame.from_dict(static_data)
        static_columns = list(static_df.columns)
        delete_syntax = 'DELETE FROM ' + statictable + " WHERE static = '" + eachstatic + "'"
        print(delete_syntax)
        sqlf.save_to_SQL(sqlconnection, statictable, static_columns, static_df, delete_syntax)


if __name__ == '__main__':
    config, found = configFunc.check_for_config('SQL_Config.csv')
    # get SQL connection working. Add database=config['Database_Name'] if not using localhost
    connection = mysql.connector.connect(host=config['Host'],
                                         user=config['User'],
                                         passwd=config['Password'],
                                         database=config['Database_Name'])
    get_Static_Performance(connection)