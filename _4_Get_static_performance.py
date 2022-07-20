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
    reportdf = sqlf.get_from_MYSQL_to_df(sqlconnection, reporttable)
    reportdf_reportsorted = reportdf.set_index('reportid')
    fighttable = 'fights'
    fightdf_original = sqlf.get_from_MYSQL_to_df(sqlconnection, fighttable)
    fightdf_original['date'] = [reportdf_reportsorted.loc[report, 'date'] for report in fightdf_original['reportid']]
    fightdf = fightdf_original.sort_values(['date', 'run_num'])
    fightdf.reset_index(inplace=True, drop=True)
    # print(reportdf)
    statictable = 'static_data'
    print(fightdf)

    bosscolumn = 'boss_zone'
    all_statics = fightdf['static'].drop_duplicates()
    # all_statics = all_statics[all_statics != 'N/A']
    static_columns = ['static', 'boss_zone', 'first_attempt', 'first_clear',
                      'total_wipes', 'total_clears', 'total_time_toclear',
                      'total_time_spent', 'zone_area_name', 'days_met', 'days_met_until_first']
    # print(all_statics)
    for eachstatic in all_statics:
        static_data = []
        print('Analyzing fights for ' + str(eachstatic))
        static_fights = fightdf[fightdf['static'] == eachstatic]
        bosses = static_fights[bosscolumn].drop_duplicates()
        # print(bosses)
        clearedbosses = static_fights[static_fights['defeated'] == 'True']
        wipedbosses = static_fights[static_fights['defeated'] == 'False']
        for eachboss in bosses:
            # print(eachboss)
            all_bosses = static_fights[static_fights['boss_zone'] == eachboss]
            dates_found = all_bosses['date']
            # print(dates_found)
            first_attempt = min(dates_found.tolist())
            total_wipes = 0
            first_clear = datetime.datetime(2000, 1, 1)
            total_clears = 0
            days_until_clear = 0
            if any(wipedbosses[bosscolumn] == eachboss):
                bosswipes = wipedbosses[wipedbosses[bosscolumn] == eachboss]
                total_wipes = len(bosswipes)
            if any(clearedbosses[bosscolumn] == eachboss):
                bossclears = clearedbosses[clearedbosses[bosscolumn] == eachboss]
                cleardates = bossclears['date'].tolist()
                if len(cleardates) > 0:
                    first_clear = min(cleardates)
                    total_clears = len(cleardates)
            days_met = len(dates_found.drop_duplicates())
            static_date_df = fightdf[(fightdf['static'] == eachstatic) & (fightdf[bosscolumn] == eachboss)]
            static_date_df.reset_index(inplace=True, drop=True)
            dates = static_date_df['date']
            clear_index = dates.where((dates == first_clear) & (static_date_df['defeated'] == 'True')).first_valid_index()
            total_time_spent = static_date_df['fight_length_min'].sum()
            if clear_index is not None:
                wipes_to_clear = static_date_df[:clear_index+1]
                days_until_clear = len(dates_found[:clear_index+1].drop_duplicates())
                total_time_toclear = wipes_to_clear['fight_length_min'].sum()
            else:
                total_time_toclear = 0
            zone_area_name = all_bosses['zone_area_name'].drop_duplicates().values[0]

            values = [eachstatic, eachboss, first_attempt, first_clear, total_wipes,
                      total_clears, total_time_toclear, total_time_spent, zone_area_name,
                      days_met, days_until_clear]
            static_data.append(dict(zip(static_columns, values)))

        print('Saving static date for: ' + str(eachstatic))
        static_df = pd.DataFrame.from_dict(static_data)
        static_columns = list(static_df.columns)
        delete_syntax = 'DELETE FROM ' + statictable + " WHERE static = '" + eachstatic + "'"
        # print(delete_syntax)
        sqlf.save_to_SQL(sqlconnection, statictable, static_columns, static_df, delete_syntax)


if __name__ == '__main__':
    config, found = configFunc.check_for_config('SQL_Config.csv')
    # get SQL connection working. Add database=config['Database_Name'] if not using localhost
    connection = mysql.connector.connect(host=config['Host'],
                                         user=config['User'],
                                         passwd=config['Password'],
                                         database=config['Database_Name'])
    get_Static_Performance(connection)
    # with pd.ExcelWriter('C:/Users/Eugene/Desktop/Projects/FFLogs Extraction/testdata.xlsx') as writer:
    #     fights.to_excel(writer, sheet_name='fights_sorted')
