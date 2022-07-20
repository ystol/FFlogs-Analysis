from Functions import Functions_Configs as configFunc
from Functions import Functions_SQL_Interfacing as sqlf
from Functions import Functions_FFlogsAPI as fflogs
import pandas as pd
import csv
import mysql.connector
import requests
import math

pd.options.display.width = None


def update_zone_data(sqlconnection):
    config_file = 'Key.csv'
    # open the csv, using the csv reader, and then convert into a list (for single column csv)
    with open(config_file) as config_data:
        reader = csv.reader(config_data)
        logskey = dict(reader)

    keysyntax = '?api_key=' + logskey['Key']
    endpoint = 'https://www.fflogs.com/v1/'
    zones = 'zones'

    print('Getting Zone Data: ')
    zones_url = endpoint + zones + keysyntax
    print(zones_url)
    zone_data_df = fflogs.fflogs_getzones(zones_url)

    print('Saving Zone Data')
    # for local debugging and checking
    zone_columns = list(zone_data_df.columns)
    ztable = 'zone_data'
    delete_syntax = 'TRUNCATE ' + ztable
    sqlf.save_to_SQL(sqlconnection, ztable, zone_columns, zone_data_df, delete_syntax)

    print('Updating Fight Database with zone data:')
    ftable = 'fights'
    fullfightsdf = sqlf.get_from_MYSQL_to_df(sqlconnection, ftable)
    fight_df = fullfightsdf[fullfightsdf['zone_area_name'] == 'Not Scanned']
    total_fights = len(fight_df)
    print('Total to update: ' + str(total_fights))
    progress = 1
    for index, eachentry in fight_df.iterrows():
        report = eachentry['reportid']
        attempt = eachentry['run_num']
        bossid = eachentry['boss']
        try:
            zone_name = zone_data_df[zone_data_df['enc_boss_id'] == bossid]['zone_name'].values[0]
        except IndexError:
            zone_name = 'Not Found'
        if math.ceil(progress / 100) == progress / 100:
            print("\r" + str(progress) + "/" + str(total_fights), end='')
        sqlf.update_MYSQL(sqlconnection, ftable, ['reportid', 'run_num'], [report, attempt], ['zone_area_name'], [zone_name])
        progress += 1
    print('\rZone Data update complete')


if __name__ == '__main__':
    config, found = configFunc.check_for_config('SQL_Config.csv')
    # get SQL connection working. Add database=config['Database_Name'] if not using localhost
    connection = mysql.connector.connect(host=config['Host'],
                                         user=config['User'],
                                         passwd=config['Password'],
                                         database=config['Database_Name'])
    update_zone_data(connection)
