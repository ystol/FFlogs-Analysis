from Functions import Functions_Configs as configFunc
from Functions import Functions_SQL_Interfacing as sqlf
from Functions import Functions_FFlogsAPI as fflogs
import pandas as pd
import csv
import mysql.connector
import requests
import math

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

print('Updating extra zone info:')
ztable = 'zone_data'
rtable = 'raid_turns'
zone_data_df = sqlf.get_from_MYSQL_to_df(connection, ztable, ['zone_name', 'boss_name'])
zone_data_df.drop_duplicates(inplace=True)
existing_zone_data = sqlf.get_from_MYSQL_to_df(connection, rtable)
zone_data_df['boss_raid'] = zone_data_df['boss_name'] + '---' + zone_data_df['zone_name']
missing_zone_data = zone_data_df[~zone_data_df['zone_name'].isin(existing_zone_data['zone_name'].tolist())]
zcolumns = list(missing_zone_data.columns)

if len(missing_zone_data) > 0:
    sqlf.save_to_SQL(connection, rtable, zcolumns, missing_zone_data)
    print('Extra zone data updated')
else:
    print('No extra zone info to update')
