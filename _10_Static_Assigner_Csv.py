from Functions import Functions_Configs as configFunc
from Functions import Functions_SQL_Interfacing as sqlf
from Functions.GUI import StaticAssigner
import pandas as pd
import mysql.connector

pd.options.display.width = None

config, found = configFunc.check_for_config('SQL_Config.csv')
# get SQL connection working. Add database=config['Database_Name'] if not using localhost
connection = mysql.connector.connect(host=config['Host'],
                                     user=config['User'],
                                     passwd=config['Password'],
                                     database=config['Database_Name'])


def assignstatics():
    print('Updating characters with static mapping.')
    character_table = 'characters'

    csv_static = 'Static_Assignments.csv'
    static_csv = pd.read_csv(csv_static)
    print(static_csv)
    for eachcharacter in static_csv.charname:
        # using .item() will give you the first element in the index/series, so be wary of duplicates
        staticdata = static_csv[static_csv.charname == eachcharacter].static.item()
        try:
            sqlf.update_MYSQL(connection, character_table, ['charname'], [eachcharacter], ['static_name'], [staticdata])
            print('Updated entry for '+eachcharacter+' with Static '+staticdata)
        except:
            print('Failed to update entry for '+eachcharacter)
    print('Characters updated according to static mapping.')


if __name__ == '__main__':
    config, found = configFunc.check_for_config('SQL_Config.csv')
    # get SQL connection working. Add database=config['Database_Name'] if not using localhost
    connection = mysql.connector.connect(host=config['Host'],
                                         user=config['User'],
                                         passwd=config['Password'],
                                         database=config['Database_Name'])
    assignstatics()
