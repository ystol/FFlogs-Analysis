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

character_table = 'characters'
character_df = sqlf.get_from_MYSQL_to_df(connection, character_table)
column1 = 'charname'
column2 = 'server'

toupdate = StaticAssigner(character_df, 'Static Assigner', column1=column1, column2=column2)
toupdate.mainloop()

print('Updating characters:')
update_list = toupdate.get_list
print(update_list)
static_name = toupdate.get_static_input

names = update_list['charname']
server = update_list['server']
merge = zip(names, server)
for eachentry in list(merge):
    name = eachentry[0]
    server = eachentry[1]
    sqlf.update_MYSQL(connection, character_table, ['charname', 'server'], [name, server], ['static_name'], [static_name])

