from Functions import Functions_Configs as configFunc
from Functions import Functions_SQL_Interfacing as sqlf
from Functions import Functions_FFlogsAPI as fflogs
import pandas as pd
import time
import mysql.connector
import _01_Extract_Data as extract
import _02_Map_Statics as mapstatic
import _03_Get_Zone_Data as zonedata
import _04_Get_static_performance as staticperformance
import _10_Static_Assigner_Csv as reassignstatics

pd.options.display.width = None

config, found = configFunc.check_for_config('SQL_Config.csv')
# get SQL connection working. Add database=config['Database_Name'] if not using localhost
connection = mysql.connector.connect(host=config['Host'],
                                     user=config['User'],
                                     passwd=config['Password'],
                                     database=config['Database_Name'])

reportowner = 'Defai'
extract.extract_data(connection, owner=reportowner)
extract.extract_data(connection, owner=reportowner)
time.sleep(1)
reassignstatics.assignstatics()
time.sleep(1)
mapstatic.map_statics(connection, reportowner)
time.sleep(1)
zonedata.update_zone_data(connection)
time.sleep(1)
# allow this to run if its refreshing and updating based on input statics
staticperformance.get_Static_Performance(connection)


