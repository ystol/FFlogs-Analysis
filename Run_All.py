from Functions import Functions_Configs as configFunc
from Functions import Functions_SQL_Interfacing as sqlf
from Functions import Functions_FFlogsAPI as fflogs
import pandas as pd
import csv
import mysql.connector
import _1_Extract_Data as extract
import _2_Map_Statics as mapstatic
import _3_Get_static_performance as staticperformance

pd.options.display.width = None

config, found = configFunc.check_for_config('SQL_Config.csv')
# get SQL connection working. Add database=config['Database_Name'] if not using localhost
connection = mysql.connector.connect(host=config['Host'],
                                     user=config['User'],
                                     passwd=config['Password'],
                                     database=config['Database_Name'])

extract.extract_data(connection)
mapstatic.map_statics(connection)
staticperformance.get_Static_Performance(connection)
