import os
import pandas as pd
import mysql.connector

pd.options.display.width = None


def save_to_SQL(connection, table, table_column_names_list, dataframe, delete_syntax_condition=''):
    # the connection assumed is using mysql.connector library
    my_cursor = connection.cursor()
    # this will join the list of strings into a single string for inputting into the sql syntax
    # the join method onto a string uses the string as the delimiter for the list of elements
    sqlDB_column_names = ', '.join(table_column_names_list)

    # optional to run an extra condition before the insertion of the data using
    # SQL statement before the insert statement for the data
    if delete_syntax_condition != '':
        my_cursor.execute(delete_syntax_condition)

    sql_syntax = "INSERT into " + table + " (" + sqlDB_column_names + ") VALUES \n"
    # accept a list as input in certain cases would be beneficial. The syntax is different for strings
    if type(dataframe) is list:
        # -have to replace single quotes with double so that its read properly as a quote within the string
        # -same principle for the backslash, a backslash represents specific types of characters,
        # flip to forward slash for simplicity. backslash might be representing italics
        # -chaining two replaces back to back allows for replacing two different types of characters in one line
        row_as_string = ["'" + str(i).replace("'", "''").replace("\\", "/") + "'" for i in dataframe]
        sql_syntax += "(" + ','.join(row_as_string) + ");"
        my_cursor.execute(sql_syntax)
    else:
        # dataframe has to be broken apart and converted to string to be compatible with the sql statements
        for rownumber in range(0, len(dataframe.iloc[:, 0])):
            row_as_string = dataframe.iloc[rownumber].tolist()
            # same principle as for single list element, certain characters have to be converted for mySQL to read
            row_as_string = ["'" + str(i).replace("'", "''").replace("\\", "/") + "'" for i in row_as_string]
            if rownumber < len(dataframe.iloc[:, 0]) - 1:
                sql_syntax += "(" + ','.join(row_as_string) + "),\n"
            else:
                sql_syntax += "(" + ','.join(row_as_string) + ");"
        my_cursor.execute(sql_syntax)

    # commit is necessary to save changes, otherwise nothing occurs
    connection.commit()
    return connection, my_cursor


def get_from_mySQL(connection, table_column, table_target):
    mycursor = connection.cursor()
    prefix = "SELECT "
    suffix = " FROM " + table_target
    multi_column = (len(table_column) > 1) and (isinstance(table_column, list))
    if multi_column:
        table_column = ','.join(table_column)
        select_query = prefix + table_column + suffix
        mycursor.execute(select_query)
        return mycursor.fetchall(), select_query
    else:
        select_query = prefix + table_column + suffix
        mycursor.execute(select_query)
        return [out[0] for out in mycursor.fetchall()], select_query


def get_from_MYSQL_to_df(connection, table_target, table_column=[]):
    prefix = "SELECT "
    suffix = " FROM " + table_target
    multi_column = (len(table_column) > 1) and (isinstance(table_column, list))
    get_table = len(table_column) == 0
    if multi_column:
        table_column = ','.join(table_column)
        select_query = prefix + table_column + suffix
    elif get_table:
        select_query = prefix + "*" + suffix
    else:
        select_query = prefix + table_column + suffix
    df = pd.read_sql(select_query, connection)
    return df


def update_MYSQL(connection, table, match_columns: list, match_values: list, update_columns: list, update_values: list):
    # single entry update
    my_cursor = connection.cursor()
    updateprefix = "UPDATE " + table + " SET "
    set_syntax = []
    where_syntax = []
    for i, eachcolumn in enumerate(match_columns):
        where_syntax.append(eachcolumn + " = '" + str(match_values[i]) + "'")
    for j, eachupdatecolumn in enumerate(update_columns):
        set_syntax.append(eachupdatecolumn + " = '" + update_values[j] + "'")
    set_syntax = ','.join(set_syntax)
    where_syntax = 'WHERE ' + ' and '.join(where_syntax)
    sql_syntax = updateprefix + set_syntax + " " + where_syntax + ";"
    my_cursor.execute(sql_syntax)
    connection.commit()
    return sql_syntax
