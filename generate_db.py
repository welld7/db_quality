# from db_quality_main import *
#
#
# conn = create_connection(database)
#
# sql_create_main_table = """ CREATE TABLE IF NOT EXISTS check_object (
#                                     load_date date,
#                                     id integer,
#                                     int_value integer,
#                                     float_value float,
#                                     char_value varchar(10),
#                                     date_value date
#                                 ); """
#
# PRAGMA schema.synchronous = 0
#
# create_table(conn, sql_create_main_table)
#
# load_data = '2015-02-09'
#
# row = (load_data, randint(0, 1000000), 1, 2.0, "hi", '2013-01-05')
# insert_new_row(conn, row)
