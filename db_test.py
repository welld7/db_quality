#from random import randint
import pytest
#from generate_db import *
from db_quality_main import *

@pytest.fixture()
def handle_connection():
    conn = create_connection(database2)
    yield conn

@pytest.fixture()
def handle_connection_and_drop():
    conn = create_connection(database)#FIXME FIXME

    #drop_object_table(conn)#TODO
    #drop_status_table(conn)#TODO


    sql_create_main_table = """ CREATE TABLE IF NOT EXISTS check_object (
                                        load_date date,
                                        id integer,
                                        int_value integer,
                                        float_value float,
                                        char_value varchar(10),
                                        date_value date
                                    ); """

    # id:non-unique
    create_table(conn, sql_create_main_table)

    sql_create_status_table = """ CREATE TABLE IF NOT EXISTS check_status (
                                        load_date date,
                                        non_unique_id_int integer,
                                        count integer,
                                        null_count integer,
                                        z0_count int,
                                        int_avg float,
                                        float_avg float,
                                        date_avg float
                                    ); """
    create_table(conn, sql_create_status_table)

    yield conn
    drop_object_table(conn)
    drop_status_table(conn)
    conn.close()


#todo: debug unique,date
@pytest.mark.sanity
def test_db_consistency(handle_connection):
    conn = handle_connection

    print_table(conn)
    cur = conn.cursor()

    # if performance becomes critical, we can create one complex query

    #sql = ''' SELECT load_date from check_status WHERE load_date>=? AND load_date<?;'''


    cur.execute(''' SELECT DISTINCT load_date from check_object ;''')
    all_ld_dates = cur.fetchall()

    for row in all_ld_dates:
        ld_date = row[0]
        next_day = get_next_day(ld_date)

        #debug
        cur.execute(''' SELECT * from check_object WHERE load_date>=? AND load_date<?;''',
                    (ld_date, next_day))#TODO delete
        rows = cur.fetchall()
        for row2 in rows:
            print(row2)

        row_in_check_object_table =\
            calculate_status_values_in_check_object_table(conn, ld_date, next_day)
        print("calculated:", row_in_check_object_table)
        cur.execute(''' SELECT * from check_status WHERE load_date>=? AND load_date<?;''',
                    (ld_date, next_day))
        #rows_status = cur.fetchall()
        rows_status = cur.fetchone()

        print("check_status:", rows_status)
        print("-------------------------------------------------------------------")


@pytest.mark.sanity
@pytest.mark.parametrize("int_input", [3, 18, 1029])
def test_avg_int_one_date(handle_connection_and_drop, int_input):
    conn = handle_connection_and_drop #create_connection(database)

    row1 = ('2017-01-05', randint(0, 1000000), 0, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row1)

    row2 = ('2017-01-05', randint(0, 1000000), int_input, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row2)

    row3 = ('2017-01-05', randint(0, 1000000), 2*int_input, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row3)

    # we don't know what's going on in add_day_status_row, so we'll check
    # the avg value in db by id
    rowid_inserted = add_day_status_row(conn, '2017-01-05')
    print_table(conn)

    assert int_input == get_int_avg_in_status_table_by_rowid(conn, rowid_inserted)

@pytest.mark.sanity
@pytest.mark.parametrize("int_input", [1,2])
def test_avg_int_different_load_dates(handle_connection_and_drop, int_input):
    conn = handle_connection_and_drop  # create_connection(database)

    row1 = ('2017-01-05', randint(0, 1000000), 0, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row1)

    row2 = ('2017-01-05', randint(0, 1000000), int_input, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row2)

    row3 = ('2017-01-05', randint(0, 1000000), 2 * int_input, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row3)

    row4 = ('2017-01-04', randint(0, 1000000), 1000000, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row1)

    #the avg_int isn't changed by the other day row
    rowid_inserted = add_day_status_row(conn, '2017-01-05')
    print_table(conn)

    assert int_input == get_int_avg_in_status_table_by_rowid(conn, rowid_inserted)

@pytest.mark.sanity
@pytest.mark.parametrize("float_input", [-1., 1.7976931348623157e+30, 1029.])
def test_avg_float_one_date(handle_connection_and_drop, float_input):
    conn = handle_connection_and_drop #create_connection(database)

    row1 = ('2017-01-05', randint(0, 1000000), 0, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row1)

    row2 = ('2017-01-05', randint(0, 1000000), float_input, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row2)

    row3 = ('2017-01-05', randint(0, 1000000), 2*float_input, 2.0, "hi", '2013-01-05')
    insert_new_row(conn, row3)

    rowid_inserted = add_day_status_row(conn, '2017-01-05')
    print_table(conn)

    assert float_input == get_int_avg_in_status_table_by_rowid(conn, rowid_inserted)


@pytest.mark.sanity
@pytest.mark.parametrize("number_of_rows", [0, 1, 300])
def test_count(handle_connection_and_drop, number_of_rows):
    conn = handle_connection_and_drop #create_connection(database)
    if conn is not None:
        day = '2017-01-05'
        for _ in range(number_of_rows):
            row = (day, 0, 0, 0, "", day)
            insert_new_row(conn, row)

    assert number_of_rows == get_count(conn, day, get_next_day(day))


#@pytest.mark.sanity
#@pytest.mark.parametrize("number_of_rows", [0, 1, 300])
#def test_count2(handle_connection_and_drop, number_of_rows):
#    conn = handle_connection_and_drop #create_connection(database)
#    if conn is not None:
#        day = '2017-01-05'
#        for _ in range(number_of_rows):
#            row = (day, 0, 0, 0, "", day)
#            insert_new_row(conn, row)
#
#    assert number_of_rows == get_count(conn, day, get_next_day(day))

conn = create_connection(database2)
print_table(conn)
conn.close()
