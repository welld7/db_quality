#from random import randint
import pytest
from db_quality_main import *

load_date1 = '2017-01-05'
some_date2 = '2013-01-05'
some_sting = "hi"

@pytest.fixture()
def handle_connection():
    conn = create_connection(database_tmp)
    yield conn
    conn.close()


@pytest.fixture()
def handle_connection_and_drop():
    conn = create_connection(database_tmp)

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
    conn.commit()
    cur = conn.cursor()

    #delete rows in case something has alredy been there
    cur.execute(''' delete from check_status; ''')
    cur.execute(''' delete from check_object; ''')
    conn.commit()

    yield conn
    drop_object_table(conn)
    drop_status_table(conn)
    conn.close()


@pytest.mark.sanity
@pytest.mark.parametrize("int_input", [3, 18, 1029])
def test_avg_int_one_date(handle_connection_and_drop, int_input):
    conn = handle_connection_and_drop

    row1 = (load_date1, randint(0, 1000000), 0, 2.0, some_sting, some_date2)
    insert_new_row(conn, row1)

    row2 = (load_date1, randint(0, 1000000), int_input, 2.0, some_sting, some_date2)
    insert_new_row(conn, row2)

    row3 = (load_date1, randint(0, 1000000), 2*int_input, 2.0, some_sting, some_date2)
    insert_new_row(conn, row3)

    # we don't know what's going on in add_day_status_row, so we'll check
    # the avg value in db by id
    rowid_inserted = add_day_status_row(conn, load_date1)
    #print_table(conn)

    assert int_input == get_int_avg_in_status_table_by_rowid(conn, rowid_inserted)


@pytest.mark.sanity
@pytest.mark.parametrize("int_input", [1,2])
def test_avg_int_different_load_dates(handle_connection_and_drop, int_input):
    conn = handle_connection_and_drop

    row1 = (load_date1, randint(0, 1000000), 0, 2.0, some_sting, some_date2)
    insert_new_row(conn, row1)

    row2 = (load_date1, randint(0, 1000000), int_input, 2.0, some_sting, some_date2)
    insert_new_row(conn, row2)

    row3 = (load_date1, randint(0, 1000000), 2 * int_input, 2.0, some_sting, some_date2)
    insert_new_row(conn, row3)

    row4 = ('2017-01-04', randint(0, 1000000), 1000000, 2.0, some_sting, some_date2)
    insert_new_row(conn, row4)

    #the avg_int isn't changed by the other day row
    rowid_inserted = add_day_status_row(conn, load_date1)
    #print_table(conn)

    assert int_input == get_int_avg_in_status_table_by_rowid(conn, rowid_inserted)


@pytest.mark.sanity
@pytest.mark.parametrize("float_input", [-1., 1.7976931348623157e+30, 1029.])
def test_avg_float_one_date(handle_connection_and_drop, float_input):
    conn = handle_connection_and_drop

    row1 = (load_date1, randint(0, 1000000), 0, 0, some_sting, some_date2)
    insert_new_row(conn, row1)

    row2 = (load_date1, randint(0, 1000000), 0, float_input, some_sting, some_date2)
    insert_new_row(conn, row2)

    row3 = (load_date1, randint(0, 1000000), 0, 2*float_input, some_sting, some_date2)
    insert_new_row(conn, row3)

    rowid_inserted = add_day_status_row(conn, load_date1)
    #print_table(conn)

    assert float_input == get_float_avg_in_status_table_by_rowid(conn, rowid_inserted)


#TODO
@pytest.mark.sanity
@pytest.mark.skip
def test_avg_float_different_load_dates(handle_connection_and_drop, float_input):
    pass


@pytest.mark.sanity
@pytest.mark.parametrize("number_of_rows", [0, 1, 300])
def test_count(handle_connection_and_drop, number_of_rows):
    conn = handle_connection_and_drop
    if conn is not None:
        day = load_date1
        for _ in range(number_of_rows):
            row = (day, 0, 0, 0, "", day)
            insert_new_row(conn, row)

    assert number_of_rows == get_count(conn, day, get_next_day(day))


@pytest.mark.sanity
@pytest.mark.parametrize("rows_x3_input", [2, 118])
def test_z0_count_one_date(handle_connection_and_drop, rows_x3_input):
    conn = handle_connection_and_drop

    for _ in range (rows_x3_input):
        row1 = (load_date1, randint(0, 1000000), 0, 0, some_sting, some_date2)
        insert_new_row(conn, row1)

        row2 = (load_date1, randint(0, 1000000), 0, 1.0, some_sting, some_date2)
        insert_new_row(conn, row2)

    # we don't know what's going on in add_day_status_row, so we'll check
    # the avg value in db by id
    rowid_inserted = add_day_status_row(conn, load_date1)
    #print_table(conn)

    assert rows_x3_input * 3 == get_z0_count_in_status_table_by_rowid(conn, rowid_inserted)


@pytest.mark.sanity
@pytest.mark.parametrize("rows_x3_input", [12, 99])
def test_null_count_one_date(handle_connection_and_drop, rows_x3_input):
    conn = handle_connection_and_drop

    for _ in range (rows_x3_input):
        row1 = (load_date1, randint(0, 1000000), 0, 0, None, some_date2)
        insert_new_row(conn, row1)

        row2 = (load_date1, randint(0, 1000000), 0, 0, None, None)
        insert_new_row(conn, row2)

    # we don't know what's going on in add_day_status_row, so we'll check
    # the avg value in db by id
    rowid_inserted = add_day_status_row(conn, load_date1)
    #print_table(conn)

    assert rows_x3_input * 3 == get_null_count_in_status_table_by_rowid(conn, rowid_inserted)


