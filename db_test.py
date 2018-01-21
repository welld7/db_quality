#from random import randint
import pytest
from db_quality_main import *

@pytest.fixture()
def handle_connection():
    conn = create_connection(database2)
    yield conn
    conn.close()


@pytest.mark.sanity_db_exists
@pytest.mark.db_exists
def test_check_status_table_exists(handle_connection):
    conn = handle_connection
    cur = conn.cursor()

    cur.execute(''' SELECT * FROM sqlite_master WHERE name ='check_status' and type='table'; ''')
    table = cur.fetchone()
    assert table


@pytest.mark.sanity_db_exists
@pytest.mark.db_exists
def test_check_object_table_exists(handle_connection):
    conn = handle_connection
    cur = conn.cursor()

    cur.execute(''' SELECT * FROM sqlite_master WHERE name ='check_object' and type='table'; ''')
    table = cur.fetchone()
    assert table


@pytest.mark.sanity_db_exists
@pytest.mark.db_exists
def test_number_of_load_dates(handle_connection):
    conn = handle_connection
    cur = conn.cursor()

    cur.execute('''SELECT count(*) from check_status;''')
    cnt1 = cur.fetchone()

    cur.execute('''SELECT count( DISTINCT load_date)  from check_object;''')
    cnt2 = cur.fetchone()

    assert cnt1[0] == cnt2[0]


@pytest.mark.db_exists
def test_load_dates_set(handle_connection):
    conn = handle_connection
    cur = conn.cursor()

    cur.execute('''SELECT load_date from check_status;''')
    set1 = set(cur.fetchall())

    cur.execute('''SELECT DISTINCT load_date from check_object;''')
    set2 = set(cur.fetchall())

    assert set1 == set2


#todo: debug unique,date
@pytest.mark.db_exists
#@pytest.mark.skip
def test_whole_db_consistency(handle_connection):
    conn = handle_connection

    print_table(conn)
    cur = conn.cursor()

    # if performance becomes critical, we can create one complex query
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
        rows_status = cur.fetchone()

        print("check_status:", rows_status)
        print("-------------------------------------------------------------------")


#FIXME: Change the same DB
@pytest.mark.db_exists
@pytest.mark.parametrize("day_input", ['2217-01-05', '3017-01-05'])
@pytest.mark.parametrize("int_input", [2, -200])
def test_add_new_day(handle_connection, day_input, int_input):
    conn = handle_connection

    row1 = (day_input, randint(0, 1000000), 0, 2.0, "hi", '2013-01-05')
    rowid1 = insert_new_row(conn, row1)

    row2 = (day_input, randint(0, 1000000), int_input, 2.0, "hi", '2013-01-05')
    rowid2 = insert_new_row(conn, row2)

    row3 = (day_input, randint(0, 1000000), 2*int_input, 2.0, "hi", '2013-01-05')
    rowid3 = insert_new_row(conn, row3)

    rowid_inserted = add_day_status_row(conn, day_input)

    assert int_input == get_int_avg_in_status_table_by_rowid(conn, rowid_inserted)

    delete_check_object_row_by_rowid(conn, rowid1)
    delete_check_object_row_by_rowid(conn, rowid2)
    delete_check_object_row_by_rowid(conn, rowid3)
    delete_check_status_row_by_rowid(conn, rowid_inserted)
