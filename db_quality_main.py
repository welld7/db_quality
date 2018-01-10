import sqlite3
from random import randint
import datetime


database = "pythonsqlite.db"


def create_connection(db):
    """ create a database connection to the SQLite database
        specified by db_file
        :return: Connection object or None
        """
    try:
        conn = sqlite3.connect(db)
    except Exception as e:
        conn = None

    return conn


def create_table(conn, create_table_sql):
    """
    create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:created table cursor
    """
    try:
        cur = conn.cursor()
        cur.execute(create_table_sql)
    except Exception as e:
        cur = None

    return cur


def insert_new_row_status(conn, row):
    """
    Create a new task
    :param conn:
    :param row:
    :return:created rowid
    """

    sql = ''' INSERT INTO check_status(load_date,non_unique_id_int,count,null_count,z0_count,int_avg,float_avg,date_avg)
              VALUES(?,?,?,?,?,?,?,?); '''
    cur = conn.cursor()
    cur.execute(sql, row)
    return cur.lastrowid


def insert_new_row(conn, row):
    """
    Create a new row
    :param conn:
    :param row:
    :return:created rowid
    """

    sql = ''' INSERT INTO check_object(load_date,id,int_value,float_value,char_value,date_value)
              VALUES(?,?,?,?,?,?); '''
    cur = conn.cursor()
    cur.execute(sql, row)
    conn.commit()

    return cur.lastrowid


def get_count(conn, day, next_day):
    """
    Count check_object rows
    :param conn:
    :param day
    :param next_day
    :return::count
    """
    sql = ''' SELECT count(*) from check_object WHERE load_date>=? AND load_date<?;'''
    cur = conn.cursor()
    cur.execute(sql, (day, next_day))
    count = cur.fetchone()[0]
    return count


def get_null_count(conn, day, next_day):
    """
    Count null in all rows and columns
    :param conn:
    :param day
    :param next_day

    :return:count
    """

    sql = ''' SELECT SUM(CASE WHEN load_date IS NULL THEN 1 ELSE 0 END
              + CASE WHEN int_value IS NULL THEN 1 ELSE 0 END
              + CASE WHEN float_value IS NULL THEN 1 ELSE 0 END
              + CASE WHEN char_value IS NULL THEN 1 ELSE 0 END
              + CASE WHEN date_value IS NULL THEN 1 ELSE 0 END) AS TotalNotNullCount
              FROM check_object 
              WHERE load_date>=? AND load_date<?;'''
    cur = conn.cursor()
    cur.execute(sql, (day, next_day))
    count = cur.fetchone()[0]
    return count


def get_z0_count(conn, day, next_day):
    """
    Count 0s
    """
    sql = ''' SELECT SUM(CASE WHEN id = 0 THEN 1 ELSE 0 END
              + CASE WHEN int_value = 0 THEN 1 ELSE 0 END
              + CASE WHEN float_value = 0 THEN 1 ELSE 0 END)
              FROM check_object
              WHERE load_date>=? AND load_date<?;'''
    cur = conn.cursor()
    cur.execute(sql, (day, next_day))
    count = cur.fetchone()[0]
    return count


def get_int_avg(conn, day, next_day):
    """
    Get int_value average
    :param conn:
    :return:
    """
    sql = ''' SELECT AVG(int_value)
              FROM check_object
              WHERE load_date>=? AND load_date<?;'''
    cur = conn.cursor()
    cur.execute(sql, (day, next_day))
    count = cur.fetchone()[0]
    return count


def get_float_avg(conn, day, next_day):
    """
    Get float_value average
    :param conn:
    :return:
    """

    sql = ''' SELECT AVG(float_value)
              FROM check_object
              WHERE load_date>=? AND load_date<?;'''

    cur = conn.cursor()
    cur.execute(sql, (day, next_day))
    count = cur.fetchone()[0]
    return count


def get_date_avg(conn, day, next_day):
    """
    Get date average
    :param conn:
    :return:
    """
    sql = ''' SELECT CAST(AVG(CAST(date_value AS INT)) AS DATETIME)
              FROM check_object
              WHERE load_date>=? AND load_date<?;'''
    cur = conn.cursor()
    cur.execute(sql, (day, next_day))
    count = cur.fetchone()[0]
    return count


def count_non_unique_id_int(conn, day, next_day):
    """
    Count non-unique id+int_value combinations
    :param conn:
    :return:
    """
    sql = ''' SELECT count(*)  FROM (SELECT DISTINCT id, int_value
          FROM check_object
          WHERE load_date>=? AND load_date<?);'''

    cur = conn.cursor()
    cur.execute(sql, (day, next_day))
    count = cur.fetchone()[0]
    return count


def get_int_avg_in_status_table_by_rowid(conn, rowid):
    sql = '''SELECT * FROM check_object WHERE id = ?;'''
    cur = conn.cursor()
    cur.execute('SELECT int_avg FROM check_status WHERE rowid=? OR rowid=?;', (rowid,rowid) )#sorry for that
    one = cur.fetchone()[0]
    return one


def get_float_avg_in_status_table_by_rowid(conn, rowid):
    sql = '''SELECT * FROM check_object WHERE id = ?;'''
    cur = conn.cursor()
    cur.execute('SELECT float_avg FROM check_status WHERE rowid=? OR rowid=?;', (rowid,rowid) )#sorry for that
    one = cur.fetchone()[0]
    return one


def print_table(conn):
    """
    Print check_object and check_status tables
    :param conn:
    :return:
    """
    sql = ''' SELECT * from check_object;'''
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    for row in rows:
        print(row)

    sql = ''' SELECT * from check_status;'''
    cur.execute(sql)
    rows = cur.fetchall()

    for row in rows:
        print(row)

    cur.execute('''SELECT count(*) from check_status;''')
    count = cur.fetchone()[0]
    print(count)


def drop_object_table(conn):
    sql = ''' DROP TABLE IF EXISTS check_object; '''
    cur = conn.cursor()
    cur.execute(sql)


def drop_status_table(conn):
    sql = ''' DROP TABLE IF EXISTS check_status; '''
    cur = conn.cursor()
    cur.execute(sql)

def get_next_day( load_data):
    ld_date = datetime.datetime.strptime(load_data, "%Y-%m-%d")
    next_day = datetime.datetime.strftime(ld_date + datetime.timedelta(days=1),
                                          "%Y-%m-%d")

    return next_day


def add_day_status_row(conn, load_data):
    #ld_date = datetime.datetime.strptime(load_data, "%Y-%m-%d")
    #next_day = datetime.datetime.strftime(ld_date + datetime.timedelta(days=1),
    #                                     "%Y-%m-%d")

    next_day = get_next_day(load_data)

    non_unique_id_int = count_non_unique_id_int(conn, load_data, next_day)
    count = get_count(conn, load_data, next_day)
    null_count = get_null_count(conn, load_data, next_day)
    z0_count = get_z0_count(conn, load_data, next_day)
    int_avg = get_int_avg(conn, load_data, next_day)
    float_avg = get_float_avg(conn, load_data, next_day)
    date_avg = get_date_avg(conn, load_data, next_day)

    row = (load_data, non_unique_id_int, count, null_count,
           z0_count, int_avg, float_avg, date_avg)

    rowid = insert_new_row_status(conn, row)
    conn.commit()

    print_table(conn)

    return rowid


def main():

    # create a database connection
    conn = create_connection(database)
    if conn is not None:
        # create projects table
        # create_table(conn, sql_create_projects_table)
        # create tasks table
        #create_table(conn, sql_create_tasks_table)

        # FIXME id is intentionally non-unique
        # So id is NOT a primary key
        sql_create_main_table = """ CREATE TABLE IF NOT EXISTS check_object (
                                            load_date date,
                                            id integer,
                                            int_value integer,
                                            float_value float,
                                            char_value varchar(10),
                                            date_value date
                                        ); """

        create_table(conn, sql_create_main_table)

        load_data = '2015-02-09'

        row = (load_data, randint(0, 1000000), 1, 2.0, "hi", '2013-01-05')
        insert_new_row(conn, row)

        row = (load_data, randint(0, 1000000), None, 10, "1hi", '2017-01-05')
        insert_new_row(conn, row)


        #drop_status_table(conn)
        #We won't count avg for VARCHAR as we don't know how to
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

        add_day_status_row(conn, load_data)
        conn.close()

    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    main()
