from db_quality_main import *
MAX = 1000000
MIN = -1000000

RANGE = 100000000 #for example

#def get_prev_day( date):
#    ld_date = datetime.datetime.strptime(date, "%Y-%m-%d")
#    prev_day = datetime.datetime.strftime(ld_date + datetime.timedelta(days=-1),
#                                          "%Y-%m-%d")
#    return prev_day

def get_prev_day_raw( date_raw):
    return date_raw + datetime.timedelta(days=-1)


#def random_date( today):
#    max_day = datetime.date.today()

def generate_db( conn, generate_days, rows_per_day):
    today = datetime.date.today()
    date = today
    for _ in range(generate_days):
        date = get_prev_day_raw(date)
        ld_date = datetime.datetime.strftime(date, "%Y-%m-%d")
        avg_int = randint(MIN, MAX)
        avg_float = uniform(MIN,MAX)

        sum = 0
        for n_th_in_day in range(rows_per_day):
            current_int = avg_int - rows_per_day + n_th_in_day * 2 + 1
            current_float = avg_float - rows_per_day + n_th_in_day * 2 + 1
            sum+=current_float
            row = ( ld_date, randint(0, 1000000), current_int, avg_float, "hi", '2013-01-05')
            insert_new_row(conn, row)

        print ( sum/rows_per_day, avg_float)

database2 = "pythonsqlite_backup.db"

conn = create_connection(database2)
cur = conn.cursor()

sql_create_main_table = """ CREATE TABLE IF NOT EXISTS check_object (
                                    load_date date,
                                    id integer,
                                    int_value integer,
                                    float_value float,
                                    char_value varchar(10),
                                    date_value date
                                ); """

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

sql = ''' PRAGMA synchronous = 0; '''
cur.execute(sql)
conn.commit()

generate_db(conn, generate_days=10, rows_per_day=16 )
#print_table(conn)

conn.close()
