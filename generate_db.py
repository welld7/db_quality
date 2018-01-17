from db_quality_main import *
from string import ascii_lowercase
#from pairing import pair, depair

MIN = -1000000
MAX = 1000000
MAX_STR_LENGTH = 10
MAX_ID = 1000000
GENERATE_DAYS=10
ROWS_PER_DAY=14

# standard Cantar pairing function
pairing_function = lambda a, b: ((a + b) * (a + b + 1) +b) / 2



def get_prev_day_raw( date_raw):
    return date_raw + datetime.timedelta(days=-1)


def generate_for_ld_date(ld_date, rows_per_day):

    avg_int = randint(MIN, MAX)
    avg_float = uniform(MIN, MAX)
    avg_date_raw = datetime.datetime(randint(1970, 2100), randint(1, 12),
                                     randint(1, 28))  # 2100 is fine
    avg_date_str = datetime.datetime.strftime(avg_date_raw, "%Y-%m-%d")

    # sum = 0
    null_cnt = 0
    z0_cnt = 0
    id_int_all_pairs_set = set()
    non_unique_pair_set = set()
    # just one pair counter for current ld_date for all pairs per day
    non_unique_pair_cnt = 0
    for n_th_in_day in range(rows_per_day):
        current_int = 0  # avg_int - rows_per_day + n_th_in_day * 2 + 1
        current_float = avg_float - rows_per_day + n_th_in_day * 2 + 1

        # count zero number (int+float)
        z0_cnt += 1 if current_int == 0 else 0
        z0_cnt += 1 if current_float == 0 else 0

        # generate a lower case random sting
        # if the random lenth is 0, None generates
        length = randint(0, MAX_STR_LENGTH)
        if length == 0:
            current_sting = None
            null_cnt += 1
        else:
            current_sting = ''.join(choice(ascii_lowercase)
                                    for _ in range(length))

        # sum += current_float
        id = randint(0, MAX_ID)
        pair = pairing_function(id, current_int)
        print(id, current_int)
        # aleady in set of pais?
        if pair not in id_int_all_pairs_set:
            # 1. new pair => to the set of all pairs
            print("1s time:", id, current_int)
            id_int_all_pairs_set.add(pair)
        else:
            # 2. met again => to the set non-unique pair
            print("again:", id, current_int)
            non_unique_pair_set.add(pair)

        row = (ld_date, id, current_int,
               avg_float, current_sting, avg_date_str)
        insert_new_row(conn, row)

    print("insert", len(non_unique_pair_set))
    print(non_unique_pair_set)
    return (ld_date, len(non_unique_pair_set), rows_per_day, null_cnt, z0_cnt, avg_int, avg_float, avg_date_str)


def generate_db(conn, generate_days, rows_per_day):
    today = datetime.date.today()
    date = today
    for _ in range(generate_days):
        date = get_prev_day_raw(date)
        ld_date = datetime.datetime.strftime(date, "%Y-%m-%d")
        row2 = generate_for_ld_date(ld_date, rows_per_day)

        insert_new_row_status(conn, row2)

        #print ( sum/rows_per_day, avg_float)

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

generate_db(conn, generate_days=GENERATE_DAYS, rows_per_day=ROWS_PER_DAY )
print_table(conn)

conn.close()
