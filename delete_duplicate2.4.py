import psycopg2
import time


def delete_du(conn_set, db_table, table_pk):
    time_start = time.time()
    print('CHECK Table:{0} key:{1}'.format(db_table, table_pk))
    connectparams = dict(entry.split('=') for entry in conn_set.split(','))
    conn = psycopg2.connect(**connectparams)
    cur = conn.cursor()
    cur.execute("select {0} as count from {1} group by {0} HAVING (COUNT(*) > 1);".format(table_pk, db_table))
    delete_results = cur.fetchall()
    rowcount = cur.rowcount
    if not delete_results:
        pass
    else:
        count = 0
        for delete_query in delete_results:
            # print("delete from {0} "
            #             "where {1}='{2}' "
            #             "and ctid=(select max(ctid) "
            #             "from {0} where {1}='{2}'" .format(db_table, table_pk, delete_query[0]))
            cur.execute("delete from {0} "
                        "where {1}='{2}' "
                        "and ctid=(select max(ctid) "
                        "from {0} where {1}='{2}')" .format(db_table, table_pk, delete_query[0]))
            count += 1
            print("Table:{0} duplicate_key:{1} {2}/{3}".format(db_table, delete_query[0], count, rowcount))
            conn.commit()

    conn.commit()
    cur.close()
    conn.close()
    print("Table:{0} duplicate key count:{1}".format(db_table, rowcount))
    print('delete {0} duplicate complete'.format(db_table))
    time_end = time.time()
    time_c = time_end - time_start
    print('time cost', time_c, 's')


def menu():
    host = input("enter ip (127.0.0.1):") or "127.0.0.1"
    dbname = input("enter dbname (trinity):") or "trinity"
    username = input("enter user (trinity):") or "trinity"
    password = input("enter password (trinity):") or "trinity"
    db_port = input("enter port (5432):") or "5432"

    conn_set = (
        'database={0}, user={1}, password={2}, host={3}, port={4},keepalives = 1, keepalives_idle = 130,'
        'keepalives_interval = 10, keepalives_count = 30'.format(dbname, username, password, host, db_port))
    log_table = [
        ['taskstepfulllog', 'tasksteploguid'],
        ['taskjoblog', 'taskjobloguid'],
        ['tasksteplog', 'tasksteploguid'],
        ['tasklog', 'taskuid'],
        ['receivefilelog', 'receivefileloguid'],
        ['taskstepoutputlog', 'tasksteploguid'],
        ['taskstatusreasonhistory', 'seq']]
    while True:
        setting = input("check public log:1\nCustom mode :0"
                        "\nenter your setting:")
        if setting == '1':
            for table_value in log_table:
                db_table = table_value[0]
                table_pk = table_value[1]
                delete_du(conn_set, db_table, table_pk)
            break
        elif setting == '2':
            for table_value in log_table:
                db_table = table_value[0]
                table_pk = table_value[1]
                delete_du(conn_set, db_table, table_pk)
            break
        elif setting == '0':
            db_table = input("enter db_table:")
            table_pk = input("enter table_pk:")
            delete_du(conn_set, db_table, table_pk)
            break

        else:
            print('error setting')
            continue


if __name__ == "__main__":
    print("delete_duplicate:2.4.0")
    menu()
    input("Please press the Enter key to proceed")