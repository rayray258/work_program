import re
import psycopg2
import time


def delete_duplicate_partition(conn_set):
    time_start = time.time()
    connectparams = dict(entry.split('=') for entry in conn_set.split(','))
    conn = psycopg2.connect(**connectparams)
    cur = conn.cursor()
    cur.execute("select tablename from pg_tables where schemaname='partition_zone';")
    results = cur.fetchall()

    count = 0
    taskstatusreasonhistory_count = 0
    tasksteplog_count = 0
    taskstepfulllog_count = 0
    tasklog_count = 0
    taskjoblog_count = 0
    receivefilelog_count = 0
    taskstepoutputlog_count = 0

    for i in results:
        conn.commit()
        db_table = i[0]
        print(db_table, 'check..')

        if db_table.split('_')[0] == 'taskstepfulllog':
            table_values = ['taskstepfulllog', 'tasksteploguid', 'starttime']
            taskstepfulllog_count += 1
        elif db_table.split('_')[0] == 'taskjoblog':
            table_values = ['taskjoblog', 'taskjobloguid', 'starttime']
            taskjoblog_count += 1
        elif db_table.split('_')[0] == 'tasksteplog':
            table_values = ['tasksteplog', 'tasksteploguid', 'starttime']
            tasksteplog_count += 1
        elif db_table.split('_')[0] == 'tasklog':
            table_values = ['tasklog', 'taskuid', 'starttime']
            tasklog_count += 1
        elif db_table.split('_')[0] == 'receivefilelog':
            table_values = ['receivefilelog', 'receivefileloguid', 'filereceiveddatetime']
            receivefilelog_count += 1
        elif db_table.split('_')[0] == 'taskstepoutputlog':
            table_values = ['taskstepoutputlog', 'tasksteploguid', 'lastupdatetime']
            taskstepoutputlog_count += 1
        elif db_table.split('_')[0] == 'taskstatusreasonhistory':
            table_values = ['taskstatusreasonhistory', 'seq', 'stamp']
            taskstatusreasonhistory_count += 1
        else:
            continue

        try:
            table_type = table_values[0]
            table_pk = table_values[1]
            keep_values = table_values[2]
            if i[0] == re.search("({0}_([0-9]*))".format(table_type), i[0]).group():
                cur.execute("select {0} as count "
                            "from partition_zone.{1} "
                            "group by {0} HAVING (COUNT(*) > 1);".format(table_pk, db_table))

                duplicate_key = cur.fetchall()

                if not duplicate_key:
                    continue
                else:
                    for delete_query in duplicate_key:
                        cur.execute("delete from {0} "
                                    "where {1}='{3}' "
                                    "and {2} not in(select max({2}) from {0} "
                                    "where {1}='{3}');".format(db_table, table_pk, keep_values, delete_query[0]))
                        conn.commit()
                        count += 1
                        print("Table:{0} duplicate_key:{1}".format(db_table, delete_query[0]))

            else:
                continue
        except AttributeError:
            re.search("({0}_([0-9]*))".format(table_values[0]), db_table)

    conn.commit()
    cur.close()
    conn.close()
    print('check table tasklog total', tasklog_count)
    print('check table taskjoblog total', taskjoblog_count)
    print('check table tasksteplog total', tasksteplog_count)
    print('check table receivefilelog total', receivefilelog_count)
    print('check table taskstepfulllog total', taskstepfulllog_count)
    print('check table taskstepoutputlog total', taskstepoutputlog_count)
    print('check table taskstatusreasonhistory total', taskstatusreasonhistory_count)
    print('duplicate key total', count)
    print('delete duplicate complete')
    time_end = time.time()
    time_c = time_end - time_start
    print('time cost', time_c, 's')


def delete_du(conn_set, db_table, table_pk, keep_values):
    time_start = time.time()
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
        print("Table:{0}\nduplicate key count:{1}".format(db_table, rowcount))
        for delete_query in delete_results:
            cur.execute("delete from {0} "
                        "where {1}='{3}' "
                        "and {2} not in(select max({2}) from {0} "
                        "where {1}='{3}');" .format(db_table, table_pk, keep_values, delete_query[0]))
            count += 1
            print("Table:{0} duplicate_key:{1} {2}/{3}".format(db_table, delete_query[0], count, rowcount))
            conn.commit()

    conn.commit()
    cur.close()
    conn.close()
    print("Table:{0}\nduplicate key count:{1}".format(db_table, rowcount))
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
        ['taskstepfulllog', 'tasksteploguid', 'starttime'],
        ['taskjoblog', 'taskjobloguid', 'starttime'],
        ['tasksteplog', 'tasksteploguid', 'starttime'],
        ['tasklog', 'taskuid', 'starttime'],
        ['receivefilelog', 'receivefileloguid', 'filereceiveddatetime'],
        ['taskstepoutputlog', 'tasksteploguid', 'lastupdatetime'],
        ['taskstatusreasonhistory', ' seq', 'stamp']]
    while True:
        setting = input("check partition_zone:1\ncheck public log:2"
                        "\nenter your setting:")
        if setting == '1':
            delete_duplicate_partition(conn_set)
            break
        elif setting == '2':
            for table_value in log_table:
                db_table = table_value[0]
                table_pk = table_value[1]
                keep_values = table_value[2]
                print(db_table, table_pk, keep_values)
                delete_du(conn_set, db_table, table_pk, keep_values)
            break
        elif setting == '0':
            db_table = input("enter db_table:")
            table_pk = input("enter table_pk:")
            keep_values = input("enter keep_values(column max):")
            delete_du(conn_set, db_table, table_pk, keep_values)
            break

        else:
            print('error setting')
            continue


if __name__ == "__main__":
    print("delete_duplicate:2.0.0")
    menu()
    input("Please press the Enter key to proceed")