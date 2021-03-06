import psycopg2
from psycopg2 import extras


def conf_mode():
    dict_values = {}
    path = './/copy.conf'
    with open(path) as f:
        lines = f.readlines()
        for line in lines:
            if line.startswith('#'):
                continue
            elif line.startswith('\n'):
                continue
            else:
                line = line.strip()
                key = line.split('=')[0]
                values = line.split('=')[1]
                dict_values[key] = values
                continue

    source_host = dict_values['source_host']
    source_db = dict_values['source_db']
    source_user = dict_values['source_user']
    source_pwd = dict_values['source_pwd']
    source_port = dict_values['source_port']

    target_host = dict_values['target_host']
    target_db = dict_values['target_db']
    target_user = dict_values['target_user']
    target_pwd = dict_values['target_pwd']
    target_port = dict_values['target_port']
    pg_size = dict_values['pg_size']
    source_list = [source_host, source_db, source_port]
    target_list = [target_host, target_db, target_port]
    while True:
        if source_list == target_list:
            print('source_db & target_db must be different\nplease reset')
            source_db = input("source dbname (trinity):") or "trinity"
            target_db = input("target dbname (trinity):") or "trinity"
            source_list[1] = source_db
            target_list[1] = target_db
            continue
        else:
            break
    source_conn_set = (
        'database={0}, user={1}, password={2}, host={3}, port={4},keepalives = 1, keepalives_idle = 130,'
        'keepalives_interval = 10, keepalives_count = 30'.format(source_db, source_user, source_pwd,
                                                                 source_host, source_port))
    target_conn_set = (
        "database={0}, user={1}, password={2}, host={3}, port={4},keepalives = 1, keepalives_idle = 130, "
        "keepalives_interval = 10, keepalives_count = 30".format(target_db, target_user, target_pwd,
                                                                 target_host, target_port))
    print('------------------------------------------------------------------')
    print('source_host:' + source_host.ljust(10) + 'target_host:'.rjust(25) + target_host)
    print('source_port:' + source_port.ljust(10) + 'target_port:'.rjust(25) + target_port)
    print('source_db:' + source_db.ljust(10) + 'target_db:'.rjust(25) + target_db)
    print('source_user:' + source_user.ljust(10) + 'target_user:'.rjust(25) + target_user)
    print('source_pwd:' + source_pwd.ljust(10) + 'target_pwd:'.rjust(25) + target_pwd)
    print('------------------------------------------------------------------')
    return source_conn_set, target_conn_set, pg_size


def input_values():
    source_host = input("source ip (127.0.0.1):") or "127.0.0.1"
    source_db = input("source dbname (trinity):") or "trinity"
    source_user = input("source user (trinity):") or "trinity"
    source_pwd = input("source password (trinity):") or "trinity"
    source_port = input("source port (5432):") or "5432"

    target_host = input("target ip (127.0.0.1):") or "127.0.0.1"
    target_db = input("target dbname (trinity):") or "trinity"
    target_user = input("target user (trinity):") or "trinity"
    target_pwd = input("target password (trinity):") or "trinity"
    target_port = input("target port (5432):") or "5432"
    pg_size = input("page_size (1000):") or "1000"
    source_list = [source_host, source_db, source_port]
    target_list = [target_host, target_db, target_port]
    while True:
        print(source_list)
        print(target_list)
        if source_list == target_list:
            print('source_db & target_db must be different\nplease reset')
            source_db = input("source dbname (trinity):") or "trinity"
            target_db = input("target dbname (trinity):") or "trinity"
            source_list[1] = source_db
            target_list[1] = target_db
            continue
        else:
            break
    source_conn_set = (
        'database={0}, user={1}, password={2}, host={3}, port={4},keepalives = 1, keepalives_idle = 130,'
        'keepalives_interval = 10, keepalives_count = 30'.format(source_db, source_user, source_pwd,
                                                                 source_host, source_port))
    target_conn_set = (
        "database={0}, user={1}, password={2}, host={3}, port={4},keepalives = 1, keepalives_idle = 130, "
        "keepalives_interval = 10, keepalives_count = 30".format(target_db, target_user, target_pwd,
                                                                 target_host, target_port))
    print('------------------------------------------------------------------')
    print('source_host:' + source_host.ljust(10) + 'target_host:'.rjust(25) + target_host)
    print('source_port:' + source_port.ljust(10) + 'target_port:'.rjust(25) + target_port)
    print('source_db:' + source_db.ljust(10) + 'target_db:'.rjust(25) + target_db)
    print('source_user:' + source_user.ljust(10) + 'target_user:'.rjust(25) + target_user)
    print('source_pwd:' + source_pwd.ljust(10) + 'target_pwd:'.rjust(25) + target_pwd)
    print('------------------------------------------------------------------')

    return source_conn_set, target_conn_set, pg_size


def timer(func):
    def func_wrapper(*args, **kwargs):
        from time import time
        time_start = time()
        result = func(*args, **kwargs)
        time_end = time()
        time_spend = time_end - time_start
        print('copy data %s cost time: %.3f s' % (args[3], time_spend))
        return result
    return func_wrapper


@timer
def cptable(source_conn_set, target_conn_set, pg_size, table_name):
    # time_start = time.time()
    # tgt_conn
    t_connectparams = dict(entry.split('=') for entry in target_conn_set.split(','))
    tgt_conn = psycopg2.connect(**t_connectparams)
    tgt_cur = tgt_conn.cursor()
    # source_conn
    s_connectparams = dict(entry.split('=') for entry in source_conn_set.split(','))
    source_conn = psycopg2.connect(**s_connectparams)
    source_cur = source_conn.cursor()
    try:
        if table_name in ['taskstepfulllog', 'tasksteplog', 'taskstepoutputlog']:
            source_cur.execute("select distinct on (tasksteploguid) * from {0};".format(table_name))
        elif table_name == 'tasklog':
            source_cur.execute("select distinct on (taskuid) * from {0};".format(table_name))
        elif table_name == 'taskjoblog':
            source_cur.execute("select distinct on (taskjobloguid) * from {0};".format(table_name))
        elif table_name == 'receivefilelog':
            source_cur.execute("select distinct on (receivefileloguid) * from {0};".format(table_name))
        elif table_name == 'taskstatusreasonhistory':
            source_cur.execute("select distinct on (seq) * from {0};".format(table_name))
        elif table_name == 'disconfig':
            tgt_cur.execute("select count(*) from pg_tables where schemaname='public' and "
                            "tablename='disconfig_installtmp';")
            tgt_results = tgt_cur.fetchone()[0]
            if tgt_results == 0:
                tgt_cur.execute("select * into public.disconfig_installtmp from public.disconfig;")
                tgt_conn.commit()
                source_cur.execute("select * from {0};".format(table_name))
            else:
                source_cur.execute("select * from {0};".format(table_name))
                pass
        else:
            source_cur.execute("select * from {0};".format(table_name))
        # truncate tgt table
        truncate_extra_table(target_conn_set, table_name)
        tgt_cur.execute("truncate table {0};".format(table_name))
        tgt_conn.commit()
        print('truncate table {0};'.format(table_name))
        insert_count = 0
        print('copy table {0} begin'.format(table_name))

        while True:
            source_results = source_cur.fetchmany(50000)
            rowcount = source_cur.rowcount
            if not source_results:
                print('Complate [Insert:{0}]'.format(rowcount))
                break
            else:
                pass

            if rowcount >= 50000:
                print('count {0} '.format(insert_count))
            else:
                pass
            insert_count += 50000

            insert_query = "insert into {0} values %s ".format(table_name)
            # insert into target
            extras.execute_values(tgt_cur, insert_query, source_results, page_size=int(pg_size), fetch=False)

            # print('copy table {0} finsh'.format(table_name))
        if table_name == 'disconfig':
            tgt_cur.execute("insert into public.disconfig select * from public.disconfig_installtmp "
                            "ON CONFLICT ON CONSTRAINT disconfig_pkey DO NOTHING;")
            tgt_conn.commit()
        else:
            pass

        source_conn.commit()
        source_cur.close()
        source_conn.close()
        tgt_conn.commit()
        tgt_cur.close()
        tgt_conn.close()

    except:
        tgt_conn.rollback()
        if table_name == 'metatablelayout':
            source_cur.execute("select metatableuid, layoutname, description, layoutseq, layouttype, dbtype, keycol, "
                               "minlength, maxlength, nullable, '0', '0', xmldata "
                               "from {0};".format(table_name))
            rowcount = source_cur.rowcount
            source_results = source_cur.fetchall()
            sql = "INSERT INTO public.metatablelayout(metatableuid, layoutname, description, layoutseq, layouttype, " \
                  "dbtype, keycol, minlength, maxlength, nullable, scale, fixedlength, xmldata) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            tgt_cur.executemany(sql, source_results)
            print('Complate [Insert:{0}]'.format(rowcount))
        elif table_name == 'metatablelayouthistory':
            source_cur.execute("select metatableuid, version, layoutname, description,layoutseq, layouttype, "
                               "dbtype, keycol, minlength, maxlength, nullable, '0', '0', xmldata "
                               "from {0};".format(table_name))

            rowcount = source_cur.rowcount
            source_results = source_cur.fetchall()
            sql = "INSERT INTO public.metatablelayouthistory(metatableuid, version, layoutname, description, " \
                  "layoutseq, layouttype, dbtype, keycol, minlength, maxlength, nullable, scale, fixedlength, " \
                  "xmldata) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            tgt_cur.executemany(sql, source_results)
            print('Complate [Insert:{0}]'.format(rowcount))
        elif table_name == 'notificationlist':
            source_cur.execute("select nl.notificationuid,nl.destinationuid,nl.destinationtype,nl.activate,"
                               "n.notificationtype from notificationlist as nl "
                               "join notification as n on nl.notificationuid=n.notificationuid ".format(table_name))

            rowcount = source_cur.rowcount
            source_results = source_cur.fetchall()
            sql = "INSERT INTO public.notificationlist(notificationuid, destinationuid, destinationtype, " \
                  "activate, notificationtype) VALUES (%s, %s, %s, %s, %s);"
            tgt_cur.executemany(sql, source_results)
            print('Complate [Insert:{0}]'.format(rowcount))
        else:
            # raise
            print('error table {0}'.format(table_name))
            global error_table
            error_table = table_name
    finally:
        source_cur.close()
        source_conn.close()
        tgt_cur.close()
        tgt_conn.close()

    # time_end = time.time()
    # time_c = time_end - time_start
    # print("copy {0} time cost {1} s".format(table_name, time_c))


def truncate_extra_table(target_conn_set, table_name):
    # tgt_conn
    log_list = ['tasklog', 'taskjoblog', 'tasksteplog', 'receivefilelog', 'taskstepfulllog',
                'taskstepoutputlog', 'taskstatusreasonhistory']
    if table_name in log_list:
        t_connectparams = dict(entry.split('=') for entry in target_conn_set.split(','))
        tgt_conn = psycopg2.connect(**t_connectparams)
        tgt_cur = tgt_conn.cursor()
        tgt_cur.execute(
            "select tablename from pg_tables where schemaname='partition_zone' "
            "and tablename like '{0}_%';".format(table_name))
        tgt_results = tgt_cur.fetchall()
        for i in tgt_results:
            print("drop table partition_zone.{0}".format(str(i[0])))
            tgt_cur.execute("drop table partition_zone.{0}".format(str(i[0])))
            tgt_conn.commit()
        tgt_cur.close()
        tgt_conn.close()
    else:
        pass
    if table_name == 'taskstepoutputlog':
        t_connectparams = dict(entry.split('=') for entry in target_conn_set.split(','))
        tgt_conn = psycopg2.connect(**t_connectparams)
        tgt_cur = tgt_conn.cursor()
        tgt_cur.execute("truncate table public.disfulltextsearchqueue;")
        print('truncate table public.disfulltextsearchqueue;')
        tgt_conn.commit()
        tgt_cur.close()
        tgt_conn.close()
    elif table_name == 'frequency':
        t_connectparams = dict(entry.split('=') for entry in target_conn_set.split(','))
        tgt_conn = psycopg2.connect(**t_connectparams)
        tgt_cur = tgt_conn.cursor()
        tgt_cur.execute("truncate table public.frequencyschedule;")
        print('truncate table public.frequencyschedule;')
        tgt_conn.commit()
        tgt_cur.close()
        tgt_conn.close()
    elif table_name == 'jobexecution':
        t_connectparams = dict(entry.split('=') for entry in target_conn_set.split(','))
        tgt_conn = psycopg2.connect(**t_connectparams)
        tgt_cur = tgt_conn.cursor()
        tgt_cur.execute("truncate table public.jobexecutionschedule;")
        print('truncate table public.jobexecutionschedule;')
        tgt_conn.commit()
        tgt_cur.close()
        tgt_conn.close()
    else:
        pass


def clear_null(target_conn_set):
    t_connectparams = dict(entry.split('=') for entry in target_conn_set.split(','))
    tgt_conn = psycopg2.connect(**t_connectparams)
    tgt_cur = tgt_conn.cursor()

    # clear null
    tgt_cur.execute("UPDATE public.jobexecution SET holidayuid = '', conflictrule = '0' WHERE holidayuid IS NULL;")
    tgt_cur.execute("UPDATE public.frequency SET wcalendaruid = '' WHERE wcalendaruid IS NULL;")
    tgt_cur.execute("UPDATE public.frequency SET wcalendaruid = '' WHERE wcalendaruid IS NULL;")
    tgt_cur.execute("UPDATE public.frequency SET bywcalendar = '0' WHERE bywcalendar IS NULL;")
    tgt_cur.execute("UPDATE public.frequency SET xmldata = '' WHERE xmldata IS NULL;")
    tgt_cur.execute("UPDATE public.frequency SET manuallyedit = '0' WHERE manuallyedit IS NULL;")
    tgt_cur.execute("UPDATE public.job SET onlinedatetime = '' WHERE onlinedatetime IS NULL;")
    tgt_cur.execute("UPDATE public.job SET offlinedatetime = '' WHERE offlinedatetime IS NULL;")
    tgt_cur.execute("UPDATE public.jobflow SET onlinedatetime = '' WHERE onlinedatetime IS NULL;")
    tgt_cur.execute("UPDATE public.jobflow SET offlinedatetime = '' WHERE offlinedatetime IS NULL;")

    print("UPDATE public.jobexecution SET holidayuid = '', conflictrule = '0' WHERE holidayuid IS NULL;\n"
          "UPDATE public.frequency SET wcalendaruid = '' WHERE wcalendaruid IS NULL;\n"
          "UPDATE public.frequency SET wcalendaruid = '' WHERE wcalendaruid IS NULL;\n"
          "UPDATE public.frequency SET bywcalendar = '0' WHERE bywcalendar IS NULL;\n"
          "UPDATE public.frequency SET xmldata = '' WHERE xmldata IS NULL;\n"
          "UPDATE public.frequency SET manuallyedit = '0' WHERE manuallyedit IS NULL;\n"
          "UPDATE public.job SET onlinedatetime = '' WHERE onlinedatetime IS NULL;\n"
          "UPDATE public.job SET offlinedatetime = '' WHERE offlinedatetime IS NULL;\n"
          "UPDATE public.jobflow SET onlinedatetime = '' WHERE onlinedatetime IS NULL;\n"
          "UPDATE public.jobflow SET offlinedatetime = '' WHERE offlinedatetime IS NULL;")
    tgt_conn.commit()
    tgt_cur.close()
    tgt_conn.close()


def menu():
    print('copy data version:3.1.0')
    fun_menu = input("Update:1\nCopy log:2\nCustom mode:0\nselect function:")
    table_list = []
    global error_table
    error_table = None
    error_table_list = []
    while True:
        if fun_menu in ['1', '2', '1-cfg', '2-cfg']:
            if fun_menu in ['1', '2']:
                input_list = input_values()
                source_conn_set = input_list[0]
                target_conn_set = input_list[1]
                pg_size = input_list[2]
            else:
                conf_list = conf_mode()
                source_conn_set = conf_list[0]
                target_conn_set = conf_list[1]
                pg_size = conf_list[2]

            if fun_menu in ['1', '1-cfg']:
                table_list = ['busentity', 'disconfig', 'domain', 'role', 'trinityuser', 'accessright',
                              'auditing', 'busentityvariable', 'busentitycategory', 'connection', 'connectioncategory',
                              'connectionlibprop', 'connectionlibref', 'connectionrelation', 'dmextjar', 'dmextpackage',
                              'dmextrule', 'domainresource', 'domainvariable', 'excludefrequency',
                              'excludefrequencylist', 'filesource', 'filesourcecategory', 'filesourcerelation',
                              'flowtargetmetatable', 'freqexclude', 'frequency', 'frequencycategory', 'frequencylist',
                              'frequencyrelation', 'groupmember', 'housekeepinglist', 'housekeepingrule', 'jcsagent',
                              'jcsvirtualagent', 'jcsvirtualagentlist', 'job', 'jobcategory', 'jobcheckoutstatus',
                              'jobdependencyrule', 'jobdependencyruleversion', 'jobexclude', 'jobexecution', 'jobflow',
                              'jobflowcheckoutstatus', 'jobflowexclude', 'jobflowmap', 'jobflowmapversion',
                              'jobflowmask', 'jobflowtxdate', 'jobflowversion', 'jobflowversionhistory', 'jobgroup',
                              'jobgrouplist', 'jobnote', 'jobowner', 'jobsourcemetatable', 'jobsourcemetatableversion',
                              'jobstep', 'jobstepdm', 'jobstepdmversion', 'jobstepversion', 'jobstream',
                              'jobstreamversion', 'jobtargetmetatable', 'jobtargetmetatableversion', 'jobtxdate',
                              'rolemember', 'jobversion', 'jobversionhistory', 'metadatabase', 'metalookuptable',
                              'metareportnotificationlist', 'metatable', 'metatablelayout', 'metatablelayouthistory',
                              'metatabletxdate', 'mutex', 'mutexcategory', 'mutexjob', 'mutexrelation', 'notification',
                              'notificationlist', 'objectalias', 'plugin', 'plugincategory', 'plugincategorylist',
                              'pluginlicense', 'pluginproperty', 'usergroup', 'workingcalendar', 'workingcalendarlist',
                              'frequencyschedule', 'jobalert', 'jobalertversion', 'jobexecutionschedule',
                              'jobexecutionversion', 'jobstatistic', 'jobvariable', 'jobvariableversion',
                              'maskclass', 'tokenclass', 'encryptclass'
                              ]
            elif fun_menu in ['2', '2-cfg']:
                table_list = ['task', 'tasklog', 'taskjoblog', 'tasksteplog', 'receivefilelog', 'taskstepfulllog',
                              'taskstepoutputlog', 'taskstatusreasonhistory']
            while True:
                yes = input('enter (y/yes) contunue:')
                if yes in ['y', 'yes']:
                    break
                else:
                    continue
            # run copy
            for table_name in table_list:
                cptable(source_conn_set, target_conn_set, pg_size, table_name)
                if error_table is None:
                    continue
                else:
                    error_table_list.append(error_table)
                    error_table = None
                    continue
            # last update
            if fun_menu in ['1', '1-cfg']:
                t_connectparams = dict(entry.split('=') for entry in target_conn_set.split(','))
                tgt_conn = psycopg2.connect(**t_connectparams)
                tgt_cur = tgt_conn.cursor()
                # key 3rd
                # tgt_cur.execute("UPDATE trinityconfig SET versionid = '4.1.1';")
                # tgt_cur.execute("INSERT INTO public.disconfig VALUES ('lc', 'np.type', '1', '') "
                #                 "ON CONFLICT ON CONSTRAINT disconfig_pkey DO NOTHING;")
                tgt_cur.execute("update disconfig set value="
                                "(select value from disconfig_installtmp "
                                "where module='server' and configname='serverIP')"
                                "where module='server' and configname='serverIP'")
                tgt_cur.execute("truncate table primaryjcsqueue")
                tgt_cur.execute("truncate table primarytaskqueue")
                tgt_cur.execute("truncate table standbyjcsqueue")
                tgt_cur.execute("truncate table standbytaskqueue")
                tgt_cur.execute("truncate table standbyoutputqueue")
                tgt_cur.execute("truncate table taskconsolequeue")
                tgt_cur.execute("truncate table disfulltextsearchqueue")

                tgt_conn.commit()
                tgt_cur.close()
                tgt_conn.close()
                clear_null(target_conn_set)
            else:
                pass
            print('error table {0}'.format(error_table_list))
            not_exist = ['frequencyschedule', 'jobalert', 'jobalertversion', 'jobexecutionschedule',
                         'jobexecutionversion', 'jobstatistic', 'jobvariable', 'jobvariableversion']
            if error_table_list == not_exist:
                print('4.0 source db does not exist these table')
            else:
                pass

            break
        elif fun_menu in ['0', '0-cfg']:
            if fun_menu == '0':
                input_list = input_values()
                source_conn_set = input_list[0]
                target_conn_set = input_list[1]
                pg_size = input_list[2]
            else:
                conf_list = conf_mode()
                source_conn_set = conf_list[0]
                target_conn_set = conf_list[1]
                pg_size = conf_list[2]

            u_table = input("enter table:")
            table_list = u_table.split(',')
            print(table_list)
            while True:
                yes = input('enter (y/yes) contunue:')
                if yes in ['y', 'yes']:
                    break
                else:
                    continue
            # run copy
            for table_name in table_list:
                cptable(source_conn_set, target_conn_set, pg_size, table_name)
                if error_table is None:
                    continue
                else:
                    error_table_list.append(error_table)
                    error_table = None
                    continue
            print('error table {0}'.format(error_table_list))

            break
        else:
            continue
    input("Please press the Enter key to proceed")


if __name__ == "__main__":
    global error_table
    menu()
