from util import connect_crdb

# This is to update the version name to the specifi osp_version
# If it is master, as based on the cycle master can be one of the versions
def update_osp_version(config, osp_name):
    if "master" in str(osp_name):
        return config['master'][0]
    else:
        return osp_name


def insert_grades_db(config, uuid, test, osp_name, avg_runtime, grade,
                     time_stamp, puddle, dlrn, concurrency, times,
                     perc95_score, ovn):
    conn = connect_crdb(config)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    classify = True
    osp_name = update_osp_version(config, osp_name)
    cur.execute("INSERT INTO {} VALUES ('{}', '{}', '{}', {}, '{}', '{}', \
                '{}', '{}', {}, {}, {}, {},'{}');" .format(config['table_name'][0],
                                                      str(uuid), str(test),
                                                      str(osp_name),
                                                      float(avg_runtime),
                                                      str(time_stamp),
                                                      str(puddle), str(dlrn),
                                                      bool(classify),
                                                      int(grade),
                                                      int(concurrency),
                                                      int(times),
                                                      float(perc95_score),
                                                      str(ovn)))


def insert_values_db(config, uuid, test, osp_name, avg_runtime,
                     time_stamp, puddle, dlrn, concurrency, times,
                     perc95_score, ovn):
    conn = connect_crdb(config)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    classify = False
    osp_name = update_osp_version(config, osp_name)
    cur.execute("INSERT INTO {} (uuid, test, osp_version, avg_runtime, \
                timestamp, rhos_puddle, dlrn_hash, classify, concurrency, \
                times, percentile95, ovn) VALUES ('{}', '{}', '{}', {}, '{}', \
                '{}', '{}', '{}', {}, {}, {},'{}')".format(config['table_name'][0],
                                                 str(uuid), str(test),
                                                 str(osp_name),
                                                 float(avg_runtime),
                                                 str(time_stamp), str(puddle),
                                                 str(dlrn), bool(classify),
                                                 int(concurrency), int(times),
                                                 float(perc95_score),
                                                 str(ovn)))


def insert_errors_db(config, uuid, errors):
    conn = connect_crdb(config)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    name_table = config['table_errors'][0]
    cur.execute("INSERT INTO {} VALUES ('{}', {});" .format(name_table,
                                                            str(uuid),
                                                            errors))


def insert_logsummary_db(config, uuid, num_errors, num_warn,
                         num_debug, num_notice, num_info):
    conn = connect_crdb(config)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("INSERT INTO {} VALUES ('{}', \
                {}, {}, {}, {}, {});".format(config['table_logsummary'][0],
                                             str(uuid),
                                             int(num_errors),
                                             int(num_warn),
                                             int(num_debug),
                                             int(num_notice),
                                             int(num_info)))

def insert_timeseriessummaries_db(config, uuid, cpu_system, cpu_user,
                                  cpu_softirq, cpu_wait, mem_used,
                                  mem_slabunrecl):
    conn = connect_crdb(config)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("INSERT INTO {} VALUES ('{}', {}, {}, {}, {}, {}, {},\
                {}, {}, {}, {}, {}, {});".format(config['table_timeseries'][0],
                                                 str(uuid),
                                                 round(cpu_system[0],2),
                                                 round(cpu_system[1],2),
                                                 round(cpu_user[0],2),
                                                 round(cpu_user[1],2),
                                                 round(cpu_softirq[0],2),
                                                 round(cpu_softirq[1],2),
                                                 round(cpu_wait[0],2),
                                                 round(cpu_wait[1],2),
                                                 round(mem_used[0],2),
                                                 round(mem_used[1],2),
                                                 round(mem_slabunrecl[0],2),
                                                 round(mem_slabunrecl[1],2)))
