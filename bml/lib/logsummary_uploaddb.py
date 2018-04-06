from elasticsearch import Elasticsearch
from util import connect_crdb
from browbeat_run import browbeat_run


def insert_logsummary_db(es, config, uuid):
    brun = browbeat_run(es, uuid, timeseries=True)
    graphite_details = brun.get_graphite_details()
    start = graphite_details[1]
    end = graphite_details[2]
    cloud_name = str(graphite_details[3])
    num_errors = es.compute_hits(start, end, cloud_name, 'error')
    num_warn = es.compute_hits(start, end, cloud_name, 'warning')
    num_debug = es.compute_hits(start, end, cloud_name, 'debug')
    num_notice = es.compute_hits(start, end, cloud_name, 'notice')
    num_info = es.compute_hits(start, end, cloud_name, 'info')
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
