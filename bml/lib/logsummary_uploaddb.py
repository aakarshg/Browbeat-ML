from util import connect_crdb
from browbeat_run import browbeat_run
from update_crdb import insert_logsummary_db


def logsummary_db(es, config, uuid):
    brun = browbeat_run(es, uuid, timeseries=True)
    graphite_details = brun.get_graphite_details()
    start = graphite_details[1]
    end = graphite_details[2]
    cloud_name = str(graphite_details[3])
    num_errors = int(es.compute_hits(start, end, cloud_name, 'error'))
    num_warn = int(es.compute_hits(start, end, cloud_name, 'warning'))
    num_debug = int(es.compute_hits(start, end, cloud_name, 'debug'))
    num_notice = int(es.compute_hits(start, end, cloud_name, 'notice'))
    num_info = int(es.compute_hits(start, end, cloud_name, 'info'))
    insert_logsummary_db(config, uuid, num_errors, num_warn, num_debug,
                         num_notice, num_info)
