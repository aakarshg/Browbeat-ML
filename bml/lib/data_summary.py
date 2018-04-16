from browbeat_run import browbeat_run
from perf_classifier import classify_value
import numpy
from update_crdb import insert_values_db
from update_crdb import insert_grades_db
from update_crdb import insert_errors_db
from util import longest_test_name


def check_hash(hash_value, puddle):
    if ("pipeline" in hash_value) or ("trunk" in hash_value):
        if 'pipeline' not in puddle:
            return True
        else:
            return False
    else:
        return True


def time_summary(config, es_backend, time_period, osp_version, update):
    if osp_version is not None:
        uuids = es_backend.grab_uuids_by_date(osp_version, time_period)
    else:
        uuids = []
        for version in config['watched_versions']:
            uuids.extend(es_backend.grab_uuids_by_date(version, time_period))
    for uuid in uuids:
        val = print_run_details(config, es_backend, uuid, update)
        if val is not False:
            print(val)


def summary_uuid(es_backend, config, uuid, update):
    val = print_run_details(config, es_backend, uuid, update)
    if val is not False:
        print(val)
        if "FAIL" in str(val):
            exit(1)
        print("Found no problems with the uuid")


def data_summary(data):
    std_dev = "{:.2f}".format(numpy.std(data)).ljust(10)
    avg = "{:.2f}".format(numpy.mean(data)).ljust(10)
    median = "{:.2f}".format(numpy.median(data)).ljust(10)
    percentile95 = "{:.2f}".format(numpy.percentile(data, 95)).ljust(10)
    summary = [avg, std_dev, median, percentile95]
    return(summary)


# Used for an easy way to look at run data
def print_run_details(config, es_backend, uuid, update):
    brun = browbeat_run(es_backend, uuid, caching=True)
    output_string = ""
    osp_version = ""
    ovn = ""
    padding = longest_test_name(config)
    test_clean_count = 0  # count of the tests that are being classified
    '''
    conn=sqlite3.connect('database.sqlite')
    c=conn.cursor()
    '''
    for test_type in config['tests']:
        test_name = test_type['test']
        data = []
        test_run = None
        check_outcome = 0  # just to see if it passses all conditions
        for test_run in brun.get_tests(test_search=test_name):
            data.extend(test_run.raw)
            osp_version = test_run.version
            ovn = test_run.ovn
        if test_run is None:
            continue
        statistics_uuid = data_summary(data)
        average_runtime = statistics_uuid[0]
        perc95_score = statistics_uuid[3]
        output_string += test_name.ljust(padding) + \
            " " + str(average_runtime) + " " + str(statistics_uuid[1]) + " " +\
            perc95_score
        test_original = test_name
        if str(test_name) in config['test_with_scenario_list']:
            test_name = str(test_run.scenario_name) + "." + str(test_name)
        times = test_run.times
        concurrency = test_run.concurrency
        time_check = float(average_runtime) > 0.0
        test_checks = test_original not in config['ignored_tests']
        cloud_check = test_run.cloud_name in config['clouds']
        dlrn_hash = test_run.dlrn_hash
        puddle = test_run.rhos_puddle
        hash_check = check_hash(dlrn_hash, puddle)
        output_prediction = "None"
        if time_check and cloud_check and hash_check:
            check_outcome = 1
            if test_checks:
                output_prediction = classify_value(config, average_runtime,
                                                   test_name, osp_version,
                                                   times, concurrency)
                test_clean_count += 1
                if update:
                    insert_grades_db(config, uuid, test_name, osp_version,
                                     average_runtime, output_prediction,
                                     test_run.timestamp, puddle,
                                     dlrn_hash, concurrency, times,
                                     perc95_score, ovn)
                if int(output_prediction) == 1:
                    print("ALERT!!!!")
                    print(uuid, test_name, osp_version, average_runtime)
                    output_string = output_string + "FAIL" + "\n"
                else:
                    output_string \
                        = output_string + str(output_prediction) + "\n"
            else:
                if update:
                    insert_values_db(config, uuid, test_name, osp_version,
                                     average_runtime, test_run.timestamp,
                                     puddle, dlrn_hash, concurrency, times,
                                     perc95_score, ovn)
                output_string += "\n"
        else:
            output_string += "\n"
        if update:
            es_backend.push_summary_es(uuid, osp_version, test_name,
                                       average_runtime,
                                       statistics_uuid[1], perc95_score,
                                       output_prediction, ovn)
    '''
    conn.commit()
    conn.close()
    '''
    header = ("Browbeat UUID: " + uuid + " OSP_version: " + osp_version + "\n")
    if check_outcome == 1:
        total_errors = brun.get_num_errors()
        if total_errors > 1000:
            total_errors = str(total_errors) + " FAIL"
        else:
            total_errors = str(total_errors)
        header = header + ("Errors: " + total_errors + "\n")
        if test_clean_count >= 13:
            if update:
                insert_errors_db(config, uuid, brun.get_num_errors())
    header += ("".ljust(80, "-")) + "\n"
    output_string = header + output_string
    return output_string
