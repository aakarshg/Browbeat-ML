from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Grabs data out of ElasticSearch, prepares for usage
# If you want to use data out of multiple ES's
# just create multiple backend objects


class Backend(object):

    def __init__(self, host, port):
        self.es = Elasticsearch([
            {'host': host,
             'port': port}],
            send_get_body_as='POST',
            retries=True,
            timeout=120)

    def grab_uuids_by_date(self, version, time_period):
        query = {
            "query": {
                "filtered": {
                    "query": {"match": {'version.osp_version': version}},
                    "filter": {
                        "range": {"timestamp": {"gt": "now-" + time_period}}
                        }
                    }
                },
            "size": 0,
            "aggs": {
                "langs": {
                    "terms": {"field": "browbeat_uuid", "size": 15000}
                    # size is max number of unique uuids that can be expected.
                    }
                }
            }
        res = self.es.search(index="browbeat-rally-*", body=query)
        if res == []:
            raise ValueError("No results!")
        number_uuid = len(res['aggregations']['langs']['buckets'])
        uuid_list = []
        for x in range(number_uuid):
            uuid = res['aggregations']['langs']['buckets'][x]['key']
            uuid_list.append(uuid)
        return uuid_list

    # Searches and grabs the raw source data for a Browbeat UUID
    def grab_uuid(self, uuid):
        query = {"query": {"match": {'browbeat_uuid': uuid}}}
        # Should use scroll later on but meanwhile using search
        # But because ideally we dont see that many hits
        # search isn't entirely bad. but in future if hits are in thousands
        # use scroll
        res = self.es.search(index="browbeat-rally-*", body=query, size=1000)
        # size ^ above is set to 1000, as we've never exceeded more than
        # 300 entries for the uuids we've seen so far
        if res == []:
            raise ValueError(uuid + " Has no results!")
        # As we switch from scroll api, we're using search to make sure
        # elasticsearch doesnt keep hitting errors
        return res['hits']['hits']

    def compute_start_end(self, uuid):
        query_input = {
            "query": {
                "match": {
                    'browbeat_uuid': uuid
                    }
                },
            "size": 1,
            "aggs": {
                "max_time": {
                    "max": {
                        "field": "timestamp"
                        }
                    },
                "min_time": {
                    "min": {
                        "field": "timestamp"
                        }}}}
        res = self.es.search(index="browbeat-rally-*", body=query_input)
        start = int(res['aggregations']['min_time']['value'])
        end = int(res['aggregations']['max_time']['value'])
        cloud_name = res['hits']['hits'][0]['_source']['cloud_name']
        grafana_url = \
            res['hits']['hits'][0]['_source']['grafana_url'][0]
        for dashboard in grafana_url:
            graphite_url = grafana_url[dashboard].split(":")[1].strip("/")
            graphite_port = "80"
            graphite_url = "http://{}:{}".format(graphite_url, graphite_port)
        return [start, end, cloud_name, graphite_url]

    def compute_hits(self, start, end, cloud_name, level_type):
        time_dict = {
            "format": "epoch_millis"
        }
        time_dict["gte"] = start
        time_dict["lte"] = end
        query_input = {
            "query": {
                "bool": {
                    "must": {
                        "query_string": {
                            "query": "browbeat.cloud_name: \
                            " + cloud_name + " AND level: " + level_type
                            }
                        },
                    "filter": {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "@timestamp": time_dict
                                    }
                                }
                            ],
                            "must_not": []
                            }}}}}
        res = self.es.search(index="logstash-*", body=query_input)
        return res['hits']['total']


    def push_summary_es(self, uuid, osp_version, test_name, mean, std_dev,
                        perc95_score, output_prediction, ovn):
        data={
        "browbeat_uuid":str(uuid),
        "osp_version":str(osp_version),
        "action":str(test_name),
        "mean":mean,
        "std_dev":std_dev,
        "percentile_95":perc95_score,
        "class": output_prediction[0],
        "with_ovn": ovn
        }
        self.es.index(index='bml_summary', doc_type='result', body=data)
