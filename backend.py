from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import tensorflow as tf
import numpy

# Grabs data out of ElasticSearch, prepares for usage
# If you want to use data out of multiple ES's just have multiple backends

class Backend(object):

    def __init__(self, host, port):
        self.es = Elasticsearch([
            {'host': host,
             'port': port}],
            send_get_body_as='POST',
            retries=True,
            sniff_on_start=False,
            sniff_on_connection_fail=True,
            sniffer_timeout=60,
            timeout=60)

    # Utility function, prints a list of UUIDS meeting a serious of requirements
    #  otherwise it's hard to gather a list of training vectors to investigate
    #  Takes forever to run and requires an absurd amount of ram, use with caution
    def get_uuids_by_cloud(self, config):
      uuids = []
      for cloud_name in config['clouds']:
        print(cloud_name)
        results = helpers.scan(self.es, {"query": {"match": {'cloud_name': cloud_name}}}, size=100,request_timeout=1000)
        for result in results:
          uuid = result['_source']['browbeat_uuid']
          if uuid not in uuids:
            uuids.append(uuid)

      for uuid in uuids:
        print(uuid)

    # Searches and grabs the raw source data for a Browbeat UUID
    def grab_uuid(self, uuid):
        results = helpers.scan(self.es, {"query": {"match": {'browbeat_uuid': uuid}}}, size=100,request_timeout=1000)

        if results == []:
            print(uuid + " Has no results!")
            exit(1)

        return results