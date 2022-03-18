import time
import re
from elasticsearch import Elasticsearch


# 详见https://cloud.tencent.com/developer/article/1587935
class es_api(object):
    def __init__(self):
        self.url = "http://hdp928.bigdata.lyjt.360es.cn:9200"
        self.port = ""
        self.username = "zhangpeng20"
        self.password = "F&uYYpb5e!dgzGozS7s9"
        self.searchIndex = "secops0:secops-opencanary_*"

    def connect(self):
        self.es = Elasticsearch(hosts=self.url, http_auth=(self.username, self.password),
                                max_retries=10, retry_on_timeout=True)

    def search_data_dsl(self, dsl):
        self.query = self.es.search(index=self.searchIndex, body=dsl,timeout="2s",scroll="2m")
        return self.query


def time2timestamp(time_str):
    ts = time.mktime(time.strptime(str(time_str), '%Y-%m-%d %H:%M:%S'))
    thirteen_ts = int(ts) * 1000
    return int(thirteen_ts)


if __name__ == "__main__":
    q = es_api()
    q.connect()
    start = time2timestamp('2021-08-01 00:00:00')
    end = time2timestamp('2021-09-01 16:19:00')
    dsl = {
        # "sort": [
        #        {
        #            "@timestamp": {
        #                "order": "asc"
        #            }
        #        }
        #    ],
        "query": {
            "bool": {
                # "must":[
                #     {"match":{"sourceAddress": "10.252.145.90"}},
                #     #{"match": {"deviceCustomString4": "shell"}}
                # ],
                "filter": [
                    {"range": {"deviceReceiptTime": {"gte": start, "lte": end, "format": "epoch_millis"}}},

                    # {"match": {"name": "连接"}},
                    # {"match": {"destinationAddress": "1.27.223.45"}},
                    # {"match":{"deviceCustomString5 ": "*47.110.23.48*"}},
                ]
                # {"term":{"sourceAddress": "10.252.145.90"}}],

            }
        }
    }
    dsl = {
        "size": 10000,
        "_source": {
            "includes": ["destinationAddress"],
            "excludes": []
        },
        "query": {
            "bool": {
                "filter": [
                    {"range": {"deviceReceiptTime": {"gte": start, "lte": end, "format": "epoch_millis"}}}
                ]

            }
        },
        "aggregations": {
            "cardinality_field": {
                "cardinality": {
                    "field": "destinationAddress"
                }
            }
        }
    }
    q.search_data_dsl(dsl=dsl)


{"index":"secops1:secops-sysmon_*","ignore_unavailable":true,"preference":1641812943172}
{"version":true,"size":500,"sort":[{"deviceReceiptTime":{"order":"desc","unmapped_type":"boolean"}}],"_source":{"excludes":[]},"aggs":{"2":{"date_histogram":{"field":"deviceReceiptTime","interval":"30s","time_zone":"Asia/Shanghai","min_doc_count":1}}},"stored_fields":["*"],"script_fields":{},"docvalue_fields":[{"field":"agentReceiptTime","format":"date_time"},{"field":"deviceCustomDate1","format":"date_time"},{"field":"deviceCustomDate2","format":"date_time"},{"field":"deviceReceiptTime","format":"date_time"},{"field":"endTime","format":"date_time"},{"field":"fileCreateTime","format":"date_time"},{"field":"fileModificationTime","format":"date_time"},{"field":"flexDate1","format":"date_time"},{"field":"managerReceiptTime","format":"date_time"},{"field":"oldFileCreateTime","format":"date_time"},{"field":"oldFileModificationTime","format":"date_time"},{"field":"startTime","format":"date_time"},{"field":"timeStamp","format":"date_time"}],"query":{"bool":{"must":[{"query_string":{"query":"sourceAddress:[10.43.120.0 TO 10.43.120.255] AND destinationProcessName:*chrome.exe*","analyze_wildcard":true,"default_field":"*"}},{"range":{"deviceReceiptTime":{"gte":1641813661328,"lte":1641814561328,"format":"epoch_millis"}}}],"filter":[],"should":[],"must_not":[]}},"highlight":{"pre_tags":["@kibana-highlighted-field@"],"post_tags":["@/kibana-highlighted-field@"],"fields":{"*":{}},"fragment_size":2147483647},"timeout":"30000ms"}
