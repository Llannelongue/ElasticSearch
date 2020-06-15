import datetime
import logging
import os
import time
from config import config_path
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q



class SearchES(config_path):
    def __init__(self, n_results=10):
        """
        Class to search a single str into the indexed database.
        Required:
        - ElasticSearch version 5.x (https://www.elastic.co/guide/en/elasticsearch/reference/5.2/_installation.html)
        It should work with ES 6.x, although it has not been tested (https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)
        - Python client for ElasticSearch, this script was developed with version 5.x (https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html)
        - High-level python client, ElasticSearch-dsl, this script was developed with version 5.x (http://elasticsearch-dsl.readthedocs.io/en/latest/)
        - config.py file for the different paths
        :param n_results: [int] (optional, default=10) number of results to be returned.
        """
        config_path.__init__(self)

        self.n_results = n_results

        self.dt = datetime.datetime.now()
        self.start_time = time.time()

        self.log_file_name = os.path.join(self.logs_dir,
                                          "searching_%s%s%s_%s_%s" % (self.dt.year, self.dt.month, self.dt.day,
                                                                      self.dt.hour, self.dt.minute))
        logging.basicConfig(filename=self.log_file_name,
                            level=logging.INFO,
                            format='%(asctime)s %(message)s',
                            datefmt='%Y/%m/%d %H:%M:%S')

    def searchIT(self):
        """
        :return: [list] list of the results
        """
        es = Elasticsearch([{'host': self.host, 'port': self.port}])

        query = open(self.string_to_search, 'r').read()  # Single string to be searched, in a txt file.

        fields = ['title', 'main_text'] # fields to be searched into
        q = Q("multi_match", query=query, fields=fields)

        s = Search(using=es, index=self.index).query(q)[:self.n_results]
        response = s.execute()

        n_hits = response.hits.total.value # number of results

        logging.info("\n=========================================================================================")
        logging.info("host: %s \t port: %s \t index: %s \t doc_type: %s" % (self.host, self.port,
                                                                            self.index, self.doc_type))
        logging.info("Nb of hits: %s" % n_hits)
        logging.info("Running time: %0.2f seconds" % (time.time() - self.start_time))

        if n_hits == 0:
            return([])
        else:
            L = []
            for j in range(min(n_hits,self.n_results)):
                L.append(response.hits[j].title + ',' + response.hits[j].year)
            return(L)


if __name__ == '__main__':
    print(SearchES(n_results=5).searchIT())
