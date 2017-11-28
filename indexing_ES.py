import datetime
import logging
import os
import time
import pandas as pd
from config import config_path
from elasticsearch import Elasticsearch


class IndexES(config_path):
    def __init__(self, delete_index=True):
        """
        Class locally indexing a database on ElasticSearch.
        Required:
        - ElasticSearch version 5.x (https://www.elastic.co/guide/en/elasticsearch/reference/5.2/_installation.html)
        It should work with ES 6.x, although it has not been tested (https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)
        - Python client for ElasticSearch, this script was developed with version 5.x (https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html)
        - config.py file for the different paths
        :param delete_index: [bool] (optional, default = True) whether or not the existing index should be deleted first.
        """
        config_path.__init__(self)

        self.delete_index = delete_index

        self.dt = datetime.datetime.now()
        self.start_time = time.time()

        self.log_file_name = os.path.join(self.logs_dir,
                                          "indexing_%s%s%s_%s_%s" % (self.dt.year, self.dt.month, self.dt.day,
                                                                      self.dt.hour, self.dt.minute))

    def indexIT(self):
        """
        Indexing of the database.
        :return: none
        """
        logging.basicConfig(filename=self.log_file_name,
                            level=logging.INFO,
                            format='%(asctime)s %(message)s',
                            datefmt='%Y/%m/%d %H:%M:%S')

        es = Elasticsearch([{'host': self.host, 'port': self.port}])

        if self.delete_index:
            # To delete any pre-existing index if necessary
            es.indices.delete(index=self.index, ignore=[400, 404])
            print("index %s deleted" % self.index)

        database = pd.read_csv(self.database_to_index,
                               header=0,
                               sep=',',
                               encoding='utf8')
        print('data loaded')

        for i in range(0, len(database)):
            doc = {
                # Insert here the different fields/columns of the database to be indexed
                "title": str(database.title[i]),
                "main_text": str(database.main_text[i]),
                "year": str(database.year[i])
            }

            _ = es.index(index=self.index, doc_type=self.doc_type, id=i, body=doc)

            if (i % 50 == 0) & (i != 0):
                print("%s/%s" % (i, len(database)))

            logging.info("\n=========================================================================================")
            logging.info("host: %s \t port: %s \t index: %s \t doc_type: %s" % (self.host, self.port,
                                                                                self.index, self.doc_type))
            logging.info("Data indexed: %s" % os.path.basename(os.path.normpath(self.database_to_index)))
            logging.info("n_lines indexed: %s" % (len(database)))
            logging.info("Running time: %0.3f seconds" % (time.time() - self.start_time))
            logging.info("-- by line: %0.6f seconds" % ((time.time() - self.start_time) / (len(database))))

if __name__ == '__main__':
    idx = IndexES(delete_index=True)
    idx.indexIT()