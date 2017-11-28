import os

class config_path:
    def __init__(self):
        """
        All useful paths ans settings
        """
        self.wd = os.path.abspath('')

        # ES settings
        self.logs_dir = os.path.join(self.wd, 'logs') # directory of the logs files
        self.host = 'localhost'
        self.port = 9200
        self.index = "my_index"
        self.doc_type = "my_doc_type"

        # Databases settings
        self.database_to_index = os.path.join(self.wd, "index.csv")
        self.string_to_search = os.path.join(self.wd, 'to_be_searched.txt')
