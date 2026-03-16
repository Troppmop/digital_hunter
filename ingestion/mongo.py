from pymongo import MongoClient

class MongoConnect:
    def __init__(self):
        self.uri = "mongodb://localhost:27017"
        self.client = MongoClient(self.uri)
        self.db = self.client['test']
        self.col = self.db['intake']

    def insert(self, doc):
        self.col.insert_one(doc)