from elasticsearch import Elasticsearch

class Elastic:
    def __init__(self):
        self.es = Elasticsearch('http://localhost:9200')
        self.index_name = 'tweets'

    def mapping(self):

        mappings = {
            "mappings": {
                "properties": {
                    "TweetID": {
                        "type": "keyword"
                    },
                    "CreateDate": {
                        "type": "text"
                    },
                    "Antisemitic": {
                        "type": "integer"
                    },
                    "text": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            }
        }
        self.es.indices.create(index=self.index_name, body=mappings)

    def index_to_elastic(self, documents: list[dict]):
        for index, document in enumerate(documents):
            r = self.es.index(index=self.index_name, id=str(index), document=document)
            print(f'inserted {index} documents')

    def update(self):
        self.es.update()


