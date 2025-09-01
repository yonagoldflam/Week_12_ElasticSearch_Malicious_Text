from elasticsearch import Elasticsearch, helpers
from elastic.classifier import Classifier


class Elastic:
    def __init__(self):
        self.es = Elasticsearch('http://es:9200')
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
        actions = []
        for index, document in enumerate(documents):
            actions.append(
                {
                    '_index': self.index_name,
                    '_id': str(index),
                    '_source': document
                }
            )
        helpers.bulk(self.es, actions)
        print(f'inserted {len(documents)} documents')

    def add_sentiment_field(self):
        docs = helpers.scan(self.es, index=self.index_name, query={"query": {"match_all": {}}})

        actions = []
        for doc in docs:
            doc_id = doc['_id']
            text = doc["_source"]["text"]

            sentiment = Classifier.sentiment_of_text(text)

            actions.append({'_op_type':'update', '_index':self.index_name, '_id': doc_id, 'doc': {'sentiment': sentiment}})

        if actions:
            helpers.bulk(self.es, actions)
            print(f' updated {len(actions)} sentiments')

    def add_weapon_field(self, weapon):
        docs = helpers.scan(self.es, index=self.index_name, query={"query": {"match_all": {}}})

        actions = []
        for doc in docs:
            doc_id = doc['_id']
            text = doc["_source"]["text"]

            weapons = Classifier.find_weapons(text, weapon)

            actions.append({'_op_type':'update', '_index':self.index_name, '_id': doc_id, 'doc': {'weapons': weapons}})

        if actions:
            helpers.bulk(self.es, actions)
            print(f' updated {len(actions)} weapons')

    def delete_not_relevant_tweets(self):
        query = {
            'query': {
                'bool': {
                    'must': [
                        {'term': {'Antisemitic': '0'}},
                        {'terms': {'sentiment': ['neutral', 'positive']}}
                    ],
                    'must_not': [
                        {'exists': {'field': 'weapons'}}
                    ]
                }
            }
        }
        response = self.es.delete_by_query(index=self.index_name, body=query)
        print(response.get('deleted',0))

    def find_antisemitic_weapons(self):

        query = {
            "size": 8000,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": "1"}},
                        {"exists": {"field": "weapons"}}
                    ]
                }
            }
        }
        results = self.es.search(index=self.index_name, body=query)
        return {len(results['hits']['hits']):results}

    def find_least_2_weapons(self):
        query = {
            "size": 8000,
            "_source": ["weapons", "Antisemitic", "text"],
            "query": {"exists": {"field": "weapons"}}
        }

        response = self.es.search(index="tweets", body=query)

        result = [doc["_source"] for doc in response['hits']['hits'] if len(doc["_source"]["weapons"]) >= 2]

        return {len(result):result}





