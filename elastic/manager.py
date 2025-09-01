from data_loader.data_loader import DataLoader
from elastic import Elastic
from elasticsearch import Elasticsearch, helpers
from classifier import Classifier

class Manager:
    def __init__(self):
        self.data_loader = DataLoader()
        self.data_loader.read_tweets_csv()
        self.data_loader.read_weapons_txt()
        self.elastic = Elastic()
        self.es = Elasticsearch('http://localhost:9200')
        self.index_name = 'tweets'
        self.processed = False
        self.index_to_elastic()
        self.add_sentiment_field()
        self.add_weapon_field()
        self.delete_empty_tweets()

    def index_to_elastic(self):
        self.elastic.mapping()
        self.elastic.index_to_elastic(self.data_loader.tweets_data)

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

    def add_weapon_field(self):
        docs = helpers.scan(self.es, index=self.index_name, query={"query": {"match_all": {}}})

        actions = []
        for doc in docs:
            doc_id = doc['_id']
            text = doc["_source"]["text"]

            weapons = Classifier.find_weapons(text, self.data_loader.weapons)

            actions.append({'_op_type':'update', '_index':self.index_name, '_id': doc_id, 'doc': {'weapons': weapons}})
            print(f' updated {len(actions)} weapons')

        if actions:
            helpers.bulk(self.es, actions)
            print(f' updated {len(actions)} weapons')

    def delete_empty_tweets(self):
        count = 0
        docs = helpers.scan(self.es, index=self.index_name, query={"query": {"match_all": {}}})
        for doc in docs:
            doc_id = doc['_id']
            antisemitic = doc["_source"]["Antisemitic"]
            sentiment = doc["_source"]["sentiment"]
            weapons = doc["_source"].get("weapons", [])
            if (antisemitic == '0') and (sentiment == 'neutral' or sentiment == 'positive') and weapons == []:
                self.es.delete(index=self.index_name, id=doc_id)
                count += 1
                print(f' deleted {count} tweets')
        self.processed = True

    def find_antisemitic_weapons(self):
        if self.processed:
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
        return {'the plane no rady yet. plees try again later'}

    def find_least_2_weapons(self):
        if self.processed:
            query = {
                "size": 8000,
                "_source": ["weapons", "Antisemitic", "text"],
                "query": {"exists": {"field": "weapons"}}
            }

            response = self.es.search(index="tweets", body=query)

            result = [doc["_source"] for doc in response['hits']['hits'] if len(doc["_source"]["weapons"]) >= 2]

            return {len(result):result}
        return {'the plane no rady yet. plees try again later'}






# if __name__ == "__main__":
#     m = Manager()
#     # m.index_to_elastic()
#     # m.add_sentiment_field()
#     # m.add_weapon_field()
#     # m.delete_empty_tweets()
#     # print(m.find_antisemitic_weapons())
#     print(m.find_least_2_weapons())