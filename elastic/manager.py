from data_loader.data_loader import DataLoader
from main import Elastic
from elasticsearch import Elasticsearch, helpers
from classifier import Classifier

class Manager:
    def __init__(self):
        self.data_loader = DataLoader()
        self.data_loader.read_tweets_csv()
        self.elastic = Elastic()
        self.es = Elasticsearch('http://localhost:9200')
        self.index_name = 'tweets'
        # self.es.indices.delete(index='tweets', ignore_unavailable=True)

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




if __name__ == "__main__":
    m = Manager()
    m.index_to_elastic()
    m.add_sentiment_field()