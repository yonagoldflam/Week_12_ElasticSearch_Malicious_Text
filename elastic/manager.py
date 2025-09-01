from data_loader.data_loader import DataLoader
from elastic import Elastic
from elasticsearch import Elasticsearch, helpers
import time


class Manager:
    def __init__(self):
        self.data_loader = DataLoader()
        self.data_loader.read_tweets_csv()
        self.data_loader.read_weapons_txt()
        self.elastic = Elastic()
        self.es = Elasticsearch('http://localhost:9200')
        self.index_name = 'tweets'

        self.process = False
        self.start_procesing()



    def start_procesing(self):
        self.elastic.mapping()
        self.elastic.index_to_elastic(self.data_loader.tweets_data)
        time.sleep(15)
        self.elastic.add_sentiment_field()
        time.sleep(30)
        self.elastic.add_weapon_field(self.data_loader.weapons)
        time.sleep(60)
        self.elastic.delete_empty_tweets()
        self.process = True

    def find_antisemitic_weapons(self):
        if self.process:
            return self.elastic.find_antisemitic_weapons()
        return {'the system has not finished processing the information yet. Please try again in a few minutes'}

    def find_least_2_weapons(self):
        if self.process:
            return self.elastic.find_antisemitic_weapons()
        return {'the system has not finished processing the information yet. Please try again in a few minutes'}


