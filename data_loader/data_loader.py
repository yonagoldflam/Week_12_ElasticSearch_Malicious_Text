import csv


class DataLoader:
    def __init__(self):
        self.tweets_data = []
        self.weapons = ''


    def read_tweets_csv(self):
        with open("../data/tweets_injected 3.csv", newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                self.tweets_data.append(row)

    def read_weapons_txt(self):
        with open("../data/weapon_list.txt", encoding='utf-8') as weapons:
            self.weapons = weapons.read()






d = DataLoader()
d.read_tweets_csv()
d.read_weapons_txt()
print(d.tweets_data[0].keys())
# print(d.weapons)

# dict_keys(['TweetID', 'CreateDate', 'Antisemitic', 'text'])