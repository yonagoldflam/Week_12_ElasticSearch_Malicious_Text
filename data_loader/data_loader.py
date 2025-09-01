import csv


class DataLoader:
    def __init__(self):
        self.data = []

    def read_csv(self):
        with open("../data/tweets_injected 3.csv", newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                self.data.append(row)



d = DataLoader()
d.read_csv()
print(d.data[0].keys())
# dict_keys(['TweetID', 'CreateDate', 'Antisemitic', 'text'])