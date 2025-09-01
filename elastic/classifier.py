import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

class Classifier:
    def __init__(self):
        pass

    @staticmethod
    def sentiment_of_text(text):
        score = SentimentIntensityAnalyzer().polarity_scores(text)
        if score['compound'] >= 0.5:
            return 'positive'
        elif score['compound'] >= -0.49:
            return  "neutral"
        else:
            return "negative"