#
import csv
import json

from lithops import Storage
from lithops.storage.cloud_proxy import os, open
import tweepy
import mtranslate
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

STORAGEBUCKET = "sdprac2python"


# Stage 1
def get_tweets(keyword, location):
    auth = tweepy.OAuthHandler("", "")
    auth.set_access_token("", "")

    twitterAPI = tweepy.API(auth, wait_on_rate_limit=True)
    searchstr = keyword + " " + location + "lang:ca OR lang:es"

    list_tweets = []

    for tweet in tweepy.Cursor(twitterAPI.search, q=searchstr, tweet_mode="extended").items(200):  # numberOftwets
        tweetstr = tweet.full_text
        url = "https://twitter.com/twitter/statuses/" + str(tweet.id)
        fecha = tweet.created_at.strftime("%m/%d/%Y %H:%M:%S")
        localizacion = str(tweet.user.location)
        packed_tweet = {
            "Texto tweet": tweetstr,
            "URL": url,
            "Fecha": fecha,
            "Ubicacion": localizacion  # Localizacion del usuario del tweet y no del tema (madrid, cataluña, etc)
        }
        list_tweets.append(packed_tweet)

    packed_tweets = {
        "tweets": list_tweets
    }

    storage = Storage()
    storage.put_object(bucket=STORAGEBUCKET, key=keyword + location + ".json", body=json.dumps(packed_tweets))


# Stage 2
def analyze_tweets(keyword, location):
    storage = Storage()
    json_tweets = storage.get_object(bucket=STORAGEBUCKET, key=keyword + location + ".json")
    packed_tweets = json.loads(json_tweets)

    analisador = SentimentIntensityAnalyzer()

    # Columnas CSV:
    # URL, Localizacion, Fecha, Sentiment

    with open(keyword + location + ".csv", 'w') as file:
        writer = csv.writer(file)
        writer.writerow(["URL", "Localizacion", "Fecha", "Sentiment"])
        for tweet in packed_tweets["tweets"]:
            tweetstr = mtranslate.translate(str(tweet["Texto tweet"]), "en", "auto")
            writer.writerow([str(tweet["URL"]), str(tweet["Ubicacion"]), str(tweet["Fecha"]), str(analisador.polarity_scores(tweetstr)['compound'])])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    get_tweets("selectividad", "madrid")
    analyze_tweets("selectividad", "madrid")
