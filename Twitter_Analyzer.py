import csv
import json
import mtranslate
import tweepy
import pandas as pd
import pandasql as psql
import lithops
from lithops import Storage
from lithops.multiprocessing import Pool
from lithops.storage.cloud_proxy import open
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

STORAGEBUCKET = "sdprac2python"


# Stage 1:  data crawling
#           download a bunch of tweets from a given keyword + location, get the data and upload
#           it to the cloud object storage
def get_tweets(keyword, location):
    auth = tweepy.OAuthHandler("", "")
    auth.set_access_token("", "")

    twitterAPI = tweepy.API(auth, wait_on_rate_limit=True)
    searchstr = keyword + " " + location + "lang:ca OR lang:es"     # Only look for tweets in catalan or spanish

    list_tweets = []    # In this dictionary array we will store the structured tweets

    # Start to iterate over the twitter API to download tweets
    for tweet in tweepy.Cursor(twitterAPI.search, q=searchstr, tweet_mode="extended").items(50):  # numberOftwets
        # Start saving tweets, separating all the relevant data
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

    # Add all the tweets from the list to another dictionary
    packed_tweets = {
        "tweets": list_tweets
    }

    # Upload them to the cloud object storage
    storage = Storage()
    storage.put_object(bucket=STORAGEBUCKET, key=keyword + location + ".json", body=json.dumps(packed_tweets))



# Stage 2: Analyzing and producing structured data from the previous stage crawl
#           Get the data from the cloud object storage and run a sentimental analysis over it
#           Store the result structured in a csv file
def analyze_tweets(keyword, location):

    # Get the data from cloud
    storage = Storage()
    json_tweets = storage.get_object(bucket=STORAGEBUCKET, key=keyword + location + ".json")
    packed_tweets = json.loads(json_tweets)

    analisador = SentimentIntensityAnalyzer()

    # Columnas CSV:
    # URL, Localizacion, Fecha, Sentiment
    with open(keyword + location + ".csv", 'w') as file:
        writer = csv.writer(file)
        writer.writerow(["URL", "Fecha", "Sentiment"])
        # Start iterating over the tweets downloaded from the cloud, execute sentimental analysis and put the result in a csv file
        for tweet in packed_tweets["tweets"]:
            tweetstr = mtranslate.translate(str(tweet["Texto tweet"]), "en", "auto")
            writer.writerow([str(tweet["URL"]), str(tweet["Fecha"]), str(analisador.polarity_scores(tweetstr)['compound'])])



# Stage 3: Define methods to do querys through the notebook

# Does a mean by the 4th column which contains the results of the sentimental analysis and returns the result
def sentymental_mean(keyword, location):

    with open(keyword + location + ".csv", 'r') as file:
        csvfile = pd.read_csv(file)
        query = """ SELECT AVG(c.Sentiment)
                    FROM csvfile c 
                    WHERE Sentiment!='NaN'"""
    return psql.sqldf(query).at[0,'AVG(c.Sentiment)']

# Executes sentimental mean for every region(location) and plots the results in a colour-based map
def plotting_mean():
    pass

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fexec = lithops.ServerlessExecutor(runtime='txikitan/lithopsprac2:0.1')
    with Pool(initargs=fexec) as pool:

        pool.map(get_tweets,  ["selectividad", "madrid"])
        #pool.wait()
        #pool.map(analyze_tweets,  ["selectividad", "madrid"])
        #pool.wait()
        #pool.map(sentymental_mean,  ["selectividad", "madrid"])
