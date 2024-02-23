import pandas as pd
import tweepy
import json
import s3fs
from datetime import datetime


def run_twitter_etl():

    access_key="******************"
    access_secret="********************************"
    consumer_key="*******************************"
    consumer_secret="******************************"

    #Twitter_Authentication
    auth=tweepy.OAuthHandler(access_key,access_secret)
    auth.set_access_token(consumer_key,consumer_secret)

    #Create API object
    api=tweepy.API(auth)

    tweets=api.user_timeline(screen_name='@elonmusk',
                            count=200,
                            include_rts=False,
                            tweet_mode='extended')


    tweet_list=[]

    for tweet in tweets:
        text=tweet.json["full_text"]

        refined_tweet={"user":tweet.user.screen_name,
                    "text":text,
                    "favourite_count":tweet.favourite_count,
                    "retweet_count":tweet.retweet_count,
                    "created_at":tweet.created_at}
        tweet_list.append(refined_tweet)


    df=pd.DataFrame(tweet_list)
    df.to_csv("elon_musk_tweet.csv")

