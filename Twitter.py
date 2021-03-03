from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time
from helper.extract_tweet_info import extract_tweet_info


class TweetStreamListener(StreamListener):
    def on_data(self, data):
        tweet = json.loads(data)

        try:
            payload = extract_tweet_info(tweet)
            if (payload):
                # print(payload)
                
                put_response = kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=payload,
                    PartitionKey=str(tweet['user']['screen_name'])
                )

            return True
        except (AttributeError, Exception) as e:
            print(e)


    def on_error(self, status):
        print(status)

stream_name = 'twitter-kinesis-datastream'
if __name__ == '__main__':
    # create kinesis client connection
    session = boto3.Session(profile_name='kasturivartak')

    # create the kinesis client
    kinesis_client = session.client('kinesis', region_name='us-east-1')

    # set twitter keys/tokens
    auth = OAuthHandler('your_consumer_key', 'your_key_secret')
    auth.set_access_token('your_access_token', 'your_access_token_secret')

    while True:
        try:
            print('Twitter streaming...')
            myStreamlistener = TweetStreamListener()
            stream = Stream(auth=auth, listener=myStreamlistener)
            stream.filter(track=["#AI", "#MachineLearning"], languages=['en'], stall_warnings=True)
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue