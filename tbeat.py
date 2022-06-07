#!/usr/bin/env python

import time
import json
import argparse
from pathlib import Path
from datetime import datetime

import tweepy
from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError


class TweetsLoader:

    def __init__(self, since_id=0, user_dict=None):
        self.since_id = int(since_id)
        self.user_dict = user_dict

    def load(self, source: str):
        if source == 'api':
            tweets = self.load_tweets_from_api()
        elif source.endswith('.js'):
            tweets = self.load_tweets_from_js(source)
        elif source.endswith(('.jl', '.jsonl')):
            tweets = self.load_tweets_from_jl(source)
        elif Path(source).is_dir():
            tweets = self.load_tweets_from_js_dir(source)
        else:
            raise RuntimeError(
                'source must be a tweet.js file from newer Twitter Archive, '
                'a "tweets" directory with monthly js files from older Twitter Archive, '
                'a .jl file that consists of one tweet per line, or "api"'
            )
        return tweets

    def inject_user_dict(self, tweet):
        '''Check if tweet.user is present. Inject tweet.user.screen_name if not.'''

        if tweet.get('user'):
            return tweet

        if not self.user_dict:
            tqdm.write('The tweet does not have a user dict and you did not provide user.screen_name.')
            raise ValueError('Please provide user.screen_name')

        tweet['user'] = self.user_dict
        return tweet

    def load_tweets_from_js(self, filename):
        '''Newer Twitter Archives have a single tweet.js file.'''

        with open(filename, 'r') as f:
            js = f.read()

        #js = js.removeprefix('window.YTD.tweet.part0 = ')
        prefix = 'window.YTD.tweet.part0 = '
        js = js[len(prefix):]
        data = json.loads(js)
        for item in data:
            tweet = item['tweet']
            if int(tweet['id']) > self.since_id:
                tweet = self.inject_user_dict(tweet)
                yield tweet

    def load_tweets_from_js_dir(self, js_dir):
        '''Older Twitter Archives have a directory with monthly js files.'''

        js_files = sorted(Path(js_dir).glob('*.js'))
        for js_file in js_files:
            with open(js_file, 'r') as f:
                # Remove the first line
                # e.g. Grailbird.data.tweets_2009_06 =
                content = ''.join(f.readlines()[1:])
                data = json.loads(content)
            for tweet in data:
                if tweet['id'] > self.since_id:
                    tweet = self.inject_user_dict(tweet)
                    yield tweet

    def load_tweets_from_api(self, tokens_filename='tokens.json'):
        '''Load tweets from Twitter API.'''

        # Authenticate against Twitter API
        with open(tokens_filename, 'r') as f:
            tokens = json.load(f)
        auth = tweepy.OAuthHandler(tokens['ck'], tokens['cs'])
        auth.set_access_token(tokens['atk'], tokens['ats'])
        api = tweepy.API(auth)
        cursor = tweepy.Cursor(
            api.user_timeline, tweet_mode='extended', trim_user=False,
            since_id=self.since_id
        ).items()

        def status_iterator(cursor):
            while True:
                try:
                    status = next(cursor)
                    tqdm.write(f'Ingesting tweet {status.id} created at {status.created_at}...')
                    yield status
                except tweepy.RateLimitError:
                    tqdm.write('Rate limit reached. Sleep 15 min.')
                    time.sleep(15 * 60)
                except StopIteration:
                    break

        for status in status_iterator(cursor):
            yield status._json

    def load_tweets_from_jl(self, filename):
        '''Load tweets from jsonl files for testing purposes.'''

        with open(filename, 'r') as f:
            for line in f:
                tweet = json.loads(line)
                if tweet['id'] > self.since_id:
                    tweet = self.inject_user_dict(tweet)
                    yield tweet


class ElasticsearchIngester:

    def __init__(self, es_url, index):
        self.es = Elasticsearch(es_url)
        self.index = index

    def get_last_tweet(self):
        try:
            resp = self.es.search(
                index=self.index,
                body={
                    'sort': [{'@timestamp': 'desc'}],
                }
            )['hits']['hits']
        except NotFoundError:
            return None

        if len(resp) > 0:
            last_tweet = resp[0]['_source']
        else:
            last_tweet = None
        return last_tweet

    def parse_timestamp(self, timestamp):
        try:
            r = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %z %Y')
        except ValueError:
            r = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S %z')
        return r

    def ingest(self, tweets):

        def gen_actions():
            for tweet in tqdm(tweets):
                timestamp = self.parse_timestamp(tweet['created_at'])
                tweet['@timestamp'] = timestamp
                action = {
                    '_index': self.index,
                    '_id': tweet['id'],
                    '_source': tweet,
                }
                yield action

        bulk(self.es, gen_actions())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('source', help='source of tweets: "tweet.js", "tweets" dir, *.jl, or "api"')
    ap.add_argument('index', help='dest index of tweets')
    ap.add_argument('--es', help='Elasticsearch address, default is localhost')
    ap.add_argument('--screen-name', help='inject user.screen_name if the value is not present in the archive.')
    args = ap.parse_args()

    ingester = ElasticsearchIngester(args.es, args.index)
    last_tweet = ingester.get_last_tweet()
    if last_tweet:
        since_id = last_tweet['id']
        tqdm.write(f'Last tweet in index {args.index} is {since_id} created at {last_tweet["created_at"]}.')
    else:
        since_id = 0
        tqdm.write(f'No last tweet found in index {args.index}.')

    if args.screen_name:
        user_dict = {
            'screen_name': args.screen_name
        }
    else:
        user_dict = None

    loader = TweetsLoader(since_id, user_dict)
    tweets = loader.load(args.source)
    ingester.ingest(tweets)


if __name__ == '__main__':
    main()
