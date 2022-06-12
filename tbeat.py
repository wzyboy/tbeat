#!/usr/bin/env python

import time
import json
import argparse
from pathlib import Path
from datetime import datetime

import tweepy
from tqdm import tqdm
from tqdm import trange
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError


class TweetsLoader:

    def __init__(self, screen_name, since_id=0, user_dict=None):
        self.since_id = int(since_id)
        self.user_dict = user_dict
        # scree_name provided by the user must match screen_name in the index.
        if screen_name and user_dict:
            if screen_name != user_dict['screen_name']:
                raise ValueError(
                    f'The screen_name provided ({user_dict["screen_name"]}) '
                    f'does not match the screen_name ({screen_name}) in the index.'
                )
        self.screen_name = screen_name
        self._api = None

    def load(self, source: str):
        if source.startswith('api:'):
            screen_name = source.split(':')[1]
            tweets = self.load_tweets_from_api(screen_name)
        elif Path(source).name == 'tweet.js':
            tweets = self.load_tweets_from_js(source)
        elif source.endswith(('.jl', '.jsonl')):
            tweets = self.load_tweets_from_jl(source)
        elif Path(source).is_dir():
            tweets = self.load_tweets_from_js_dir(source)
        elif Path(source).name == 'like.js':
            tweets = self.load_tweets_from_like_js(source)
        else:
            raise RuntimeError(
                'source must be a tweet.js or like.js file from a newer Twitter Archive, '
                'a "tweets" directory with monthly js files from an older Twitter Archive, '
                'a .jl file that consists of one tweet per line, or "api:<screen_name>"'
            )
        return tweets

    def inject_user_dict(self, tweet):
        '''Check if tweet.user is present. Inject tweet.user.screen_name if not.'''

        if tweet.get('user'):
            # Verify if user.screen_name of the incoming tweet matches what we
            # have in the index, to avoid importing tweets into the wrong
            # index.
            if self.screen_name and self.screen_name != tweet['user']['screen_name']:
                raise ValueError(
                    f'Incoming tweet has user.screen_name={tweet["user"]["screen_name"]}, '
                    f'which does not match the last tweet in the index ({self.screen_name}).'
                )
            # Return the tweet unmodified if sanity check passes.
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

    @property
    def api(self):
        if self._api:
            return self._api

        # Authenticate against Twitter API
        tokens_filename = 'tokens.json'
        with open(tokens_filename, 'r') as f:
            tokens = json.load(f)
        auth = tweepy.OAuthHandler(tokens['ck'], tokens['cs'])
        auth.set_access_token(tokens['atk'], tokens['ats'])
        self._api = tweepy.API(auth)
        return self._api

    def load_tweets_from_api(self, screen_name):
        '''Load tweets from Twitter API.'''

        kwargs = {
            'tweet_mode': 'extended',
            'trim_user': False,
            'screen_name': screen_name,
        }
        if self.since_id:
            kwargs['since_id'] = self.since_id
        cursor = tweepy.Cursor(self.api.user_timeline, **kwargs).items()

        def status_iterator(cursor):
            while True:
                try:
                    status = next(cursor)
                    tqdm.write(f'Ingesting tweet {status.id} by {status.user.screen_name} created at {status.created_at}...')
                    yield status
                except tweepy.RateLimitError:
                    tqdm.write('Rate limit reached. Sleep 15 min.')
                    time.sleep(15 * 60)
                except StopIteration:
                    break

        for status in status_iterator(cursor):
            tweet = self.inject_user_dict(status._json)
            yield tweet

    def load_tweets_from_jl(self, filename):
        '''Load tweets from jsonl files for testing purposes.'''

        with open(filename, 'r') as f:
            for line in f:
                tweet = json.loads(line)
                if tweet['id'] > self.since_id:
                    tweet = self.inject_user_dict(tweet)
                    yield tweet

    def load_tweets_from_like_js(self, filename):
        '''Load tweets from like.js file.'''

        with open(filename, 'r') as f:
            js = f.read()

        prefix = 'window.YTD.like.part0 = '
        js = js[len(prefix):]
        data = json.loads(js)

        # Get a list of sorted tweet ids
        tweet_ids = sorted(datum['like']['tweetId'] for datum in data)

        # Look up tweets by id, up to 100 at a time
        # https://docs.tweepy.org/en/v3.10.0/api.html#API.statuses_lookup
        chunk_size = 100
        for i in trange(0, len(tweet_ids), chunk_size):
            chunk = tweet_ids[i:i + chunk_size]
            while True:
                try:
                    statuses = self.api.statuses_lookup(chunk, include_entities=True)
                    if statuses:
                        break
                except tweepy.RateLimitError:
                    tqdm.write('Rate limit reached. Sleep 15 min.')
                    time.sleep(15 * 60)
            tweets = [self.inject_user_dict(status._json) for status in statuses]
            for tweet in tweets:
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
    ap.add_argument('source', help='source of tweets: "tweet.js", "like.js", "tweets" dir, "*.jl", or "api:<screen_name>"')
    ap.add_argument('index', help='dest index of tweets')
    ap.add_argument('--es', help='Elasticsearch address, default is localhost')
    ap.add_argument('--screen-name', help='inject user.screen_name if the value is not present in the source.')
    args = ap.parse_args()

    ingester = ElasticsearchIngester(args.es, args.index)
    last_tweet = ingester.get_last_tweet()
    if last_tweet:
        since_id = last_tweet['id']
        last_user = last_tweet.get('user', {}).get('screen_name')
        tqdm.write(f'Last tweet in index {args.index} is {since_id} by {last_user} created at {last_tweet["created_at"]}.')
    else:
        since_id = 0
        last_user = None
        tqdm.write(f'No last tweet found in index {args.index}.')

    if args.screen_name:
        user_dict = {
            'screen_name': args.screen_name
        }
    else:
        user_dict = None

    loader = TweetsLoader(last_user, since_id, user_dict)
    tweets = loader.load(args.source)
    ingester.ingest(tweets)


if __name__ == '__main__':
    main()
