#!/usr/bin/env python

import time
import json
import argparse
from datetime import datetime

import tweepy
from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


DATEFMT = '%a %b %d %H:%M:%S %z %Y'


def load_tweets_from_js(filename):

    with open(filename, 'r') as f:
        js = f.read()

    #js = js.removeprefix('window.YTD.tweet.part0 = ')
    prefix = 'window.YTD.tweet.part0 = '
    js = js[len(prefix):]
    data = json.loads(js)
    for item in data:
        yield item['tweet']


def load_tweets_from_jl(filename):

    with open(filename, 'r') as f:
        for line in f:
            tweet = json.loads(line)
            yield tweet


def _get_api(tokens_filename='tokens.json'):

    with open(tokens_filename, 'r') as f:
        tokens = json.load(f)
    auth = tweepy.OAuthHandler(tokens['ck'], tokens['cs'])
    auth.set_access_token(tokens['atk'], tokens['ats'])
    api = tweepy.API(auth)

    return api


def load_tweets_from_api(since_id, tokens_filename='tokens.json'):
    since_id = int(since_id)

    api = _get_api()
    cursor = tweepy.Cursor(
        api.user_timeline, tweet_mode='extended', trim_user=True,
        since_id=since_id
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


def get_last_tweet(es, index):
    last_tweet = es.search(
        index=index,
        body={
            'sort': [{'@timestamp': 'desc'}],
        }
    )['hits']['hits'][0]['_source']
    return last_tweet


def ingest(tweets, es, index):

    def gen_actions():
        for tweet in tqdm(tweets):
            timestamp = datetime.strptime(tweet['created_at'], DATEFMT)
            tweet['@timestamp'] = timestamp
            action = {
                '_index': index,
                '_id': tweet['id'],
                '_source': tweet,
            }
            yield action

    bulk(es, gen_actions())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('source', help='source of tweets: *.js, *.jl, or "api"')
    ap.add_argument('index', help='dest index of tweets')
    ap.add_argument('--es', nargs='*', help='Elasticsearch address, default is localhost')
    args = ap.parse_args()

    es = Elasticsearch(args.es)
    if args.source == 'api':
        last_tweet = get_last_tweet(es, args.index)
        tqdm.write(f'Last tweet is {last_tweet["id"]} created at {last_tweet["created_at"]}.')
        tweets = load_tweets_from_api(last_tweet['id'])
    elif args.source.endswith('.js'):
        tweets = load_tweets_from_js(args.source)
    elif args.source.endswith(('.jl', '.jsonl')):
        tweets = load_tweets_from_jl(args.source)
    else:
        raise RuntimeError('source must be a .js file from Twitter export, a .jl file that consists of one tweet per line, or "api"')

    ingest(tweets, es, args.index)


if __name__ == '__main__':
    main()
