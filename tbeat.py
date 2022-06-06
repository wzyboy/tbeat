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


DATEFMT = '%a %b %d %H:%M:%S %z %Y'


def load_tweets_from_js(filename, since_id=0):
    '''Newer Twitter Archives have a single tweet.js file.'''

    since_id = int(since_id)

    with open(filename, 'r') as f:
        js = f.read()

    #js = js.removeprefix('window.YTD.tweet.part0 = ')
    prefix = 'window.YTD.tweet.part0 = '
    js = js[len(prefix):]
    data = json.loads(js)
    for item in data:
        if int(item['tweet']['id']) > since_id:
            yield item['tweet']


def load_tweets_from_js_dir(js_dir, since_id=0):
    '''Older Twitter Archives have a directory with monthly js files.'''

    # Old date format
    global DATEFMT
    DATEFMT = '%Y-%m-%d %H:%M:%S %z'

    since_id = int(since_id)

    js_files = sorted(Path(js_dir).glob('*.js'))
    for js_file in js_files:
        with open(js_file, 'r') as f:
            # Remove the first line
            # e.g. Grailbird.data.tweets_2009_06 =
            content = ''.join(f.readlines()[1:])
            data = json.loads(content)
        for item in data:
            if item['id'] > since_id:
                yield item


def load_tweets_from_jl(filename, since_id=0):
    since_id = int(since_id)

    with open(filename, 'r') as f:
        for line in f:
            tweet = json.loads(line)
            if tweet['id'] > since_id:
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
        api.user_timeline, tweet_mode='extended', trim_user=False,
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
    try:
        resp = es.search(
            index=index,
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
    ap.add_argument('source', help='source of tweets: "tweet.js", "tweets" dir, *.jl, or "api"')
    ap.add_argument('index', help='dest index of tweets')
    ap.add_argument('--es', help='Elasticsearch address, default is localhost')
    args = ap.parse_args()

    es = Elasticsearch(args.es)
    last_tweet = get_last_tweet(es, args.index)
    if last_tweet:
        since_id = last_tweet['id']
        tqdm.write(f'Last tweet is {since_id} created at {last_tweet["created_at"]}.')
    else:
        tqdm.write('No last tweet found.')
        since_id = 0
    if args.source == 'api':
        tweets = load_tweets_from_api(since_id)
    elif args.source.endswith('.js'):
        tweets = load_tweets_from_js(args.source, since_id)
    elif args.source.endswith(('.jl', '.jsonl')):
        tweets = load_tweets_from_jl(args.source, since_id)
    elif Path(args.source).is_dir():
        tweets = load_tweets_from_js_dir(args.source)
    else:
        raise RuntimeError('source must be a tweet.js file from newer Twitter Archive, a "tweets" directory with monthly js files from older Twitter Archive, a .jl file that consists of one tweet per line, or "api"')

    ingest(tweets, es, args.index)


if __name__ == '__main__':
    main()
