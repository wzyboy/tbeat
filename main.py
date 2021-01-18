#!/usr/bin/env python

import json
import argparse
from datetime import datetime

from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


DATEFMT = '%a %b %d %H:%M:%S %z %Y'


def load_tweets_from_js(filename):

    with open(filename, 'r') as f:
        js = f.read()

    js = js.removeprefix('window.YTD.tweet.part0 = ')
    data = json.loads(js)
    for item in data:
        yield item['tweet']


def load_tweets_from_jl(filename):

    with open(filename, 'r') as f:
        for line in f:
            tweet = json.loads(line)
            yield tweet


def load_tweets_from_api(since_id):
    raise NotImplementedError()


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
        tweets = load_tweets_from_api(None)
    elif args.source.endswith('.js'):
        tweets = load_tweets_from_js(args.source)
    elif args.source.endswith(('.jl', '.jsonl')):
        tweets = load_tweets_from_jl(args.source)
    else:
        raise RuntimeError('source must be a .js file from Twitter export, a .jl file that consists of one tweet per line, or "api"')

    ingest(tweets, es, args.index)


if __name__ == '__main__':
    main()
