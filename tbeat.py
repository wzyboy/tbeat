#!/usr/bin/env python

import time
import json
import argparse
from pathlib import Path
from datetime import datetime
from html.parser import HTMLParser

from typing import Optional

import tweepy
from tqdm import tqdm
from tqdm import trange
from mastodon import Mastodon
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError


class TweetsLoader:

    tokens_filename = Path('tokens.json')

    def __init__(self, screen_name: Optional[str] = None, since_id: Optional[int] = None, user_dict: Optional[dict] = None):
        self.since_id = since_id or 0
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
        elif source.startswith('api-fav:'):
            screen_name = source.split(':')[1]
            tweets = self.load_tweets_from_api(screen_name, api_name='favorites')
        elif Path(source).name.startswith(('tweet.js', 'tweets.js', 'tweets-part')):
            tweets = self.load_tweets_from_js(Path(source))
        elif source.endswith(('.jl', '.jsonl')):
            tweets = self.load_tweets_from_jl(Path(source))
        elif Path(source).is_dir():
            tweets = self.load_tweets_from_js_dir(Path(source))
        elif Path(source).name == 'like.js':
            tweets = self.load_tweets_from_like_js(Path(source))
        else:
            raise ValueError('Invalid source. Please see documentation for a list of supported sources.')
        return tweets

    def inject_user_dict(self, tweet: dict):
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

    def load_tweets_from_js(self, filename: Path):
        '''Newer Twitter Archives have a single tweet.js file.'''

        with open(filename, 'r') as f:
            js = f.read()

        #js = js.removeprefix('window.YTD.tweet.part0 = ')
        prefix = 'window.YTD.tweet.part0 = '
        js = js[len(prefix):]
        data = json.loads(js)
        for item in data:
            tweet = item['tweet']
            if int(tweet['id']) > int(self.since_id):
                tweet = self.inject_user_dict(tweet)
                yield tweet

    def load_tweets_from_js_dir(self, js_dir: Path):
        '''Older Twitter Archives have a directory with monthly js files.'''

        js_files = sorted(js_dir.glob('*.js'))
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
        with open(self.tokens_filename, 'r') as f:
            tokens = json.load(f)
        auth = tweepy.OAuthHandler(tokens['ck'], tokens['cs'])
        auth.set_access_token(tokens['atk'], tokens['ats'])
        self._api = tweepy.API(auth)
        return self._api

    def load_tweets_from_api(self, screen_name: str, api_name: str = 'user_timeline'):
        '''Load tweets from Twitter API.'''

        kwargs = {
            'tweet_mode': 'extended',
            'trim_user': False,
            'screen_name': screen_name,
        }
        if self.since_id:
            kwargs['since_id'] = self.since_id
        cursor = tweepy.Cursor(getattr(self.api, api_name), **kwargs).items()

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
            if api_name == 'user_timeline':
                tweet = self.inject_user_dict(status._json)
            else:
                tweet = status._json
            yield tweet

    def load_tweets_from_jl(self, filename: Path):
        '''Load tweets from jsonl files for testing purposes.'''

        with open(filename, 'r') as f:
            for line in f:
                tweet = json.loads(line)
                if tweet['id'] > self.since_id:
                    tweet = self.inject_user_dict(tweet)
                    yield tweet

    def load_tweets_from_like_js(self, filename: Path):
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
            tweets = [self.inject_user_dict(status._json) for status in statuses]  # type: ignore
            for tweet in tweets:
                yield tweet


class MastodonLoader:
    tokens_filename = Path('mastodon_tokens.json')

    def __init__(self, fqn: Optional[str] = None, since_id: Optional[str] = None) -> None:
        self.fqn = fqn
        self._api = None
        self.since_id = since_id

    def load(self, source: str):
        if source.startswith('masto-api:'):
            _, fqn = source.split(':')
            if self.fqn and self.fqn != fqn:
                raise ValueError(
                    'Username we are about to fetch statuses from does not match username of the last status in Elasticsearch index: '
                    f'{fqn} != {self.fqn}'
                )
            toots = self.load_toots_from_api(fqn)
        else:
            raise NotImplementedError()
        return toots

    @property
    def api(self):
        if self._api:
            return self._api

        # Authenticate against Mastodon API
        with open(self.tokens_filename, 'r') as f:
            tokens = json.load(f)
        self._api = Mastodon(
            api_base_url=tokens['api_base_url'],
            access_token=tokens['access_token'],
            version_check_mode='none',
        )
        return self._api

    def _strip_html_tags(self, html: str):
        parser = HTMLTagStripper()
        parser.feed(html)
        return parser.get_data()

    def load_toots_from_api(self, user_id: str):
        '''Use an infinite loop to load toots from Mastodon API. If since_id is
        set, the loop stops when it is reached; else, the loop stops until API
        returns empty array or throws exception.'''

        max_id = None
        reached_since_id = False
        while not reached_since_id:
            toots = self.api.account_statuses(user_id, max_id=max_id)
            if not toots:
                break

            for toot in toots:
                toot_id = toot['id']
                created_at = toot['created_at']
                toot['content_text'] = self._strip_html_tags(toot['content'])
                if self.since_id and toot_id <= self.since_id:
                    reached_since_id = True
                    break
                else:
                    tqdm.write(f'Ingesting toot {toot_id} by {toot["account"]["fqn"]} created at {created_at}...')
                    yield toot

            # If since_id is not yet reached or not set at all, start again
            # with a new max_id
            else:
                max_id = toots[-1]['id']


class HTMLTagStripper(HTMLParser):

    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


class ElasticsearchIngester:

    def __init__(self, es_url, index):
        self.es = Elasticsearch(es_url)
        self.index = index

    def get_last_status(self):
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
            last_status = resp[0]['_source']
        else:
            last_status = None
        return last_status

    def parse_timestamp(self, timestamp):
        if isinstance(timestamp, datetime):
            return timestamp

        try:
            r = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %z %Y')
        except ValueError:
            r = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S %z')
        return r

    def ingest(self, statuses):

        def gen_actions():
            for status in tqdm(statuses):
                timestamp = self.parse_timestamp(status['created_at'])
                status['@timestamp'] = timestamp
                action = {
                    '_index': self.index,
                    '_id': status['id'],
                    '_source': status,
                }
                yield action

        bulk(self.es, gen_actions())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('source', help='source of statuses; see documentation.')
    ap.add_argument('index', help='dest Elasticsearch index of statuses')
    ap.add_argument('--es', help='Elasticsearch address, default is localhost')
    ap.add_argument('--screen-name', help='(Twitter) inject user.screen_name if the value is not present in the source')
    ap.add_argument('--skip-last-status-check', action='store_true', help=(
        'import statuses without checking existing ones first; '
        'useful when importing unsorted tweets from multiple files'
    ))
    args = ap.parse_args()

    ingester = ElasticsearchIngester(args.es, args.index)
    if not args.skip_last_status_check:
        last_status = ingester.get_last_status()
    else:
        last_status = None
    if last_status:
        since_id = last_status['id']
        twitter_user = last_status.get('user', {}).get('screen_name')
        mastodon_user = last_status.get('account', {}).get('fqn')
        last_user = twitter_user or mastodon_user
        tqdm.write(f'Last status in index {args.index} is {since_id} by {last_user} created at {last_status["created_at"]}.')
    else:
        since_id = None
        last_user = None
        tqdm.write(f'No last status found in index {args.index}.')

    if args.screen_name:
        user_dict = {
            'screen_name': args.screen_name
        }
    else:
        user_dict = None

    if args.source.startswith('masto'):
        loader = MastodonLoader(last_user, since_id)
    else:
        loader = TweetsLoader(last_user, since_id, user_dict)

    statuses = loader.load(args.source)
    ingester.ingest(statuses)


if __name__ == '__main__':
    main()
