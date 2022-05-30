# Twitter Beat: Ingest Your Tweets into Elasticsearch

## Usage

1. Set up venv and install requirements.
2. [Download a copy of your Twitter archive](https://help.twitter.com/en/managing-your-account/how-to-download-your-twitter-archive).
3. Load the tweets in the archive: `./tbeat.py tweet.js my-tweets`.
4. Copy `tokens.example.json` to `tokens.json` and fill in your [API details](https://developer.twitter.com/en/apps).
5. Use cron / systemd timer to periodically run `./tbeat.py api my-tweets` and keep your tweets updated.


## Notes

### API limits

Twitter API has strict API rate limits. It is strongly recommended that you download a copy of your existing tweets and load them into Elasticsearch as mentioned above in steps 2 and 3, instead of fetching all your tweets from the API. With your existing tweets loaded into Elasticsearch, the script will fetch tweets that are newer than the last tweet in the database. On hitting rate limits, the script will pause for 15 min and retry.

### Difference between Twitter Archive and Twitter API

A `tweet` object in Twitter Archive (`data/tweet.js`) used to identical to its counterpart returned by Twitter API. However, some time between 2019-04 and 2020-03, the Archive version diverged from its API counterpart. The Archive version lacks a few dict keys, namely:

- The `user` dict, which contains information about the tweet author, like `user.id` and `user.screen_name`.
- The `retweeted` bool and `retweeted_status` dict. In the API version, the `retweeted_status` embeds the original tweet in the form of another `tweet` object. However, in the archive version, the `retweeted` bool is always `false`.

If you happend to have an archive file that has a fuller data structure, consider ingesting it first before ingesting archive files downloaded later. If your Twitter Archive has a `tweets` directory that contains monthly `.js` files, it's probably an old one.
