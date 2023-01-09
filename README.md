# Twitter/Mastodon Beat: Ingest Your Tweets/Toots into Elasticsearch

This single-file script loads your tweets/toots into Elasticsearch.

It supports Twitter API and Mastodon-compatible API (e.g. Mastodon and Pleroma). In fact, the author only tested this against [his own Pleroma server](https://dabr.ca/about).

## Usage

1. Set up venv and install requirements.
2. Set up credentials if the source requires it (e.g. Twitter API).
3. Run the script once `./tbeat.py <source> <es-index>` to make sure everything works as expected. See below for valid sources.
4. Use cron / systemd timer to periodically run the script and keep your Elasticsearch index updated.

## Sources

The script supports ingesting tweets/toots from various sources. The script will first query Elasticsearch to get the latest status ID and ingest anything newer than that from the source. You can ingest from different sources of the same type into the same index.

| Source                 | Examples                                                           |
|------------------------|--------------------------------------------------------------------|
| Twitter Archive        | data/tweet.js<br>data/tweets.js<br>data/like.js<br>data/js/tweets/ |
| Twitter API            | api:wzyboy<br>api-fav:wzyboy                                       |
| Twitter API (local)    | statuses.jsonl<br>statuses.jl                                      |
| Mastodon API (Pleroma) | masto-api:wzyboy<br>masto-api:someone@example.org                  |

### Twitter Archive

This is useful when using the script for the first time to load all your existing tweets as Twitter imposes strict API limitations. You can only fetch the recent ~3200 tweets from API and there is rate limit to deal with.

It takes more than 24 hours to [download a copy of your Twitter archive](https://help.twitter.com/en/managing-your-account/how-to-download-your-twitter-archive). Extract the zip file and you can find your tweets in `data/tweet.js` or `data/tweets.js` file.

If you happen to have an old Twitter Archive from a few years ago, your tweets might be organized in monthly `.js` files like this:

```
data/js/tweets/
├── 2010_08.js
├── 2010_09.js
├── 2010_10.js
├── 2010_11.js
└── 2010_12.js
```

In this case, pass the path of `tweets` directory to the script and it will be handled as well.

### Twitter API

It is recommended to use this source to "catch up" with your latest tweets after loading your existing tweets with an archive (see above). 

To use Twitter API as a source, copy `tokens.example.json` to `tokens.json` and fill in your [API details](https://developer.twitter.com/en/apps).

To ingest all tweets posted by a user, use `api:username` as a source. To ingest all tweets that this user liked, use `api-fav:username` as a source.

### Twitter API (local)

For testing and debugging purposes only. The script expects a [JSON Lines](http://jsonlines.org/) file, each line of which being a [Twitter API status object](https://developer.twitter.com/en/docs/twitter-api/v1/tweets/post-and-engage/api-reference/get-statuses-show-id).

### Mastodon API (Pleroma)

To use Mastodon API or Pleroma as a source, copy `mastodon_tokens.example.json` to `mastodon_tokens.json` and fill in API domain and access token.

For Mastodon users, you can generate an access token in settings. Pleroma does not have a UI for generating tokens, so you can [use cURL to generate one](https://tinysubversions.com/notes/mastodon-bot/).

To ingest all statuses posted by a user, use `masto-api:username@example.org` as a source. If the user lives on the same server as the owner of the access token, the `@example.org` can be omitted.

## Notes

### Twitter API limits

Twitter API has strict API rate limits. It is strongly recommended that you download a copy of your existing tweets and load them into Elasticsearch as mentioned above, instead of fetching all your tweets from the API. With your existing tweets loaded into Elasticsearch, the script will fetch tweets that are newer than the last tweet in the database. On hitting rate limits, the script will pause for 15 min and retry.

### Difference between Twitter Archive and Twitter API

A `tweet` object in Twitter Archive (`data/tweet.js`) used to identical to its counterpart returned by Twitter API. However, some time between 2019-04 and 2020-03, the Archive version diverged from its API counterpart. The Archive version lacks a few dict keys, namely:

- The `user` dict, which contains information about the tweet author, like `user.id` and `user.screen_name`.
- The `retweeted` bool and `retweeted_status` dict. In the API version, the `retweeted_status` embeds the original tweet in the form of another `tweet` object. However, in the archive version, the `retweeted` bool is always `false`.

The script mandates that any tweet has at least `user.screen_name` key present. Use `--screen-name` to provide a value and the script will inject it when the tweet from the source does not have a `user` dict.

If you happend to have an archive file that has a fuller data structure, consider ingesting it first before ingesting archive files downloaded later. If your Twitter Archive has a `tweets` directory that contains monthly `.js` files, it's probably an old one.
