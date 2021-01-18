# Twitter Beat: Ingest Your Tweets into Elasticsearch

## Usage

1. Set up venv and install requirements.
2. [Download a copy of your Twitter archive](https://help.twitter.com/en/managing-your-account/how-to-download-your-twitter-archive).
3. Load the tweets in the archive: `./main.py tweet.js my-tweets`.
4. Copy `tokens.example.json` to `tokens.json` and fill in your [API details](https://developer.twitter.com/en/apps).
5. Use cron / systemd timer to periodically run `./main.py api my-tweets` and keep your tweets updated.


## Notes

Twitter API has strict API rate limites. It is strongly recommended that you download a copy of your existing tweets and load them into Elasticsearch as mentioned above in steps 2 and 3, instead of fetching all your tweets from the API. With your existing tweets loaded into Elasticsearch, the script will fetch tweets that are newer than the last tweet in the database. On hitting rate limits, the script will pause for 15 min and retry.
