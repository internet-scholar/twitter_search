import argparse
from internet_scholar import AthenaLogger, read_dict_from_s3_url, AthenaDatabase
from pathlib import Path
import itertools
import csv
import gzip
import boto3
from datetime import datetime
import twint
import json
import tweepy
import sqlite3

UNKNOWN_VIDEO_IDS = """
select id.videoId as id
from youtube_related_video
where not exists
  (select *
   from youtube_channel_node
   where youtube_channel_node.id = youtube_related_video.snippet.channelId)
"""

FILTER_TERMS = """
select track
from twitter_filter
where name='{name}'
"""

EXTRACT_VIDEO_IDS = """
WITH split(url, str) AS (
    SELECT '', urls||',' FROM tweets
    UNION ALL SELECT
    substr(str, 0, instr(str, ',')),
    substr(str, instr(str, ',')+1)
    FROM split WHERE str!=''
)
SELECT distinct
  substr(
    substr(url, 1, case INSTR(url, '?') when 0 then length(url) else INSTR(url, '?') - 1 end),
    INSTR(url, '.be/') + 4) as video_id
FROM split
WHERE url != '' and INSTR(url, 'youtu.be/') <> 0
order by video_id
"""

USERS = """
select distinct screen_name
from tweets
order by screen_name
"""

NEW_VIDEOS_YESTERDAY = """
select id
from
  youtube_twitter_addition
where
  creation_date = cast(current_date - interval '1' day as varchar)
"""

# NEW_VIDEOS_TODAY = """
# select id
# from
#   youtube_video_snippet
# where
#   creation_date = cast(current_date - interval '1' day as varchar)
# """

NEW_VIDEOS_TODAY = """
select distinct
  url_extract_parameter(validated_url, 'v') as id
from
  validated_url
where
  url_extract_host(validated_url) = 'www.youtube.com'
  and url_extract_parameter(validated_url, 'v') not in (select id from youtube_twitter_addition)
UNION DISTINCT
select distinct
  id.videoId as id
from
  youtube_related_video
where
  creation_date = cast(current_date as varchar)
  and id.videoId not in (select id from youtube_twitter_addition)
"""

CREATE_TABLE_TWEET_FROM_VIDEO_ID = """
CREATE TABLE IF NOT EXISTS tweet_from_video_id
(
    id_str text primary key,
    query text,
    screen_name text,
    tweet json,
    created_at timestamp default current_timestamp
)
"""

CREATE_TABLE_TWEET_FROM_SCREEN_NAME = """
CREATE TABLE IF NOT EXISTS tweet_from_screen_name
(
    id_str text primary key,
    query text,
    screen_name text,
    tweet json,
    created_at timestamp default current_timestamp
)
"""

CREATE_TABLE_YOUTUBE_VIDEO_ID = """
CREATE TABLE IF NOT EXISTS youtube_video_id
(
    id text primary key,
    processed integer default 0,
    created_at timestamp default current_timestamp
)
"""

CREATE_TABLE_USER = """
create table if not exists twitter_user
(
    screen_name text,
    processed integer default 0,
    created_at timestamp default current_timestamp 
)
"""


class TwitterSearch:
    def __init__(self, credentials, athena_data, s3_admin, s3_data):
        self.credentials = credentials
        self.athena_data = athena_data
        self.s3_admin = s3_admin
        self.s3_data = s3_data

    TOLERANCE = 5

    def update_table_youtube_twitter_addition(self):
        athena_db = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)
        new_videos_filename = Path(Path(__file__).parent, 'tmp', 'new_videos_today.csv')
        Path(new_videos_filename).parent.mkdir(parents=True, exist_ok=True)
        new_videos = athena_db.query_athena_and_download(query_string=NEW_VIDEOS_TODAY, filename=new_videos_filename)

        new_videos_compressed = Path(Path(__file__).parent, 'tmp', 'new_videos.csv.gz')
        with open(str(new_videos), 'rt') as f_in:
            with gzip.open(str(new_videos_compressed), 'wt') as f_out:
                reader = csv.DictReader(f_in)
                for video_id in reader:
                    f_out.write(video_id['id'] + '\n')

        s3 = boto3.resource('s3')
        s3_filename = "youtube_twitter_addition/creation_date={}/video_ids.csv.gz".format(datetime.utcnow().strftime("%Y-%m-%d"))
        s3.Bucket(self.s3_data).upload_file(str(new_videos_compressed), s3_filename)

        athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE youtube_twitter_addition")

    def twint_resilient(self, filename, query, num_attempts=0):
        try:
            c = twint.Config()
            c.Search = query
            c.Database = str(filename)
            twint.run.Search(c)
            num_attempts = 0
        except:
            if num_attempts >= 5:
                raise
            else:
                self.twint_resilient(filename=filename, query=query, num_attempts=num_attempts+1)

    def collect_user_tweets_tweepy(self, filter_terms, new_videos_yesterday_file, num_attempts=0):
        database_file = Path(Path(__file__).parent, 'tmp', 'twitter_search.sqlite')
        Path(database_file).parent.mkdir(parents=True, exist_ok=True)
        database = sqlite3.connect(str(database_file))
        database.row_factory = sqlite3.Row
        try:
            auth = tweepy.OAuthHandler(consumer_key=self.credentials['consumer_key'],
                                       consumer_secret=self.credentials['consumer_secret'])
            auth.set_access_token(key=self.credentials['access_token'],
                                  secret=self.credentials['access_token_secret'])
            api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


            database.execute(CREATE_TABLE_YOUTUBE_VIDEO_ID)
            database.execute(CREATE_TABLE_TWEET_FROM_VIDEO_ID)
            database.execute(CREATE_TABLE_USER)
            database.execute(CREATE_TABLE_TWEET_FROM_SCREEN_NAME)

            cursor_insert = database.cursor()
            with open(new_videos_yesterday_file, newline='', encoding="utf8") as csv_reader:
                reader = csv.DictReader(csv_reader)
                for video_id in reader:
                    cursor_insert.execute("insert or ignore into youtube_video_id (id) values (?)", (video_id['id'],))
                database.commit()

            cursor_videos = database.cursor()
            cursor_videos.execute("select id from youtube_video_id where processed = 0")
            depleted_iterator = False
            while not depleted_iterator:
                top_5 = itertools.islice(cursor_videos, 5)
                depleted_iterator = True
                query = ""
                for new_video in top_5:
                    depleted_iterator = False
                    query = "https://www.youtube.com/watch?v={v} OR {query}".format(v=new_video['id'], query=query)
                    database.execute("update youtube_video_id set processed = 1 where id = ?", (new_video['id'],))
                if not depleted_iterator:
                    query = "({query}) {filter}".format(query=query[:-4],
                                                        filter=" ".join(
                                                            ['-' + x.strip() for x in filter_terms.split(',')]))
                    print(str(datetime.utcnow()) + ' ' + query)
                    for status in tweepy.Cursor(api.search, q=query, result_type="recent").items():
                        database.execute(
                            "insert or ignore into tweet_from_video_id (id_str, query, screen_name, tweet) values (?, ?, ?, ?)",
                            (status.id_str, query, status.user.screen_name, json.dumps(status._json)))
                    database.commit()

            database.execute("insert or ignore into twitter_user (screen_name) "
                             "select distinct screen_name from tweet_from_video_id")
            database.commit()
            cursor_user = database.cursor()
            cursor_user.execute("select screen_name from twitter_user where processed = 0")
            depleted_iterator = False
            while not depleted_iterator:
                top_5 = itertools.islice(cursor_user, 5)
                depleted_iterator = True
                query = ""
                for user in top_5:
                    depleted_iterator = False
                    query = "from:{user} OR {query}".format(user=user['screen_name'], query=query)
                    database.execute("update twitter_user set processed = 1 where screen_name = ?", (user['screen_name'],))
                if not depleted_iterator:
                    query = "({query}) (youtu.be OR youtube) {filter} filter:links".format(
                        query=query[:-4],
                        filter=" ".join(['-' + x.strip() for x in filter_terms.split(',')]))
                    print(str(datetime.utcnow()) + ' ' + query)
                    for status in tweepy.Cursor(api.search, q=query, result_type="recent").items():
                        database.execute(
                            "insert or ignore into tweet_from_screen_name (id_str, query, screen_name, tweet) values (?, ?, ?, ?)",
                            (status.id_str, query, status.user.screen_name, json.dumps(status._json)))
                    database.commit()
                    num_attempts = 0
        except:
            if num_attempts >= self.TOLERANCE:
                raise
            else:
                self.collect_user_tweets_tweepy(filter_terms=filter_terms,
                                                new_videos_yesterday_file=new_videos_yesterday_file,
                                                num_attempts=num_attempts + 1)
        finally:
            database.close()

    def collect_user_tweets_twint(self, filter_terms, new_videos_yesterday_file, num_attempts=0):
        database_file = Path(Path(__file__).parent, 'tmp', 'twitter_search.sqlite')
        Path(database_file).parent.mkdir(parents=True, exist_ok=True)
        database = sqlite3.connect(str(database_file))
        database.row_factory = sqlite3.Row
        try:

            tweet_from_video_id = Path(Path(__file__).parent, 'tmp', 'tweet_from_video_id.sqlite')
            tweet_from_screen_name = Path(Path(__file__).parent, 'tmp', 'tweet_from_screen_name.sqlite')

            database.execute(CREATE_TABLE_YOUTUBE_VIDEO_ID)
            database.execute(CREATE_TABLE_USER)

            cursor_insert = database.cursor()
            with open(new_videos_yesterday_file, newline='', encoding="utf8") as csv_reader:
                reader = csv.DictReader(csv_reader)
                for video_id in reader:
                    cursor_insert.execute("insert or ignore into youtube_video_id (id) values (?)", (video_id['id'],))
                database.commit()

            cursor_videos = database.cursor()
            cursor_videos.execute("select id from youtube_video_id where processed = 0")
            depleted_iterator = False
            while not depleted_iterator:
                top_5 = itertools.islice(cursor_videos, 5)
                depleted_iterator = True
                query = ""
                for new_video in top_5:
                    depleted_iterator = False
                    query = "https://www.youtube.com/watch?v={v} OR {query}".format(v=new_video['id'], query=query)
                    database.execute("update youtube_video_id set processed = 1 where id = ?", (new_video['id'],))
                if not depleted_iterator:
                    query = "({query}) {filter}".format(query=query[:-4],
                                                        filter=" ".join(
                                                            ['-' + x.strip() for x in filter_terms.split(',')]))
                    print(str(datetime.utcnow()) + ' ' + query)
                    self.twint_resilient(filename=tweet_from_video_id,query=query)
                    database.commit()
                    num_attempts = 0

            tweet_from_video_id_db = sqlite3.connect(str(tweet_from_video_id))
            tweet_from_video_id_db.row_factory = sqlite3.Row
            cursor_new_users = tweet_from_video_id_db.cursor()
            cursor_new_users.execute("select distinct screen_name from tweets")
            for new_user in cursor_new_users:
                database.execute("insert or ignore into twitter_user (screen_name) values (?)",
                                 (new_user['screen_name'],))
            database.commit()
            tweet_from_video_id_db.close()

            cursor_user = database.cursor()
            cursor_user.execute("select screen_name from twitter_user where processed = 0")
            depleted_iterator = False
            while not depleted_iterator:
                top_5 = itertools.islice(cursor_user, 5)
                depleted_iterator = True
                query = ""
                for user in top_5:
                    depleted_iterator = False
                    query = "from:{user} OR {query}".format(user=user['screen_name'], query=query)
                    database.execute("update twitter_user set processed = 1 where screen_name = ?", (user['screen_name'],))
                if not depleted_iterator:
                    query = "({query}) (youtu.be OR youtube) {filter} filter:links".format(
                        query=query[:-4],
                        filter=" ".join(['-' + x.strip() for x in filter_terms.split(',')]))
                    print(str(datetime.utcnow()) + ' ' + query)
                    self.twint_resilient(filename=tweet_from_screen_name,query=query)
                    database.commit()
                    num_attempts = 0
        except:
            if num_attempts >= self.TOLERANCE:
                raise
            else:
                self.collect_user_tweets_twint(filter_terms=filter_terms,
                                               new_videos_yesterday_file=new_videos_yesterday_file,
                                               num_attempts=num_attempts + 1)
        finally:
            database.close()

    def collect_ancillary_tweets(self, filter_name, method='twint'):
        athena_db = AthenaDatabase(database=self.athena_data, s3_output=self.s3_admin)

        filter_terms = athena_db.query_athena_and_get_result(query_string=FILTER_TERMS.format(name=filter_name))['track']

        new_videos_yesterday = Path(Path(__file__).parent, 'tmp', 'new_videos_yesterday.csv')
        Path(new_videos_yesterday).parent.mkdir(parents=True, exist_ok=True)
        new_videos_yesterday_file = athena_db.query_athena_and_download(query_string=NEW_VIDEOS_YESTERDAY,
                                                                        filename=new_videos_yesterday)
        if method == 'twint':
            self.collect_user_tweets_twint(filter_terms=filter_terms,
                                           new_videos_yesterday_file=new_videos_yesterday_file)
        else:
            self.collect_user_tweets_tweepy(filter_terms=filter_terms,
                                            new_videos_yesterday_file=new_videos_yesterday_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='S3 Bucket with configuration', required=True)
    parser.add_argument('-m', '--method', help='twint or tweepy?', required=True)
    args = parser.parse_args()

    config = read_dict_from_s3_url(url=args.config)
    logger = AthenaLogger(app_name="twitter-search",
                          s3_bucket=config['aws']['s3-admin'],
                          athena_db=config['aws']['athena-admin'])
    try:
        twitter_search = TwitterSearch(credentials=config['twitter'],
                                       athena_data=config['aws']['athena-data'],
                                       s3_admin=config['aws']['s3-admin'],
                                       s3_data=config['aws']['s3-data'])
        twitter_search.collect_ancillary_tweets(filter_name=config['parameter']['filter'], method=args.method)
        #twitter_search.update_table_youtube_twitter_addition()
    finally:
        logger.save_to_s3()
        logger.recreate_athena_table()


if __name__ == '__main__':
    main()