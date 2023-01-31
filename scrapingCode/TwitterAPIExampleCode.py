import json
import sys
import tweepy
import constant
#import tweepy_utils
import csv
import pdb
from datetime import datetime, timedelta
import os
import panda
import beautifulsoup4
#Shanghai OR Beijing OR Wuhan OR Urumqi
#上海 OR 北京 OR 武汉 OR 乌鲁木齐
QUERY = 'Shanghai OR Beijing OR Wuhan OR Urumqi'

expansions = ['geo.place_id', 'author_id', 'referenced_tweets.id', 'entities.mentions.username',
              'in_reply_to_user_id', 'attachments.media_keys']
place_fields = ['country', 'name', 'place_type', 'geo', 'country_code', 'contained_within']
tweet_fields = ['author_id', 'created_at', 'entities', 'geo', 'in_reply_to_user_id', 'public_metrics',
                'referenced_tweets', 'lang', 'attachments', 'context_annotations', 'conversation_id',
                'possibly_sensitive', 'withheld', 'reply_settings', 'source']
user_fields = ['description', 'created_at', 'location', 'public_metrics', 'profile_image_url', 'url',
               'verified', 'pinned_tweet_id']
media_fields = ['preview_image_url', 'url']

def main():
    try:
        client = tweepy.Client(bearer_token=constant.bearer_token,
                               consumer_key=constant.api_key,
                               consumer_secret=constant.api_key_secret,
                               access_token=constant.access_token,
                               access_token_secret=constant.access_token_secret,
                               wait_on_rate_limit=True,
                               return_type=dict)
        next_token = None
        end_time = datetime.now() - timedelta(days=14)
        # start_time = end_time - timedelta(hours=1)  # 1hrs ahead
        start_time = end_time - timedelta(minutes=1)  # 1mins ahead
        out_dir = 'USC-MINDS-TwitterV1ToV2/raw_data/'+QUERY+'/'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        out_fp = out_dir+start_time.strftime('%Y_%m_%d_%H_%M')+'.json'  # aws
        # out_fp = 'raw_data/'+start_time.strftime('%Y_%m_%d_%H_%M')+'.json'  # local
        response = client.search_all_tweets(query=QUERY, user_fields=user_fields, tweet_fields=tweet_fields,
                                    place_fields=place_fields, expansions=expansions, media_fields=media_fields,
                                               start_time=start_time, end_time=end_time)
        with open(out_fp, 'a', encoding='utf-8') as f:
            f.write(json.dumps(response) + '\n')
        # pagination
        while True:
            #query=QUERY, max_results=100, user_fields=user_fields,
            #                                       tweet_fields=tweet_fields, place_fields=place_fields,
            #                                       expansions=expansions, media_fields = media_fields,
            #                                       next_token=next_token
            response = client.search_recent_tweets(query=QUERY, user_fields=user_fields,
                                                   tweet_fields=tweet_fields, place_fields=place_fields,
                                                   expansions=expansions, media_fields = media_fields,
                                                   next_token=next_token)
            with open(out_fp, 'a', encoding='utf-8') as f:
                f.write(json.dumps(response)+'\n')
            if 'next_token' in response['meta'].keys():
                next_token = response['meta']['next_token']
            else:
                break
            # break condition
            cr_time = datetime.now()
            time_interval_s = (cr_time-end_time).total_seconds()
            print(time_interval_s)
            #if time_interval_s >= 864000:  # 2mins
            #    break
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()