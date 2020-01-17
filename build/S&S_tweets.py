import mysql.connector
import tables
import create_db as cdb
import calendar
import tweepy
import json
import datetime
import re
from mongoengine import connect
import pymongo
import mongoengine as ME
import elastic_search as ET
from ES_schema import get_ES_schema
from mysql_schema import get_mysql_schema

class MyStreamListener(tweepy.StreamListener):

    def __init__(self, t_tweets, t_users, 
                 t_loc, t_hashtags , t_rt, 
                 t_mention, t_reply, ES_index_name):
        self._t_tweets = t_tweets
        self._t_users = t_users
        self._t_loc = t_loc
        self._t_hashtags = t_hashtags
        self._t_rt = t_rt
        self._t_reply = t_reply
        self._t_mention = t_mention
        self._ES_index_name = ES_index_name

    def on_data(self,data):
        month_name = dict((v,k) for k,v in enumerate(calendar.month_abbr))
        my_data = json.loads(data)
        if 'limit' in my_data:   #if streaming too much data, nothing gets returned.
            pass
        else:
            loc_id = None
            loc_insert = False
            reply_insert = False
            rt_insert = False

            #user information-------------------------------------------------------------------------------
            user_ca = my_data['user']['created_at']
            n_time = user_ca[-1:-5:-1][::-1] + '-' + str(month_name[user_ca[4:7]])\
                     + '-' + user_ca[8:11] + user_ca[11:19]
            user_name = my_data['user']['screen_name']
            user_id = my_data['user']['id']
            if 'description' in my_data['user']:
                user_desc = str(my_data['user']['description'])
                user_desc = re.sub('[\'\"\\\"\\\']+', '`', user_desc)
                user_desc = re.sub(r'[\\$]+','|', user_desc)
                user_desc = user_desc.replace("\\", "|")
            else: user_desc = None
            verified = str(my_data['user']['verified'])
            follower_count = my_data['user']['followers_count']
            friend_count = my_data['user']['friends_count']
            if 'location' in my_data['user']:
                user_loc = str(my_data['user']['location'])
                user_loc = user_loc.replace("\"", "")
                user_loc = re.sub('[\'\"\\\"\\\']+', '`', user_loc)
                user_loc = re.sub(r'[^\\\\$]+', '|', user_loc)
            else: user_loc = None      
            user_input = f"""INSERT INTO {self._t_users} VALUES({user_id},'{user_name}',
                         '{n_time}',\'{user_desc}\',{verified},{follower_count},
                         {friend_count},'{user_loc}')"""
           
            #location information ---------------------------------------------------------------------------
            if my_data['place']:
                loc_insert = True
                loc_id = my_data['place']['id']
                p_loc = my_data['place']['full_name']
                p_coord = my_data['place']['bounding_box']['coordinates'][0]
                country = my_data['place']['country']
                loc_input = f'''INSERT INTO {self._t_loc} VALUES('{loc_id}',"{p_loc}",
                            "{country}")'''
            else: 
                p_loc = None 
                p_coord = None
            
            #tweet information -------------------------------------------------------------------------------
            tweet_id = my_data['id']
            t_ca = my_data['created_at']
            nt_ca = t_ca[-1:-5:-1][::-1] + '-' + str(month_name[t_ca[4:7]]) + '-'\
                    + t_ca[8:11] + t_ca[11:19]
            tweet_loc_id = loc_id
            if 'media' in my_data['entities']:
                media_type = my_data['entities']['media'][0]['type']
            else: media_type = None
            if my_data["truncated"]:
                tweet = str(my_data['extended_tweet']['full_text'])
                tweet_es = str(my_data['extended_tweet']['full_text'])
            else: 
                tweet = str(my_data['text'])
                tweet_es = str(my_data['text'])
            tweet = re.sub('[\'\"]+|\\\'|\\\"', '`', tweet)
            tweet = tweet.replace("\\", "|")
            tweet_input = rf"""INSERT INTO {self._t_tweets} VALUES({tweet_id},'{nt_ca}',
                          '{tweet}','{media_type}',{user_id},'{tweet_loc_id}')"""
            
            #hashtag info----------------------------------------------------------------------------------
            if my_data["truncated"]:
                hashtags = my_data['extended_tweet']['entities']['hashtags']
            else: hashtags = my_data['entities']['hashtags']
            es_hashtags = hashtags
            for i in es_hashtags:
                i.pop('indices')
            hashtag_list = []
            for hash in hashtags:
                hashtag_list.append(hash['text'])  
            
            #mention info----------------------------------------------------------------------------------------
            if my_data["truncated"]:
                mentions = my_data['extended_tweet']['entities']['user_mentions']
            else: mentions = my_data['entities']['user_mentions']
            mention_list = []
            for men in mentions:
                mention_list.append((men['id'],men['screen_name']))
            
            #quote info--------------------------------------------------------------------------------------------
            if my_data['in_reply_to_status_id']:
                reply_insert = True
                reply_id = my_data['in_reply_to_user_id']
                reply_to_tweet_id = my_data['in_reply_to_status_id']
                reply_input = (f"INSERT INTO {self._t_reply} VALUES({user_id},{reply_id},"
                              f"{reply_to_tweet_id})")
            
            #retweet info-------------------------------------------------------------------------------------------
            if 'retweeted_status' in my_data:
                rt_insert = True
                orig_tweet_id = my_data['retweeted_status']['id']
                rt_input = f"INSERT INTO {self._t_rt} VALUES({user_id},{orig_tweet_id})"
            
            #load ElasticSearch info----------------------------------------------------------------------------------
            es_content = {
                'id': tweet_id, 
                'user_name': user_name,
                'created_at': nt_ca,
                'tweet': tweet_es,
                'tweet_words': re.split('https://\w+\.co/\w+|[\s,.:;]+|RT', tweet_es),
                'media_type': media_type,
                'location': p_loc,
                'coordinates': p_coord,
                'hashtags': hashtag_list,
                'verified': verified.lower()
            } 
            
            #load MongoDB info------------------------------------------------------------------------------------------
            Mdb_content = Tweets(id=tweet_id, user_name=user_name, created_at=nt_ca,
                                 tweet=tweet_es, tweets_words=re.split('https://\w+\.co/\w+|[\s,.:;]+|RT', tweet_es),
                                 media_type=media_type, location=p_loc, verified=my_data['user']['verified'],
                                 coordinates=p_coord)
            Mdb_hash = HashTags(text=es_hashtags)
            Mdb_content.hashtags = Mdb_hash
            
            #insert into SQL!!!!!!!---------------------------------------------------------------------------------------
            tables.CUD_table(cursor, self._t_users, user_input,
                             cnx, autocommit=False)
            if loc_insert:
                tables.CUD_table(cursor, self._t_loc, loc_input,
                                 cnx, autocommit=False)
            tables.CUD_table(cursor, self._t_tweets, tweet_input,
                             cnx, autocommit=False)
            for items in hashtag_list:
                hashtag_input = (f'INSERT INTO {self._t_hashtags} VALUES({tweet_id},'
                                 f'{user_id}, "{items}")')
                tables.CUD_table(cursor, self._t_hashtags, hashtag_input, cnx,\
                                 autocommit=False)
            for items in mention_list:
                mention_input = (f'INSERT INTO {self._t_mention} VALUES({user_id},'
                                f'{items[0]}, "{items[1]}", {tweet_id})')
                tables.CUD_table(cursor, self._t_mention, mention_input, cnx,\
                                 autocommit=False)
            if reply_insert:
                tables.CUD_table(cursor, self._t_reply, reply_input, cnx,\
                                 autocommit=False)
            if rt_insert:
                tables.CUD_table(cursor, self._t_rt, rt_input, cnx,\
                                 autocommit=False)
            cnx.commit()

            #insert into elastic search!!!!!!-----------------------------------------------------------------
            ET.insert_into_index(ES, self._ES_index_name, es_content)
            
            #insert into Mongodb!!!!--------------------------------------------------------------------------
            Mdb_content.save()

    def on_error(self, status_code):
        if status_code != 200:
            return False
    
    def on_connect(self):
        print('Connected to the twitter API.')  


class HashTags(ME.EmbeddedDocument):
    text = ME.ListField()


class Tweets(ME.Document):
    id = ME.LongField(required=True, primary_key=True)
    user_name = ME.StringField(required=True)
    created_at = ME.DateTimeField(required=True)
    tweet = ME.StringField(required=True)
    tweets_words = ME.ListField(ME.StringField())
    media_type = ME.StringField()
    location = ME.StringField()
    coordinates = ME.MultiPointField()
    hashtags = ME.EmbeddedDocumentField(HashTags)
    verified = ME.BooleanField(required=True)

def mongodb_connect(database):
    """This function connects to the Mongo DB server 
        and returns the Mgb object.
        ----------------------------
        arg1: database name
    """
    db_uri = "mongodb+srv://<user>:<password>@tester-gvt8u.azure.mongodb.net/"
    db = connect(db=database, host=db_uri)
    print(f'Connected to Mongodb server! -> Using "{database}"')
    return db



if __name__ == "__main__":

    #constants-----------------------------------------
    test_db = 'testing_DB'
    test_db_utd = 'testing_DB_utd'
    test_db_motd = 'testing_DB_testing'
    test_db_city = 'testing_DB_city'
    test_db_nfl = 'testing_DB_nfl'
    test_ind = 'testing_index'
    t_tweets = 'tweets'
    i_tweets_utd = 'tweets_utd'
    i_tweets_city = 'tweets_city'
    i_tweets_nfl = 'tweets_nfl'
    t_users = 'users'
    t_hashtags = 'hashtags'
    t_symbols = 'symbols'
    t_loc = 'locations'
    t_rt = 'retweets'
    t_mention = 'mentions'
    t_reply = 'replies'
    t_quote = 'quotes'
    consumer_key = your_twitter_consumer_key
    consumer_secret = your_twitter_consumer_secret
    access_token = your_twitter_api_access_token
    access_secret = your_twitter_api_access_secret
    Mongodb_db_name = None
    
    #Initiate database connects ------------------------------------------------------------------
    ES = ET.connect_elasticsearch()
    cnx = mysql.connector.connect(user=<username>, password=<password>, host='127.0.0.1')
    cursor = cnx.cursor()
    Mdb = mongodb_connect(Mongodb_db_name)
    
    #Initiate schemas--------------------------------------------------------------------
    tweet_settings = get_ES_schema()
    mysql_tables = get_mysql_schema(t_users, t_tweets, t_loc, 
                                    t_hashtags, t_mention, t_reply, t_rt)
    #cdb.drop_database(cursor,test_db_utd)
    cdb.create_database(cursor,test_db_utd)
    cdb.use_database(cursor,test_db_utd)
    #ET.delete_index(ES,i_tweets_utd)
    #ET.create_index(ES,i_tweets_utd,tweet_settings)
    #tables.bulk_drop_table(cursor,[f'{t_reply}',f'{t_rt}',
    #                               f'{t_mention}',f'{t_hashtags}',
    #                               f'{t_tweets}',f'{t_users}',f'{t_loc}'])
    #tables.create_table(cursor,mysql_tables)
    keywords = ['Manchester United','ManUtd','MUFC','man utd']
    '''
    Some sample keywords
    keywords = ['Manchester United','ManUtd','red devils','MUFC','man utd']
    keywords = ['NBA']
    keywords = ['arsenal','CRYARS']
    keywords = ['Tottenham','Tottenham Hotspur','COYS','THFC','LFC','Liverpool','TOTLIV','YNWA']
    keywords = ['Manchester City','Man City','ManCity','AVLMCI','Aston Villa','AVFC']
    keywords = ['HoustonTexans','Houston Texans','WeAreTexans','HOUvsKC',
                'Kansas City Chiefs','Chiefs','ChiefsKingdom']
    '''
    #Stream tweets----------------------
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token,access_secret)
    api = tweepy.API(auth)
    mySL = MyStreamListener(t_tweets, t_users, t_loc, t_hashtags,
                            t_rt, t_mention, t_reply, i_tweets_utd)
    myStream = tweepy.Stream(auth=api.auth, listener=mySL)
    try:
        myStream.filter(track=keywords, languages=['en'])
    except KeyboardInterrupt:
        print('Stopping stream...')
        myStream.disconnect()
    else:
        print('Error occured - Stopping stream...')
        myStream.disconnect()
    finally:
        print('Done.')
        myStream.disconnect()
    cursor.close()
    cnx.close()
