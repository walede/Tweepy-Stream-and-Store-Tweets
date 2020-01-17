import mysql.connector

def get_mysql_schema(t_users,t_tweets,t_loc,t_hashtags,t_mention,t_reply,t_rt):
    """Returns the mysql table schemas.
       -----------------
       args:Table names
    """
    twitter_tables={}
    #user table
    twitter_tables[f'{t_users}']=(
        f"""CREATE TABLE {t_users}(
            id BIGINT PRIMARY KEY,
            user_name VARCHAR(255) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT NOW(),
            description VARCHAR(1000) NOT NULL,
            verified VARCHAR(20) DEFAULT NULL,
            follower_count INT,
            friend_count INT,
            location VARCHAR(255)
        );""")

    #location table
    twitter_tables[f'{t_loc}']=(
        f"""CREATE TABLE {t_loc}(
            id VARCHAR(255) PRIMARY KEY,
            location VARCHAR(255) NOT NULL, 
            country VARCHAR(255) NOT NULL
        );""")

    #tweets table
    twitter_tables[f'{t_tweets}']=(
        f"""CREATE TABLE {t_tweets}(
            id BIGINT PRIMARY KEY,
            time DATETIME NOT NULL DEFAULT NOW(),
            tweet VARCHAR(1000) NOT NULL,
            media_type VARCHAR(255) DEFAULT NULL,
            user_id BIGINT,
            location_id VARCHAR(255),
            FOREIGN KEY(user_id) REFERENCES {t_users}(id)
        );""")

    #twitter hashtag table
    twitter_tables[f'{t_hashtags}']=(
        f"""CREATE TABLE {t_hashtags}(
            tweet_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            hashtag VARCHAR(255) NOT NULL,
            PRIMARY KEY(hashtag,tweet_id,user_id),
            FOREIGN KEY(user_id) REFERENCES {t_users}(id),
            FOREIGN KEY(tweet_id) REFERENCES {t_tweets}(id)
        );""")

    #mention table
    twitter_tables[f'{t_mention}']=(
        f"""CREATE TABLE {t_mention}(
            user_id BIGINT NOT NULL,
            mention_id BIGINT NOT NULL,
            mention_name VARCHAR(255) NOT NULL,
            tweet_id BIGINT NOT NULL,
            PRIMARY KEY(user_id,mention_id,tweet_id)        
        );""")

    #quotes table
    twitter_tables[f'{t_reply}']=(
        f"""CREATE TABLE {t_reply}(
            user_id BIGINT NOT NULL,
            reply_id BIGINT NOT NULL,
            tweet_id BIGINT NOT NULL,
            PRIMARY KEY(user_id,reply_id,tweet_id)        
        );""")

    #retweets table
    twitter_tables[f'{t_rt}']=(
        f"""CREATE TABLE {t_rt}(
            user_id BIGINT NOT NULL,
            original_tweet_id BIGINT NOT NULL,
            PRIMARY KEY(user_id,original_tweet_id)        
        );""")
    return twitter_tables

def mysql_connect(database=None):
    '''Returns the connection object'''
    cnx = mysql.connector.connect(user=<user>, password=<password>, host='127.0.0.1',database=database)
    return cnx
