def get_ES_schema():
    """Returns the elasticsearch schema.
    """
    tweet_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "user_name": {
                    "type": "text"
                },
                "created_at": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-M-dd HH:mm:ss"
                },
                "tweet": { 
                    "type": "text",
                    "analyzer": "whitespace",
                },
                "tweet_words": {
                    "type": "keyword"
                },
                "media_type": {
                    "type": "text"
                },
                "verified": {
                    "type": "boolean"
                },                
                "location": {
                    "type": "text"
                },
                "coordinates": {
                    "type": "geo_point"
                },                
                "country": {
                    "type": "text"
                },
                "hashtags": {
                    "type": "keyword"
                }
            }
        }
    }
    return tweet_settings
