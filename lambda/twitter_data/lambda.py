from config import get_oltp_creds, get_twitter_creds
from db_interface import DBConnection, execute_json_upsert
from twitter_interface import TwitterConnection


def lambda_handler(event, context):

    query = "uranium"
    count = 1000

    twitter = TwitterConnection(get_twitter_creds())
    tweets = twitter.get_tweets(query=query, count=count)

    with DBConnection(get_oltp_creds()).managed_cursor() as curr:
        execute_json_upsert(
            json_data=tweets,
            table_name="tweet",
            constraint_key="twitter_id",
            curr=curr,
        )

    return {"Status": "Success!"}


# sam local invoke -e ./twitter_data/lambda_event.json TwitterDataFunction
