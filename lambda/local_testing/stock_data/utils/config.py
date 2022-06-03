import os

import boto3

from db import DBParams


def get_db_creds() -> DBParams:
    """
    temp
    """

    aws_client = boto3.client("ssm")

    parameters = aws_client.get_parameters(
        Names=[
            "stock_twitter_db_user",
            "stock_twitter_db_password",
        ],
        WithDecryption=True,
    )

    credentials = {}
    for parameter in parameters["Parameters"]:
        credentials[parameter["Name"]] = parameter["Value"]

    return DBParams(
        user=credentials["stock_twitter_db_user"],
        password=credentials["stock_twitter_db_password"],
        db="postgres",
        host="stock-twitter-db.cdx9enjjuwaj.us-east-1.rds.amazonaws.com",
        port="5432",
    )
