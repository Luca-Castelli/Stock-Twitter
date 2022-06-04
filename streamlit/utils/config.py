from dataclasses import dataclass
from typing import List

import boto3


@dataclass
class DbParams:
    db: str
    user: str
    password: str
    host: str
    port: int


def get_oltp_creds() -> DbParams:

    names = ["stock_twitter_db_user", "stock_twitter_db_password"]
    ssm_params = get_ssm_params(names)

    db_params = DbParams(
        user=ssm_params["stock_twitter_db_user"],
        password=ssm_params["stock_twitter_db_password"],
        db="postgres",
        host="stock-twitter-db.cdx9enjjuwaj.us-east-1.rds.amazonaws.com",
        port="5432",
    )

    return db_params


def get_olap_creds() -> DbParams:

    db_params = DbParams(
        user="",
        password="",
        db="",
        host="",
        port="",
    )

    return db_params


@dataclass
class TwitterParams:
    twitter_api_key: str
    twitter_api_secret: str
    twitter_access_token: str
    twitter_access_secret: str


def get_twitter_creds() -> TwitterParams:

    names = [
        "twitter_api_key",
        "twitter_api_secret",
        "twitter_access_token",
        "twitter_access_secret",
    ]
    ssm_params = get_ssm_params(names)

    twitter_params = TwitterParams(
        twitter_api_key=ssm_params["twitter_api_key"],
        twitter_api_secret=ssm_params["twitter_api_secret"],
        twitter_access_token=ssm_params["twitter_access_token"],
        twitter_access_secret=ssm_params["twitter_access_secret"],
    )

    return twitter_params


def get_ssm_params(names: List[str]) -> List[str]:

    aws_client = boto3.client("ssm")
    parameters = aws_client.get_parameters(
        Names=names,
        WithDecryption=True,
    )
    credentials = {}
    for parameter in parameters["Parameters"]:
        credentials[parameter["Name"]] = parameter["Value"]

    return credentials
