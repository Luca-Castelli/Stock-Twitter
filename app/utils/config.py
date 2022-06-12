from dataclasses import dataclass
from typing import List

import boto3


@dataclass
class DbParams:
    """Object to store generic DB parameters."""

    db: str
    user: str
    password: str
    host: str
    port: int


def get_batch_creds() -> DbParams:
    """Returns DBParams object containing credentials to connect to the batch DB (hosted on AWS RDS)."""

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


def get_stream_creds() -> DbParams:
    """Returns DBParams object containing credentials to connect to the stream DB (hosted locally on Docker container)."""

    db_params = DbParams(
        user="admin",
        password="admin",
        db="postgres",
        host="localhost",
        port="5432",
    )

    return db_params


@dataclass
class TwitterParams:
    """Object to store generic twitter credentials."""

    twitter_api_key: str
    twitter_api_secret: str
    twitter_access_token: str
    twitter_access_secret: str


def get_twitter_creds() -> TwitterParams:
    """Returns TwitterParams object containing credentials to connect to the Twitter API."""

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
    """Returns parameters from the AWS SSM parameter store. Must have AWS credentials configured in .aws file."""

    aws_client = boto3.client("ssm", region_name="us-east-1")

    parameters = aws_client.get_parameters(
        Names=names,
        WithDecryption=True,
    )

    credentials = {}
    for parameter in parameters["Parameters"]:
        credentials[parameter["Name"]] = parameter["Value"]

    return credentials
