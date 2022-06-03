from utils.config import get_db_creds
from utils.db import DBConnection


def lambda_handler(event, context):
    """
    lambda function to extract stock data from Yahoo Finance and load it into RDS.
    """

    return {"Status": "Success!"}


# sam local invoke -e .lambda/local_testing/stock_data/lambda_event.json StockDataFunction
