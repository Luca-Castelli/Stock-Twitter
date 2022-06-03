def lambda_handler(event, context):
    """
    lambda function to extract stock data from Yahoo Finance and load it into RDS.
    """
    return {"Status": "Success!"}


# sam local invoke -e .lambda/local_testing/twitter_data/lambda_event.json TwitterDataFunction
