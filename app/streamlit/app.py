import time
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import streamlit as st
from utils.config import get_olap_creds, get_oltp_creds
from utils.db_interface import DBConnection


def get_stream_tweet_data():
    with DBConnection(get_olap_creds()).managed_cursor() as curr:
        curr.execute(
            "SELECT created_at, username, text FROM tweet_stream ORDER BY created_at DESC LIMIT 5;"
        )
        data = curr.fetchall()
        cols = [i[0] for i in curr.description]
    df = pd.DataFrame(data, columns=cols)
    return df


def get_stream_tweet_count():
    with DBConnection(get_olap_creds()).managed_cursor() as curr:
        curr.execute("SELECT COUNT(id) FROM tweet_stream;")
        count = curr.fetchall()[0][0]
    return count


@st.cache(ttl=60 * 60, allow_output_mutation=True)
def get_batch_tweet_data():
    with DBConnection(get_oltp_creds()).managed_cursor() as curr:
        curr.execute("SELECT * FROM tweet ORDER BY created_at DESC;")
        data = curr.fetchall()
        cols = [i[0] for i in curr.description]
    df = pd.DataFrame(data, columns=cols)
    df = df.set_index(df.id).drop(columns=["id"], axis=1)
    return df


@st.cache(ttl=60 * 60, allow_output_mutation=True)
def get_batch_stock_data():
    with DBConnection(get_oltp_creds()).managed_cursor() as curr:
        curr.execute("SELECT * FROM stock_price ORDER BY timestamp DESC;")
        data = curr.fetchall()
        cols = [i[0] for i in curr.description]
    df = pd.DataFrame(data, columns=cols)
    return df


def aggregate_tweet_sentiment_helper(row, value):
    if row["sentiment"] == value:
        return 1
    else:
        return 0


def aggregate_tweet_sentiment(df):
    df["negative"] = df.apply(
        lambda row: aggregate_tweet_sentiment_helper(row, "negative"), axis=1
    )
    df["neutral"] = df.apply(
        lambda row: aggregate_tweet_sentiment_helper(row, "neutral"), axis=1
    )
    df["positive"] = df.apply(
        lambda row: aggregate_tweet_sentiment_helper(row, "positive"), axis=1
    )
    df["total"] = df.apply(lambda row: 1, axis=1)

    df = df.set_index("created_at").groupby(pd.Grouper(freq="D")).sum()

    df["negative_percent"] = df["negative"] / df["total"]
    df["neutral_percent"] = df["neutral"] / df["total"]
    df["positive_percent"] = df["positive"] / df["total"]

    df = df[["negative_percent", "neutral_percent", "positive_percent"]]

    return df


def combine_stock_sentiment_data(stock_price_df, tweet_sentiment_df):
    df = stock_price_df.reset_index().merge(
        tweet_sentiment_df,
        how="inner",
        left_on="timestamp",
        right_on="created_at",
    )
    return df


# Streamlit dashboard starts here.


def main():

    st.title("Twitter and Stock Price Analysis")
    st.write("#")

    col1, col2 = st.columns(2)
    with col1:
        st.selectbox("Select Twitter Keyword", ("Uranium",))
    with col2:
        st.selectbox("Select Stock Ticker", ("URA",))
    st.info(
        "Currently only configured for Uranium and URA. Next iteration will allow user input."
    )

    st.write("#")
    st.header("Live Tweet Stream")
    st.success("Live data refreshing every 5 seconds.")
    with st.empty():
        stream_tweet_count = get_stream_tweet_count()
        st.metric(
            "Streamed Tweet Count",
            stream_tweet_count,
        )
    with st.empty():
        stream_tweet_data = get_stream_tweet_data()
        st.dataframe(stream_tweet_data)

    batch_tweet_data = get_batch_tweet_data()
    batch_stock_data = get_batch_stock_data()
    batch_tweet_sentiment = aggregate_tweet_sentiment(batch_tweet_data)
    combined_data = combine_stock_sentiment_data(
        batch_stock_data, batch_tweet_sentiment
    )
    st.write("#")
    st.header("Twitter Sentiment and Stock Price")
    st.info("Data updated every weekday at 5pm EST.")
    fig = make_subplots(rows=2, cols=1)
    fig.append_trace(
        go.Line(
            x=combined_data["timestamp"],
            y=combined_data["price"],
            name="Uranium ETF",
        ),
        row=1,
        col=1,
    )
    fig.append_trace(
        go.Bar(
            x=combined_data["timestamp"],
            y=combined_data["negative_percent"],
            name="Negative",
            marker_color="red",
            text=combined_data["negative_percent"],
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=combined_data["timestamp"],
            y=combined_data["neutral_percent"],
            name="Neutral",
            marker_color="grey",
            text=combined_data["neutral_percent"],
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=combined_data["timestamp"],
            y=combined_data["positive_percent"],
            name="Positive",
            marker_color="green",
            text=combined_data["positive_percent"],
        ),
        row=2,
        col=1,
    )
    fig.update_layout(barmode="stack", height=800)
    fig.update_traces(texttemplate="%{text:.2%}")
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(title_text="Price", row=1, col=1, showgrid=False, tickformat=".2f")
    fig.update_yaxes(
        title_text="Sentiment", row=2, col=1, showgrid=False, tickformat=".0%"
    )
    st.plotly_chart(fig, use_container_width=True)

    time.sleep(5)
    st.experimental_rerun()


st.set_page_config(page_title="Twitter and Stock Price Analysis", page_icon="🚀")
main()
