from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import streamlit as st
from utils.config import get_oltp_creds
from utils.db_interface import DBConnection


@st.cache(ttl=60 * 60)
def load_tweet_data():
    with DBConnection(get_oltp_creds()).managed_cursor() as curr:
        curr.execute("SELECT * FROM tweet;")
        data = curr.fetchall()
        cols = [i[0] for i in curr.description]
    df = pd.DataFrame(data, columns=cols)
    df = df.set_index(df.id).drop(columns=["id"], axis=1)
    return df


@st.cache(ttl=60 * 60)
def load_stock_price_data():
    with DBConnection(get_oltp_creds()).managed_cursor() as curr:
        curr.execute("SELECT * FROM stock_price;")
        data = curr.fetchall()
        cols = [i[0] for i in curr.description]
    df = pd.DataFrame(data, columns=cols)
    df = df.set_index(df["timestamp"]).drop(columns=["timestamp"], axis=1)
    return df


def conditionally_fill_column(row, value):
    if row["sentiment"] == value:
        return 1
    else:
        return 0


def process_tweet_sentiment_data(df):
    df["negative"] = df.apply(
        lambda row: conditionally_fill_column(row, "negative"), axis=1
    )
    df["neutral"] = df.apply(
        lambda row: conditionally_fill_column(row, "neutral"), axis=1
    )
    df["positive"] = df.apply(
        lambda row: conditionally_fill_column(row, "positive"), axis=1
    )
    df["total"] = df.apply(lambda row: 1, axis=1)
    df = df.set_index("created_at").groupby(pd.Grouper(freq="D")).sum()
    df["negative_percent"] = df["negative"] / df["total"]
    df["neutral_percent"] = df["neutral"] / df["total"]
    df["positive_percent"] = df["positive"] / df["total"]
    df = df[["negative_percent", "neutral_percent", "positive_percent"]]
    return df


def process_stock_price_data(df):
    df = df.drop("ticker", axis=1)
    return df


def combine_data_sets(stock_price_df, tweet_sentiment_df):
    df = stock_price_df.reset_index().merge(
        tweet_sentiment_df,
        how="inner",
        left_on="timestamp",
        right_on="created_at",
    )
    return df


#
# Streamlit dashboard starts here.
#

st.title("Uranium Tweet Sentiment")

data_load_state = st.text("Loading data...")
tweet_data = load_tweet_data()
stock_price_data = load_stock_price_data()
data_load_state.text("Done!")

stock_price_data = process_stock_price_data(stock_price_data)
tweet_sentiment_data = tweet_data.copy()
tweet_sentiment_data = process_tweet_sentiment_data(tweet_sentiment_data)
combined_data = combine_data_sets(stock_price_data, tweet_sentiment_data)

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
fig.update_yaxes(title_text="Sentiment", row=2, col=1, showgrid=False, tickformat=".0%")
st.plotly_chart(fig, use_container_width=True)

if st.checkbox("Show Chart Data"):
    st.subheader("Chart Data")
    st.write(combined_data)

if st.checkbox("Show Raw Tweet Data"):
    st.subheader("Raw Tweet Data")
    st.write(tweet_data)
