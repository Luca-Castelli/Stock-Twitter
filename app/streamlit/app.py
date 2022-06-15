import time
import warnings

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import streamlit as st
from utils import config, db_interface


def get_stream_tweet_data() -> pd.DataFrame:
    """Fetches data from the tweet_stream table in the stream DB."""

    with db_interface.DBConnection(config.get_stream_creds()).managed_cursor() as curr:
        curr.execute(
            "SELECT twitter_id, created_at, username, verified_user, followers, sentiment, text FROM tweet_stream ORDER BY created_at DESC;"
        )
        data = curr.fetchall()
        cols = [i[0] for i in curr.description]

    df = pd.DataFrame(data, columns=cols)
    df["created_at"] = (
        df["created_at"]
        .dt.tz_localize("UTC")
        .dt.tz_convert("US/Eastern")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    return df


@st.cache(ttl=60 * 60, allow_output_mutation=True)
def get_batch_tweet_data() -> pd.DataFrame:
    """Fetches data from the tweet table in the batch DB."""

    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        curr.execute("SELECT * FROM tweet ORDER BY created_at DESC;")
        data = curr.fetchall()
        cols = [i[0] for i in curr.description]

    df = pd.DataFrame(data, columns=cols)
    df = df.set_index(df.id).drop(columns=["id"], axis=1)

    return df


@st.cache(ttl=60 * 60, allow_output_mutation=True)
def get_batch_stock_data() -> pd.DataFrame:
    """Fetches data from the stock_price table in the batch DB."""

    with db_interface.DBConnection(config.get_batch_creds()).managed_cursor() as curr:
        curr.execute("SELECT * FROM stock_price ORDER BY timestamp DESC;")
        data = curr.fetchall()
        cols = [i[0] for i in curr.description]

    df = pd.DataFrame(data, columns=cols)

    return df


def aggregate_tweet_sentiment_helper(row: pd.Series, sentiment: str) -> int:
    """Idea is to get a column for each of negative, neutral, and positive.
    Each with 1s if the sentiment for that row matches."""

    if row["sentiment"] == sentiment:
        return 1
    return 0


def aggregate_tweet_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sentiment on a per day basis. Calculate percentage belonging to
    negative, neutral, and positive sentiment."""

    sentiment_types = ["negative", "neutral", "positive"]

    for sentiment in sentiment_types:
        df[sentiment] = df.apply(
            lambda row: aggregate_tweet_sentiment_helper(row=row, sentiment=sentiment),
            axis=1,
        )
    df["total"] = df.apply(lambda row: 1, axis=1)

    df = df.set_index("created_at").groupby(pd.Grouper(freq="D")).sum()

    for sentiment in sentiment_types:
        df[f"{sentiment}_percent"] = df[sentiment] / df["total"]

    df = df[["negative_percent", "neutral_percent", "positive_percent"]]

    return df


def combine_stock_sentiment_data(
    stock_price_df: pd.DataFrame, tweet_sentiment_df: pd.DataFrame
) -> pd.DataFrame:
    """Combines stock_price and tweet_sentiment data. Combined data is used for plotting."""

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

    stream_tweet_data = get_stream_tweet_data()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_stream_count = stream_tweet_data["twitter_id"].count()
        st.metric(
            "Total Tweet Count",
            total_stream_count,
        )
    with col2:
        negative_stream_count = stream_tweet_data[
            stream_tweet_data["sentiment"] == "negative"
        ]["twitter_id"].count()
        st.metric(
            "Negative Tweet Count",
            negative_stream_count,
        )
    with col3:
        neutral_stream_count = stream_tweet_data[
            stream_tweet_data["sentiment"] == "neutral"
        ]["twitter_id"].count()
        st.metric(
            "Neutral Tweet Count",
            neutral_stream_count,
        )
    with col4:
        positive_stream_count = stream_tweet_data[
            stream_tweet_data["sentiment"] == "positive"
        ]["twitter_id"].count()
        st.metric(
            "Positive Tweet Count",
            positive_stream_count,
        )

    st.write("Verified User Tweets")
    with st.empty():
        filtered_stream_tweet_data = stream_tweet_data[
            stream_tweet_data["verified_user"]
        ]
        filtered_stream_tweet_data = filtered_stream_tweet_data[
            ["created_at", "username", "followers", "sentiment", "text"]
        ]
        st.dataframe(filtered_stream_tweet_data)

    st.write("Non-verified User Tweets")
    with st.empty():
        filtered_stream_tweet_data = stream_tweet_data[
            ~stream_tweet_data["verified_user"]
        ]
        filtered_stream_tweet_data = filtered_stream_tweet_data[
            ["created_at", "username", "followers", "sentiment", "text"]
        ]
        st.dataframe(filtered_stream_tweet_data)

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


warnings.filterwarnings("ignore", category=DeprecationWarning)
st.set_page_config(page_title="Twitter and Stock Price Analysis", page_icon="🚀")
main()
