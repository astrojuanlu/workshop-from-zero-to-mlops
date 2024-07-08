"""This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.6
"""

import polars as pl
from nltk.sentiment.vader import SentimentIntensityAnalyzer


def exclude_social_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(pl.all().exclude("score", "num_comments", "upvote_ratio"))


def enrich_submissions(df: pl.DataFrame) -> pl.DataFrame:
    # Two types of posts: AITA and WIBTA https://www.reddit.com/r/AmItheAsshole/wiki/howtopost/
    enriched_df = df.with_columns(
        pl.col("title").str.extract(r"^(AITA|WIBTA)", 1).alias("post_type"),
        pl.col("selftext").str.len_chars().alias("text_length"),
    )
    return enriched_df


def sentiment_analysis_by_sentences(df: pl.DataFrame) -> pl.DataFrame:
    sia = SentimentIntensityAnalyzer()
    sentences = (
        df.with_columns(
            pl.col("selftext")
            .str.split(".")
            .list.eval(pl.element().str.strip_chars())
            .alias("sentences")
        )
        .select(pl.col("permalink", "sentences"))
        .explode("sentences")
        .with_columns(
            pl.col("sentences")
            .map_elements(
                lambda s: sia.polarity_scores(s),
                return_dtype=pl.Struct(
                    {
                        "neg": pl.Float64,
                        "neu": pl.Float64,
                        "pos": pl.Float64,
                        "compound": pl.Float64,
                    }
                ),
            )
            .alias("sentiment_scores"),
        )
    )
    return sentences


def create_model_input_table(df: pl.DataFrame, sentences: pl.DataFrame) -> pl.DataFrame:
    return df.join(
        (
            sentences.group_by("permalink").agg(
                pl.col("sentiment_scores")
                .struct.field("compound")
                .mean()
                .alias("compound_sentiment"),
            )
        ),
        on="permalink",
        how="left",
    )
