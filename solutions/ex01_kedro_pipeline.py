
def exclude_social_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(pl.all().exclude("score", "num_comments", "upvote_ratio"))

from kedro.pipeline import pipeline


pipe = pipeline([
    node(
        func=exclude_social_features,
        inputs="reddit_submissions_raw",
        outputs="reddit_submissions_filtered",
    ),
    node(
        func=enrich_submissions,
        inputs="reddit_submissions_filtered",
        outputs="reddit_submissions_enriched",
    ),
    node(
        func=sentiment_analysis_by_sentences,
        inputs="reddit_submissions_raw",
        outputs="reddit_sentiment_by_sentences",
    ),
    node(
        func=create_model_input_table,
        inputs=["reddit_submissions_enriched", "reddit_sentiment_by_sentences"],
        outputs="reddit_model_input",
    ),
])
