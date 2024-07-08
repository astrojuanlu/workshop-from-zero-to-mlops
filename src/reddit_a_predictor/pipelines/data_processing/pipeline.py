"""This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    create_model_input_table,
    enrich_submissions,
    exclude_social_features,
    sentiment_analysis_by_sentences,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
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
        ]
    )
