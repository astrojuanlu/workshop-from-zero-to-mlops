"""This is a boilerplate pipeline 'training'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import split_data, train_classifier


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_data,
                inputs=[
                    "reddit_model_input",
                    "params:training_options.val_size",
                    "params:training_options.random_state",
                ],
                outputs=["X_train", "X_val", "y_train", "y_val"],
            ),
            node(
                func=train_classifier,
                inputs=["X_train", "y_train"],
                outputs="classifier",
            ),
        ]
    )
