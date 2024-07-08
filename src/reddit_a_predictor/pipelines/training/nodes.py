"""This is a boilerplate pipeline 'training'
generated using Kedro 0.19.6
"""

from __future__ import annotations

import polars as pl
from sklearn.compose import make_column_transformer
from sklearn.model_selection import GridSearchCV, StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder
from sklearn.tree import DecisionTreeClassifier


def split_data(
    df: pl.DataFrame, val_size: float = 0.1, random_state: int = 42
) -> tuple[pl.DataFrame, pl.DataFrame, pl.Series, pl.Series]:
    df_train = df.filter(pl.col("flair_text").is_in(["Not the A-hole", "Asshole"]))

    X = df_train.drop("flair_text")
    y = df_train["flair_text"]

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=val_size, random_state=random_state
    )

    return X_train, X_val, y_train, y_val


def train_classifier(X_train: pl.DataFrame, y_train: pl.Series) -> Pipeline:
    classifier = Pipeline(
        [
            (
                "transformer",
                make_column_transformer(
                    ("passthrough", ["text_length"]),
                    (MinMaxScaler((-1, 1)), ["compound_sentiment"]),
                    (
                        OneHotEncoder(sparse_output=False, handle_unknown="ignore"),
                        ["sfw", "post_type"],
                    ),
                    remainder="drop",
                ).set_output(transform="polars"),
            ),
            ("classifier", DecisionTreeClassifier(class_weight="balanced")),
        ]
    )

    cv = GridSearchCV(
        classifier,
        param_grid={
            "classifier__max_depth": range(1, 6),
            "transformer__minmaxscaler": [MinMaxScaler((-1, 1)), "drop"],
        },
        cv=StratifiedKFold(5),
    )
    cv.fit(X_train, y_train)

    return cv.best_estimator_
