"""
Main pipeline entry point.

Input:
    Raw data sources:
    movies.dat
    ratings.dat
    users.dat

Output:
    movies_ratings: data containing the max, min, average ratings for each movie
    user_movies: data containing top 3 films for each user

Logging statements are used for debugging and monitoring purposes.
"""

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
import logging

LOG = logging.getLogger()
SPARK = SparkSession.builder.getOrCreate()
RAW_DATA_DIRECTORY = "data/raw/"
OUTPUT_DATA_DIRECTORY = "data/output"

RAW_DATA_SCHEMAS = {
    "movies": "movie_id int, title string, genres string",
    "ratings": "user_id int, movie_id int, rating int, timestamp int",
    "users": "user_id int, gender string, age string, occupation int, `Zip-code` string",
}


USERS_OCCUPATION_MAPPING = {
    0: "other",
    1: "academic/educator",
    2: "artist",
    3: "clerical/admin",
    4: "college/grad student",
    5: "customer service",
    6: "doctor/health care",
    7: "executive/managerial",
    8: "farmer",
    9: "homemaker",
    10: "K-12 student",
    11: "lawyer",
    12: "programmer",
    13: "retired",
    14: "sales/marketing",
    15: "scientist",
    16: "self-employed",
    17: "technician/engineer",
    18: "tradesman/craftsman",
    19: "unemployed",
    20: "writer",
}


def valid_user_id():
    """
    user_id is a valid value if not null and in the range 1 to 6040
    """
    return (
        (f.col("user_id").isNull()) | (f.col("user_id") < 1) | (f.col("user_id") > 6040)
    )


def valid_movie_id():
    """
    movie_id is a valid value if not null and in the range 1 to 3952
    """
    return (
        (f.col("movie_id").isNull())
        | (f.col("movie_id") < 1)
        | (f.col("movie_id") > 3952)
    )


def valid_ratings():
    """
    ratings is a valid value if not null and on a 5-star review system
    """
    return (f.col("rating").isNull()) | (f.col("rating") < 1) | (f.col("rating") > 5)


def ingest_raw_data(data_source: str) -> DataFrame:
    """
    Ingest the .dat raw data sources and return a spark dataframe.
    Use explicit schema definition to ensure correct data types.
    """
    source_path = f"{RAW_DATA_DIRECTORY}/{data_source}.dat"

    df = SPARK.read.csv(source_path, sep="::", schema=RAW_DATA_SCHEMAS[data_source])

    LOG.info(f"ingested {df.count()} rows from source {source_path}")

    return df


def preprocess_movie_data(raw_movies_df: DataFrame) -> DataFrame:
    """
    Clean the movie data:
     - drop records if title or genres is empty
     - drop duplicates on movie_id
    """
    df = (
        raw_movies_df.filter(valid_movie_id())
        .filter((f.col("title").isNull()) | (f.col("genres").isNull()))
        .dropDuplicates(subset=["movie_id"])
    )

    LOG.info(f"preprocessed movies data contains {df.count()} rows")

    return df


def preprocess_ratings_data(raw_ratings_df: DataFrame) -> DataFrame:
    """
    Cleans the ratings raw data
    """
    df = (
        raw_ratings_df.filter(valid_user_id())
        .filter(valid_movie_id())
        .filter(valid_ratings())
    )

    LOG.info(f"preprocessed ratings data has {df.count()} rows")

    return df


def preprocess_users_data(raw_users_df: DataFrame) -> DataFrame:
    """
    Cleans the users raw data
    """

    df = raw_users_df.filter(valid_user_id())

    # if time, add demographic and occupation mappings here for better readability

    LOG.info(f"preprocessed users data has {df.count()} rows")

    return df


def transform_movies_ratings(preprocess_movies_df: DataFrame, preprocess_ratings_df: DataFrame) -> DataFrame:
    """
    Return a dataframe that contains all movie data, as well as max, min, average
    ratings for each of the movies
    """

    ratings_df = (
        preprocess_ratings_df.groupBy("movie_id")
        .agg(
            f.min("rating").alias("min_ratings"),
            f.max("rating").alias("min_ratings"),
            f.avg("rating").alias("avg_ratings"),
        )
    )

    transform_df = preprocess_movies_df.join(ratings_df, on=["movie_id"], how="left")

    return transform_df


def transform_user_ratings():
    """
    Returns a dataframe that contains all users and their top 3 movies based on the
    ratings given
    """
    pass


def export_dataframe(df: DataFrame, destination: str) -> None:
    pass


def main():
    """
    This is the entry point to run the pipeline
    """

    # ingest raw data
    raw_movies_df = ingest_raw_data(data_source="movies")
    # raw_movies_df.limit(5).show()
    # print(raw_movies_df.schema)

    raw_ratings_df = ingest_raw_data(data_source="ratings")
    # raw_ratings_df.limit(5).show()
    # print(raw_movies_df.schema)

    raw_users_df = ingest_raw_data(data_source="users")
    # raw_users_df.limit(5).show()
    # print(raw_movies_df.schema)

    # preprocess data
    preprocess_movies_df = preprocess_movie_data(raw_movies_df=raw_movies_df)
    preprocess_ratings_df = preprocess_ratings_data(raw_ratings_df=raw_ratings_df)

    # transform and output movies new dataframe
    transform_movies_df = transform_movies_ratings()

    # tranform and output users new dataframe


main()
