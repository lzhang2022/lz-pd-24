"""
Main pipeline entry point
"""

from pyspark.sql import DataFrame, SparkSession

SPARK = SparkSession.builder.appName("Create DataFrame").getOrCreate()
RAW_DATA_DIRECTORY = "data/raw/"
OUTPUT_DATA_DIRECTORY = "data/output"

RAW_DATA_SCHEMAS = {
    "movies": "movie_id int, title string, genres string",
    "ratings": "user_id int, movie_id int, rating int, timestamp int",
    "users": "user_id int, gender string, age string, occupation int, `Zip-code` string",
}


def ingest_raw_data(data_source: str) -> DataFrame:
    """
    Ingest the .dat raw data sources and return a spark dataframe.
    Use explicit schema definition to ensure correct data types.
    """
    df = SPARK.read.csv(
        f"{RAW_DATA_DIRECTORY}/{data_source}.dat",
        sep="::",
        schema=RAW_DATA_SCHEMAS[data_source]
    )

    return df


def preprocess_movie_data(raw_movies_df: DataFrame) -> DataFrame:
    """
    Clean the movie data, drop duplicates on movie_id and if title is empty
    """


def main():
    """
    This is the entry point to run the pipeline
    """

    # ingest raw data
    raw_movies_df = ingest_raw_data(data_source="movies")
    raw_movies_df.limit(5).show()
    print(raw_movies_df.schema)

    raw_ratings_df = ingest_raw_data(data_source="ratings")
    raw_ratings_df.limit(5).show()
    print(raw_movies_df.schema)

    raw_users_df = ingest_raw_data(data_source="users")
    raw_users_df.limit(5).show()
    print(raw_movies_df.schema)

    # preprocess data

    # transform and output movies new dataframe


    # tranform and output users new dataframe


main()
