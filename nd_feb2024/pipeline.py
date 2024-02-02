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
from pyspark.sql.window import Window

LOG = logging.getLogger()
SPARK = SparkSession.builder.getOrCreate()
RAW_DATA_DIRECTORY = "data/raw/"
OUTPUT_DATA_DIRECTORY = "data/output"

RAW_DATA_SCHEMAS = {
    "movies": "movie_id int, title string, genres string",
    "ratings": "user_id int, movie_id int, rating int, timestamp int",
    "users": "user_id int, gender string, age string, occupation int, zip_code string",
}


def valid_user_id():
    """
    user_id is a valid value if not null and in the range 1 to 6040
    """
    return (
        (f.col("user_id").isNotNull())
        & (f.col("user_id") >= 1)
        & (f.col("user_id") <= 6040)
    )


def valid_movie_id():
    """
    movie_id is a valid value if not null and in the range 1 to 3952
    """
    return (
        (f.col("movie_id").isNotNull())
        & (f.col("movie_id") >= 1)
        & (f.col("movie_id") <= 3952)
    )


def valid_ratings():
    """
    ratings is a valid value if not null and on a 5-star review system
    """
    return (
        (f.col("rating").isNotNull()) & (f.col("rating") >= 1) & (f.col("rating") <= 5)
    )


def ingest_raw_data(data_source: str) -> DataFrame:
    """
    Ingest the .dat raw data sources and return a spark dataframe.
    Use explicit schema definition to ensure correct data types.
    """
    source_path = f"{RAW_DATA_DIRECTORY}/{data_source}.dat"

    df = SPARK.read.csv(source_path, sep="::", schema=RAW_DATA_SCHEMAS[data_source])

    # LOG.info(f"ingested {df.count()} rows from source {source_path}")
    print(f"ingested {df.count()} rows from source {source_path}")

    return df


def preprocess_movie_data(raw_movies_df: DataFrame) -> DataFrame:
    """
    Clean the movie data:
     - drop records if title is empty, this specifies what the movie is
     - drop duplicates on movie_id
    """
    df = (
        raw_movies_df.filter(valid_movie_id())
        .filter((f.col("title").isNotNull()))
        .dropDuplicates(subset=["movie_id"])
    )

    # LOG.info(f"preprocessed movies data contains {df.count()} rows")
    print(f"preprocessed movies data contains {df.count()} rows")

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

    # LOG.info(f"preprocessed ratings data has {df.count()} rows")
    print(f"preprocessed ratings data has {df.count()} rows")

    return df


def preprocess_users_data(raw_users_df: DataFrame) -> DataFrame:
    """
    Cleans the users raw data
    """

    df = raw_users_df.filter(valid_user_id())

    # if required, add demographic and occupation mappings here for better readability

    # LOG.info(f"preprocessed users data has {df.count()} rows")
    print(f"preprocessed users data has {df.count()} rows")

    return df


def transform_movies_ratings(
    preprocess_movies_df: DataFrame, preprocess_ratings_df: DataFrame
) -> DataFrame:
    """
    Return a dataframe that contains all movie data, as well as max, min, average
    ratings for each of the movies
    """

    ratings_df = preprocess_ratings_df.groupBy("movie_id").agg(
        f.min("rating").alias("min_rating"),
        f.max("rating").alias("max_rating"),
        # keep the raw value of average and no rounding for precision
        f.avg("rating").alias("avg_rating"),
    )

    # print("this is the ratings_df after agg")
    # ratings_df.limit(10).show()

    transform_df = preprocess_movies_df.join(ratings_df, on=["movie_id"], how="left")

    print(f"transform_movie_ratings has {transform_df.count()} rows")

    return transform_df


def transform_user_ratings(
    preprocess_users_df: DataFrame,
    preprocess_ratings_df: DataFrame,
    preprocess_movies_df: DataFrame,
) -> DataFrame:
    """
    Returns a dataframe that contains all users and their top 3 movies based on the
    ratings given. This retains any demographic information that was voluntarily
    provided by the user, as well as movie information. The ranking for each user and
    movie is also provided.

    Note, if a user has the same maximum rating for more than 3 movies, then the top 3
    are selected based on ascending movie_id. This is to ensure consistency.
        e.g. user_id 1 has rated 5 movies with 4 star, and this is the maximum they have
        given any movie. The movie IDs are 1,2,3,4, 5. In this transformation movie IDs
        1,2,3 are selected as they are the first 3 in ascending order.
    """

    # for each user_id, find the top 3 movie_id
    window_user = Window.partitionBy("user_id").orderBy(
        f.col("rating").desc(), f.col("movie_id").asc()
    )
    top_movies_df = (
        preprocess_ratings_df.withColumn("row_num", f.row_number().over(window_user))
        .filter(f.col("row_num") <= 3)
        .select(
            "user_id", "movie_id", "rating", f.col("row_num").alias("user_movie_rank")
        )
    )

    # print("top movies are")
    # top_movies_df.show()

    # join data together
    transform_df = (
        top_movies_df.join(preprocess_users_df, on=["user_id"], how="left").join(
            preprocess_movies_df, on=["movie_id"], how="left"
        )
        # select columns for outputting
        .select(
            "user_id",
            "gender",
            "age",
            "occupation",
            "zip_code",
            "movie_id",
            "rating",
            "user_movie_rank",
            f.col("title").alias("movie_title"),
            f.col("genres").alias("movie_genres"),
        )
    )

    print("the users top movies so far")
    transform_df.limit(10).show()
    print(transform_df.schema)

    return transform_df


def export_dataframe(
    df: DataFrame, file_name: str, destination: str = OUTPUT_DATA_DIRECTORY
) -> None:
    """
    Exports the dataframe to a csv file in the destination folder. Using csv format
    so that it can easily be viewed. Otherwise using something like parquet would be
    faster
    """
    full_file_path = f"{destination}/{file_name}.csv"

    # using pandas for simplicity
    pandas_df = df.toPandas()
    pandas_df.to_csv(full_file_path, header=True, index=False)

    # LOG.info(f"{df.count()} rows outputted to file {full_file_path}.csv")
    print(f"{df.count()} rows outputted to file {full_file_path}")


def main():
    """
    This is the entry point to run the pipeline
    """

    # ingest raw data
    raw_movies_df = ingest_raw_data(data_source="movies")
    # print("raw_movies_df is")
    # raw_movies_df.limit(5).show()
    # print(raw_movies_df.schema)

    raw_ratings_df = ingest_raw_data(data_source="ratings")
    # print("raw ratings_df is")
    # raw_ratings_df.limit(5).show()
    # print(raw_ratings_df.schema)

    raw_users_df = ingest_raw_data(data_source="users")
    # raw_users_df.limit(5).show()
    # print(raw_users_df.schema)

    # preprocess data
    preprocess_movies_df = preprocess_movie_data(raw_movies_df=raw_movies_df)
    # preprocess_movies_df.limit(10).show()

    preprocess_ratings_df = preprocess_ratings_data(raw_ratings_df=raw_ratings_df)
    # preprocess_ratings_df.limit(10).show()

    preprocess_users_df = preprocess_users_data(raw_users_df=raw_users_df)
    preprocess_users_df.limit(10).show()

    # transform movies new dataframe
    transform_movies_df = transform_movies_ratings(
        preprocess_movies_df=preprocess_movies_df,
        preprocess_ratings_df=preprocess_ratings_df,
    )
    # transform_movies_df.limit(15).show()
    # print(transform_movies_df.schema)

    # transform users new dataframe
    transform_users_df = transform_user_ratings(
        preprocess_movies_df=preprocess_movies_df,
        preprocess_ratings_df=preprocess_ratings_df,
        preprocess_users_df=preprocess_users_df,
    )

    # output the raw data and transformed data
    export_dataframe(df=raw_movies_df, file_name="raw_movies")
    export_dataframe(df=raw_ratings_df, file_name="raw_ratings")
    export_dataframe(df=raw_users_df, file_name="raw_users")
    export_dataframe(df=transform_movies_df, file_name="transform_movies")
    export_dataframe(df=transform_users_df, file_name="transform_users")


main()
