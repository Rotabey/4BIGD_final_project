import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, size, sum, countDistinct
from pymongo import MongoClient

class IMDbDataPipeline:
    def __init__(self, imdb_tsv_path_t, imdb_tsv_path_r, hdfs_clean_path, mongo_uri, mongo_db, mongo_collection):
        self.spark = SparkSession.builder \
            .appName("IMDb Data Pipeline") \
            .config("spark.executorEnv.JAVA_HOME", "/usr/lib/jvm/java-8-openjdk-amd64") \
            .config("spark.mongodb.output.uri", f"{mongo_uri}/{mongo_db}.{mongo_collection}") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .getOrCreate()

        self.imdb_tsv_path_t = imdb_tsv_path_t
        self.imdb_tsv_path_r = imdb_tsv_path_r
        self.hdfs_clean_path = hdfs_clean_path
        self.mongo_client = MongoClient(mongo_uri)
        self.mongo_db = self.mongo_client[mongo_db]
        self.mongo_collection = self.mongo_db[mongo_collection]

    def read_data(self):
        # Lecture des fichiers TSV pour les titres et les évaluations
        titles_df = self.spark.read.option("header", "true").option("sep", "\t").csv(self.imdb_tsv_path_t)
        ratings_df = self.spark.read.option("header", "true").option("sep", "\t").csv(self.imdb_tsv_path_r)
        return titles_df, ratings_df

    def clean_data(self, titles_df, ratings_df):
        # Nettoyage des données des titres
        columns_to_process = ["startYear", "endYear", "runtimeMinutes"]
        for column in columns_to_process:
            titles_df = titles_df.withColumn(column, when(titles_df[column] == "\\N", None).otherwise(titles_df[column].cast("int")))
        columns_to_process = ["tconst", "titleType", "primaryTitle", "originalTitle", "genres"]
        for column in columns_to_process:
            titles_df = titles_df.withColumn(column, when(titles_df[column] == "\\N", None).otherwise(titles_df[column].cast("string")))

        return titles_df, ratings_df

    def transform_data(self, titles_df, ratings_df):
        # Re-vérification du nombre de valeurs nulles après la transformation
        null_counts = []
        for column in titles_df.columns:
            null_counts.append(sum(col(column).isNull().cast("int")).alias(column))
        null_counts_df = titles_df.select(null_counts)
        null_counts_df.show()

        # Vérification et suppression des doublons dans le DataFrame des titres
        count_t = titles_df.count()
        deduplicated_df_t = titles_df.dropDuplicates()
        deduplicated_count_t = deduplicated_df_t.count()
        if count_t == deduplicated_count_t:
            print("No duplicates found in Titles.")
        else:
            print("Duplicates found and removed in Titles.")
        deduplicated_df_t.show()

        # Vérification et suppression des doublons dans le DataFrame des évaluations
        count_r = ratings_df.count()
        deduplicated_df_r = ratings_df.dropDuplicates()
        deduplicated_count_r = deduplicated_df_r.count()
        if count_r == deduplicated_count_r:
            print("No duplicates found in Ratings.")
        else:
            print("Duplicates found and removed in Ratings.")
        deduplicated_df_r.show()

        # Transformation de la colonne genres en plusieurs colonnes distinctes
        deduplicated_df_t = deduplicated_df_t.withColumn("genres_array", split(col("genres"), ","))
        max_values = deduplicated_df_t.select(size(col("genres_array")).alias("num_values")).agg({"num_values": "max"}).collect()[0][0]
        print(f"Maximal number of values in a row: {max_values}")
        for i in range(max_values):
            deduplicated_df_t = deduplicated_df_t.withColumn(f"genre_{i+1}", col("genres_array").getItem(i))
        deduplicated_df_t = deduplicated_df_t.drop("genres_array")
        deduplicated_df_t.show(truncate=False)
        deduplicated_df_t = deduplicated_df_t.drop("genres")
        deduplicated_df_t.show(5)

        return deduplicated_df_t, deduplicated_df_r

    def save_to_hdfs(self, df, df_type):
        # Sauvegarde des données nettoyées dans HDFS
        path = f"{self.hdfs_clean_path}/{df_type}"
        df.write.mode("overwrite").parquet(path)
        print(f"Cleaned data saved to HDFS at {path}")

    def save_to_mongodb(self, df, df_type):
        # Sauvegarde des données nettoyées dans MongoDB
        df.write.format("mongo").mode("overwrite").save()
        print(f"{df_type.capitalize()} data saved to MongoDB")

    def run(self):
        # Lecture des données
        titles_df, ratings_df = self.read_data()

        # Nettoyage des données
        titles_df, ratings_df = self.clean_data(titles_df, ratings_df)

        # Transformation des données
        titles_df, ratings_df = self.transform_data(titles_df, ratings_df)

        # Sauvegarde des données nettoyées dans HDFS et MongoDB
        self.save_to_hdfs(titles_df, "titles")
        self.save_to_hdfs(ratings_df, "ratings")
        self.save_to_mongodb(titles_df, "titles")
        self.save_to_mongodb(ratings_df, "ratings")