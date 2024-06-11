from dataset_downloader import IMDBDatasetDownloader
from pipeline import IMDbDataPipeline

if __name__ == "__main__":
    # URLs des datasets
    imdb_datasets = {
        "title_basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "title_ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz"
    }

    # Noms des fichiers
    imdb_gz_files = {
        "title_basics": "title.basics.tsv.gz",
        "title_ratings": "title.ratings.tsv.gz"
    }

    imdb_tsv_files = {
        "title_basics": "title.basics.tsv",
        "title_ratings": "title.ratings.tsv"
    }

    hdfs_base_dir = "/imdb"
    hdfs_url = "http://hadoop-master:50070"
    hdfs_user = "root"

    # Télécharger et uploader les datasets sur HDFS
    for key in imdb_datasets.keys():
        loader = IMDBDatasetDownloader(imdb_datasets[key], imdb_gz_files[key], imdb_tsv_files[key], hdfs_url, hdfs_user, hdfs_base_dir)
        loader.run()

    # Chemins HDFS
    imdb_tsv_paths = {
        "title_basics": "hdfs://hadoop-master:9000/imdb/title.basics.tsv",
        "title_ratings": "hdfs://hadoop-master:9000/imdb/title.ratings.tsv"
    }

    hdfs_clean_paths = {
        "title_basics": "hdfs://hadoop-master:9000/imdb_clean/title_basics_clean",
        "title_ratings": "hdfs://hadoop-master:9000/imdb_clean/title_ratings_clean"
    }

    mongo_uri = "mongodb://mongodb:27017"
    mongo_db = "mydb"
    mongo_collection = {
        "title_basics": "title_basics",
        "title_ratings": "title_ratings"
    }

    # Initialisation et exécution du pipeline pour chaque fichier
    pipeline = IMDbDataPipeline(
        imdb_tsv_paths["title_basics"],
        imdb_tsv_paths["title_ratings"],
        hdfs_clean_paths["title_basics"],
        mongo_uri,
        mongo_db,
        mongo_collection["title_basics"]
    )
    pipeline.run()

    pipeline = IMDbDataPipeline(
        imdb_tsv_paths["title_basics"],
        imdb_tsv_paths["title_ratings"],
        hdfs_clean_paths["title_ratings"],
        mongo_uri,
        mongo_db,
        mongo_collection["title_ratings"]
    )
    pipeline.run()