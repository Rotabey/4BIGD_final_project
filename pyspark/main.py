from dataset_downloader import IMDBDatasetDownloader
from pipeline import IMDbDataPipeline

if __name__ == "__main__":
    # URLs et noms de fichiers pour les deux datasets
    imdb_datasets = [
        ("https://datasets.imdbws.com/title.basics.tsv.gz", "title.basics.tsv.gz", "title.basics.tsv"),
        ("https://datasets.imdbws.com/title.ratings.tsv.gz", "title.ratings.tsv.gz", "title.ratings.tsv")
    ]

    hdfs_base_dir = "/imdb"
    hdfs_url = "http://hadoop-master:50070"
    hdfs_user = "root"

    for url, gz_file, tsv_file in imdb_datasets:
        loader = IMDBDatasetDownloader(url, gz_file, tsv_file, hdfs_url, hdfs_user, hdfs_base_dir)
        loader.run()

    imdb_tsv_path_t = "hdfs://hadoop-master:9000/imdb/title.basics.tsv"
    imdb_tsv_path_r = "hdfs://hadoop-master:9000/imdb/title.ratings.tsv"
    hdfs_clean_path = "hdfs://hadoop-master:9000/imdb_clean"
    mongo_uri = "mongodb://mongodb:27017"
    mongo_db = "mydb"
    mongo_collection = "imdb_data"

    # Initialisation et exécution du pipeline pour les deux fichiers
    pipeline = IMDbDataPipeline(imdb_tsv_path_t, imdb_tsv_path_r, hdfs_clean_path, mongo_uri, mongo_db, mongo_collection)
    pipeline.run()