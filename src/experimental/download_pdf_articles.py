import os
import urllib.request
import urllib.parse
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from tools import setup_logs


def collect_urls(source_dir: str) -> List[str]:

    APP_NAME = "Download pubmed pdf articles"
    logging.info(f"Starting Spark application {APP_NAME}...\n\n")
    spark = SparkSession.builder \
        .appName("") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "3g") \
        .getOrCreate()

    logging.info(f"Spark application {APP_NAME} is now running !")
    logging.info("collecting urls of pubmed pdfs...")
    url_metadata_df = spark.read.parquet(source_dir)
    logging.info(f"Collected url of pubmed PDF articles from '{source_dir}' .")
    urls = [row.fileUrl for row in url_metadata_df.select('fileUrl').collect()]

    return urls

def download_pdf_from_ftp(ftp_url, dest_dir="downloads", filename=None):
    try:
        if filename is None:
            parsed_url = urllib.parse.urlparse(ftp_url)
            filename = os.path.basename(parsed_url.path)
            if not filename.endswith('.pdf'):
                filename += '.pdf'

        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        file_path = os.path.join(dest_dir, filename)

        logging.info(f"Downloading: {ftp_url}")
        logging.info(f"Saving to: {file_path}")
        urllib.request.urlretrieve(ftp_url, file_path)
        logging.info("Download completed successfully!")

        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            file_size = os.path.getsize(file_path)
            logging.info(f"File saved: {file_path} ({file_size:,} bytes)")
            return file_path
        else:
            logging.info("Error: Downloaded file is empty or doesn't exist")
            return None

    except urllib.error.URLError as e:
        logging.info(f"URL Error: {e}")
        return None
    except Exception as e:
        logging.info(f"Error downloading file: {e}")
        return None


def download_pdfs_multithread(urls: List[str], dest_dir: str = "downloads", max_threads: int = 5):
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    results = []

    logging.info("Starting the Pubmed PDF downloading process...")
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_url = {
            executor.submit(download_pdf_from_ftp, url, dest_dir): url
            for url in urls
        }

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                file_path = future.result()
                if file_path:
                    logging.info(f"Downloaded successfully: {file_path}")
                else:
                    logging.warning(f"Failed to download: {url}")
                results.append((url, file_path))
            except Exception as e:
                logging.error(f"Exception during download of {url}: {e}")
                results.append((url, None))

    return results


if __name__=="__main__":

    SILVER_PATH = "data/silver"
    DOWNLOADS_PATH = "data/raw/data/pdf"
    MAX_THREADS = 10

    setup_logs()
    last_dir = os.listdir(SILVER_PATH)[-1]
    source_dir = f"{SILVER_PATH}/{last_dir}/pubmed_cleaned_url_metadata.parquet"
    urls = collect_urls(source_dir)
    download_pdfs_multithread(urls,dest_dir=DOWNLOADS_PATH, max_threads=MAX_THREADS)
