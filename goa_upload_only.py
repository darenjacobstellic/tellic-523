#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Copyright 2019 tellic LLC. All rights reserved.

Author: Daren Jacobs
Created: 2019-03-28
Last Modified: 2019-04-05

Jira Ticket: TELLIC-523 - ETL the OMIM, Gene Ontology, +1Data

Description:
This script downloads .gz files from:
http://current.geneontology.org/annotations/index.html
and uploads the extracted files to:
https://console.cloud.google.com/storage/browser/tellic-dev/geneontology
That's all this does and should not be used as goa.py downloads gz files
and puts the data in BigQuery
"""

import gzip
import logging
import os
import re
import tempfile
import time
import shutil
import urllib.request
from google.cloud import storage


# create Logger
LOGGER = logging.getLogger('Gene Ontology Ingestion')

# Set log level
LOGGER.setLevel(logging.DEBUG)

# create console handler and set level to debug
CONSOLE_HANDLER = logging.StreamHandler()
CONSOLE_HANDLER.setLevel(logging.DEBUG)

# create formatter
FORMATTER = logging.Formatter("%(name)s - %(levelname)s - %(message)s")

# add formatter to console handler
CONSOLE_HANDLER.setFormatter(FORMATTER)

# clear the handlers to stop repeat notifications
if LOGGER.hasHandlers():
    LOGGER.handlers.clear()

# add console handler to Logger
LOGGER.addHandler(CONSOLE_HANDLER)


def timer(func):
    """A timer for function"""

    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapse = time.time() - start
        LOGGER.info("%s function ran in: %d sec", func.__name__, elapse)
        print("{} function ran in: {:.2f} sec".format(func.__name__, elapse))
        return result

    return wrapper


def create_session(project, bucket_name):
    """Create GCS client"""

    LOGGER.info("Creating GCS client")
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'tellic-dev-2807ffb4dd7f.json'
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket_name)

    return bucket


def get_bucket_info(upload_bucket, subdir):
    """Create client, set bucket, return bucket information"""

    # Get a list of files
    blobs = upload_bucket.list_blobs(prefix=subdir)
    blob_list = []

    for blob in blobs:
        blob_list.append(blob.name)

    return blob_list


def copy_to_bucket(filename, upload_bucket, subdir):
    """Copy the zipped file to GCS"""

    blob = upload_bucket.blob(subdir + '/' + filename)
    blob.upload_from_filename(filename)


def goa_file_extract(url, bucket):
    """Check if uploaded, download, extract, to temp location upload to GCS
    and delete local temp files
    """

    filename = url.split('/')[-1]
    temp_dest = tempfile.mkdtemp('_go') + '/' + filename
    my_dir = SUB_DIR
    blob_list = get_bucket_info(bucket, my_dir)

    # check if file has been uploaded
    if my_dir + '/' + filename[:-3] in blob_list:
        LOGGER.info("File already exists, skipping download: %s",
                    filename[:-3])

    # Download, extract, upload to GCS, and delete local temp files
    else:
        LOGGER.info("Downloading file to %s", temp_dest)
        urllib.request.urlcleanup()
        urllib.request.urlretrieve(url, temp_dest)

        with gzip.open(temp_dest) as f_in:
            with open(filename[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                copy_to_bucket(filename[:-3], bucket, my_dir)
                print("Out File:", filename[:-3])
                os.remove(temp_dest)
                os.remove(filename[:-3])


def get_files(url):
    """Get list of gaf gz files"""

    pattern = re.compile(r'(goa.*human.*gaf+\.gz)"')
    with urllib.request.urlopen(url) as response:
        html = response.read().decode('utf-8')
    files = pattern.findall(html)

    return files


def goa_files(goa_url):
    """ Return URL List of gz files """

    all_files = get_files(goa_url)
    for file_name in all_files:
        yield goa_url + file_name


@timer
def run():
    """ run from this script """

    # Get the gzip files
    goa_url = 'http://current.geneontology.org/annotations/'
    urls = goa_files(goa_url)

    # Set a client session and get the bucket
    bucket = create_session(PROJECT, BUCKET_NAME)

    # Extract the tar files
    for url in urls:
        print(url)
        goa_file_extract(url, bucket)


if __name__ == '__main__':
    PROJECT = 'tellic-dev'
    BUCKET_NAME = 'tellic-dev'
    SUB_DIR = 'GeneOntology'
    run()
