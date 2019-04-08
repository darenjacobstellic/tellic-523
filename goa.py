#!/usr/bin/env python
# coding: utf-8
"""Copyright 2019 tellic LLC. All rights reserved.

Author: Daren Jacobs
Created: 2019-03-28
Last Modified: 2019-04-05

Jira Ticket: TELLIC-523 - ETL the OMIM, Gene Ontology, +1Data

GOAL: Put the downloaded and extracted Gene Ontology gz files data in BigQuery

Description:
This script downloads .gz files from:
http://current.geneontology.org/annotations/index.html
and uploads the extracted files to:
https://console.cloud.google.com/storage/browser/tellic-dev/geneontology
and loads the data to:
https://console.cloud.google.com/bigquery?project=tellic-dev&organizationId=23262195837&p=tellic-dev&d=GeneOntology&t=GAF_files&page=table
"""


import gzip
import locale
import logging
import os
import tempfile
import time
import shutil
import urllib.request
import pandas as pd
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


def create_session(project, bucket_name):
    """Create GCS client"""

    LOGGER.info("Creating GCS client")
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'tellic-dev-2807ffb4dd7f.json'
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket_name)

    return bucket


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


def get_bucket_info(upload_bucket, subdir):
    """Create client, set bucket, return bucket information"""

    # Get a list of files
    blobs = upload_bucket.list_blobs(prefix=subdir)
    blob_list = []

    for blob in blobs:
        blob_list.append(blob.name)

    return blob_list


def copy_to_bucket(filename, upload_bucket, subdir, temp_dest):
    """Copy extracted gz file to GCS"""

    with gzip.open(temp_dest) as f_in:
        with open(filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            blob = upload_bucket.blob(subdir + '/' + filename)
            blob.upload_from_filename(filename)


def get_total_lines(temp_dest):
    """ Get total number of lines in the extracted gz file """
    total_lines = 0
    with gzip.open(temp_dest) as f_in:
        f_temp = f_in.read()
        total_lines = len(f_temp)
        LOGGER.info("TOTAL LINES: %s", total_lines)
        return total_lines


def create_text_block(filename, temp_dest, num_lines):
    """Write NUM_LINES number of lines to a text file
       This makes it so the files are in blocks
    """

    # Write lines to file
    line_count = 0
    this_file = 'text.' + filename
    scratch_file = open(this_file, 'a')
    with gzip.open(temp_dest) as f_in:
        for line in f_in:
            if line_count > num_lines:
                scratch_file.close()
                break

            this_line = line.decode('utf-8')
            if this_line[0] == '!':
                continue

            scratch_file.write(this_line)
            line_count += 1


    return this_file


def load_lines(file_name):
    """ write the text block to BigQuery table """
    LOGGER.info("Writing text block to BigQuery")

    # create article CSV
    csv_file = file_name
    column_names = ['db', 'db_object_id', 'db_object_symbol', 'qualifier',
                    'go_id', 'db_reference', 'evidence_code', 'with_or_from',
                    'aspect', 'db_object_name', 'db_object_synonym',
                    'db_object_type', 'taxon', 'date', 'assigned_by',
                    'annotation_extension']
    dataframe = pd.read_csv(csv_file, delimiter='\t', index_col=False,
                            names=column_names, verbose=True)
    dataframe.to_csv('new.csv', encoding='utf-8', index=False)
    dataframe.to_gbq(destination_table=DESTINATION_TABLE, project_id=PROJECT,
                     if_exists='append')


@timer
def main(url, num_lines):
    """
    Create a GCS client and get a bucket object,
    Download and extract the .gz file from the URL,
    Check if if the file exists in GCS
    and delete local temp files
    """

    # Set a client session and get the bucket
    bucket = create_session(PROJECT, BUCKET_NAME)

    my_dir = SUB_DIR
    filename = url.split('/')[-1]

    # Output path for the extacted file
    temp_dest = tempfile.mkdtemp('_go') + '/' + filename
    blob_list = get_bucket_info(bucket, my_dir)
    LOGGER.info("HERE IS THE BLOB_LIST: %s", blob_list)

    # check if gz file is in GCS
    if my_dir + '/' + filename[:-3] in blob_list:
        LOGGER.info("File already exists, skipping download: %s",
                    filename[:-3])

    # Download the gz file
    else:
        LOGGER.info("Downloading file to %s", temp_dest)
        urllib.request.urlcleanup()
        urllib.request.urlretrieve(url, temp_dest)

        # Copy gz file to GCS
        copy_to_bucket(filename[:-3], bucket, my_dir, temp_dest)

        # Get total number of lines
        total_lines = get_total_lines(temp_dest)

        # create while loop that writes total number of lines are written
        lines_written = 0
        to_go = 0

        # I want the countdown numbers to look nice
        locale.setlocale(locale.LC_ALL, 'en_US.utf8')
        while lines_written < total_lines:
            current_file = create_text_block(filename[:-3], temp_dest,
                                             num_lines)
            lines_written += len(open(current_file).readlines())
            load_lines(current_file)
            to_go = total_lines - lines_written
            countdown = locale.format_string("%d", to_go, grouping=True)
            LOGGER.info("%d lines of %d written\n %s lines to go",
                        lines_written, total_lines, countdown)
            os.remove(current_file)

        os.remove(temp_dest)


if __name__ == '__main__':

    # List of gz files to process
    # http://current.geneontology.org/annotations/goa_human.gaf.gz
    # http://current.geneontology.org/annotations/goa_human_complex.gaf.gz
    # http://current.geneontology.org/annotations/goa_human_isoform.gaf.gz
    # http://current.geneontology.org/annotations/goa_human_rna.gaf.gz

    GOA_URL = 'http://current.geneontology.org/annotations/goa_human.gaf.gz'
    PROJECT = 'tellic-dev'
    BUCKET_NAME = 'tellic-dev'
    SUB_DIR = 'GeneOntology'
    DATASET = 'GeneOntology'
    TABLE = 'GAF_files'
    DESTINATION_TABLE = (DATASET + '.' + TABLE)
    NUM_LINES = 999999

    main(GOA_URL, NUM_LINES)
