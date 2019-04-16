#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Copyright 2019 tellic LLC. All rights reserved.

Author: Daren Jacobs
Created: 2019-03-28
Last Modified: 2019-04-10

Jira Ticket: TELLIC-523 - ETL the OMIM, Gene Ontology, +1Data

GOAL: Put the Gene Ontology files in GCS

Description:
This script downloads .gz files from:
http://current.geneontology.org/annotations/index.html
and uploads the extracted files to:
https://console.cloud.google.com/storage/browser/tellic-dev/geneontology
and lodads ddata to:
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


def copy_to_bucket(bucket, subdir, filename):
    """Copy extracted gz file to GCS"""

    # upload to GCS
    blob = bucket.blob(subdir + '/' + filename)
    blob.upload_from_filename(filename)


def get_bad_lines(temp_dest):
    """ Get lines that stat with !"""
    ignore_lines = 0
    with gzip.open(temp_dest) as f_in:
        for line in f_in:
            this_line = line.decode('utf-8')
            if this_line[0] == '!':
                ignore_lines += 1
                continue

    return ignore_lines


def extract_gz_file(filename, temp_dest):
    """Extact and return the gz file and number of lines
    """

    ignore_lines = get_bad_lines(temp_dest)
    with gzip.open(temp_dest) as f_in:
        with open(filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            total_lines = len(open(filename).readlines())

    total_lines = total_lines - ignore_lines
    return filename, total_lines


def create_text_block(f_in, num_lines):
    """Write extracted file to text file in chunks of num_lines"""

    this_file = 'text_block.txt'
    scratch_file = open(this_file, 'w+')
    line_count = 0

    for line in f_in:
        if line_count > num_lines:
            break

        # skip the line if it starts with and !
        if line[0] == '!':
            continue

        scratch_file.write(line)
        line_count += 1

    return this_file


def load_lines(file_name):
    """Load text block into Big Query"""
    LOGGER.info("Writing text block to BigQuery")

    # create article CSV
    csv_file = file_name
    column_names = ['db', 'db_object_id', 'db_object_symbol', 'qualifier',
                    'go_id', 'db_reference', 'evidence_code', 'with_or_from',
                    'aspect', 'db_object_name', 'db_object_synonym',
                    'db_object_type', 'taxon', 'date', 'assigned_by',
                    'annotation_extension']

    dataframe = pd.read_csv(csv_file, delimiter='\t', index_col=False,
                            names=column_names, error_bad_lines=False,
                            warn_bad_lines=True, skip_blank_lines=True,
                            verbose=True)
    dataframe.to_csv('new.csv', encoding='utf-8', index=False)
    dataframe.to_gbq(destination_table=DESTINATION_TABLE, project_id=PROJECT, if_exists='append')

    os.remove('new.csv')


@timer
def main():
    """
    Create a GCS client and get a bucket object, download and extract the .gz
    file from the URL, check if the file exists in GCS, upload the file to GCS,
    copy chunks of the extracted file to a text block, create a csv from the
    text block, write the csv to BQ, and delete local temp files
    """
    # Set url
    url = GOA_URL
    # Set the name of the gz file
    filename = url.split('/')[-1]
    # Temp location for gz file
    temp_dest = tempfile.mkdtemp('_go') + '/' + filename

    # Create a client session and get the bucket object
    bucket = create_session(PROJECT, BUCKET_NAME)

    # Get list of objects in the bucket
    my_dir = SUB_DIR
    blob_list = get_bucket_info(bucket, my_dir)
    LOGGER.info("GCS Bucket BLOB_LIST: %s", blob_list)

    # check if file is already in GCS
    if my_dir + '/' + filename[:-3] in blob_list:
        LOGGER.info("File already exists, skipping download: %s", filename[:-3])
    # Download the gz file
    else:
        LOGGER.info("Downloading file to %s", temp_dest)
        urllib.request.urlcleanup()
        urllib.request.urlretrieve(url, temp_dest)

        # Get extracted file object & number of lines in the extracted file
        extracted_file, total_lines = extract_gz_file(filename[:-3], temp_dest)

        # Copy gz file to GCS
        copy_to_bucket(bucket, my_dir, extracted_file)

        # Set up variables for the while loop
        if total_lines < NUM_LINES:
            num_lines = total_lines
        else:
            num_lines = NUM_LINES

        LOGGER.info("TOTAL LINES: %d", total_lines)
        LOGGER.info("NUM LINES: %d", num_lines)
        lines_written = 0
        to_go = 0
        locale.setlocale(locale.LC_ALL, 'en_US.utf8')

        # create while-loop that writes extracted file in chunks
        with open(extracted_file) as f_in:
            while lines_written < total_lines:
                to_go = total_lines - lines_written
                if to_go < num_lines:
                    num_lines = to_go

                current_file = create_text_block(f_in, num_lines)
                lines_written += len(open(current_file).readlines())
                load_lines(current_file)

                # Output some information about current status
                countdown = locale.format_string("%d", to_go, grouping=True)
                LOGGER.info("%d lines of %d written\n %s lines to go",
                            lines_written, total_lines, countdown)
                os.remove(current_file)


        os.remove(temp_dest)
        os.remove(extracted_file)

if __name__ == '__main__':

    ## List of gz files to process
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
    NUM_LINES = 50000

    main()
