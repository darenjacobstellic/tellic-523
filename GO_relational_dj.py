#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copyright 2018 tellic LLC. All rights reserved.

@author: Henry Crosby
Created on Thu Mar 28 15:05:48 2019
Descripton:

"""

import pandas as pd
from obo_funs import read_obo


def write_go_to_tsv(path_name, my_csv):
    """ Write Go.tsv file """
    graph = read_obo("http://current.geneontology.org/ontology/go.obo",
                     "networkx")

    rows = []
    for node in graph.nodes():
        dict_ = graph.node.get(node)
        rows.append({"id": node,
                     "name": dict_.get('name'),
                     "def": dict_.get('def'),
                     "namespace": dict_.get('namespace')})
    dataframe = pd.DataFrame(rows)
    column_names = ['id', 'name', 'namespace', 'def']
    dataframe[['id', 'name', 'namespace', 'def']].to_csv(path_name, sep='\t', index=False)

    out_df = pd.read_csv(path_name, delimiter='\t', header=0, names=column_names)
    out_df.to_csv(my_csv, sep='\t', index=False)
    out_df.to_gbq(destination_table=DESTINATION_TABLE, project_id=PROJECT,
                  if_exists='replace')


if __name__ == '__main__':
    FILENAME = "GO.tsv"
    MY_CSV = "Order.csv"
    PROJECT = 'tellic-dev'
    DATASET = 'GeneOntology'
    TABLE = 'GO_relational'
    DESTINATION_TABLE = (DATASET + '.' + TABLE)
    write_go_to_tsv(FILENAME, MY_CSV)
