#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  3 15:39:31 2024

@author: ricardi
"""
import argparse
from kgforge.core import KnowledgeGraphForge
import json
import os
import pandas as pd
from typing import Optional, List

from src.arguments import define_arguments

from src.helpers import initialize_objects


def search(forge: KnowledgeGraphForge, type_: str, start: int = 0, amount: int = 200):
    bucket = forge._store.bucket
    exp_type = forge._model.context().expand(type_)
    query = f"""SELECT ?id ?_constrainedBy WHERE {{ Graph ?g {{?id rdf:type <{exp_type}>;
        <https://bluebrain.github.io/nexus/vocabulary/constrainedBy> ?_constrainedBy;
        <https://bluebrain.github.io/nexus/vocabulary/deprecated> 'false'^^xsd:boolean;
        <https://bluebrain.github.io/nexus/vocabulary/project> <https://bbp.epfl.ch/nexus/v1/projects/{bucket}>. 
        }}}}
      ORDER BY ?id 
      OFFSET {start} LIMIT {amount}""" 
    return forge.sparql(query, debug=False)

def find_mismatches(forge: KnowledgeGraphForge, type_: str,
                    schema_id: str, issues: Optional[List] = None, 
                    chunk_size: int = 200):
    if issues is None:
        issues = []
    cnt = 0
    while True:
        resources = search(forge, type_, start=cnt*chunk_size, amount=chunk_size)
        if not resources:
            break
        for resource in resources:
            if resource._constrainedBy != schema_id:
                issues.append({'id': resource.id, 'type': type_, 'schema': resource._constrainedBy})
        cnt += 1
    return issues

def do(forge: KnowledgeGraphForge, types: List, chunk_size=200):
    issues = []
    for type_ in types:
        schema_id = forge._model.schema_id(type_)
        issues = find_mismatches(forge, type_, schema_id,
                                 issues=issues, chunk_size=chunk_size)
    return pd.DataFrame(issues)

if __name__ == '__main__':
    parser = define_arguments(argparse.ArgumentParser())
    parser.add_argument("--chunk_size", help="How many resources should be searched at once",
        type=int, default=200)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--typelist", help="The list of types to check",
        type=str)
    group.add_argument("--typelist_filepath", help="A json file containing the list of types to check",
        type=str)
    received_args, leftovers = parser.parse_known_args()
    if received_args.typelist_filepath:
        with open(received_args.typelist_filepath, 'r') as f:
            types = [i.strip() for i in json.load(f)]
    else:
        types = [i.strip() for i in received_args.typelist.split(",")]
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    output_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(output_directory, exist_ok=True)
    
    token, forge, _ = initialize_objects(received_args.username, received_args.password, org, project, is_prod=True)
    
    df = do(forge, types, received_args.chunk_size)
    df.to_csv(os.path.join(output_dir, f'{org}_{project}_constraint_match.tsv'), sep='\t')
                
            