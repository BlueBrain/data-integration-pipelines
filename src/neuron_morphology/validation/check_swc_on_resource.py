#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 13:31:35 2024

@author: ricardi
"""
from datetime import datetime, timedelta
import json
from collections import defaultdict
from typing import Tuple, Dict, List

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.mappings import DictionaryMapping
from kgforge.specializations.mappers import DictionaryMapper

from src.logger import logger
from src.helpers import allocate, get_token, CustomEx
from src.neuron_morphology.validation.check_swc import parse_header_and_comments, SWC_EXPECTED_COLUMNS_SAVE
from src.neuron_morphology.validation.validator import validation_report, validation_report_complete, get_tsv_report_line, get_tsv_header_columns
from src.neuron_morphology.feature_annotations.create_update_annotations import get_contribution, get_generation
from kgforge.core.commons.actions import LazyAction
from morph_tool.converter import convert
from neurom import load_morphology
import os
import pandas as pd


def read_swc(file):
    return pd.read_csv(file, sep='\s+', index_col=0, dtype='str', comment='#', header=None)


###############################################################################
#  Ensure distribution is ok (swc, asc, h5, optional obj), only expected columns in swc
###############################################################################


def get_swc_path(resource: Resource, swc_download_folder: str, forge: KnowledgeGraphForge) -> str:
    distributions = resource.distribution if isinstance(resource.distribution, list) else [resource.distribution]
    distribution_name = next(d.name for d in distributions if d.encodingFormat.split('/')[-1] == "swc")

    swcfpath = os.path.join(swc_download_folder, distribution_name)
    if not os.path.isfile(swcfpath):  # If already present, no need to download
        forge.download(resource, follow='distribution.contentUrl', content_type='application/swc', path=swc_download_folder)

    return swcfpath


def check_swc_on_resource(resource: Resource, swc_download_folder: str, forge: KnowledgeGraphForge) -> bool:
    initial_format = "swc"

    update = False  # whether to update the resource or not
    derived_formats = ["asc", "h5"]

    reconvert = dict((k, False) for k in derived_formats)
    all_formats = derived_formats + [initial_format]
    expected_len = dict((k, 1) for k in all_formats)

    distributions_per_format = defaultdict(list)

    new_distributions = []

    if not isinstance(resource.distribution, list):
        resource.distribution = [resource.distribution]
        # TODO at this step is it expected that the resource only has a swc file?

    for distr in resource.distribution:
        frmt = distr.encodingFormat.split('/')[-1]
        distributions_per_format[frmt].append(distr)

    if not len(distributions_per_format[initial_format]) == expected_len[initial_format]:
        raise CustomEx(
            f"{expected_len[initial_format]} expected swc file,"
            f" {len(distributions_per_format[initial_format])} found"
        )

    swcfpath = get_swc_path(resource, swc_download_folder=swc_download_folder, forge=forge)

    df = read_swc(swcfpath)

    logger.info(f"Check that morphology {resource.name}"'s swc has the appropriate columns')

    # More columns than the expected
    if len(df.columns) != len(SWC_EXPECTED_COLUMNS_SAVE):
        logger.info(
            f"Expected {len(SWC_EXPECTED_COLUMNS_SAVE)} columns when reading swc as dataframe, {len(df.columns)} found."
            f"Will attempt parsing and re-assigning")
        try:
            columns, comments = parse_header_and_comments(swcfpath)
            df.columns = columns
        except Exception as e:
            raise CustomEx(
                f"Expected {len(SWC_EXPECTED_COLUMNS_SAVE)} columns when reading swc as dataframe, {len(df.columns)} found. "
                "Couldn't parse and reassign column"
            ) from e

        logger.info(
            "Re-assignment successful, asc and h5 files will have to be reconverted and the resource will need to be updated"
        )
        with open(swcfpath, 'w') as f:
            for comment in comments:
                f.write(comment)

        df.reindex(columns=SWC_EXPECTED_COLUMNS_SAVE).to_csv(swcfpath, sep=' ', mode='a')
        new_distributions.append(forge.attach(swcfpath, content_type='application/swc'))
        reconvert = dict((k, True) for k in derived_formats)
        update = True
    else:
        new_distributions.append(distributions_per_format['swc'])

    for frmt in derived_formats:
        if not reconvert[frmt]:
            if len(distributions_per_format[frmt]) == expected_len[frmt]:
                new_distributions.append(distributions_per_format[frmt])
            else:
                reconvert[frmt] = True
                update = True
                logger.info(
                    f"Couldn't find file of {frmt}, "
                    f"the .{initial_format} file will be converted and the resource will be updated"
                )

        if reconvert[frmt]:  # NB. Not else!!
            logger.log(f"Converting {resource.name} to {frmt}")
            outfile = f'{resource.name}.{frmt}'  # TODO specify where it is created
            convert(swcfpath, outfile)
            new_distributions.append(forge.attach(outfile, content_type=f'application/{frmt}'))

    if len(distributions_per_format['obj']) == 1:
        new_distributions.append(distributions_per_format['obj'][0])
    elif len(distributions_per_format['obj']) != 0:
        raise CustomEx(f"More than one 'obj' file {resource.name}")

    if update:
        resource.distribution = new_distributions

    distributions_per_format = defaultdict(list)
    for distr in resource.distribution:
        if isinstance(distr, LazyAction):
            frmt = distr.args[0].split('.')[-1]
        else:
            frmt = distr.encodingFormat.split('/')[-1]
        distributions_per_format[frmt].append(distr)

    for frmt in all_formats:
        if not len(distributions_per_format[frmt]) == expected_len[frmt]:
            raise CustomEx(
                f"End - Unexpected number of {frmt}:"
                f" {len(distributions_per_format[frmt])} instead of {expected_len[frmt]}"
            )

    expected_obj_len = [0, 1]
    if not len(distributions_per_format['obj']) in expected_obj_len:
        raise CustomEx(
            f"End - Unexpected number of 'obj' distributions"
            f" {len(distributions_per_format['obj'])} instead of {expected_obj_len}"
        )

    return update
