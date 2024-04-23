#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 13:02:49 2024

@author: ricardi
"""
from typing import Dict, Tuple, List, Union, Optional

import cachetools
import copy
from datetime import datetime, timedelta
import glob
import json
from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.mappings import DictionaryMapping
from kgforge.specializations.mappers import DictionaryMapper
from src.helpers import allocate, get_token, ASSETS_DIRECTORY
from morph_tool.converter import convert
from neurom import load_morphology
import numpy as np
import os
import pandas as pd
import re
import shutil
import zipfile

from src.logger import logger
from src.neuron_morphology.creation_helpers import get_generation, get_contribution

is_prod = True
token = get_token(is_prod=is_prod, prompt=False)
forge = allocate("bbp-external", "seu", is_prod=is_prod, token=token)

###############################################################################
### Extract content
###############################################################################
folder = os.path.join(ASSETS_DIRECTORY, '2nd_delivery_SEU_01162024')

re_extract = False  # set to True to re-extract all zips

if re_extract:
    brains = list(glob.iglob(os.path.join(folder, '*.zip')))
    logger.info(f"Extracting {len(brains)} archives (per mouse id)")
    for fpath in brains:
        with zipfile.ZipFile(fpath, 'r') as zip_ref:
            logger.info(f"Extracting archive {fpath}")
            zip_ref.extractall(folder)

re_convert = False  # set to True to convert again
name_to_file = {}

start_format = 'swc'  # the format of the delivery, to convert from
other_formats = [i for i in ['swc', 'asc', 'h5'] if i != start_format]

for fpath in glob.iglob(os.path.join(folder, '**/*.swc'), recursive=True):
    fol, swc_name = os.path.split(fpath)
    basename, _ = os.path.splitext(swc_name)

    if basename in name_to_file:
        raise ValueError(f'Morphologies with the same name {fpath}!!')

    name_to_file[basename] = fpath

    for out_format in other_formats:
        outfile = os.path.join(fol, f"{basename}.{out_format}")

        if not os.path.isfile(outfile) or re_convert:
            logger.info(f"Converting {fpath} into format {out_format}")
            convert(fpath, outfile)

print(f'Working on {len(name_to_file)} morphologies')

###############################################################################
### Reading and processing metadata
###############################################################################

exclude = ['processed_metadata.xlsx']
xlsx_files = [i for i in glob.glob(os.path.join(folder, '*.xlsx')) if not any(j in i for j in exclude)]

if len(xlsx_files) != 1:
    raise FileNotFoundError(f"""Cannot identify metadata excel file.
    Provide a single excel file in {folder} or change this notebook appropriately"""
)

xlsx_file = xlsx_files[0]
metadata = pd.read_excel(xlsx_file, skiprows=1, na_values=' ')

excel_file = 'processed_metadata.xlsx'  # the final filepath for processed metadata

# Checking that we have metadata for all swc files and swc files for all the metadata
assert set(name_to_file.keys()) == set(metadata['Cell Name (Cell ID)']), 'Incomplete correspondence between data (swc) and metadata (rows in excel)'

rowdict = {  # dictionary of fields which are the same for all morphologies
    'type': ['Dataset', 'NeuronMorphology', 'ReconstructedNeuronMorphology'],
    'description': 'Initial neuron morphology shared by provider',
    'brainLocation.type': 'BrainLocation',
    'brainLocation.coordinatesInBrainAtlas.valueX.type': 'xsd:float',
    'brainLocation.coordinatesInBrainAtlas.valueY.type': 'xsd:float',
    'brainLocation.coordinatesInBrainAtlas.valueZ.type': 'xsd:float',
    'subject.species.label': 'Mus musculus',
    'subject.species.id': 'http://purl.obolibrary.org/obo/NCBITaxon_10090',
    'subject.type': 'Subject',
    'contribution.type': 'Contribution',
    'contribution.agent.type': ['Agent', 'Organization'],
    'contribution.agent.id': 'https://www.grid.ac/institutes/grid.263826.b',
    'objectOfStudy.type': 'ObjectOfStudy',
    'objectOfStudy.label': 'Single Cell',
    'objectOfStudy.id': 'http://bbp.epfl.ch/neurosciencegraph/taxonomies/objectsofstudy/singlecells',
    'generation.type': 'Generation',
    'generation.activity.type': ['ExperimentalActivity', 'Activity', 'ReconstructionFromImage'],
    'generation.activity.hadProtocol.id': 'https://bbp.epfl.ch/neurosciencegraph/data/781094e0-8fcb-4058-b3b9-9c67467e47d5',
    'generation.activity.hadProtocol.type': ['ExperimentalProtocol', 'Protocol'],
    'isRegisteredIn.id': 'https://bbp.epfl.ch/neurosciencegraph/data/allen_ccfv3_spatial_reference_system',
    'isRegisteredIn.type': ['AtlasSpatialReferenceSystem', 'BrainAtlasSpatialReferenceSystem'],
    'atlasRelease.id': 'https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885',
    'atlasRelease.type': ['BrainAtlasRelease', 'AtlasRelease']
}

with open(os.path.join(ASSETS_DIRECTORY, "ordered_columns.json"), "r") as f:
    columns_ordered = json.load(f)


contribution = get_contribution(token=token, production=is_prod)
generation = get_generation()


@cachetools.cached(cache=cachetools.LRUCache(maxsize=100))
def cacheresolve(text, scope='ontology', strategy='EXACT_MATCH'):
    return forge.resolve(text=text, scope=scope, strategy=strategy)


nrows = []
incomplete, not_done = {}, {}


def append_errors(register_flag, err_str, i):
    err_append = (None if err_str is None else (incomplete if register_flag else not_done))
    if err_append:
        err_append[i] = err_str


log_name = 'log.json'
log_path = os.path.join(folder, log_name)
strategy = 'EXACT_CASE_INSENSITIVE_MATCH'


def make_dict(
        name, distribution, subject_name, brain_region,
        coordinates, subject_strain, layer
) -> Dict:
    nrow = copy.deepcopy(rowdict)
    nrow['name'] = name
    nrow['distribution'] = distribution
    nrow['subject.name'] = subject_name
    nrow['brainLocation.brainRegion'] = brain_region
    nrow['brainLocation.layer'] = layer
    nrow['subject.strain'] = subject_strain

    for axis, v in coordinates.items():
        nrow[f'brainLocation.coordinatesInBrainAtlas.value{axis}.@value'] = float(v)

    month_year = re.search(r'\w+ \d+', row['No.']).group()
    start = datetime.strptime(month_year, "%B %Y")
    end = start + timedelta(days=1)
    nrow['generation.activity.startedAtTime'] = datetime.isoformat(start, sep='T', timespec='auto')
    nrow['generation.activity.endedAtTime'] = datetime.isoformat(end, sep='T', timespec='auto')

    return nrow


def _get_strain(row_strain, i) -> Tuple[Optional[Dict], Optional[str], bool]:

    resolved_strain = cacheresolve(text=row_strain, scope='ontology', strategy=strategy)
    if resolved_strain is None:
        return None, f'could not resolve strain = "{row_strain}" for row {i}', True
    else:
        return {"label": resolved_strain.label, "id": resolved_strain.id}, None, True


def _get_layer(row_layer, i: int, brain_loc_label: str) -> Tuple[Optional[Union[Dict, List]], Optional[str], bool]:
    '''Value if it's there, error message if it's there, whether to register or not'''
    if not pd.notna(row_layer):
        return None, None, True

    text = f"{brain_loc_label}, layer {row_layer}"
    layer = cacheresolve(text=text, scope='ontology', strategy=strategy)
    if layer is not None:
        return {'label': layer.label, 'id': layer.id}, None, True

    layers = cacheresolve(text=text, scope='ontology', strategy='ALL_MATCHES')

    if layers is None:
        return None, f'could not resolve layer = "{text}" for row {i}', True

    if len(layers) > 2:
        layers = [i for i in layers if 'Structure' in i.id]

    if len(layers) == 2:
        return [{'label': l.label, 'id': l.id} for l in layers], None, True
    else:
        return None, f"could not resolve tex = \"{text}", False


def _get_brain_region(row_br, i) -> Tuple[Optional[Dict], Optional[str], bool]:
    brain_loc = cacheresolve(text=row_br, scope='ontology', strategy=strategy)
    brain_loc = forge.retrieve(brain_loc.id, cross_bucket=True)
    if brain_loc is None:
        return None, f"could not resolve brain location = \"{row_br}\" for row {i}", False
    else:
        return {"label": brain_loc.label, "id": brain_loc.id}, None, True


for i, row in zip(metadata.index, metadata.loc):
    name = row['Cell Name (Cell ID)']
    swc_file = name_to_file[name]
    distribution = [f'{swc_file[:-3]}asc', f'{swc_file[:-3]}h5', swc_file]

    strain = row['Animal strain or treatment (e.g. VPA)']
    animal_id = row['Animal ID']
    subject_name = f'{strain};{animal_id}'
    coordinates = dict((axis, row[f'{axis} coordinates']) for axis in ['X', 'Y', 'Z'])

    subject_strain, err_str_strain, register_strain = _get_strain(strain, i)
    append_errors(register_strain, err_str_strain, i)

    brain_region, err_str_br, register_br = _get_brain_region(row['Brain Region'], i)
    append_errors(register_br, err_str_br, i)

    row_layer = row['Layer\n(1,2, 3, etc.)']

    layer_value, err_str_layer, register_layer = _get_layer(
        row_layer, i, brain_region["label"] if brain_region else None
    )
    append_errors(register_layer, err_str_layer, i)

    nrow = make_dict(
        name=name,
        distribution=distribution,
        subject_name=subject_name,
        brain_region=brain_region,
        coordinates=coordinates,
        subject_strain=subject_strain,
        layer=layer_value
    )

    nrows.append(nrow)
with open(log_path, 'w') as f:
    json.dump({'not registered': not_done, 'incomplete': incomplete}, f, indent=2)

print(f'Written log of incomplete data and not registered morphologies in {log_path}')

df = pd.DataFrame(nrows)
df = df.reindex(columns=columns_ordered)  # to always have them in the same order, for readability
df.to_excel(os.path.join(folder, excel_file))

exit()
resources = forge.from_dataframe(df, na=np.nan, nesting=".")
# forge.register(resources, "datashapes:neuronmorphology")
# ids = [res.id for res in resources]
# timestamp = datetime.today().strftime('%Y%m%d_%Hh%M')
# logname = f'registered_resources_ids_{timestamp}.json'
# with open(logname, 'w') as f:
#     json.dump(ids, f, indent=2)

to_zip_fol = os.path.join(folder, 'to_zip')
name = 'processed_morphologies'
zipfp = os.path.join(folder, "morphologies.zip")
if not os.path.isdir(to_zip_fol):
    os.mkdir(to_zip_fol)
for _, fpath in name_to_file.items():
    path, fname = os.path.split(fpath)
    basename = fname[:-4]
    for format in ['swc', 'asc', 'h5']:
        source = os.path.join(path, f'{basename}.{format}')
        dest = os.path.join(to_zip_fol, f'{basename}.{format}')
        shutil.copy(source, dest)
shutil.copy(os.path.join(folder, excel_file), os.path.join(to_zip_fol, excel_file))  # processed metadata
basepath, zipfolname = os.path.split(to_zip_fol)
len_ = len(basepath)
with zipfile.ZipFile(zipfp, "w") as zf:
    for dirname, subdirs, files in os.walk(to_zip_fol):
        assert dirname[:len_] == basepath
        newfp = name if dirname == to_zip_fol else dirname[len_ + 1:]
        zf.write(dirname, newfp)
        for filename in files:
            zf.write(os.path.join(dirname, filename), os.path.join(newfp, filename))
print(f'written zip file {zipfp}')
shutil.rmtree(to_zip_fol)  # this folder is no longer needed

###############################################################################
### Register delivery as datacatalog
###############################################################################
orig_zipfp = '/home/ricardi/SEU/2nd_delivery_SEU_01162024.zip'

def make_catalog_resource(name: str, description: str):
    info = {
        "@context": "https://bbp.neuroshapes.org",
        "@type": [
            "Dataset",
            "DataCatalog"
        ],
        "about": "NeuronMorphology",
        "name": name,
        "description": description,
        "brainLocation": {
            "@type": "BrainLocation",
            "brainRegion": {
                "@id": "http://api.brain-map.org/api/v2/data/Structure/997",
                "label": "root"
            }
        },
        "contribution": {
            "@type": "Contribution",
            "agent": {
                "@id": "https://www.grid.ac/institutes/grid.263826.b",
                "@type": "Agent"
            }
        },
        "subject": {
            "@type": "Subject",
            "species": {
                "@id": "http://purl.obolibrary.org/obo/NCBITaxon_10090",
                "label": "Mus musculus"
            }
        }
    }
    return Resource.from_json(info)


datacatalog = make_catalog_resource("", "")
datacatalog.distribution = forge.attach(zipfile, content_type="application/zip")

# forge.register(datacatalog)

print('Your datacatalog has the following ID', datacatalog.id)

datacatalog.description = 'processed_morphologies'
datacatalog.distribution = forge.attach(zipfp, content_type='application/zip')  # this can fail for large datasets


datacatalog.hasPart = [{"@id": id_, "@type": "NeuronMorphology"} for id_ in ids]
forge.update(datacatalog, schema_id="https://bbp.epfl.ch/shapes/dash/datacatalog")
