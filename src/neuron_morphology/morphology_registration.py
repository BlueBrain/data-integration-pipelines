#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 13:02:49 2024

@author: ricardi
"""

import cachetools
import copy
from datetime import datetime, timedelta
import glob
import json
from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.mappings import DictionaryMapping
from kgforge.specializations.mappers import DictionaryMapper
from src.helpers import allocate, get_token
from src.neuron_morphology.validation import validation_report, validation_report_complete, getTsvReportLine
from src.neuron_morphology.feature_annotations.create_annotations import get_contribution, get_generation
from morph_tool.converter import convert
from neurom import load_morphology
import numpy as np
import os
import pandas as pd
import re
import shutil
import zipfile

is_prod = True
token = get_token(is_prod=is_prod, prompt=True)
forge = allocate("bbp-external", "seu", is_prod=is_prod, token=token)

###############################################################################
### Register delivery as datacatalog
###############################################################################
orig_zipfp = '/home/ricardi/SEU/2nd_delivery_SEU_01162024.zip'

info = {
  "@context": "https://bbp.neuroshapes.org",
  "@type": [
    "Dataset",
    "DataCatalog"
  ],
  "about": "NeuronMorphology",
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
info['name'] = ''  # fill appropriately
info['description'] = ''  # fill appropriately

datacatalog = Resource.from_json(info)
datacatalog.distribution = forge.attach(zipfile, content_type="application/zip")
#forge.register(datacatalog)

print('Your datacatalog has the following ID', datacatalog.id)

###############################################################################
### Extract content
###############################################################################
folder = '/home/ricardi/SEU/2nd_delivery_SEU_01162024'

re_extract = False  # set to True to re-extract all zips
re_convert = False  # set to True to conver again
name_to_file = {}  

start_frmt = 'swc'  # the format of the delivery, to convert from
other_frmts = [i for i in ['swc', 'asc', 'h5'] if i != start_frmt]

for fpath in glob.iglob(os.path.join(folder, '**/*.swc'), recursive=True):
    fol, swcname = os.path.split(fpath)
    basename = swcname[:-4]
    if basename in name_to_file:
        print(fpath)
        raise ValueError('Morphologies with the same name!!')
    name_to_file[basename] = fpath
    for outformat in other_frmts:
        outfile = f'{fpath[:-3]}{outformat}'
        if not os.path.isfile(outfile) or re_convert:
            convert(fpath, outfile)

print(f'Working on {len(name_to_file)} morphologies')
###############################################################################
### Reading and processing metadata
###############################################################################
exclude = ['processed_metadata.xlsx']
xlsx_files = [i for i in glob.glob(os.path.join(folder, '*.xlsx')) if not any([j in i for j in exclude])]
if len(xlsx_files) != 1:
    raise FileNotFoundError(f"""Cannot identify metadata excel file.
    Provide a single excel file in {folder} or change this notebook appropriately""")
xlsx_file = xlsx_files[0]
metadata = pd.read_excel(xlsx_file, skiprows=1, na_values=' ')
excel_file = 'processed_metadata.xlsx' # the final filepath for processed metadata
            
# Checking that we have metadata for all swc files and swc files for all the metadata
assert set(name_to_file.keys()) == set(metadata['Cell Name (Cell ID)']), 'Incomplete correspondence between data (swc) and metadata (rows in excel)'

rowdict  = {  # dictionary of fields which are the same for all morphologies
    'type': ['Dataset', 'NeuronMorphology', 'ReconstructedNeuronMorphology'],
    'description': 'Initial neuron morphology shared by provider',
    'brainLocation.type': 'BrainLocation',
    'brainLocation.coordinatesInBrainAtlas.valueX.type': 'xsd:float',
    'brainLocation.coordinatesInBrainAtlas.valueY.type': 'xsd:float',
    'brainLocation.coordinatesInBrainAtlas.valueZ.type': 'xsd:float',
    'subject.species.label' : 'Mus musculus',
    'subject.species.id' : 'http://purl.obolibrary.org/obo/NCBITaxon_10090',
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

columns_ordered = [
    'type', 'name', 'description', 'brainLocation.type',  # let's have the columns always in the same order
    'brainLocation.brainRegion.label', 'brainLocation.brainRegion.id',
    'brainLocation.layer',
    'brainLocation.coordinatesInBrainAtlas.valueX.@value',
    'brainLocation.coordinatesInBrainAtlas.valueX.type',
    'brainLocation.coordinatesInBrainAtlas.valueY.@value',
    'brainLocation.coordinatesInBrainAtlas.valueY.type',
    'brainLocation.coordinatesInBrainAtlas.valueZ.@value',
    'brainLocation.coordinatesInBrainAtlas.valueZ.type',
    'distribution', 'subject.name', 'subject.type', 'subject.species.label',
    'subject.species.id', 'subject.strain.label', 'subject.strain.id',
    'contribution.type', 'contribution.agent.type', 'contribution.agent.id',
    'objectOfStudy.type', 'objectOfStudy.label', 'objectOfStudy.id',
    'generation.type', 'generation.activity.type',
    'generation.activity.startedAtTime', 'generation.activity.endedAtTime',
    'generation.activity.hadProtocol.id',
    'generation.activity.hadProtocol.type', 'isRegisteredIn.id',
    'isRegisteredIn.type', 'atlasRelease.id', 'atlasRelease.type'
]

@cachetools.cached(cache=cachetools.LRUCache(maxsize=100))
def cacheresolve(text, scope='ontology', strategy='EXACT_MATCH'):
    return forge.resolve(text=text, scope=scope, strategy=strategy)

nrows = []
incomplete, not_done = {}, {}
log_name = 'log.json'
log_path = os.path.join(folder, log_name)
strategy = 'EXACT_CASE_INSENSITIVE_MATCH'
for i in metadata.index:
    register = True
    row = metadata.loc[i]
    name = row['Cell Name (Cell ID)']
    nrow = copy.deepcopy(rowdict)
    nrow['name'] = name
    for axis in ['X', 'Y', 'Z']:
        nrow[f'brainLocation.coordinatesInBrainAtlas.value{axis}.@value'] = row[f'{axis} coordinates']
    swc_file = name_to_file[name]
    nrow['distribution'] = [f'{swc_file[:-3]}asc', f'{swc_file[:-3]}h5', swc_file]
    strain = row['Animal strain or treatment (e.g. VPA)']
    animal_id = row['Animal ID']
    nrow['subject.name'] = f'{strain};{animal_id}'
    brain_loc = cacheresolve(text=row['Brain Region'], scope='ontology', strategy=strategy)
    brain_loc = forge.retrieve(brain_loc.id, cross_bucket=True)
    if brain_loc == None:
        register = False
        not_done[i] = f"could not resolve brain location = \"{row['Brain Region']}\" for row {i}"
    else:
        nrow['brainLocation.brainRegion.label'] = brain_loc.label
        nrow['brainLocation.brainRegion.id'] = brain_loc.id
    row_layer = row['Layer\n(1,2, 3, etc.)']
    if pd.notna(row_layer):
        text = f"{brain_loc.label}, layer {row_layer}"
        layer = cacheresolve(text=text, scope='ontology', strategy=strategy)
        if layer == None:
            layer_again = cacheresolve(text=text, scope='ontology', strategy='ALL_MATCHES')
            if layer_again == None:
                incomplete[i] = f'could not resolve layer = "{text}" for row {i}'
                nrow['brainLocation.layer'] = None
            elif len(layer_again) == 2:
                nrow['brainLocation.layer'] = [{'label':l.label, 'id': l.id} for l in layer_again]
            elif len(layer_again) > 2:
                layers = [i for i in layer_again if 'Structure' in i.id]
                if len(layers) == 2:
                    nrow['brainLocation.layer'] = [{'label':l.label, 'id': l.id} for l in layers]
                else:
                    register = False
                    not_done[i] = f"could not resolve tex = \"{text}"
            else:
                register = False
                not_done[i] = f"could not resolve tex = \"{text}"
        else:
            nrow['brainLocation.layer'] = {'label':layer.label, 'id': layer.id}
    else:
        nrow['brainLocation.layer'] = None
    resolved_strain = cacheresolve(text=strain, scope='ontology', strategy=strategy)
    if resolved_strain == None:
        incomplete[i] = f'could not resolve strain = "{strain}" for row {i}'
        nrow['subject.strain.label'] = None
        nrow['subject.strain.id'] = None
    else:
        nrow['subject.strain.label'] = resolved_strain.label
        nrow['subject.strain.id'] = resolved_strain.id
    month_year = re.search(r'\w+ \d+', row['No.']).group()
    start = datetime.strptime(month_year, "%B %Y")
    end = start + timedelta(days=1)
    nrow['generation.activity.startedAtTime'] = datetime.isoformat(start, sep='T', timespec='auto')
    nrow['generation.activity.endedAtTime'] = datetime.isoformat(end, sep='T', timespec='auto')
    nrows.append(nrow)
with open(log_path, 'w') as f:
    json.dump({'not registered': not_done, 'incomplete': incomplete}, f, indent=2)
print(f'Written log of incomplete data and not registered morphologies in {log_path}')

df = pd.DataFrame(nrows)
df = df.reindex(columns=columns_ordered)  # to always have them in the same order, for readability
df.to_excel(os.path.join(folder, excel_file))
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
shutil.copy(os.path.join(folder,excel_file), os.path.join(to_zip_fol, excel_file))  # processed metadata
basepath, zipfolname = os.path.split(to_zip_fol)
len_ = len(basepath)
with zipfile.ZipFile(zipfp, "w") as zf:
    for dirname, subdirs, files in os.walk(to_zip_fol):
        assert dirname[:len_] == basepath
        newfp = name if dirname == to_zip_fol else dirname[len_+1:] 
        zf.write(dirname, newfp)
        for filename in files:
            zf.write(os.path.join(dirname, filename), os.path.join(newfp, filename))
print(f'written zip file {zipfp}')
shutil.rmtree(to_zip_fol)  # this folder is no longer needed
datacatalog.description = 'processed_morphologies'
datacatalog.distribution = forge.attach(zipfp, content_type='application/zip')  # this can fail for large datasets
datacatalog.hasPart = [{ "@id": id_, "@type": "NeuronMorphology"} for id_ in ids]
forge.update(datacatalog, schema_id="https://bbp.epfl.ch/shapes/dash/datacatalog")

