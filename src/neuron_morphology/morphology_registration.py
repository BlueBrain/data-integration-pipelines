import argparse
from typing import Dict, Tuple, List, Union, Optional

import cachetools
import copy
from datetime import datetime, timedelta
import glob
import json
from kgforge.core import KnowledgeGraphForge, Resource
from src.helpers import allocate, ASSETS_DIRECTORY, authenticate, _as_list
from morph_tool.converter import convert
import numpy as np
import os
import pandas as pd
import re
import shutil
import zipfile

from src.logger import logger
from src.neuron_morphology.arguments import define_morphology_arguments
from src.neuron_morphology.creation_helpers import get_generation, get_contribution


strategy = 'EXACT_CASE_INSENSITIVE_MATCH'
MORPHOLOGY_EXTENSIONS = ['swc', 'asc', 'h5']


def extract_zip(zip_file_path: str, dst_dir: str, re_extract: bool):

    if re_extract:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(dst_dir)

    filename, _ = os.path.splitext(os.path.basename(zip_file_path))
    zip_files_per_brain = os.path.join(dst_dir, filename)
    brains = list(glob.iglob(os.path.join(zip_files_per_brain, "*.zip")))

    logger.info(f"Extracting {len(brains)} archives (per mouse id)")

    for f_path in brains:
        with zipfile.ZipFile(f_path, 'r') as zip_ref:
            if re_extract:
                logger.info(f"Extracting archive {f_path}")
                zip_ref.extractall(zip_files_per_brain)
                os.remove(f_path)

    directories = [x[0] for x in os.walk(zip_files_per_brain)]
    return zip_files_per_brain, directories


def convert_swcs(dst_folders: List[str], re_convert: bool) -> Dict:
    name_to_file = {}

    start_format = 'swc'  # the format of the delivery, to convert from
    other_formats = [i for i in MORPHOLOGY_EXTENSIONS if i != start_format]

    for dst_folder in dst_folders:
        for f_path in list(glob.iglob(os.path.join(dst_folder, '*.swc'))):
            fol, swc_name = os.path.split(f_path)
            basename, _ = os.path.splitext(swc_name)

            if basename in name_to_file:
                raise ValueError(f'Morphologies with the same name {f_path}!!')

            name_to_file[basename] = f_path

            for out_format in other_formats:
                outfile = os.path.join(fol, f"{basename}.{out_format}")

                if re_convert:
                    logger.info(f"Converting {f_path} into format {out_format}")
                    convert(f_path, outfile)
                elif not os.path.isfile(outfile):
                    logger.error(f"Converting {f_path} into format {out_format} needs to happen, but re-convert was set to False")

    return name_to_file


def load_excel_file(folder: str) -> pd.DataFrame:
    xlsx_files = list(glob.glob(os.path.join(folder, '*.xlsx')))

    if len(xlsx_files) != 1:
        raise FileNotFoundError(
            f"Cannot identify metadata excel file. Provide a single excel file "
            f"in {folder} or change this script appropriately"
        )

    xlsx_file = xlsx_files[0]
    return pd.read_excel(xlsx_file, skiprows=1, na_values=' ')


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


@cachetools.cached(cache=cachetools.LRUCache(maxsize=100))
def cacheresolve(text, forge, scope='ontology', strategy='EXACT_MATCH', target=None):
    return forge.resolve(text=text, scope=scope, strategy=strategy, target=target)


def make_dict(
        name, distribution, subject_name, brain_region, coordinates, subject_strain, layer, row_number
) -> Dict:
    nrow = copy.deepcopy(rowdict)
    nrow['name'] = name
    nrow['distribution'] = distribution
    nrow['subject.name'] = subject_name

    if brain_region:
        nrow['brainLocation.brainRegion.id'] = brain_region['id']
        nrow['brainLocation.brainRegion.label'] = brain_region['label']

    if layer:
        nrow['brainLocation.layer'] = layer

    if subject_strain:
        nrow['subject.strain.id'] = subject_strain["id"]
        nrow['subject.strain.label'] = subject_strain["label"]

    for axis, v in coordinates.items():
        nrow[f'brainLocation.coordinatesInBrainAtlas.value{axis}.@value'] = float(v)

    month_year = re.search(r'\w+ \d+', row_number).group()
    start = datetime.strptime(month_year, "%B %Y")
    end = start + timedelta(days=1)
    nrow['generation.activity.startedAtTime'] = datetime.isoformat(start, sep='T', timespec='auto')
    nrow['generation.activity.endedAtTime'] = datetime.isoformat(end, sep='T', timespec='auto')

    return nrow


def _get_strain(row_strain: str, i: int, forge: KnowledgeGraphForge) -> Tuple[Optional[Dict], Optional[str], bool]:

    resolved_strain = cacheresolve(text=row_strain, forge=forge, scope='ontology', strategy=strategy, target='Strain')
    if resolved_strain is None:
        return None, f'could not resolve strain = "{row_strain}" for row {i}', True
    else:
        return {"label": resolved_strain.label, "id": resolved_strain.id}, None, True


def _get_layer(row_layer, i: int, brain_loc_label: str, forge: KnowledgeGraphForge) -> Tuple[Optional[Union[Dict, List]], Optional[str], bool]:
    '''Value if it's there, error message if it's there, whether to register or not'''
    def find_layer(row_layer):
        text = f"layer {row_layer}"
        return cacheresolve(text=text, forge=forge, scope='ontology', strategy=strategy, target='BrainRegion')
    
    if pd.isna(row_layer):
        return None, None, True
    if '/' in str(row_layer):
        layer = [find_layer(i) for i in row_layer.split('/')]
        if None in layer:
            return None, f'could not resolve layer = "{row_layer}" for row {i}', True
        return [{'id': i.id, 'label': i.label} for i in layer], None, True
    layer = find_layer(row_layer)
    if layer is None:
        return None, f'could not resolve layer = "{row_layer}" for row {i}', True
    return {'id':layer.id, 'label':layer.label}, None, True


def _get_brain_region(row_br: str, row_layer, i: int, forge: KnowledgeGraphForge) -> Tuple[Optional[Dict], Optional[str], bool]:
    brain_reg = cacheresolve(text=row_br, forge=forge, scope='ontology', strategy=strategy, target='BrainRegion')
    if brain_reg is None:
        return None, f"could not resolve brain location = \"{row_br}\" for row {i}", False
    # return {"label": brain_loc.label, "id": brain_loc.id}, None, True
    text = f"{brain_reg.label}, layer {row_layer}"
    brain_reg_spec = cacheresolve(text=text, forge=forge, scope='ontology', strategy=strategy, target='BrainRegion')
    if brain_reg_spec is None:
        return {'id': brain_reg.id, 'label': brain_reg.label}, None, True
    return {'id': brain_reg_spec.id, 'label': brain_reg_spec.label}, None, True

def do(metadata: pd.DataFrame, name_to_file: Dict, forge: KnowledgeGraphForge) -> Tuple[List[Dict], Dict, Dict]:
    nrows = []
    incomplete, not_done = {}, {}

    def append_errors(register_flag, err_str, i):
        if err_str is None:
            return None
        err_append = incomplete if register_flag else not_done
        log_fc = logger.warning if register_flag else logger.error
        log_fc(err_str)
        err_append[i] = err_str

    for i, row in list(zip(metadata.index, metadata.loc)):

        name = row['Cell Name (Cell ID)']
        logger.info(f"Processing morphology #{i+1} {name}")
        swc_file = name_to_file[name]

        swc_file_path, swc_file_name_ext = os.path.split(swc_file)
        swc_file_name, _ = os.path.splitext(swc_file_name_ext)

        distribution = [os.path.join(swc_file_path, f"{swc_file_name}.{m_ext}") for m_ext in MORPHOLOGY_EXTENSIONS]

        strain = row['Animal strain or treatment (e.g. VPA)']
        animal_id = row['Animal ID']
        subject_name = f'{strain};{animal_id}'
        coordinates = dict((axis, row[f'{axis} coordinates']) for axis in ['X', 'Y', 'Z'])

        subject_strain, err_str_strain, register_strain = _get_strain(strain, i, forge)
        append_errors(register_strain, err_str_strain, i)

        row_layer = row['Layer\n(1,2, 3, etc.)']
        brain_region, err_str_br, register_br = _get_brain_region(row['Brain Region'], row_layer, i, forge)
        append_errors(register_br, err_str_br, i)

        layer_value, err_str_layer, register_layer = _get_layer(
            row_layer, i, brain_region["label"] if brain_region else None, forge=forge
        )
        append_errors(register_layer, err_str_layer, i)

        nrow = make_dict(
            name=name,
            distribution=distribution,
            subject_name=subject_name,
            brain_region=brain_region,
            coordinates=coordinates,
            subject_strain=subject_strain,
            layer=layer_value,
            row_number=row['No.']
        )

        nrows.append(nrow)

    return nrows, incomplete, not_done


def make_catalog_resource(name: str, description: str, zip_file_path: str, forge: KnowledgeGraphForge):
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
    datacatalog = Resource.from_json(info)
    datacatalog.distribution = forge.attach(zip_file_path, content_type="application/zip")
    return datacatalog


def to_excel(dst_path: str, dataframe: pd.DataFrame):
    writer = pd.ExcelWriter(dst_path, engine='xlsxwriter')
    dataframe.to_excel(writer, index=False, sheet_name='Sheet1')
    worksheet = writer.sheets['Sheet1']
    for i, col in enumerate(dataframe.columns):
        width = max(dataframe[col].apply(lambda x: len(str(x))).max(), len(col))
        worksheet.set_column(i, i, width)
    writer.close()


def zip_output(working_directory: str, dst_root_folder: str, processed_metadata_file_path: str, log_path: str, zip_path: str):
    logger.info(f"Zipping output into {zip_path}.zip")
    to_zip_folder = os.path.join(working_directory, 'to_zip')
    os.makedirs(to_zip_folder, exist_ok=True)
    shutil.copytree(dst_root_folder, to_zip_folder, dirs_exist_ok=True)
    shutil.copy(processed_metadata_file_path, to_zip_folder)
    shutil.copy(log_path, to_zip_folder)
    shutil.make_archive(zip_path, 'zip', to_zip_folder)
    shutil.rmtree(to_zip_folder)


if __name__ == "__main__":
    parser = define_morphology_arguments(argparse.ArgumentParser())
    received_args, leftovers = parser.parse_known_args()
    working_directory = os.path.join(os.getcwd(), received_args.output_dir)
    os.makedirs(working_directory, exist_ok=True)

    original_zip_file = os.path.join(ASSETS_DIRECTORY, "2nd_delivery_SEU_01162024.zip")

    local_test = False

    if local_test:
        dst_directory = os.path.join(os.getcwd(), "output")
        dst_root_folder, dst_folders = extract_zip(zip_file_path=original_zip_file, dst_dir=dst_directory, re_extract=False)
        logger.info("Converting swc files into other extensions")
        name_to_file = convert_swcs(dst_folders, re_convert=False)
    else:
        dst_directory = working_directory
        dst_root_folder, dst_folders = extract_zip(zip_file_path=original_zip_file, dst_dir=working_directory, re_extract=True)
        logger.info("Converting swc files into other extensions")
        name_to_file = convert_swcs(dst_folders, re_convert=True)

    print(f'Working on {len(name_to_file)} morphologies')
    metadata = load_excel_file(dst_root_folder)

    is_prod = True
    token = authenticate(username=received_args.username, password=received_args.password)
    forge_instance = allocate("bbp-external", "seu", is_prod=is_prod, token=token)
    contribution = get_contribution(token=token, production=is_prod)
    generation = get_generation()

    datacatalog_name, datacatalog_description = "", ""  # TODO
    datacatalog = make_catalog_resource(name=datacatalog_name, description=datacatalog_description, zip_file_path=original_zip_file, forge=forge_instance)
    # forge.register(datacatalog)
    # print('Your datacatalog has the following ID', datacatalog.id)  # Initial catalog without changes

    # Checking that we have metadata for all swc files and swc files for all the metadata
    assert set(name_to_file.keys()) == set(metadata['Cell Name (Cell ID)']), 'Incomplete correspondence between data (swc) and metadata (rows in excel)'

    excel_file = 'processed_metadata.xlsx'  # the final filepath for processed metadata

    nrows, incomplete, not_done = do(metadata, name_to_file, forge_instance)

    log_path = os.path.join(working_directory, 'log.json')

    with open(log_path, 'w') as f:
        json.dump({'not registered': not_done, 'incomplete': incomplete}, f, indent=2)

    logger.info(f'Written log of incomplete data and not registered morphologies in {log_path}')

    df = pd.DataFrame(nrows)
    df = df.reindex(columns=columns_ordered)  # to always have them in the same order, for readability

    processed_metadata_file_path = os.path.join(working_directory, excel_file)

    to_excel(processed_metadata_file_path, df)

    zip_path = os.path.join(working_directory, "morphologies")
    zip_output(working_directory, dst_root_folder, processed_metadata_file_path, log_path, zip_path=zip_path)

    # resources = forge_instance.from_dataframe(df, na=np.nan, nesting=".")
    # forge.register(resources, "datashapes:neuronmorphology")
    # ids = [res.id for res in resources]
    # timestamp = datetime.today().strftime('%Y%m%d_%Hh%M')
    # logname = f'registered_resources_ids_{timestamp}.json'
    # with open(logname, 'w') as f:
    #     json.dump(ids, f, indent=2)

    # After processing, with processed excel file
    # datacatalog.description = 'processed_morphologies'
    # datacatalog.distribution = forge.attach(zipfp, content_type='application/zip')  # this can fail for large datasets
    # datacatalog.hasPart = [{"@id": id_, "@type": "NeuronMorphology"} for id_ in ids]
    # forge.update(datacatalog, schema_id="https://bbp.epfl.ch/shapes/dash/datacatalog")



