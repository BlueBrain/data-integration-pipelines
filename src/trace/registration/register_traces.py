import os
import time
import pandas as pd
import json
from collections import defaultdict
from pathlib import Path
from typing import Union, Set, List, Optional, Dict, Tuple

from kgforge.core import Resource, KnowledgeGraphForge
from kgforge.specializations.mappings import DictionaryMapping
from kgforge.specializations.mappers import DictionaryMapper

from src.logger import logger
from src.helpers import allocate_with_default_views, Deployment, ASSETS_DIRECTORY, get_filename_and_ext_from_filepath
from src.trace.visualization.lnmc_nwb_visualization import generate
from src.trace.types_and_schemas import (
    TRACE_WEB_DATA_CONTAINER_SCHEMA,
    TRACE_SCHEMA,
    DATASET_SCHEMA,
    EXPERIMENTAL_TRACE_SCHEMA,
    SIMULATION_TRACE_SCHEMA,
    SINGLE_CELL_TRACE_TYPE)
from src.trace.stimulus_type_ontology_querying import stimulus_type_ontology
from src.trace.registration.trace_web_data_container import create_twdc_from_trace
from src.common_metadata import create_existing_agent_contribution, create_brain_region



def generate_nwb_products(forge: KnowledgeGraphForge, nwb_path: str,
                          stimulus_type_id_to_label: Dict[str, str]) -> Dict:
    """ Generate the .rab and .png files, as well as the image file resources
        in nexus.
    Returns: {'distribution', 'image', 'stimulus', 'rab_path}
    """

    dir_path = os.path.dirname(nwb_path)
    trace_name, _ = get_filename_and_ext_from_filepath(nwb_path)
    rab_path = f"{dir_path}/{trace_name}.rab"
    generate(nwb_path, rab_path, dir_path)

    trace_distribution = forge.attach(nwb_path, content_type='application/nwb')
    png_paths = [str(x) for x in Path(dir_path).iterdir() if str(x).endswith('png')]
    image_obj = defaultdict(list)
    stimuli = []
    stimulus_obj = []
    for path in png_paths[:]:
        name, _ = get_filename_and_ext_from_filepath(path) 
        fragments = name.split("__")
        if fragments[2] == "response":
            about = "nsg:ResponseTrace"
        elif fragments[2] == "stimulus":
            about = "nsg:StimulationTrace"
        else:
            about = "nsg:Trace"
        image = forge._store.upload_image(forge=forge, path=path, about=about, content_type="image/png")
        identifier = image.get_identifier()
        repetition = int(fragments[-1].split(".")[0])
        stimulus_identifier = f"http://bbp.epfl.ch/neurosciencegraph/ontologies/stimulustypes/{fragments[1]}"
        image_obj[fragments[0]].append(
            forge.from_json({
                "id": identifier,
                "about": about,
                "repetition": repetition,
                "stimulusType": {
                    "id": stimulus_identifier
                }
            })
        )
        obj = image_obj[fragments[0]]
        stimuli = []
        for o in obj:
            stimuli.append(o.stimulusType.id)

    for stim_id in set(stimuli):
        stimulus_obj.append(
            forge.from_json({
                    "type": "Stimulus",
                    "stimulusType": {
                      "id": stim_id,
                      "label": stimulus_type_id_to_label[stim_id]
                    }
                   }))
    return {'distribution': trace_distribution,
            'image': image_obj[fragments[0]],
            'stimuli': stimulus_obj,
            'rab_path': rab_path
            }


def create_trace_resource(forge: KnowledgeGraphForge, nwb_path: str, metadata: Dict, trace_type: str,
                          stimuli_dicts: Tuple[Dict]):

    single_cell_stimulus_type_id_to_label, stimulus_type_id_to_label = stimuli_dicts
    mapping_trace = DictionaryMapping.load(os.path.join(ASSETS_DIRECTORY, 'GeneralTraceMapping.hjson'))

    assert 'brainRegion' in metadata, "Missing brain region information in the metadata"
    assert 'contribution' in metadata, "Missing contribution information in the metadata"
    assert 'description' in metadata, "Missing description of the resource in the metadata"
    assert 'objectOfStudy' in metadata, "Missing the object of study of the resource in the metadata"
    assert 'subject' in metadata, "Missing the subject information in the metadata"

    metadata['name'], _ = get_filename_and_ext_from_filepath(nwb_path)

    nwb_products = generate_nwb_products(forge=forge, nwb_path=nwb_path,
                                         stimulus_type_id_to_label=stimulus_type_id_to_label)

    for k, v in nwb_products.items():
        if k != 'rab_path':
            metadata[k] = v

    trace = forge.map(metadata, mapping_trace, DictionaryMapper)
    # add types to trace
    is_single_cell = all([True for stimulus in metadata['stimuli']
                         if stimulus.stimulusType.get_identifier() in single_cell_stimulus_type_id_to_label])

    trace.type.append(trace_type)
    if is_single_cell:
        trace.type.append(SINGLE_CELL_TRACE_TYPE)

    return trace


def register_trace_resources(forge: KnowledgeGraphForge, nwb_path: str, metadata: Dict, trace_type: str,
                             stimuli_dicts: Tuple[Dict]) -> None:

    dir_path = os.path.basename(nwb_path)

    trace_resource = create_trace_resource(forge, nwb_path, metadata, trace_type,
                                           stimuli_dicts)

    forge.validate(trace_resource, type_="Dataset", execute_actions_before=True)
    if not trace_resource._last_action.succeeded:
        logger.error(f"Failed to validate the trace resource. Error: {trace_resource._last_action.message}")
        return (trace_resource, None)
    forge.register(trace_resource, schema_id=DATASET_SCHEMA)

    if not trace_resource._last_action.succeeded:
        logger.error(f"Failed to register the trace resource. Error: {trace_resource._last_action.message}")
        return (trace_resource, None)

    web_data_container = create_twdc_from_trace(trace_resource=trace_resource,
                                                forge=forge, dir_path=dir_path)

    # register the TraceWebDataContainer
    forge.register(web_data_container, schema_id=TRACE_WEB_DATA_CONTAINER_SCHEMA)

    if not web_data_container._last_action.succeeded:
        logger.error(f"Failed to register the web data container resource. Error: {web_data_container._last_action.message}")
        return (trace_resource, web_data_container)

    # update trace resource with hasPart pointing to the twdc
    trace_resource.hasPart = forge.from_json({'id': web_data_container.get_identifier(),
                                              'type': 'TraceWebDataContainer'})

    if trace_type == "ExperimentalTrace":
        forge.update(trace_resource, schema_id=EXPERIMENTAL_TRACE_SCHEMA)
    elif trace_type == "SimulationTrace":
        forge.update(trace_resource, schema_id=SIMULATION_TRACE_SCHEMA)
    else:
        forge.update(trace_resource, schema_id=TRACE_SCHEMA)

    if not trace_resource._last_action.succeeded:
        logger.error(f"Failed to update the trace resource. Error: {trace_resource._last_action.message}")
    return (trace_resource, web_data_container)


if __name__ == "__main__":
    org, project = ('dke', 'kgforge')
    deployment = Deployment["STAGING"]

    token = ""
    forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=token)

    stimuli_dicts = stimulus_type_ontology(
        deployment_str=deployment.value, token=token
    )

    metadata = {}

    metadata['brainRegion'] = create_brain_region(forge=forge_instance, region_label="Reticular nucleus of the thalamus")
    metadata['description'] = "example of trace resource"
    metadata['objectOfStudy'] = {
        "id": "http://bbp.epfl.ch/neurosciencegraph/taxonomies/objectsofstudy/singlecells",
        "type": "ObjectOfStudy",
        "label": "Single Cell"
    }
    metadata['contribution'] = {
        "type": "Contribution",
        "agent": {"id": "https://bbp.epfl.ch/nexus/v1/realms/bbp/users/romani",
                  "type": [
                    "Person",
                    "Agent"
                  ],
                  "email": "armando.romani@epfl.ch",
                  "familyName": "Romani",
                  "givenName": "Armando",
                  "name": "Armando Romani"}
    }
    metadata["subject"] = {
        "type": "Subject",
        "species": {
          "id": "http://purl.obolibrary.org/obo/NCBITaxon_10090",
          "label": "Mus musculus"
        }
    }
    # create_existing_agent_contribution(forge=forge_instance, name="Romani")

    trace_resources = register_trace_resources(forge=forge_instance, nwb_path='./output/tmp/Rt_RC_cAD_noscltb_7.nwb',
                                               metadata=metadata, trace_type='ExperimentalTrace',
                                               stimuli_dicts=stimuli_dicts)
