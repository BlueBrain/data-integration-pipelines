import os
import copy

from src.helpers import get_ext_path, allocate_with_default_views, Deployment
from src.trace.visualization.lnmc_nwb_visualization import nwb2rab, get_nwb_object
from kgforge.core import Resource, KnowledgeGraphForge


def create_twdc_from_trace(trace_resource: Resource, forge: KnowledgeGraphForge,
                           dir_path="./output"):

    # download the .nwb distribution
    files_path = f"{dir_path}/tmp"
    os.makedirs(files_path, exist_ok=True)
    nwb_path = get_ext_path(trace_resource, ext_download_folder=files_path,
                            forge=forge, ext='nwb')
    nwb = get_nwb_object(nwb_path)
    filename = nwb_path.split("/")[-1].split(".")[0]
    rab_path = os.path.join(files_path, f"{filename}.rab")
    nwb2rab(nwb, rab_path)

    twdc = copy.deepcopy(trace_resource)
    delattr(twdc, 'id')
    delattr(twdc, 'distribution')
    if hasattr(twdc, 'image'):
        delattr(twdc, 'image')
    trace_resource._store_metadata = None
    twdc.type = ['Dataset', 'TraceWebDataContainer']
    if 'ExperimentalTrace' in trace_resource.type:
        trace_type = 'ExperimentalTrace'
    elif 'SimulationTrace' in trace_resource.type:
        trace_type = 'SimulationTrace'
    else:
        trace_type = 'Trace'
    twdc.isPartOf = forge.from_json({'id': trace_resource.get_identifier(),
                                     'type': trace_type
                                    })
    distribution = forge.attach(rab_path, content_type='application/rab')
    twdc.distribution = distribution
    
    return twdc


if __name__ == "__main__":
    org, project = ('public', 'thalamus')
    token = ""
    deployment = Deployment["PRODUCTION"]

    forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=token)

    trace_resource = forge_instance.retrieve("https://bbp.epfl.ch/neurosciencegraph/data/traces/d028d6ee-5ee1-4a6a-b53d-6186903e139e")

    tdwc = create_twdc_from_trace(trace_resource, forge=forge_instance)
    print(tdwc)