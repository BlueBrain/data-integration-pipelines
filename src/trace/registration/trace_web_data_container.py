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
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI5T0R3Z1JSTFVsTTJHbFphVDZjVklnenJsb0lzUWJmbTBDck1icXNjNHQ4In0.eyJleHAiOjE3Mjc4OTQzMDksImlhdCI6MTcyNzg3NTM3MywiYXV0aF90aW1lIjoxNzI3ODU4MzA5LCJqdGkiOiI5NTBkZDk0OC05NTJkLTRmYTEtOTlmMi02OTUxYzc0NTMxOTgiLCJpc3MiOiJodHRwczovL2JicGF1dGguZXBmbC5jaC9hdXRoL3JlYWxtcy9CQlAiLCJhdWQiOlsiaHR0cHM6Ly9zbGFjay5jb20iLCJjb3Jlc2VydmljZXMtZ2l0bGFiIiwiYWNjb3VudCJdLCJzdWIiOiJmOjBmZGFkZWY3LWIyYjktNDkyYi1hZjQ2LWM2NTQ5MmQ0NTljMjpjZ29uemFsZSIsInR5cCI6IkJlYXJlciIsImF6cCI6ImJicC1uaXNlLW5leHVzLWZ1c2lvbiIsIm5vbmNlIjoiZTg0NDcyMzhjMTNhNDQxNWFiN2EyOGQyYjcxMWZmNTQiLCJzZXNzaW9uX3N0YXRlIjoiNWJlNjk5YWMtNzAwYS00OGY5LWEzMzYtZmNiOTczYTk0OThlIiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImJicC1wYW0tYXV0aGVudGljYXRpb24iLCJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1iYnAiXX0sInJlc291cmNlX2FjY2VzcyI6eyJodHRwczovL3NsYWNrLmNvbSI6eyJyb2xlcyI6WyJyZXN0cmljdGVkLWFjY2VzcyJdfSwiY29yZXNlcnZpY2VzLWdpdGxhYiI6eyJyb2xlcyI6WyJyZXN0cmljdGVkLWFjY2VzcyJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJvcGVuaWQgbmV4dXMgcHJvZmlsZSBsb2NhdGlvbiBlbWFpbCIsInNpZCI6IjViZTY5OWFjLTcwMGEtNDhmOS1hMzM2LWZjYjk3M2E5NDk4ZSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJuYW1lIjoiQ3Jpc3RpbmEgRWxpemFiZXRoIEdvbnphbGV6IEVzcGlub3phIiwibG9jYXRpb24iOiJCMSAzIDI4NC4wNTIiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJjZ29uemFsZSIsImdpdmVuX25hbWUiOiJDcmlzdGluYSBFbGl6YWJldGggR29uemFsZXoiLCJmYW1pbHlfbmFtZSI6IkVzcGlub3phIiwiZW1haWwiOiJjcmlzdGluYS5nb256YWxlemVzcGlub3phQGVwZmwuY2gifQ.giae1vTnMyAQx-Ju7PjKYSBR7KZSsaGamn5Caz4pPT2fY3CIIfdH04p_k0ZDbNwHRIwCtcGXMmiuvIK3gzdeg5Jk1AUqzLi3WQ_SApAbxeUFILKrck-dOsDkHFVJlTH4kWDMt3RYbnz4QKehUCJfw1DpIZ7R9nkjelLfHJxArX_JI3FYjS4o0AlAwPz435js9o1Qs0_uFep25icpEMCxOvS_4wxFXbfgxXxeIRFLBs4O2eC_0R6yIXomz1u6SqInl9bfkAnEXfeQTeyKdtHlGIOP_4kwx9Tm6AxCkGb9omlJ6WnlLeU96wSKSmUjCtM0PZFtwFuBEu4LBUPrIJkqtQ"
    deployment = Deployment["PRODUCTION"]

    forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=token)

    trace_resource = forge_instance.retrieve("https://bbp.epfl.ch/neurosciencegraph/data/traces/d028d6ee-5ee1-4a6a-b53d-6186903e139e")

    tdwc = create_twdc_from_trace(trace_resource, forge=forge_instance)
    print(tdwc)