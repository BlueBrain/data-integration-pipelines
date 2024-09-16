import getpass
from typing import Dict, Tuple

from kgforge.core import KnowledgeGraphForge, Resource

from src.helpers import allocate


# See summary of all e-models: https://docs.google.com/spreadsheets/d/1d0C1FToTc30TMubteFUHMbEph8qNBph5YxruOaahfl0/edit?gid=0#gid=0

FIELD_CA1_ID = "http://api.brain-map.org/api/v2/data/Structure/382"
MUS_MUSCULUS_ID = "http://purl.obolibrary.org/obo/NCBITaxon_10090"
RAT_ID = "http://purl.obolibrary.org/obo/NCBITaxon_10116"


MAIN_OLFACTORY_BULB_ID = "http://api.brain-map.org/api/v2/data/Structure/507"
CEREBELLUM_ID = "http://api.brain-map.org/api/v2/data/Structure/512"
CAUDOPUTAMEN_ID = "http://api.brain-map.org/api/v2/data/Structure/672"

# Source: Aurelien linked to these

# https://bbpteam.epfl.ch/project/spaces/display/CELLS/Placeholder+canonical+e-models+for+MMB "E-models based on mouse thalamus data"
placeholder_thalamus = [
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/e53e6d3d-dd4f-4f5f-824d-c2e09f98a13e",  # cAD_noscltb
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/4171c8c5-6cc5-44b5-a158-acca33995ddc",  # cNAD_noscltb
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/cc57563c-7700-48ba-ae78-b5382012f0cc",  # dAD_ltb
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/20ed8beb-ae87-4800-94be-c8b6c3e0f544",  # dNAD_ltb
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/a33f329b-97ff-44de-a84b-f80be0cb05e7"  # bAC_IN
]  # 5

# https://bbpteam.epfl.ch/project/spaces/display/CELLS/Detailed+morphology+canonical+e-models "E-models based on mouse thalamus data"
detailed_thalamus = [
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/a91d8b6f-80f3-4cee-95d4-74f6a48985d2",  # cAD_noscltb
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/97f80229-0137-40f9-85e0-b329601d38fc",  # cNAD_noscltb
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/c2149725-c6d7-4b43-a07c-e85e72ec49a9",  # dAD_ltb
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/10fd5d5d-e480-4788-9834-3821bfa733d8",  # dNAD_ltb
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/bdc856d2-4e95-4b04-9ea6-406f7213c133"  # bAC_IN
]  # 5

thalamus_extra = [
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/d9d3a725-b237-4126-8769-11294c1df34e"
]  # old iteration

thalamus_id_list = placeholder_thalamus + detailed_thalamus + thalamus_extra


# https://bbpteam.epfl.ch/project/spaces/display/CELLS/Placeholder+canonical+e-models+for+MMB "E-models based on Rat SSCx ephys data"
placeholder_sscx = [
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/613ebf19-588f-4430-b5e8-6d4ea069eb1e",  # cADpyr
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/4d977438-2a17-4e67-acd7-efb14f036af9",  # bAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/ac4761ca-fea9-480c-8299-73f6679aca24",  # cAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/1009d440-eb71-4f2b-bcd2-45eb2b5c807f",  # cNAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/cb4edbf5-e937-4a48-901b-177e9f875480",  # bNAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/561f788b-c67e-4184-b3ec-63c982917507",  # dNAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/c2307d93-3a2b-49d9-8c60-c3ebded6a9a6",  # cIR
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/e1d33370-cada-4a33-b8d8-9a9ec5852af7",  # bIR
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/e5ead7d3-bb7b-46be-ba37-aa9837912309",  # bSTUT
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/5b0e476d-3f8c-43f2-ba2b-0fc725480ec3",  # cSTUT
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/f81141fb-0e8b-44c0-b6d9-eedb2d14f8a3"  # dSTUT
]  # 11

# https://bbpteam.epfl.ch/project/spaces/display/CELLS/Detailed+morphology+canonical+e-models "E-models based on Rat SSCx ephys data"
detailed_sscx = [
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/e990f748-a856-4be0-a7d3-9f0bc336447c",  # cADpyr
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/66296306-e949-4aaa-8f52-662e0c804809",  # bAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/94733a60-5229-46bc-9cd4-ffa47e028d8a",  # cAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/1e6903ec-09f1-49c6-ad10-b48f69da3a23",  # cNAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/17d09aab-b64c-4580-92d7-42338fa63427",  # bNAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/4e518c6c-474f-41f8-a5b2-58312a7c6714",  # dNAC
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/c356905e-4141-48e2-be50-b7c2a7936b40",  # cIR
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/2960cf57-b75d-4693-b382-025f16daa0f6",  # bIR
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/39983e44-1575-404e-887d-b03f70e373a8",  # bSTUT
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/90b23b78-48ae-4a71-bda2-92755ca34c69",  # cSTUT
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/e7b43750-ce2e-4ab4-9e00-fdf04e82d116",  # dSTUT
]  # 11

gen_sscx = [
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/642fbfec-3e40-4123-8596-650bd03daf7b",  # detailed gen exc
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/09ae10e7-bf17-49cf-a698-109182e470a4",  # detailed gen inh
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/d68d199e-3d0f-4095-a499-f1df4d19b587",  # placeholder gen exc
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/2682bc08-31b5-4e4e-a599-171ffd0f9048"  # placeholder gen inh
]  # 2+2

sscx_five_extra = [
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/c059ad73-d688-4111-b963-79f6d7052845",
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/7a3be0d1-63b3-4c15-96a2-2070583f86af",
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/f2acd233-eae4-40d1-849a-7c54952eba58",
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/0fad94d5-e7d3-4087-95e7-d742d41349f6",
    "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/e72757fe-4595-4e78-90db-84ee81ef2508",
]  # old iterations

sscx_id_list = placeholder_sscx + detailed_sscx + gen_sscx + sscx_five_extra


def curated_e_models(forge: KnowledgeGraphForge):
    sscx_curated = forge.retrieve(placeholder_sscx + detailed_sscx + gen_sscx)
    thalamus_curated = forge.retrieve(placeholder_thalamus + detailed_thalamus)

    hippocampus_rat_e_models = get_emodels_in_hippocampus(forge, get_rat=True)
    hippocampus_mouse_e_models = get_emodels_in_hippocampus(forge, get_rat=False)

    other_br = [
        i for br in [CEREBELLUM_ID, CAUDOPUTAMEN_ID, MAIN_OLFACTORY_BULB_ID] for i in forge.search(
            {
                "type": "EModel",
                "brainLocation": {"brainRegion": {"id": br}}
            },

            limit=10000
        )
    ]

    all_e_models = other_br + sscx_curated + thalamus_curated + hippocampus_rat_e_models + hippocampus_mouse_e_models

    assert len(all_e_models) == 112
    return all_e_models


def get_emodels_in_hippocampus(forge_emodels, get_rat: bool):
    res = "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/2f00630e-dd48-4acc-9909-d464858c929e"
    collection = forge_emodels.retrieve(res)
    parts = [forge_emodels.retrieve(i.get_identifier()) for i in collection.hasPart]
    part_from_hippo = [
        i for i in parts
        if i.brainLocation.brainRegion.get_identifier() == FIELD_CA1_ID
    ]

    expected_count = 39 if get_rat else 14

    part_from_hippo_and_species = [
        i for i in part_from_hippo
        if i.subject.species.get_identifier() == (RAT_ID if get_rat else MUS_MUSCULUS_ID)
    ]

    assert len(part_from_hippo_and_species) == expected_count  # according to text file from Darshan + EModel excel
    return part_from_hippo_and_species


def get_e_models_and_categorisation(forge: KnowledgeGraphForge) -> Dict[str, Tuple[Resource, bool]]:

    hippocampus_rat_e_models = get_emodels_in_hippocampus(forge, get_rat=True)
    hippocampus_mouse_e_models = get_emodels_in_hippocampus(forge, get_rat=False)

    thalamus_e_models = forge.retrieve(thalamus_id_list)
    sscx_e_models = forge.retrieve(sscx_id_list)

    sscx_thalamus = thalamus_e_models + sscx_e_models
    assert len(sscx_thalamus) == (18 + 18 + 6)

    expected_full = hippocampus_mouse_e_models + hippocampus_rat_e_models + sscx_thalamus

    expected_incomplete = [
        i for br in [CEREBELLUM_ID, CAUDOPUTAMEN_ID, MAIN_OLFACTORY_BULB_ID] for i in forge.search(
            {
                "type": "EModel",
                "brainLocation": {"brainRegion": {"id": br}}
            },

            limit=10000
        )
    ]
    assert len(expected_incomplete) == 23

    expected_full_as_dict = dict(
        (i.get_identifier(), (i, True))
        for i in expected_full
    )
    expected_incomplete_as_dict = dict(
        (i.get_identifier(), (i, False))
        for i in expected_incomplete
    )

    all_e_models = forge.search({"type": "EModel"}, limit=10000)

    id_to_flag = {**expected_incomplete_as_dict, **expected_full_as_dict}

    assert len(id_to_flag) == len(all_e_models)

    return id_to_flag


if __name__ == "__main__":
    org, project = "bbp", "mmb-point-neuron-framework-model"
    token = getpass.getpass()
    forge_instance = allocate(org, project, is_prod=True, token=token)

    res = get_e_models_and_categorisation(forge_instance)
    # res = curated_e_models(forge_instance)
