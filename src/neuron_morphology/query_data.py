from kgforge.core import KnowledgeGraphForge
from kgforge.core.wrappings import FilterOperator, Filter


def get_neuron_morphologies(forge: KnowledgeGraphForge, reconstructed=True, curated="yes", limit=10000, debug=False):
    type_ = "ReconstructedNeuronMorphology" if reconstructed else "NeuronMorphology"
    filters = [Filter(operator=FilterOperator.EQUAL, path=["type"], value=type_)]

    if curated in ["yes", "no"]:
        filters.append(
            Filter(
                operator=FilterOperator.EQUAL if curated == "yes" else FilterOperator.NOT_EQUAL,
                path=["annotation", "hasBody", "label"], value="Curated"
             )
        )
    elif curated == "both":
        pass
    else:
        raise Exception(f"Unknown curated flag when retrieving neuron morphologies {curated}")

    return forge.search(*filters, limit=limit, debug=debug)
