from kgforge.core import KnowledgeGraphForge



def get_neuron_morphologies(forge: KnowledgeGraphForge, reconstructed=True, curated="yes", limit=10000, debug=False):

    q = {"type": "ReconstructedNeuronMorphology" if reconstructed else "NeuronMorphology"}

    if curated == "yes":
        q["annotation"] = {
            "hasBody": {
                "label": "Curated"
            }
        }
    elif curated == "no":
        print("todo")  # TODO filter that curated flag shouldn't be there

    return forge.search(q, limit=limit, debug=debug)
