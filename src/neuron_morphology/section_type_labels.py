from neurom import NeuriteType


# In the final schemas, the types "nsg:xxx" are going to be used
def neurite_type_to_ontology_term(neurite_type: NeuriteType) -> str:
    mapping = {
        NeuriteType.undefined: "nsg:UndefinedNeurite",
        # just for compliance with SWC spec, not going to be used here
        NeuriteType.soma: "nsg:Soma",
        NeuriteType.axon: "nsg:Axon",
        NeuriteType.basal_dendrite: "nsg:BasalDendrite",
        NeuriteType.apical_dendrite: "nsg:ApicalDendrite",
        NeuriteType.custom5: "nsg:CustomNeurite"
        # just for compliance with SWC spec, not going to be used here
    }
    return mapping.get(neurite_type, "nsg:CustomNeurite")
    # TODO there was no other value for anything above 5 before, should I leave it that way...
    #  pyswcparser morphologies probably didn't support them, and that's why they didn't occur?


def neurite_type_to_name(neurite_type: NeuriteType) -> str:
    #  everything before custom
    if (isinstance(neurite_type.value, int) and neurite_type.value < 5) \
            or not isinstance(neurite_type.value, int):  # axon carrying dendrite

        return neurite_type.name.replace("_", " ")

    return "custom neurite or soma type"  # 5 or above. Would include all customX types and 32 = all
    # TODO there was no other value for anything above 5 before, should I leave it that way...
    #  pyswcparser morphologies probably didn't support them, and that's why they didn't occur?
