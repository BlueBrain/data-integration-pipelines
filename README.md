# data-integration-pipelines
A collection of pipelines to register and curate experimentally obtained morphologies (nsg:ReconstructedNeuronMorphologies) and electrophysiology recordings (bmo:ExperimentalTraces)


## Install
```
git clone https://github.com/BlueBrain/data-integration-pipelines.git
cd data-integration-pipelines
pip install .
```

## Available pipelines
#### Neuron Morphology pipelines 
Can be ran on the buckets of Nexus holding Neuron Morphology-s featured in OBP. Those buckets are the following
- bbp-external/seu 
- bbp/mouselight
- public/sscx
- public/thalamus
- public/hippocampus
- mmb-point-neuron-framework-model

The pipelines available: 
- **brain region** *(bbp-external/seu only)*: checks that the brain region that was attributed to the neuron morphology and the brain region where the morphology lands when placed in the atlas by its coordinates are the same
- **check_links**: checks that when a reference to another Resource is made in a morphology (by @id), this id can be retrieved. Additonally, if this id is paired with a label, checks that the label of the referenced Resource is the same as the duplicated label in the Neuron Morphology.
- **check_morphologies_schema**: checks that Neuron Morphology-s are constrained by a schema. If so, check that they pass validation against the schema that constraints them (this doesn't check the schema is the appropriate one for the type though)
- **quality_measurement_annotations**: runs quality measurement reports on morphologies (registration/update of those reports in Nexus is currently not enabled)
- **feature_annotations**: registers/update neuron morphology feature annotations extracted by neurom
- **morphology_registration**: runs registration/update of morphologies provided by SEU assuming a zip file is provided (TODO: where to upload the zip file, too big for git. + Update mechanism)
- **check_schema_validation**: collects all resources in a bucket, by type, and checks if they conform with their
current schema. Similar to check_morphologies_schema pipeline, it doesn't check if the schema is appropriate.
- **check_changed_schemas**: collects all resources in a bucket constrained by a list of schemas, and checks if they conform with their
current schema. Similar to check_morphologies_schema pipeline, it doesn't check if the schema is appropriate.

# Steps in morphology full integration

1. Morphology registration
    - metadata mapping
1. Morphology curation
    - metadata curation
        - to pass schema validation
    - dataset file transformation
        - swc, h5, asc
        - Make sure there is one distribution of each type only, and 0 or 1 '.obj'
    - dataset checks
        - morphochecks
        - quality metrics' annotations
1. Morphology features' annotations registration
1. Morphology embeddings generation
1. Rules' update

Steps 2, 3, and can be done in parallel, as long as the morphology files don't cause extra issues.

# Steps in trace integration

Trace Registration
   - generation of stimuli images
   - registration of Trace object
   - generation of .rab file
   - registration of TraceWebDataContainer object, linking to the main Trace resource through the **.isPartOf** path
   - update of Trace object to add the TraceWebDataContainer object id as **hasPart**

# Funding and Acknowledgement

The development of this software was supported by funding to the Blue Brain Project, a research center of the École polytechnique fédérale de Lausanne (EPFL), from the Swiss government’s ETH Board of the Swiss Federal Institutes of Technology.

Copyright (c) 2024 Blue Brain Project/EPFL
