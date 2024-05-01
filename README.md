# data-integration-pipelines

See https://bbpteam.epfl.ch/project/spaces/display/BBKG/Morphology+Curation+Pipeline


## Running pipelines: 
- Go to 
https://bbpgitlab.epfl.ch/dke/apps/data-integration-pipelines/-/pipelines
- Set **Run for branch name or tag** to main
- Set a variable of key REALLY_UPDATE and value *yes* or *no*
- Set a variable of key LIMIT and value a neuron morphology query limit (input 10000 for no limit)
- Set a variable of key STAGING and value "yes" or "no"
- Set a variable of key CURATED and value "yes" or "both"
- Click the blue **Run Pipeline** button


A pipeline will be created under the latest commit in main here https://bbpgitlab.epfl.ch/dke/apps/data-integration-pipelines/-/pipelines.
Clicking on the round button in the stages column will unfold a list of all jobs, including the ones that were started.
Clicking on a job will show its progress and log.
If successful, on the right-side column, there will be a section for artifacts, which are the outputs of the pipeline. 
You can download them or browse them
### Available pipelines
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
- **check_schema**: checks that Neuron Morphology-s are constrained by a schema. If so, check that they pass validation against the schema that constraints them (this doesn't check the schema is the appropriate one for the type though)
- **quality_measurement_annotations**: runs quality measurement reports on morphologies (registration/update of those reports in Nexus is currently not enabled)
- **feature_annotations**: registers/update neuron morphology feature annotations extracted by neurom
- **morphology_registration**: runs registration/update of morphologies provided by SEU assuming a zip file is provided (TODO: where to upload the zip file, too big for git. + Update mechanism)

