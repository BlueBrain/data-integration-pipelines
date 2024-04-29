from src.pyswcparser.Dotdict import Dotdict

SWC_NODE_TYPES_BY_NAME = Dotdict({
  "UNDEFINED": 0,
  "SOMA": 1,
  "AXON": 2,
  "BASAL_DENDRITE": 3,
  "APICAL_DENDRITE": 4,
  "CUSTOM": 5,
})

SWC_NODE_TYPES_BY_ID = Dotdict({
  0: "UNDEFINED",
  1: "SOMA",
  2: "AXON",
  3: "BASAL_DENDRITE",
  4: "APICAL_DENDRITE",
  5: "CUSTOM",
})

SWC_NODE_TYPE_NAMES_BY_ID = Dotdict({
  0: "undefined",
  1: "sona",
  2: "axon",
  3: "basal dendrite",
  4: "apical dendrite",
  5: "custom neurite or soma type",
})