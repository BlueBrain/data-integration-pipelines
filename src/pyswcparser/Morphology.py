from src.pyswcparser.SWC_NODE_TYPES import *
from src.pyswcparser.Dotdict import Dotdict

class Morphology:
  def __init__(self, sections_by_ids):
    self._sections_by_ids = sections_by_ids
    self._section_list = sections_by_ids.values()

    self._section_classification = Dotdict({
      # sections that begin by a node with no parent,
      # most likely only one, of type soma.
      "starting": [],

      # sections that do not have any child,
      # their last node are leaf node in the morphology tree
      "ending": [],

      # Lists of sections by type of node/neurite
      "by_type": {
        SWC_NODE_TYPES_BY_NAME.UNDEFINED: [],
        SWC_NODE_TYPES_BY_NAME.SOMA: [],
        SWC_NODE_TYPES_BY_NAME.AXON: [],
        SWC_NODE_TYPES_BY_NAME.BASAL_DENDRITE: [],
        SWC_NODE_TYPES_BY_NAME.APICAL_DENDRITE: [],
        SWC_NODE_TYPES_BY_NAME.CUSTOM: [],
      },
    })

    # starting some indexing for the above categories
    for section in self._section_list:
      if section.get_parent() == None:
        self._section_classification.starting.append(section)

      if len(section.get_children()) == 0:
        self._section_classification.ending.append(section)

      self._section_classification.by_type[section.get_type()].append(section)


  def get_number_of_sections(self):
    return len(_section_list)


  def get_section_by_id(self, id):
    return self._sections_by_ids[id]


  def get_starting_sections(self):
    return self._section_classification.starting
  

  def get_ending_sections(self):
    return self._section_classification.ending

  
  def get_sections_by_type(self, type):
    if type not in SWC_NODE_TYPES_BY_ID:
      raise Exception('This type is invalid')

    return self._section_classification.by_type[type]


  def get_sections(self):
    return self._section_list


  def get_ending_nodes(self):
    ending_nodes = map(lambda x: x.get_last_node(), self._section_classification.ending)
    # keeping only the non-soma nodes
    
    def is_non_soma(node):
      return node.get_type() != SWC_NODE_TYPES_BY_NAME.SOMA

    return list( filter(is_non_soma, ending_nodes) )


  def get_longest_branches_per_type(self):
    ending_nodes = self.get_ending_nodes()
    branch_by_type = {}

    for node in ending_nodes:
      n_type = node.get_type()
      if n_type not in branch_by_type:
        branch_by_type[n_type] = {
          "size": 0,
          "nodes": [],
          # "number_of_nodes": 0,
        }

      (branch, branch_size) = node.traverse_to_root()
      if branch_size > branch_by_type[n_type]["size"]:
        branch_by_type[n_type]["size"] = branch_size
        branch_by_type[n_type]["nodes"] = branch
        # branch_by_type[n_type]["number_of_nodes"] = len(branch)
    return branch_by_type