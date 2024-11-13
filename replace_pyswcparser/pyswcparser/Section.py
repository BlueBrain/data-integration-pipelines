import math
from replace_pyswcparser.pyswcparser.SWC_NODE_TYPES import *

class Section:
  def __init__(self):
    self._id = None
    self._parent_section = None
    self._child_sections = []
    self._child_section_id_lookup = {}
    self._type = SWC_NODE_TYPES_BY_NAME.UNDEFINED
    self._nodes = []


  def __repr__(self):
    child_ids_str = 'none'

    if len(self._child_sections):
      child_ids_str = f"[{' '.join(list(map(lambda sec: str(sec.get_id()), self._child_sections)))}]"

    description = (
    f"[NODE]\n"
    f"ID:          {self._id}\n"
    f"type:        {SWC_NODE_TYPES_BY_ID[self._type]}\n"
    f"size (μm):   {self.get_size()}\n"
    f"nodes:       {len(self._nodes)}\n"
    f"parent ID:   {self._parent_section.get_id() if self._parent_section else 'none'}\n"
    f"parent type: {SWC_NODE_TYPES_BY_ID[self._parent_section.get_type()] if self._parent_section else 'none'}\n"
    f"child IDs:   {child_ids_str}\n"
    )
    return description


  def set_id(self, id):
    self._id = id

  
  def get_id(self):
    return self._id


  def set_type(self, type):
    if type in SWC_NODE_TYPES_BY_ID:
      self._type = type
    else:
      raise Exception("The provided type is invalid.")

  
  def get_type(self):
    return self._type


  def get_type_name(self):
    return SWC_NODE_TYPE_NAMES_BY_ID[self._type]


  def add_node(self, node):
    self._nodes.append(node)


  def set_nodes(self, nodes):
    self._nodes = nodes


  def get_number_of_nodes(self):
    return len(self._nodes)

  
  def get_nodes(self):
    return self._nodes


  def get_first_node(self):
    return self._nodes[0]


  def get_last_node(self):
    return self._nodes[-1]


  def set_parent(self, parent_section):
    self._parent_section = parent_section


  def get_parent(self):
    return self._parent_section


  def add_child(self, child_section):
    if child_section.get_id() in self._child_section_id_lookup:
      return
    self._child_sections.append(child_section)
    self._child_section_id_lookup[child_section.get_id()] = True


  def has_child(self, child_section):
    return child_section.get_id() in self._child_section_id_lookup


  def get_children(self):
    return self._child_sections


  def get_size(self):
    sum = 0
    for i in range(0, len(self._nodes) - 1):
      p1 = self._nodes[i].get_position()
      p2 = self._nodes[i + 1].get_position()
      dx = p1[0] - p2[0]
      dy = p1[1] - p2[1]
      dz = p1[2] - p2[2]
      sum = sum + math.sqrt(dx ** 2 + dy ** 2 + dz ** 2)
    return sum