import json
import math
from replace_pyswcparser.pyswcparser.SWC_NODE_TYPES import *

class Node:
  def __init__(self, id, type, position, radius):
    self._id = id
    self._type = type
    self._position = [position[0], position[1], position[2]] # deep copy to avoid reuse issue
    self._radius = radius

    # Those are will be initialized in a second pass
    self._child_nodes = []
    self._parent_node = None

    # This attribute may seem redundant since we already have _parent_node,
    # but in the parsing process of SWC, nodes can be in a reversed order
    # so that a node is being referenced by one of its child before the parent
    # is showing. In other words, the child appears earlier than the parent
    # in the SWC file.
    # Then, this _parent_id attribute is used as a temporary placeholder until
    # all the nodes are parsed.
    self._parent_id = None


  def __repr__(self):
    child_ids_str = 'none'

    if len(self._child_nodes):
      child_ids_str = f"[{' '.join(list(map(lambda node: str(node.get_id()), self._child_nodes)))}]"

    description = (
    f"\n[NODE]\n"
    f"ID:        {self._id}\n"
    f"type:      {SWC_NODE_TYPES_BY_ID[self._type]}\n"
    f"position:  [{self._position[0]}, {self._position[1]}, {self._position[2]}]\n"
    f"radius:    {self._radius}\n"
    f"parent ID: {self._parent_node.get_id() if self._parent_node else 'none'}\n"
    f"child IDs: {child_ids_str}\n"
    )
    return description


  def _add_child(self, child_node):
    if not self.does_already_have_child(child_node):
      self._child_nodes.append(child_node)


  def get_id(self):
    return self._id

  
  def get_type(self):
    return self._type

  
  def get_position(self):
    return self._position

  
  def get_radius(self):
    return self._radius

  
  def is_soma(self):
    return SWC_NODE_TYPES_BY_NAME.SOMA == self._type


  def does_already_have_child(self, child_node):
    # TODO: test if list comprehension is faster
    for c  in self._child_nodes:
      if c.get_id() == child_node.get_id():
        return True
    return False
  

  def set_parent(self, parent_node):
    if parent_node.get_id() != self._parent_id:
      raise Exception('The parent node does not match the predefined parent node id.')

    self._parent_node = parent_node
    parent_node._add_child(self)


  def get_parent(self):
    return self._parent_node


  def set_parent_id(self, id):
    self._parent_id = id


  def get_parent_id(self):
    return self._parent_id


  def get_children(self):
    return self._child_nodes

  
  def dive(self, node_list):
    node_list.append(self)
    children = self._child_nodes

    if len(children) == 1 and children[0].get_type() == self._type:
      return children[0].dive(node_list)
    return children


  def distance_to(self, other_node):
    p1 = self._position
    p2 = other_node.get_position()
    dx = p1[0] - p2[0]
    dy = p1[1] - p2[1]
    dz = p1[2] - p2[2]
    return math.sqrt(dx ** 2 + dy ** 2 + dz ** 2)

  
  def traverse_to_root(self):
    """
    This methods build the list of nodes from this node up to as close
    as possible to the root by traversing using the parent.
    """
    branch_size = 0
    branch = []

    moving_node = self
    branch.append(self)
    
    while moving_node.get_parent() and moving_node.get_type() == self._type:
      parent = moving_node.get_parent()
      branch_size = branch_size + moving_node.distance_to(parent)
      branch.append(parent)
      moving_node = parent

    return (branch, branch_size)
