import sys
import re
import uuid
from src.pyswcparser.Node import Node
from src.pyswcparser.Section import Section
from src.pyswcparser.Morphology import Morphology



def parse(swc_string):
  nodes = []
  nodes_by_id = {}
  starting_nodes = []

  # The first pass is about parsing the rows of the swc file
  # and create Node instances from each row.
  for raw_line in swc_string.splitlines():
    line = raw_line.strip()
    if line.startswith('#'):
      continue

    if len(line) == 0:
      continue

    row_values = line.split()
    if len(row_values) < 7:
      continue

    node_id = int(row_values[0])
    node = Node(
      id = node_id,
      type = int(row_values[1]),
      position = [float(row_values[2]), float(row_values[3]), float(row_values[4])],
      radius = float(row_values[5])
    )

    # if the parent node id is -1 then it's a starting node (most likely in the soma)
    parent_node_id = int(row_values[6])
    if parent_node_id != -1:
      node.set_parent_id(parent_node_id)
    else:
      starting_nodes.append(node)

    # adding nodes to the list and index by id
    nodes.append(node)
    nodes_by_id[node_id] = node

  # make sure the recursion limit is large enough to handle a potential
  # neurite of a single section that would go beyond system limit of recursion call limit
  if sys.getrecursionlimit() < len(nodes):
    sys.setrecursionlimit(len(nodes))

  # The second pass is about linking the parent nodes to their child.
  for node in nodes:
    parent_node_id = node.get_parent_id()

    if parent_node_id == None or parent_node_id not in nodes_by_id:
      continue

    parent_node = nodes_by_id[parent_node_id]
    node.set_parent(parent_node)

  # Then, we build sections. A section is a contiguous list of nodes. A sections starts
  # from a starting_node, when a node is of diferent type than its parent or at a node
  # that has more than one child (forking point)
  stack = []
  for node in starting_nodes:
    stack.append({
      "node": node,
      "parent_section_id": None,
    })

  def build_section(starting_node, parent_section_id):
    # the node_list is the list of node for the section we are building.
    # Let's say it's just a simpler version of the future section object
    node_list = []

    # for each starting node, we actually have to start by adding its parent
    # to start the branch from its very basis
    if starting_node.get_parent():
      node_list.append(starting_node.get_parent())

    # the node_list is being filled with the following nodes of similar type,
    # until forking or end
    next_nodes = starting_node.dive(node_list)
    
    # building a section
    current_section_id = uuid.uuid4()
    section = Section()
    section.set_nodes(node_list)
    section.set_type(starting_node.get_type())
    section.set_id(current_section_id)

    # linking to the parent section
    if parent_section_id != None:
      parent_section = sections_by_id[parent_section_id]
      section.set_parent(parent_section)
      parent_section.add_child(section)

    # adding the next nodes as new section starting points
    for node in next_nodes:
      stack.append({
      "node": node,
      "parent_section_id": current_section_id,
    })

    return section


  sections = []
  sections_by_id = {}
  while len(stack):
    stack_elem = stack.pop()
    section = build_section(stack_elem["node"], stack_elem["parent_section_id"])
    sections.append(section)
    sections_by_id[section.get_id()] = section


  morph = Morphology(sections_by_id)
  return morph

