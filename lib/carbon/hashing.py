try:
  from hashlib import md5
except ImportError:
  from md5 import md5
import bisect
from carbon.conf import settings


class ConsistentHashRing:
  def __init__(self, nodes, replica_count=100):
    self.ring = []
    self.nodes = set()
    self.server_nodes = set()
    self.replica_count = replica_count
    for node in nodes:
      self.add_node(node)

  def compute_ring_position(self, key):
    big_hash = md5( str(key) ).hexdigest()
    small_hash = int(big_hash[:4], 16)
    return small_hash

  def add_node(self, node):
    self.nodes.add(node)
    self.server_nodes.add(node[0])
    for i in range(self.replica_count):
      replica_key = "%s:%d" % (node, i)
      position = self.compute_ring_position(replica_key)
      entry = (position, node)
      bisect.insort(self.ring, entry)

  def remove_node(self, node):
    self.nodes.discard(node)
    self.server_nodes.discard(node[0])
    for old_node in self.nodes:
      self.server_nodes.add(old_node[0])
    self.ring = [entry for entry in self.ring if entry[1] != node]

  def get_node(self, key):
    assert self.ring
#    position = self.compute_ring_position(key)
#    search_entry = (position, None)
#    index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
#    entry = self.ring[index]
#    return entry[1]
    node = None
    node_iter = self.get_nodes(key, 1)
    node = node_iter.next()
    node_iter.close()
    return node

  def get_nodes(self, key, count = -1):
    if count == -1 or count > len(self.server_nodes):
      count = len(self.server_nodes)
    nodes = []
    servers = set()
    position = self.compute_ring_position(key)
    search_entry = (position, None)
    index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
    last_index = (index - 1) % len(self.ring)
    while len(servers) < count and index != last_index:
      next_entry = self.ring[index]
      (position, next_node) = next_entry
      if next_node[0] not in servers:
        servers.add(next_node[0])
        nodes.append(next_node)

      index = (index + 1) % len(self.ring)
    return nodes
