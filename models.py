import random
import yaml

with open("hyper-parameters.yaml", "r") as conf_file:
    config = yaml.safe_load(conf_file)


class Node:
    def __init__(self, exec_time=None):
        if exec_time is None:
            self.exec_time = random.randint(
                config["node"]["min_exec_time"], config["node"]["max_exec_time"]
            )
        else:
            self.exec_time = exec_time
        self.out_nodes = [] # List of nodes that there is a link from this node to them.
                            # So this node should complete before they can start.
        self.in_nodes = []  # List of nodes that there is a link from them to this node.
                            # So they all should have completed before this node can start.
        self.max_path_to_me = 0 # Maximum weight between all paths that start from the graph's source and end in this node.
    
    def will_depend_on(self, other):
        """
        Makes a dependency between this node and the other, such that this node will depend on the other.
        In other words, the edge `other --> self` will be added to the graph.

        Args:
            - other: the node that should be completed before this task can start
        """

        self.in_nodes.append(other)
        other.out_nodes.append(self)
    
    def update_max_path_to_me(self):
        self.max_path_to_me = 0
        for in_node in self.in_nodes:
            self.max_path_to_me = max(self.max_path_to_me, in_node.max_path_to_me)
        self.max_path_to_me += self.exec_time

class Graph:
    def __init__(self):
        self.num_mid_nodes = random.randint( # Number of nodes (excluding the start and the end nodes)
            config["graph"]["min_mid_nodes"], config["graph"]["max_mid_nodes"]
        )
        self.start = Node(exec_time=0) # The source of the graph. Task execution will start from this node.
        self.end = Node(exec_time=0)  # The sink of the graph. Task execution will end after this node is completed.
        self.mid_node_list = [Node() for i in range(self.num_nodes)]

        # Add edges to the graph. The direction of the edge will be determined by the order of the nodes.
        for i, node in enumerate(self.mid_node_list):
            node.will_depend_on(self.start)
            self.end.will_depend_on(node)
            for prev_node in self.mid_node_list[:i]:
                p = random.uniform()
                if p < config["graph"]["edge_probability"]:
                    node.will_depend_on(prev_node)
            node.update_max_path_to_me()
        self.end.update_max_path_to_me()
        self.critical_path_length = self.end.max_path_to_me
        
        # The relative deadline of the graph will be equal to the smallest number x such that
        # 2 ** x > critical path length.
        self.rel_deadine = 1
        while self.rel_deadine <= self.critical_path_length:
            self.rel_deadine <<= 1
        