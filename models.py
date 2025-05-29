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
        self.out_nodes = []
        self.in_nodes = []
        self.max_path_to_me = 0
    
    def will_depend_on(self, other):
        self.in_nodes.append(other)
        other.out_nodes.append(self)
    
    def update_max_path_to_me(self):
        self.max_path_to_me = 0
        for in_node in self.in_nodes:
            self.max_path_to_me = max(self.max_path_to_me, in_node.max_path_to_me)
        self.max_path_to_me += self.exec_time

class Graph:
    def __init__(self):
        self.num_mid_nodes = random.randint(
            config["graph"]["min_mid_nodes"], config["graph"]["max_mid_nodes"]
        )
        self.start = Node(exec_time=0)
        self.end = Node(exec_time=0)
        self.mid_node_list = [Node() for _ in range(self.num_mid_nodes)]

        for i, node in enumerate(self.mid_node_list):
            node.will_depend_on(self.start)
            self.end.will_depend_on(node)
            for prev_node in self.mid_node_list[:i]:
                p = random.random()  # fixed: use random.random() for [0,1)
                if p < config["graph"]["edge_probability"]:
                    node.will_depend_on(prev_node)
            node.update_max_path_to_me()
        self.end.update_max_path_to_me()
        self.critical_path_length = self.end.max_path_to_me
        
        self.rel_deadine = 1
        while self.rel_deadine <= self.critical_path_length:
            self.rel_deadine <<= 1