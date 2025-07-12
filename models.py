import math
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
        self.resource_requests = {}  # key are relative times of requests, values are (resource, duration)
        self.task_graph : TaskGraph
        self._resource_use_count = {} # key are resources, values are numbers of access
    
    def will_depend_on(self, other):
        self.in_nodes.append(other)
        other.out_nodes.append(self)
    
    def update_max_path_to_me(self):
        self.max_path_to_me = 0
        for in_node in self.in_nodes:
            self.max_path_to_me = max(self.max_path_to_me, in_node.max_path_to_me)
        self.max_path_to_me += self.exec_time
    
    def use_resource_another_time(self, resource):
        self._resource_use_count[resource] = self._resource_use_count.get(resource, 0) + 1

    def distribute_request_times(self):
        """
        Initiates the `resource_requests` dictionary in a way that the appropriate
        portion of this node's runtime will be devoted to critical sections.
        The algorithm used to do so is the famous "fixed rand-sum" method.
        The exact value of this portion is a hyperparameter and can be found in the associated confing file.
        """
        cirtical_section_portion = config["node"]["critical_section_portion"]
        n_critical_section = sum(self._resource_use_count.values())
        n_normal_section = n_critical_section + 1
        n_total_section = n_critical_section + n_normal_section
        critical_section_boundaries = [
            random.random() * self.exec_time * cirtical_section_portion for _ in range(n_critical_section - 1)
        ]
        critical_section_boundaries.sort()
        critical_section_boundaries.insert(0, 0)
        critical_section_boundaries.append(self.exec_time * cirtical_section_portion)
        normal_section_boundaries = [
            random.random() * self.exec_time * (1 -  cirtical_section_portion) for _ in range(n_normal_section - 1)
        ]
        normal_section_boundaries.sort()
        normal_section_boundaries.insert(0, 0)
        normal_section_boundaries.append(self.exec_time * (1 - cirtical_section_portion))
        requests = []
        for resource in self._resource_use_count:
            requests += [resource] * self._resource_use_count[resource]
        random.shuffle(requests)

        assert len(requests) == n_critical_section

        t = 0
        for i in range(n_critical_section):
            normal_interval_length = normal_section_boundaries[i + 1] - normal_section_boundaries[i]
            critical_interval_length = critical_section_boundaries[i + 1] - critical_section_boundaries[i]
            t += normal_interval_length
            self.resource_requests[t] = (requests[i], critical_interval_length)
            t += critical_interval_length


class TaskGraph:
    remained_execution = -1
    is_busy_waiting = False
    busy_waiting_resource  = None
    start_run_time = 0

    def __init__(self):
        self.num_mid_nodes = random.randint(
            config["graph"]["min_mid_nodes"], config["graph"]["max_mid_nodes"]
        ) # number of all nodes (except the source and sink of the DAG)
        self.start = Node(exec_time=0) # start node of the DAG
        self.end = Node(exec_time=0) # end node of the DAG
        self.mid_node_list = [Node() for _ in range(self.num_mid_nodes)] # list of all middle nodes (all nodes except the start and the end)
        self.start.task_graph = self.end.task_graph = self


        for i, node in enumerate(self.mid_node_list):
            node.task_graph = self
            node.will_depend_on(self.start)
            self.end.will_depend_on(node)
            for prev_node in self.mid_node_list[:i]:
                p = random.random()
                if p < config["graph"]["edge_probability"]:
                    node.will_depend_on(prev_node)
            node.update_max_path_to_me()
        self.end.update_max_path_to_me()
        self.critical_path_length = self.end.max_path_to_me # length of the critical path
        
        self.rel_deadine = 1 # relative deadline of the task WHICH IS THE SAME as its period
        while self.rel_deadine <= self.critical_path_length:
            self.rel_deadine <<= 1
        
        self.wcet = sum([node.exec_time for node in self.mid_node_list]) # worst-case execution time
        self.utilization = self.wcet / self.rel_deadine                  # the famous formula: U = C / T
        self.dedicated_cpus_required = math.ceil(
            (self.wcet - self.critical_path_length) / (self.rel_deadine - self.critical_path_length)
        )
        if self.utilization <= 1:
            self.dedicated_cpus_required = 0
        self.assigned_cpus = [] # list of cpus that this task can run on
        self.accessed_resources = {} # keys are resources, values are the number of accesses
    
    def assign_cpu(self, cpu_number):
        self.assigned_cpus.append(cpu_number)
    
    def use_resource_another_time(self, resource):
        """
        Adds one unit to the resource use count. Also, updates the resource's usage statistics.
        """
        self.accessed_resources[resource] = self.accessed_resources.get(resource, 0) + 1
        resource.might_be_accessed_by(self)
    
    def assign_resource_use_to_nodes(self):
        """
        Assigns the resources used by this task to the internal nodes.
        """
        for resource in self.accessed_resources:
            access_time = self.accessed_resources[resource]
            for _ in range(access_time):
                node = random.choice(self.mid_node_list)
                node.use_resource_another_time(resource)
        
        for node in self.mid_node_list:
            assert node.task_graph == self
            node.distribute_request_times()

        

class Resource:
    def __init__(self):
        self._using_cpu = None    # The signle CPU that will use this task, or -1 if more than one CPUs will do so.
        self.min_deadline = None # minimum relative deadline between all those tasks.
        self.is_global = False   # a resource is not global if it is only accessed by
        self.is_locked = False
    
    def might_be_accessed_by(self, task : TaskGraph):
        """
        Updates the `is_global` and `min_deadline` attributes.
        """
        assert task.assigned_cpus != []

        if self.min_deadline is None or self.min_deadline > task.rel_deadine:
            self.min_deadline = task.rel_deadine
                
        if self._using_cpu is None:
            if len(task.assigned_cpus) == 1:
                self._using_cpu = task.assigned_cpus[0]
            else:
                self._using_cpu = -1
                self.is_global = True
        
        elif self._using_cpu != -1:
            if len(task.assigned_cpus) > 1 or task.assigned_cpus[0] != self._using_cpu:
                self._using_cpu = -1
                self.is_global = True

class CPU:
    def __init__(self, id):
        self.id = id
        self.tasks = []
        self.actives = []
        self.ceil =  1000000 # min value of min_deadline for all locked resources local to this CPU
        self.current_task : TaskGraph | None = None
    
    def add_task(self, task : TaskGraph):
        self.tasks.append(task)

    def change_current_task(self, t, task : TaskGraph):
        if self.current_task is not None:
            if not self.current_task.is_busy_waiting:
                self.current_task.remained_execution -= t - self.current_task.start_run_time
        self.current_task = task
        self.current_task.start_run_time = t
        if task.is_busy_waiting:
            waiting_resource = task.busy_waiting_resource
            if not waiting_resource.is_locked:
                task.is_busy_waiting = False