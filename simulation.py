import math
import random
import yaml
import sys
import numpy as np

from enum import Enum
from typing import Callable, List, Dict


with open("hyper-parameters.yaml", "r") as conf_file:
    config = yaml.safe_load(conf_file)
CEIL_INF = config["node"]["max_exec_time"] * config["graph"]["max_mid_nodes"] * 100000

### CLASSES ###
###############

class Node:
    def __init__(self, exec_time=None):
        if exec_time is None:
            self.exec_time = random.randint(
                config["node"]["min_exec_time"], config["node"]["max_exec_time"]
            )
        else:
            self.exec_time = exec_time
        self.exec_time *= 1000
        self.out_nodes : List[Node] = []
        self.in_nodes : List[Node] = []
        self.max_path_to_me = 0
        self.resource_requests = {}     # keys are relative times of requests, values are (resource, duration)
        self.release_times = {}         # keys are relative times of requests, values are resource
        self.lock_times = {}            # keys are relative times of releases, values are resource
        self.task_graph : TaskGraph
        self._resource_use_count = {}   # keys are resources, values are numbers of access
        self.passed_time = 0
        self.passed_time_last_update = None
        self.running_cpu : int | None = None
        self.resume_resource : Resource | None = None
    
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
        Initializes the `resource_requests` dictionary in a way that the appropriate
        portion of this node's runtime will be devoted to critical sections.
        The algorithm used to do so is the famous "fixed rand-sum" method.
        The exact value of this portion is a hyperparameter and can be found in the associated confing file.
        """
        critical_section_portion = config["node"]["critical_section_portion"]
        n_critical_section = sum(self._resource_use_count.values())
        n_normal_section = n_critical_section + 1
        critical_sections_duration = int(self.exec_time * critical_section_portion)
        normal_sections_duration = self.exec_time - critical_sections_duration
        if n_critical_section > 0:
            critical_section_boundaries = list(np.random.choice(np.arange(1, critical_sections_duration), n_critical_section - 1, replace=False))
            critical_section_boundaries.sort()
        else:
            critical_section_boundaries = []
        critical_section_boundaries.insert(0, 0)
        critical_section_boundaries.append(critical_sections_duration)
        normal_section_boundaries = list(
            np.random.choice(np.arange(1, normal_sections_duration), n_normal_section - 1, replace=False)
        )
        normal_section_boundaries.sort()
        normal_section_boundaries.insert(0, 0)
        normal_section_boundaries.append(normal_sections_duration)
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
            self.lock_times[t] = requests[i]
            t += critical_interval_length
            self.release_times[t] = requests[i]

    def update_passed_time(self, t):
        assert self.passed_time_last_update <= t
        self.passed_time += t - self.passed_time_last_update
        self.passed_time_last_update = t
        assert self.passed_time <= self.exec_time
    
    def is_done(self):
        assert self.passed_time <= self.exec_time
        return self.exec_time == self.passed_time
    
    def is_ready(self):
        if self.is_done(): return False
        for in_node in self.in_nodes:
            if not in_node.is_done():
                return False
        return True
    
    def get_next_lock(self):
        """
        Assuming that the passed time is updated, returns the relative time distance to the next lock request,
        along with the soon-to-be requested resource. If no other resources are going to be requested until
        the task finishes, returns (None, None)
        """
        for lock_time in sorted(self.lock_times):
            if lock_time >= self.passed_time:
                return lock_time - self.passed_time, self.lock_times[lock_time]
        return None, None
    
    def get_next_release(self):
        """
        Assuming that the passed time is updated, returns the relative time distance to the next resource release,
        along with the soon-to-be released resource. If no other resources are going to be released until
        the task finishes, returns (None, None)
        """
        for release_time in sorted(self.release_times):
            if release_time >= self.passed_time:
                return release_time - self.passed_time, self.release_times[release_time]
        return None, None 
    
class TaskGraph:
    def __init__(self):
        self.num_mid_nodes = random.randint(
            config["graph"]["min_mid_nodes"], config["graph"]["max_mid_nodes"]
        ) # number of all nodes (except the source and sink of the DAG)
        self.start = Node(exec_time=0) # start node of the DAG
        self.end = Node(exec_time=0) # end node of the DAG
        self.mid_node_list = [Node() for _ in range(self.num_mid_nodes)] # list of all middle nodes (all nodes except the start and the end)
        self.start.task_graph = self.end.task_graph = self
        
        self.pend_subtasks : List[Node] = []
        self.ready_subtasks : List[Node] = []
        self.running_subtasks : List[Node] = []
        self.bw_subtasks : Dict[Resource, Node] = {}
        self.suspends : Dict[Resource, List[Node]] = {}

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
        
        self.rel_deadline = 1 # relative deadline of the task WHICH IS THE SAME as its period
        while self.rel_deadline <= self.critical_path_length:
            self.rel_deadline <<= 1
        
        self.exec_time = sum([node.exec_time for node in self.mid_node_list]) # worst-case execution time
        self.utilization = self.exec_time / self.rel_deadline                  # the famous formula: U = C / T
        self.dedicated_cpus_required = math.ceil(
            (self.exec_time - self.critical_path_length) / (self.rel_deadline - self.critical_path_length)
        )
        if self.utilization <= 1:
            self.dedicated_cpus_required = 0
        self.assigned_cpus = [] # list of cpus that this task can run on
        self.accessed_resources = {} # keys are resources, values are the number of accesses
        self.passed_time = 0
        self.passed_time_last_update = 0
        self.all_lock_times = {} # keys are relative times of request (w.r.t. the start time of the whole task graph), values are the requested resources.    
        self.all_release_times = {}     # keys are relative times of request (w.r.t. the start time of the whole task graph), values are the released resources.    

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
        
        node_start_time = 0
        for node in self.mid_node_list:
            assert node.task_graph == self
            node.distribute_request_times()
            for request_time in node.resource_requests:
                resource, duration = node.resource_requests[request_time]
                self.all_lock_times[node_start_time + request_time] = resource
                self.all_release_times[node_start_time + request_time + duration] = resource
            node_start_time += node.exec_time

        
        assert len(self.all_lock_times) == sum(self.accessed_resources.values())
    
    def update_passed_time(self, t):
        assert self.passed_time_last_update <= t
        self.passed_time += t - self.passed_time_last_update
        self.passed_time_last_update = t
        assert self.passed_time <= self.exec_time, f"task:{self}, passed: {self.passed_time}, exec: {self.exec_time}"
    
    def get_next_lock(self):
        """
        Assuming that the passed time is updated, returns the relative time distance to the next lock request,
        along with the soon-to-be requested resource. If no other resources are going to be requested until
        the task finishes, returns (None, None)
        """
        for lock_time in sorted(self.all_lock_times):
            if lock_time >= self.passed_time:
                return lock_time - self.passed_time, self.all_lock_times[lock_time]
        return None, None
    
    def get_next_release(self):
        """
        Assuming that the passed time is updated, returns the relative time distance to the next resource release,
        along with the soon-to-be released resource. If no other resources are going to be released until
        the task finishes, returns (None, None)
        """
        for release_time in sorted(self.all_release_times):
            if release_time >= self.passed_time:
                return release_time - self.passed_time, self.all_release_times[release_time]
        return None, None

    def is_light(self):
        assert self.assigned_cpus != []
        return self.dedicated_cpus_required == 0

    def try_to_accomodate_node(self, node : Node, t : int):
        assert node in self.ready_subtasks
        assert node.running_cpu is None
        assert not self.is_light()
        for cpu_id in self.assigned_cpus:
            if cpus[cpu_id].current_subtask is None:
                cpus[cpu_id].current_subtask = node
                node.running_cpu = cpu_id
                self.ready_subtasks.remove(node)
                self.running_subtasks.append(node)

                if node.passed_time == 0:
                    subtask_start_event = Event(
                        t,
                        EventType.SUBTASK_STARTED,
                        subtask_started,
                        {"subtask" : node},
                    )
                    events.append(subtask_start_event)
                else:
                    subtask_resume_event = Event(
                        t,
                        EventType.SUBTASK_RESUMED,
                        subtask_resumed,
                        {"subtask" : node},
                    )
                    events.append(subtask_resume_event)
                return

    def __lt__(self, other):
        return self.rel_deadline < other.rel_deadline

class Resource:
    def __init__(self):
        self.using_cpu = None    # The signle CPU that will use this task, or -1 if more than one CPUs will do so.
        self.min_deadline = None # minimum relative deadline between all those tasks.
        self.is_global = False   # a resource is not global if it is only accessed by
        self.is_locked = False
        self.current_user : TaskGraph | Node | None = None
        self.busy_waiters = []
    
    def might_be_accessed_by(self, task : TaskGraph):
        """
        Updates the `is_global` and `min_deadline` attributes.
        """
        assert task.assigned_cpus != []

        if self.min_deadline is None or self.min_deadline > task.rel_deadline:
            self.min_deadline = task.rel_deadline
                
        if self.using_cpu is None:
            if len(task.assigned_cpus) == 1:
                self.using_cpu = task.assigned_cpus[0]
            else:
                self.using_cpu = -1
                self.is_global = True
        
        elif self.using_cpu != -1:
            if len(task.assigned_cpus) > 1 or task.assigned_cpus[0] != self.using_cpu:
                self.using_cpu = -1
                self.is_global = True
        
    def add_busy_waiter(self, task):
        assert self.current_user is not None
        self.busy_waiters.append(task)

    def pop_busy_waiter(self):
        if self.busy_waiters == []:
            return None
        return self.busy_waiters.pop(0)

class CPU:
    def __init__(self, id):
        self.id = id
        self.tasks = []
        self.ready_tasks : List[TaskGraph] = []
        self.active_tasks : List[TaskGraph] = []
        self.ceil = CEIL_INF    # Ceil is defined as max(ceil of all locked resources) 
                                # and implemented as min(min_deadline of all resources),
                                # or inf when no resource is locked.
        self.current_task : TaskGraph | None
        self.local_resources : List[Resource] = []
        self.busy_waiting = False
        self.current_task : TaskGraph | None = None
        self.current_subtask : Node | None = None
    
    def add_task(self, task : TaskGraph):
        self.tasks.append(task)

    def add_local_resource(self, resource : Resource):
        assert not resource.is_global and resource.using_cpu == self.id
        assert resource not in self.local_resources
        self.local_resources.append(resource)
    
    def set_current_task_from_ready(self, t):
        assert self.ready_tasks != []
        self.current_task = min(self.ready_tasks)
        self.ready_tasks.remove(self.current_task)
        assert self.current_task not in self.active_tasks
        self.active_tasks.append(self.current_task)
        self.current_task.passed_time_last_update = t
        assert self.current_task.passed_time == 0
        finish_event = Event(
            t + self.current_task.exec_time,
            EventType.TASK_FINISHED,
            check_for_finish,
            {"task" : self.current_task},
        )
        events.append(finish_event)
        next_lock, resource = self.current_task.get_next_lock()
        if next_lock is not None:
            lock_event = Event(
                t + next_lock,
                EventType.TASK_TRIED_TO_LOCK_RESOURCE,
                task_tried_to_lock_resource,
                {
                    "task" : self.current_task,
                    "resource" : resource,
                    "expected_passed" : next_lock,
                },
            )
            events.append(lock_event)


    def check_for_entrance(self, t):
        if self.busy_waiting: return
        if self.current_task is not None:
            self.current_task.update_passed_time(t)
        if self.ready_tasks == []: return

        candid = min(self.ready_tasks)
        if candid.rel_deadline < self.ceil:
            if self.current_task is None or candid.rel_deadline < self.current_task.rel_deadline:
                self.set_current_task_from_ready(t)
    
    def current_task_is_finished(self, t):
        assert self.current_task is not None
        assert self.current_task in self.active_tasks
        assert self.current_task not in self.ready_tasks
        self.active_tasks.remove(self.current_task)

        if self.ready_tasks != []:
            candid = min(self.ready_tasks)
            if candid.rel_deadline < self.ceil:
                if self.active_tasks == [] or candid.rel_deadline < min(self.active_tasks).rel_deadline:
                    self.set_current_task_from_ready(t)
                    return
        if self.active_tasks != [] : # resume the top of the active list
            self.current_task = min(self.active_tasks)
            self.current_task.passed_time_last_update = t
            finish_event = Event(
                t + self.current_task.exec_time - self.current_task.passed_time,
                EventType.TASK_FINISHED,
                check_for_finish,
                {"task" : self.current_task},
            )
            events.append(finish_event)
            next_lock, resource_to_lock = self.current_task.get_next_lock()
            next_release, resource_to_release = self.current_task.get_next_release()
            if next_lock is not None and next_lock > 0:
                assert next_release is not None
                if next_release == 0 or next_lock < next_release:
                    resource_lock_event = Event(
                        t + next_lock,
                        EventType.TASK_TRIED_TO_LOCK_RESOURCE,
                        task_tried_to_lock_resource,
                        {
                            "task" : self.current_task,
                            "resource" : resource_to_lock,
                            "expected_passed" : self.current_task.passed_time + next_lock,
                        },
                    )
                    events.append(resource_lock_event)
            if next_release is not None and next_release > 0:
                if next_lock is None or next_lock == 0 or next_release < next_lock:
                    resource_release_event = Event(
                        t + next_release,
                        EventType.TASK_RELEASED_RESOURCE,
                        task_released_resource,
                        {
                            "task" : self.current_task,
                            "resource" : resource_to_release,
                            "expected_passed" : self.current_task.passed_time + next_release,
                        },
                    )
                    events.append(resource_release_event)
        else:
            assert self.ceil == CEIL_INF
            self.current_task = None
    
    def update_ceiling(self):
        self.ceil = CEIL_INF
        for resource in self.local_resources:
            if resource.current_user is not None:
                self.ceil = min(self.ceil, resource.min_deadline)
       
class EventType(Enum):
    DEADLINE_ARRIVED = 0
    TASK_TRIED_TO_LOCK_RESOURCE = 1
    SUBTASK_TRIED_TO_LOCK_RESOURCE = 2
    TASK_RELEASED_RESOURCE = 3
    GLOBAL_RESOURCE_RELEASED = 4
    TASK_FINISHED = 5
    CHECK_SUBTASK_COMPLETED = 6
    TASK_ARRIVED = 7
    PERIOD_STARTED = 8
    SUBTASK_STARTED = 9
    SUBTASK_RESUMED = 10
    


class Event:
    def __init__(self, t : int, event_type : EventType, handler : Callable, params : dict):
        self.time = t
        self.params = params
        self.type = event_type
        self.handler = handler
    
    def act(self):
        self.handler(self.time, **self.params)
    
    def __lt__(self, other):
        return self.time < other.time or (self.time == other.time and self.type.value < other.type.value)


### INITIALIZATIONS ###
#######################

events : List[Event] = []
cpus : List[CPU]
# Generate tasks and resources
n_tasks = config["graph"]["number_of_tasks"]
tasks = [TaskGraph() for _ in range(n_tasks)]

n_resources = random.randint(config["resource"]["min_number"], config["resource"]["max_number"])
resources = [Resource() for _ in range(n_resources)]


# Compute number of CPUs, dedicate them to tasks
total_utilization = sum([task.utilization for task in tasks])
u_norm = config["cpu"]["utilization_norm"]
n_cpu = math.ceil(total_utilization / u_norm) # IMPORTANT: CPUs are numbered from 0 to n_cpu - 1
cpus = [CPU(id) for id in range(n_cpu)]

last_used_cpu_num = -1
for task in tasks: # assigning dedicated CPUs to heavy tasks (ie, tasks with utilization > 1)
    if task.utilization > 1:
        cpus_granted = task.dedicated_cpus_required
        if last_used_cpu_num + cpus_granted - 1 >= n_cpu:
            # print("scheduing failed: not enough CPUs to acommodate heavy tasks")
            sys.exit(-1)
        for cpu_num in range(last_used_cpu_num, last_used_cpu_num + cpus_granted):
            task.assign_cpu(cpu_num)
            cpus[cpu_num].add_task(task)
        last_used_cpu_num += cpus_granted

# CPUs with numbers from last_used_cpu_num + 1 to n_cpu - 1 will be used for light tasks
cpu_utilizations = [0.0 for _ in range(n_cpu - last_used_cpu_num - 1)]
for task in tasks:
    if task.utilization <= 1: # we'll put this task on a CPU which has the lowest utilization
        if cpu_utilizations == []:
            sys.exit(-1)
        least_utilized_cpu_idx = cpu_utilizations.index(min(cpu_utilizations))  # between all CPUs that are not already dedicated to heavy tasks.
        if cpu_utilizations[least_utilized_cpu_idx] + task.utilization > 1:
            # print("scheduing failed: not enough CPUs to acommodate light tasks")
            sys.exit(-1)
        task.assign_cpu(least_utilized_cpu_idx + last_used_cpu_num + 1)
        cpus[least_utilized_cpu_idx + last_used_cpu_num + 1].add_task(task)
        cpu_utilizations[least_utilized_cpu_idx] += task.utilization

# Add local resources to the corresponding lists in CPUs
for resource in resources:
    cpu_id = resource.using_cpu
    if cpu_id is not None:
        cpus[cpu_id].add_local_resource(resource)

# Make tasks use resources
access_count_options = config["resource"]["total_access_options"]
for resource in resources:
    total_access = random.choice(access_count_options)
    for access in range(total_access):
        accessing_task = random.choice(tasks)
        accessing_task.use_resource_another_time(resource)


# Distribute resource usage of each task between its nodes
for task in tasks:
    task.assign_resource_use_to_nodes()

scheduling_finish_time = math.lcm(*[task.rel_deadline for task in tasks])


### EVENT HANDLERS ###
######################

def task_arrived(t : int, task : TaskGraph):
    task.passed_time = 0
    task.passed_time_last_update = None
    next_arrival = Event(t + task.rel_deadline, EventType.TASK_ARRIVED, task_arrived, {"task" : task})
    next_deadline = Event(t + task.rel_deadline, EventType.DEADLINE_ARRIVED, deadline_arrived, {"task" : task})
    events.append(next_arrival)
    events.append(next_deadline)
    assert task.is_light()
    cpu_id = task.assigned_cpus[0]
    cpus[cpu_id].ready_tasks.append(task)
    cpus[cpu_id].check_for_entrance(t)        

def deadline_arrived(t : int, task : TaskGraph):
    global scheduling_failed
    assert task.is_light()
    cpu_id = task.assigned_cpus[0]
    if task == cpus[cpu_id].current_task:
        task.update_passed_time(t)
    if task.passed_time < task.exec_time:
        scheduling_failed = True

def task_tried_to_lock_resource(t : int, task : TaskGraph, resource : Resource, expected_passed : int):
    assert task.is_light()
    cpu_id = task.assigned_cpus[0]
    if cpus[cpu_id].current_task != task: return
    task.update_passed_time(t)
    if task.passed_time != expected_passed: return

    if resource.is_global:
        if resource.current_user is None:
            resource.current_user = task
            next_release, resource_to_release = task.get_next_release()
            assert resource == resource_to_release
            assert next_release is not None
            release_event = Event(
                t + next_release,
                EventType.TASK_RELEASED_RESOURCE,
                task_released_resource,
                {
                    "task" : task,
                    "resource" : resource,
                    "expected_passed" : task.passed_time + next_release,
                },
            )
            events.append(release_event)
        else:
            resource.add_busy_waiter(task)
            cpus[cpu_id].busy_waiting = True
    else:
        assert resource.current_user is None
        resource.current_user = task
        next_release, resource_to_release = task.get_next_release()
        assert next_release is not None
        assert resource == resource_to_release
        release_event = Event(
            t + next_release,
            EventType.TASK_RELEASED_RESOURCE,
            task_released_resource,
            {
                "task" :  task,
                "resource" : resource,
                "expected_passed" : task.passed_time + next_release,
            }
        )
        events.append(release_event)
        cpus[cpu_id].update_ceiling()

def task_released_resource(t : int, task : TaskGraph, resource : Resource, expected_passed : int):
    assert task.is_light()
    cpu_id = task.assigned_cpus[0]
    if cpus[cpu_id].current_task != task: return
    task.update_passed_time(t)
    if task.passed_time != expected_passed: return

    next_lock, resource_to_lock = task.get_next_lock()
    if next_lock is not None:
        lock_event = Event(
            t + next_lock,
            EventType.TASK_TRIED_TO_LOCK_RESOURCE,
            task_tried_to_lock_resource,
            {
                "task" : task,
                "resource" : resource_to_lock,
                "expected_passed" : task.passed_time + next_lock,
            },
        )
        events.append(lock_event)

    if resource.is_global:
        release_event = Event(
            t,
            EventType.GLOBAL_RESOURCE_RELEASED,
            global_resource_released,
            {"resource" : resource},
        )
        events.append(release_event)
    else:
        resource.current_user = None
        cpus[cpu_id].update_ceiling()
        cpus[cpu_id].check_for_entrance(t)

def check_for_finish(t : int, task : TaskGraph):
    assert task.is_light()
    cpu_id = task.assigned_cpus[0]
    if cpus[cpu_id].current_task != task: return
    if cpus[cpu_id].busy_waiting: return
    task.update_passed_time(t)
    if task.passed_time < task.exec_time:
        return

    cpus[cpu_id].current_task_is_finished(t)



def global_resource_released(t, resource : Resource):
    assert resource.is_global
    next_user = resource.pop_busy_waiter()
    resource.current_user = next_user
    if isinstance(next_user, TaskGraph):
        assert next_user.is_light()
        cpu_id = next_user.assigned_cpus[0]
        assert cpus[cpu_id].current_task == next_user # Ensure that the new user has been busywaiting until just now
        next_user.passed_time_last_update = t
        next_release, resource_to_release = next_user.get_next_release()
        assert resource == resource_to_release
        assert next_release is not None
        release_event = Event(
            t + next_release,
            EventType.TASK_RELEASED_RESOURCE,
            task_released_resource,
            {
                "task" : next_user,
                "resource" : resource,
                "expected_passed" : next_user.passed_time + next_release,
            },
        )
        events.append(release_event)
        finish_event = Event(
            t + next_user.exec_time - next_user.passed_time,
            EventType.TASK_FINISHED,
            check_for_finish,
            {"task" : next_user},
        )
        events.append(finish_event)
        assert cpus[cpu_id].busy_waiting
        cpus[cpu_id].busy_waiting = False
        cpus[cpu_id].check_for_entrance(t)
    elif isinstance(next_user, Node):
        # next user should now be 'running'
        assert not next_user.task_graph.is_light()
        next_user.passed_time_last_update = t
        assert next_user == next_user.task_graph.bw_subtasks[resource]
        del next_user.task_graph.bw_subtasks[resource]
        next_user.task_graph.running_subtasks.append(next_user)
        next_release, resource_to_release = next_user.get_next_release()
        assert resource_to_release == resource
        global_release_event = Event(
            t + next_release,
            EventType.GLOBAL_RESOURCE_RELEASED,
            global_resource_released,
            {"resource" : resource},
        )
        events.append(global_release_event)
        completion_event = Event(
            t + next_user.exec_time - next_user.passed_time,
            EventType.CHECK_SUBTASK_COMPLETED,
            check_if_subtask_completed,
            {"subtask" : next_user},
        )
        # the first suspender in next user's cluster should be ready and, once dispatched, make a request for this resource
        if next_user.task_graph.suspends.get(resource, []) != []:
            top_suspender = next_user.task_graph.suspends[resource].pop(0)
            next_user.task_graph.ready_subtasks.append(top_suspender)
            top_suspender.resume_resource = resource

def period_started(t : int, task : TaskGraph):
    global scheduling_failed
    assert not task.is_light()
    if t > 0:
        for subtask in task.mid_node_list:
            if subtask.passed_time < subtask.exec_time:
                scheduling_failed = True

    next_period_start = Event(
        t + task.rel_deadline,
        EventType.PERIOD_STARTED,
        period_started,
        {"task" : task},
    )
    events.append(next_period_start)
    for cpu_id in task.assigned_cpus:
        cpus[cpu_id].current_subtask = None
    task.pend_subtasks = []
    task.ready_subtasks = []
    task.running_subtasks = []
    task.bw_subtasks = {}
    task.suspends = {}
    for node in task.mid_node_list:
        node.passed_time = 0
        node.passed_time_last_update = None
        node.running_cpu = None
        if node.is_ready():
            task.ready_subtasks.append(node)
            task.try_to_accomodate_node(node, t)
        else:
            task.pend_subtasks.append(node)
    

def subtask_started(t : int, subtask : Node):
    assert subtask.passed_time == 0
    completion_event = Event(
        t + subtask.exec_time,
        EventType.CHECK_SUBTASK_COMPLETED,
        check_if_subtask_completed,
        {"subtask" : subtask},
    )
    events.append(completion_event)
    subtask.passed_time = 0
    subtask.passed_time_last_update = t
    next_lock, resource_to_lock = subtask.get_next_lock()
    if next_lock is not None:
        lock_event = Event(
            t + next_lock,
            EventType.SUBTASK_TRIED_TO_LOCK_RESOURCE,
            subtask_tried_to_lock_a_resource,
            {
                "subtask" : subtask,
                "resource" : resource_to_lock,
            },
        )
        events.append(lock_event)


def subtask_resumed(t : int, subtask : Node):
    completion_event = Event(
        t + subtask.exec_time - subtask.passed_time,
        EventType.CHECK_SUBTASK_COMPLETED,
        check_if_subtask_completed,
        {"subtask" : subtask},
    )
    events.append(completion_event)
    subtask.passed_time_last_update = t
    if subtask.resume_resource is not None:
        resource_lock_event = Event(
            t,
            EventType.SUBTASK_TRIED_TO_LOCK_RESOURCE,
            subtask_tried_to_lock_a_resource,
            {
                "subtask" : subtask,
                "resource" : subtask.resume_resource,
            },
        )
        events.append(resource_lock_event)
        subtask.resume_resource = None

def check_if_subtask_completed(t : int, subtask : Node):
    if subtask not in subtask.task_graph.running_subtasks: return
    subtask.update_passed_time(t)

    if subtask.passed_time < subtask.exec_time:
        completion_event = Event(
            t + subtask.exec_time - subtask.passed_time,
            EventType.CHECK_SUBTASK_COMPLETED,
            check_if_subtask_completed,
            {"subtask" : subtask},
        )
        events.append(completion_event)
        return
    assert subtask.running_cpu is not None
    cpus[subtask.running_cpu].current_subtask = None
    subtask.running_cpu = None
    subtask.task_graph.running_subtasks.remove(subtask)
    
    no_more_pends = []
    for pend_node in subtask.task_graph.pend_subtasks:
        if pend_node.is_ready():
            no_more_pends.append(pend_node)
    for no_more_pend in no_more_pends:
        subtask.task_graph.pend_subtasks.remove(no_more_pend)
        subtask.task_graph.ready_subtasks.append(no_more_pend)
    ready_copy = [element for element in subtask.task_graph.ready_subtasks]
    for ready_node in ready_copy:
        subtask.task_graph.try_to_accomodate_node(ready_node, t)

def subtask_tried_to_lock_a_resource(t : int, subtask : Node, resource : Resource):
    assert subtask in subtask.task_graph.running_subtasks
    subtask.update_passed_time(t)
    if resource.current_user is None:
        resource.current_user = subtask
        next_release, resource_to_release = subtask.get_next_release()
        assert resource_to_release == resource
        assert next_release is not None
        global_resource_release_event = Event(
            t + next_release,
            EventType.GLOBAL_RESOURCE_RELEASED,
            global_resource_released,
            {"resource" : resource},
        )
        events.append(global_resource_release_event)
    else:
        if resource in subtask.task_graph.bw_subtasks.keys():
            if not resource in subtask.task_graph.suspends.keys():
                subtask.task_graph.suspends[resource] = []
            subtask.task_graph.suspends[resource].append(subtask)
            subtask.task_graph.running_subtasks.remove(subtask)
            assert subtask.running_cpu is not None
            cpus[subtask.running_cpu].current_subtask = None
            subtask.running_cpu = None
    
            ready_copy = [element for element in subtask.task_graph.ready_subtasks]
            for ready_node in ready_copy:
                subtask.task_graph.try_to_accomodate_node(ready_node, t)
        else:
            subtask.task_graph.running_subtasks.remove(subtask)
            subtask.task_graph.bw_subtasks[resource] = subtask
            resource.busy_waiters.append(subtask)


### MAIN LOOP ###
#################


scheduling_failed = False
for task in tasks:
    if task.is_light():
        events.append(Event(
            0,
            EventType.TASK_ARRIVED,
            task_arrived,
            {"task" : task},
        ))
    else:
        events.append(Event(
            0,
            EventType.PERIOD_STARTED,
            period_started,
            {"task" : task},
        ))


time = 0
next_event = min(events)
while events and not scheduling_failed and next_event.time <= scheduling_finish_time:
    #print(f"{next_event.time}:\t{next_event.type}", next_event.params)
    events.remove(next_event)
    next_event.act()
    time = next_event.time
    if events:
        next_event = min(events)
    

if scheduling_failed:
    #print("deadline missed")
    sys.exit(-1)
else:
    #print("SUCCEEDED - time =", time, " /", scheduling_finish_time)
    sys.exit(0)
