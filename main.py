from models import TaskGraph, Resource, CPU, Event
from typing import List
import random
import yaml
import math
import sys

with open("hyper-parameters.yaml", "r") as conf_file:
    config = yaml.safe_load(conf_file)


# Generate tasks and resources
n_tasks = config["graph"]["number_of_tasks"]
tasks = [TaskGraph() for _ in range(n_tasks)]

n_resources = random.randint(config["resource"]["min_number"], config["resource"]["max_number"])
resources = [Resource() for _ in range(n_resources)]


# Compute number of CPUs, dedicate them to tasks
total_utilization = sum([task.utilization for task in tasks])
u_norm = config["cpu"]["utilization_norm"]
n_cpu = math.ceil(total_utilization / u_norm) # IMPORTANT: CPUs are numbered from 0 to n_cpu - 1
cpus : List[CPU] = [CPU(id) for id in range(n_cpu)]

last_used_cpu_num = -1
for task in tasks: # assigning dedicated CPUs to heavy tasks (ie, tasks with utilization > 1)
    if task.utilization > 1:
        cpus_granted = task.dedicated_cpus_required
        for cpu_num in range(last_used_cpu_num, last_used_cpu_num + cpus_granted):
            task.assign_cpu(cpu_num)
            cpus[cpu_num].add_task(task)
        last_used_cpu_num += cpus_granted

if last_used_cpu_num >= n_cpu:
    print("scheduing failed: not enough CPUs to acommodate heavy tasks")
    sys.exit(0)
# CPUs with numbers from last_used_cpu_num + 1 to n_cpu - 1 will be used for light tasks
cpu_utilizations = [0.0 for _ in range(n_cpu - last_used_cpu_num - 1)]
for task in tasks:
    if task.utilization <= 1: # we'll put this task on a CPU which has the lowest utilization
        least_utilized_cpu_idx = cpu_utilizations.index(min(cpu_utilizations))  # between all CPUs that are not already dedicated to heavy tasks.
        if cpu_utilizations[least_utilized_cpu_idx] + task.utilization > 1:
            print("scheduing failed: not enough CPUs to acommodate light tasks")
            sys.exit(0)
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

# Main scheduling logic



scheduling_failed = False

while events and not scheduling_failed:
    next_event = min(events)
    next_event.act()
    events.remove(next_event)
    

if scheduling_failed:
    print("FAILED")
else:
    print("SUCCEEDED")