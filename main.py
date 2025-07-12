from models import Node, TaskGraph, Resource, CPU
import random
import yaml
import math

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
cpus = [CPU(id) for id in range(n_cpu)]

last_used_cpu_num = -1
for task in tasks: # assigning dedicated CPUs to heavy tasks (ie, tasks with utilization > 1)
    if task.utilization > 1:
        cpus_granted = task.dedicated_cpus_required
        for cpu_num in range(last_used_cpu_num, last_used_cpu_num + cpus_granted):
            task.assign_cpu(cpu_num)
            cpus[cpu_num].add_task(task)
        last_used_cpu_num += cpus_granted

if last_used_cpu_num >= n_cpu:
    #TODO: NOT SCHEDULABLE
    # reason: not enough CPUs to acommodate heavy tasks
    ...
# CPUs with numbers from last_used_cpu_num + 1 to n_cpu - 1 will be used for light tasks
cpu_utilizations = [0.0 for _ in range(n_cpu - last_used_cpu_num - 1)]
for task in tasks:
    if task.utilization <= 1: # we'll put this task on a CPU which has the lowest utilization
        least_utilized_cpu_idx = cpu_utilizations.index(min(cpu_utilizations))  # between all CPUs that are not already dedicated to heavy tasks.
        if cpu_utilizations[least_utilized_cpu_idx] + task.utilization > 1:
            # TODO: NOT SCHEDULABLE
            # reason: not enough CPUs to acommodate light tasks
            ...
        task.assign_cpu(least_utilized_cpu_idx + last_used_cpu_num + 1)
        cpus[least_utilized_cpu_idx + last_used_cpu_num + 1].add_task(task)
        cpu_utilizations[least_utilized_cpu_idx] += task.utilization


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

scheduling_finish_time = math.lcm(*[task.rel_deadine for task in tasks])

# Main scheduling logic

events = []

def handle_event(event):
    global events
    t = event[0]
    event_type = event[1]
    if event_type == "task arrived":
        task = event[2]
        events.append((t + task.rel_deadline, "task arrived", task))
        if task.remained_execution > 0:
            # TODO : SCHEDULING FAILS HERE
            # reason: task missed its deadline
            ...
        task_cpu = task.assigned_cpus[0]
        if task.rel_deadline < task_cpu.ceil:
            task_cpu.change_current_task(t, task)
        events.append((t + task.wcet, "completion check", task))
        return
    if event_type == "cpu ceiling changed":
        cpu = event[2]
        for task in cpu.tasks:
            if task.rel_deadline < cpu.ceil:
                if task not in cpu.actives:
                    cpu.actives.append(task)
        


while events:
    next_event = min(events)
    events.remove(next_event)
    handle_event(next_event)