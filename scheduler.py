
import random
import math
import yaml
from models import Graph

# Load configuration
with open("hyper-parameters.yaml", "r") as f:
    config = yaml.safe_load(f)

# Generate task graphs
num_tasks = config.get("tasks", {}).get("number_of_tasks", 1)
tasks = [Graph() for _ in range(num_tasks)]

# Compute WCET, period, and utilization for each task
heavy_tasks = []
light_tasks = []
task_infos = []
for idx, task in enumerate(tasks):
    wcet = sum(node.exec_time for node in task.mid_node_list)
    task.wcet = wcet
    period = task.rel_deadine
    task.period = period
    task.utilization = wcet / period
    task.id = idx
    task_infos.append((idx, task.utilization))
    if task.utilization > 1:
        heavy_tasks.append(task)
    else:
        light_tasks.append(task)

# Diagnostic: all task utilizations
print("--- All Task Utilizations ---")
for idx, util in task_infos:
    print(f"Task {idx}: U={util:.2f}")

print("#" * 10)
import math


total_heavy_needed = sum(math.ceil(t.utilization) for t in heavy_tasks)
print(f"Recommended minimum 'scheduling.system_processors': {total_heavy_needed}")

config["scheduling"]["system_processors"] = total_heavy_needed



print("#" * 10)

# Federated scheduling diagnostics
system_procs = config["scheduling"]["system_processors"]
used = 0
# Diagnostic: total processors needed for heavy tasks
total_heavy_needed = sum(math.ceil(t.utilization) for t in heavy_tasks)
print(f"Total heavy tasks: {len(heavy_tasks)}, total processors needed for heavy: {total_heavy_needed}")
print(f"System processors available: {system_procs}")
print("--- Heavy Tasks (U>1) ---")
for task in heavy_tasks:
    procs_needed = math.ceil(task.utilization)
    print(f"Task {task.id}: U={task.utilization:.2f}, procs_needed={procs_needed}")

# Assign dedicated processors to heavy tasks
for task in heavy_tasks:
    procs_needed = math.ceil(task.utilization)
    if used + procs_needed > system_procs:
        print(f"Error: Not enough for heavy tasks at task {task.id}")
        print(f"Used so far: {used}, needed: {procs_needed}, available: {system_procs}")
        raise ValueError(f"Not enough processors for heavy tasks: required {used + procs_needed}, available {system_procs}")
    task.assigned_processors = list(range(used, used + procs_needed))
    print(f"Assigned Task {task.id} to processors {task.assigned_processors}")
    used += procs_needed

print(f"Processors used by heavy tasks: {used}")
print(f"Remaining processors: {system_procs - used}")

# Partitioned scheduling diagnostics for light tasks
remaining_procs = list(range(used, system_procs))
proc_capacity = {pid: 1.0 for pid in remaining_procs}

print("--- Light Tasks (U<=1) sorted by decreasing U ---")
light_tasks.sort(key=lambda t: t.utilization, reverse=True)
for task in light_tasks:
    print(f"Task {task.id}: U={task.utilization:.2f}")
print(f"Available processors for light tasks: {remaining_procs}")

# Fit light tasks
for task in light_tasks:
    placed = False
    for pid in remaining_procs:
        if proc_capacity[pid] >= task.utilization:
            task.assigned_processors = [pid]
            proc_capacity[pid] -= task.utilization
            print(f"Assigned Task {task.id} to processor {pid}, remaining cap {proc_capacity[pid]:.2f}")
            placed = True
            break
    if not placed:
        print(f"Error: Cannot place Task {task.id}, U={task.utilization:.2f}, remaining capacities={proc_capacity}")
        raise ValueError(f"Not enough processing capacity for light tasks: needed {task.utilization}, capacities={proc_capacity}")

# Resource generation and allocation (unchanged)
num_resources = random.randint(config["resource"]["min_number"], config["resource"]["max_number"])
resources = {}
for rid in range(num_resources):
    total_accesses = random.choice(config["resource"]["total_access_options"])
    weights = [random.random() for _ in tasks]
    total_weight = sum(weights)
    accesses = [int(w / total_weight * total_accesses) for w in weights]
    diff = total_accesses - sum(accesses)
    for i in range(diff):
        accesses[i % len(accesses)] += 1
    for idx, task in enumerate(tasks):
        task.resource_accesses = getattr(task, 'resource_accesses', {})
        task.resource_accesses[rid] = accesses[idx]
    resources[rid] = {"total_accesses": total_accesses}

# Detect local vs global resources
for rid, info in resources.items():
    proc_set = set()
    for task in tasks:
        if task.resource_accesses.get(rid, 0) > 0:
            proc_set.update(task.assigned_processors)
    info["type"] = "local" if len(proc_set) <= 1 else "global"

# Final output
print("--- Final Task Assignments ---")
for idx, util in task_infos:
    print(f"Task {idx}: U={util:.2f}, procs={tasks[idx].assigned_processors}")

print("--- Resource Access & Types ---")
for rid, info in resources.items():
    print(f"Resource {rid}: total_accesses={info['total_accesses']}, type={info['type']}")