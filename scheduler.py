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

print("#" * 50)

# Calculate total processors needed using federated scheduling formula
total_utilization = sum(task.utilization for task in tasks)
total_processors_needed = math.ceil(total_utilization)

# For heavy tasks, calculate dedicated processors using the federated formula
heavy_processors_needed = 0
for task in heavy_tasks:
    # Using federated formula: m_i = ceil((C_i - L_i) / (D_i - L_i))
    # Since D_i = T_i = rel_deadline and L_i = critical_path_length
    if task.period > task.critical_path_length:
        dedicated_procs = math.ceil((task.wcet - task.critical_path_length) / (task.period - task.critical_path_length))
    else:
        dedicated_procs = math.ceil(task.utilization)
    
    task.dedicated_processors = dedicated_procs
    heavy_processors_needed += dedicated_procs

# Light tasks get one dedicated processor each
light_processors_needed = len(light_tasks)

# Total system processors needed
system_processors_needed = heavy_processors_needed + light_processors_needed

print(f"Heavy tasks processors needed: {heavy_processors_needed}")
print(f"Light tasks processors needed: {light_processors_needed}")
print(f"Total system processors needed: {system_processors_needed}")

# Update config if not set
if config["scheduling"]["system_processors"] is None:
    config["scheduling"]["system_processors"] = system_processors_needed
    print(f"Set system_processors to: {system_processors_needed}")
else:
    print(f"Using configured system_processors: {config['scheduling']['system_processors']}")

system_procs = config["scheduling"]["system_processors"]

# Check if we have enough processors
if system_procs < system_processors_needed:
    raise ValueError(f"Not enough processors: need {system_processors_needed}, have {system_procs}")

print("#" * 50)

# Assign processors to tasks using federated scheduling
used_processors = 0

print("--- Heavy Tasks (U>1) Processor Assignment ---")
for task in heavy_tasks:
    procs_needed = task.dedicated_processors
    if used_processors + procs_needed > system_procs:
        raise ValueError(f"Not enough processors for heavy task {task.id}")
    
    task.assigned_processors = list(range(used_processors, used_processors + procs_needed))
    print(f"Task {task.id}: U={task.utilization:.2f}, L={task.critical_path_length}, "
          f"dedicated_procs={procs_needed}, assigned={task.assigned_processors}")
    used_processors += procs_needed

print("--- Light Tasks (U<=1) Processor Assignment ---")
for task in light_tasks:
    task.assigned_processors = [used_processors]
    print(f"Task {task.id}: U={task.utilization:.2f}, assigned=[{used_processors}]")
    used_processors += 1

print(f"Total processors used: {used_processors}")
print("#" * 50)

# Resource generation
num_resources = random.randint(config["resource"]["min_number"], config["resource"]["max_number"])
resources = {}

print(f"--- Generating {num_resources} Resources ---")

for rid in range(num_resources):
    total_accesses = random.choice(config["resource"]["total_access_options"])
    weights = [random.random() for _ in tasks]
    total_weight = sum(weights)
    accesses = [int(w / total_weight * total_accesses) for w in weights]
    
    # Distribute remaining accesses
    diff = total_accesses - sum(accesses)
    for i in range(diff):
        accesses[i % len(accesses)] += 1
    
    # Assign resource accesses to tasks
    for idx, task in enumerate(tasks):
        if not hasattr(task, 'resource_accesses'):
            task.resource_accesses = {}
        task.resource_accesses[rid] = accesses[idx]
    
    resources[rid] = {"total_accesses": total_accesses}
    print(f"Resource {rid}: total_accesses={total_accesses}, distribution={accesses}")

print("#" * 50)

# Distribute resource accesses to nodes within each task
print("--- Distributing Resources to Nodes ---")

for task in tasks:
    print(f"\nTask {task.id} resource distribution:")
    task.node_resources = {}  # Dictionary: node_index -> {resource_id: access_count}
    
    # Initialize node resources
    for i, node in enumerate(task.mid_node_list):
        task.node_resources[i] = {}
    
    # Distribute each resource's accesses among nodes
    for rid, total_task_accesses in task.resource_accesses.items():
        if total_task_accesses == 0:
            continue
            
        # Create a list of node indices for random distribution
        node_indices = list(range(len(task.mid_node_list)))
        
        # Distribute accesses randomly among nodes
        for access_count in range(total_task_accesses):
            node_idx = random.choice(node_indices)
            if rid not in task.node_resources[node_idx]:
                task.node_resources[node_idx][rid] = 0
            task.node_resources[node_idx][rid] += 1
        
        print(f"  Resource {rid}: {total_task_accesses} total accesses distributed")
    
    # Print node-level resource distribution
    for node_idx, node_resources in task.node_resources.items():
        if node_resources:  # Only print nodes that have resources
            print(f"    Node {node_idx}: {node_resources}")

print("#" * 50)

# Create execution sections for each node
print("--- Creating Execution Sections ---")

critical_section_portion = config["node"]["critical_section_portion"]

for task in tasks:
    print(f"\nTask {task.id} execution sections:")
    task.node_execution_sections = {}  # node_index -> [section_lengths]
    
    for i, node in enumerate(task.mid_node_list):
        sections = []
        remaining_time = node.exec_time
        node_resources = task.node_resources.get(i, {})
        
        if not node_resources:
            # No resources, single execution section
            sections = [remaining_time]
        else:
            # Has resources, create alternating execution and critical sections
            total_resource_accesses = sum(node_resources.values())
            
            # Calculate critical section time
            total_critical_time = int(remaining_time * critical_section_portion)
            total_normal_time = remaining_time - total_critical_time
            
            # Distribute time among sections
            if total_resource_accesses > 0:
                critical_per_access = total_critical_time // total_resource_accesses
                normal_sections = total_resource_accesses + 1
                normal_per_section = total_normal_time // normal_sections if normal_sections > 0 else 0
                
                # Create alternating pattern: normal -> critical -> normal -> critical -> ... -> normal
                for access_idx in range(total_resource_accesses):
                    # Add normal execution section
                    if normal_per_section > 0:
                        sections.append(normal_per_section)
                    # Add critical section
                    if critical_per_access > 0:
                        sections.append(critical_per_access)
                
                # Add final normal section
                remaining_normal = total_normal_time - (normal_per_section * normal_sections)
                final_normal = normal_per_section + remaining_normal
                if final_normal > 0:
                    sections.append(final_normal)
                
                # Adjust for any remaining time due to integer division
                remaining_critical = total_critical_time - (critical_per_access * total_resource_accesses)
                if remaining_critical > 0 and len(sections) > 1:
                    # Add remaining critical time to the last critical section
                    for j in range(1, len(sections), 2):  # Critical sections are at odd indices
                        if j < len(sections):
                            sections[j] += remaining_critical // max(1, total_resource_accesses)
            else:
                sections = [remaining_time]
        
        # Ensure total time matches
        total_sections_time = sum(sections)
        if total_sections_time != remaining_time:
            if sections:
                sections[-1] += (remaining_time - total_sections_time)
        
        task.node_execution_sections[i] = sections
        print(f"  Node {i} (exec_time={node.exec_time}): sections={sections}, resources={node_resources}")

print("#" * 50)

# Detect local vs global resources
for rid, info in resources.items():
    proc_set = set()
    for task in tasks:
        if task.resource_accesses.get(rid, 0) > 0:
            proc_set.update(task.assigned_processors)
    info["type"] = "local" if len(proc_set) <= 1 else "global"

# Final output
print("--- Final Task Assignments ---")
for task in tasks:
    print(f"Task {task.id}: U={task.utilization:.2f}, procs={task.assigned_processors}")

print("\n--- Resource Access & Types ---")
for rid, info in resources.items():
    print(f"Resource {rid}: total_accesses={info['total_accesses']}, type={info['type']}")

print("\n--- Summary ---")
print(f"Total tasks: {len(tasks)}")
print(f"Heavy tasks (U>1): {len(heavy_tasks)}")
print(f"Light tasks (U<=1): {len(light_tasks)}")
print(f"Total processors used: {used_processors}")
print(f"Total resources: {num_resources}")