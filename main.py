import yaml
import subprocess


def evaluate_config(runs=20):
    for i in range(runs):
        result = subprocess.run(["python3", "simulation.py"])
        count_success = 0
        
        if result.returncode == 0:
            count_success += 1

    print(f"Out of {runs} runs, exit code 0 occurred {count_success} times.")


with open("defaults.yaml", "r") as conf_file:
    config = yaml.safe_load(conf_file)

# Phase A
for u_norm in [0.1, 0.3, 0.5, 0.7, 1]:
    print(f"U_NORM = {u_norm}")
    config["cpu"]["utilization_norm"] = u_norm
    with open("hyper-parameters.yaml", "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    evaluate_config()

config["cpu"]["utilization_norm"] = 0.5

# Phase B
for num_req in [10, 30, 50]:
    print(f"Number of requests per resource = {num_req}")
    config["resource"]["total_access_options"] = [num_req]
    with open("hyper-parameters.yaml", "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    evaluate_config()

config["resource"]["total_access_options"] = [30]

# Phase C
for num_resource in [2, 4, 6, 8]:
    print(f"Number of resources = {num_resource}")
    config["resource"]["min_number"] = config["resource"]["max_number"] = num_resource
    with open("hyper-parameters.yaml", "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    evaluate_config()

config["resource"]["min_number"] = config["resource"]["max_number"] = 4

# Phase D
for csp in [0.1, 0.3, 0.5, 0.7, 0.9]:
    print(f"CSP = {csp}")
    config["node"]["critical_section_portion"] = csp
    with open("hyper-parameters.yaml", "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    evaluate_config()

config["node"]["critical_section_portion"] = 0.5

# Phase E
for num_task in [4, 6, 8]:
    print(f"Number of tasks: {num_task}")
    config["graph"]["number_of_tasks"] = num_task
    with open("hyper-parameters.yaml", "w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    evaluate_config()
