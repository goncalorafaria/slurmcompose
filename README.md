# SlurmCompose

A lightweight Python package for maintaining fixed-composition clusters on SLURM. Inspired by Docker Compose, SlurmCompose lets you declare your desired cluster state and automatically maintains it.

## Features

- **Fixed Composition**: Keep a specific number of each job type running at all times
- **Preemptible Support**: Handle checkpoint instances that can be preempted
- **Automatic Recovery**: Automatically recreate failed or terminated jobs
- **Simple YAML Configuration**: Define your cluster using easy-to-read YAML files
- **State Persistence**: Maintain state across restarts

## Installation

```bash
pip install slurmcompose
```

## Quick Start

### 1. Define your machines in YAML

```yaml
# machines.yaml
a40-4:
  type: gpu
  gpu_type: a40
  device_count: 4
  hours: 24
  mem: 128
  cpus: 16

l40s-4:
  type: gpu
  gpu_type: l40s
  device_count: 4
  hours: 24
  mem: 96
  cpus: 24
```

### 2. Define your job configurations

```yaml
# gsmrm.yaml
script_type: tasks.serve_reward_model
args:
  model_name: TuluGSM/gsmrm-1.7b
  device_count: ${DEVICE_COUNT}
```

### 3. Launch and maintain your cluster

```python
from slurmcompose.cluster import SlurmCluster
from slurmcompose.clustermonitor import ClusterStateMonitor
import time

# Initialize cluster
cluster = SlurmCluster(
    configs={"gsmrm": "configs/gsmrm.yaml"},
    devices_path="configs/machines.yaml",
    state_file="configs/cluster_state.json",
    user="yourusername",
    account="youraccount"
)

# Define desired cluster composition
configurations = [
    {"device_name": "a40-4", "script_spec": "gsmrm", "target_instances": 2},
    {"device_name": "l40s-4", "script_spec": "gsmrm", "target_instances": 1}
]

# Create and start the monitor
monitor = ClusterStateMonitor(
    cluster, 
    configs=configurations, 
    check_interval=60  # Check every minute
)

try:
    print("Starting cluster state monitor...")
    monitor.start()
    
    # Keep the main thread alive
    while True:
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\nStopping monitor...")
    monitor.stop()
    monitor.join()
    
    # Optionally terminate all jobs when stopping
    print("Terminating all jobs...")
    cluster.terminate()
```

## Command Line Usage

SlurmCompose includes a CLI for common operations:

```bash
# Launch cluster with default config
python -m slurmcompose cluster

# Check registry status
python -m slurmcompose registry

# View summary of running instances
python -m slurmcompose summary

# Terminate all managed jobs
python -m slurmcompose terminate

# Specify account and user
python -m slurmcompose cluster --account=myaccount --user=myusername
```

## Architecture

SlurmCompose has three main components:

1. **SlurmCluster**: Handles job submission, status checking, and termination
2. **ClusterStateMonitor**: Maintains the desired cluster state in a background thread
3. **SlurmScriptGenerator**: Generates SLURM job scripts from configuration templates

## Configuration Reference

### Machine Configuration

| Field | Description |
|-------|-------------|
| `type` | "gpu" or "cpu" |
| `gpu_type` | GPU type (a40, l40s, etc.) |
| `device_count` | Number of devices per node |
| `hours` | Maximum runtime in hours |
| `mem` | Memory in GB |
| `cpus` | Number of CPUs |

### Job Configuration

| Field | Description |
|-------|-------------|
| `script_type` | Python module to run |
| `args` | Dictionary of arguments to pass to the script |

## License

MIT