from slurmcompose.cluster import SlurmCluster
from slurmcompose.clustermonitor import ClusterStateMonitor
from slurmcompose.slurm_utils import get_conda_path_from_env
import time
import pprint
from termcolor import colored
import asyncio
from literegistry import RegistryClient, FileSystemKVStore, RedisKVStore
import os
import socket
# RegistryClient

import subprocess
import time
import os
import yaml

from literegistry import redis
import fire 

CLUSTER = None

def launch_cluster(port=6379, configurations=None):

    redis_url = redis.start_redis_server(port=port)
    
    for k in CLUSTER.script_specs:
        CLUSTER.script_specs[k]["args"]["registry"] = redis_url

    # Use provided configurations or fall back to default
    if configurations is None:
        raise ValueError("No configurations provided")

    print("Cluster topology:")
    pprint.pprint(configurations)
    print("--" * 20)

    # Create and start the monitor
    monitor = ClusterStateMonitor(
        CLUSTER, configs=configurations, check_interval=60  # Check every minute
    )

    try:
        print("Starting cluster state monitor...")
        monitor.start()

        # wait on monitor (a thread ) to finish
        monitor.join()

    except KeyboardInterrupt:
        print("\nStopping monitor...")
        monitor.stop()
        monitor.join()

        # Optionally terminate all jobs when stopping
        print("Terminating all jobs...")
        CLUSTER.terminate()

        print("Monitor stopped successfully")


def launch_run(
    device_name="l40-8",
    script_spec="llama8b",
    terminal="zsh",
):

    bash_script = CLUSTER.generate_script(device_name, script_spec)

    print("submitting script:")
    print(bash_script)
    print("--" * 20)

    os.execvp(terminal, [terminal, "-c", bash_script])


def load_topology(topology_file=None):
    """
    Load cluster topology from a YAML file.
    
    Args:
        topology_file: Path to YAML file containing topology
        
    Returns:
        List of configuration dictionaries
    """
    if topology_file:
        with open(topology_file, 'r') as f:
            data = yaml.safe_load(f)
        
        # Support both {"configurations": [...]} and direct list format
        if isinstance(data, dict) and 'configurations' in data:
            return data['configurations']
        elif isinstance(data, list):
            return data
        else:
            raise ValueError("Topology file must contain 'configurations' key or be a list")
    
    return None


def expand_device_wildcards(configurations, cluster):
    """
    Expand wildcard device names into specific device configurations.
    
    Supports patterns like:
    - "*-4" matches all devices with 4 GPUs (a40-4, l40-4, l40s-4)
    - "l40*-8" matches l40-8 and l40s-8
    - "a40-*" matches all a40 devices
    
    Args:
        configurations: List of configuration dictionaries
        cluster: SlurmCluster instance with device_specs
        
    Returns:
        List of configurations with wildcards expanded
    """
    if not configurations:
        return configurations
    
    import fnmatch
    expanded_configs = []
    
    for config in configurations:
        device_name = config.get('device_name', '')
        
        # Check if device_name contains wildcard characters
        if '*' in device_name or '?' in device_name:
            # Get all available devices from cluster
            available_devices = list(cluster.devices_specs.keys())
            
            # Find matching devices
            matching_devices = [
                device for device in available_devices 
                if fnmatch.fnmatch(device, device_name)
            ]
            
            if not matching_devices:
                print(f"Warning: No devices match pattern '{device_name}', skipping...")
                continue
            
            print(f"Expanding '{device_name}' to: {', '.join(matching_devices)}")
            
            # Create a configuration for each matching device
            for device in matching_devices:
                new_config = config.copy()
                new_config['device_name'] = device
                expanded_configs.append(new_config)
        else:
            # No wildcard, just pass through
            expanded_configs.append(config)
    
    return expanded_configs


def process_inline_configs(configurations, cluster):
    """
    Process configurations and register any inline model configs with the cluster.
    
    Each configuration can either:
    1. Reference an existing script_spec: {"device_name": "...", "script_spec": "llama8b", ...}
    2. Define an inline config: {"device_name": "...", "inline_config": {...}, ...}
    
    For inline configs, this function will:
    - Generate a unique name
    - Register it with the cluster
    - Replace inline_config with script_spec in the configuration
    
    Returns:
        List of processed configurations (all using script_spec)
    """
    if not configurations:
        return configurations
    
    processed_configs = []
    inline_counter = 0
    
    for config in configurations:
        if 'inline_config' in config:
            # Generate a unique name for this inline config
            inline_counter += 1
            config_name = f"inline_config_{inline_counter}"
            
            # Register the inline config with the cluster
            cluster.script_specs[config_name] = config['inline_config']
            
            # Create a new config dict with script_spec instead of inline_config
            new_config = {k: v for k, v in config.items() if k != 'inline_config'}
            new_config['script_spec'] = config_name
            print("--" * 20)
            print(f"Registered inline config '{config_name}' for device '{config.get('device_name', 'unknown')}'")
            ## print the inline config
            print("Inline config:")
            pprint.pprint(config['inline_config'])
            
            processed_configs.append(new_config)
        else:
            # Config already uses script_spec, just pass through
            processed_configs.append(config)
    
    return processed_configs


def prep_cluster(account="cse", user="gfaria", devices="machines.yaml", state_file=None, conda_env="multilora", topology=None):
    global CLUSTER

    CLUSTER = SlurmCluster(
        configs={
        },
        account=account,
        user=user,
        devices_path=devices,
        state_file= state_file if state_file is not None else "cluster_state.json",
        env_defaults={
            "conda_path": get_conda_path_from_env(),
            "conda_env": conda_env,
        },
    )
    # Load topology if provided
    configurations = load_topology(topology)
    # Expand any device wildcards (e.g., "*-4" -> "a40-4", "l40-4", "l40s-4")
    configurations = expand_device_wildcards(configurations, CLUSTER)
    # Process any inline configs and register them with the cluster
    configurations = process_inline_configs(configurations, CLUSTER)
    
    return configurations

def main_func(
    mode: str = "cluster",
    account="cse",
    device="l40-8",
    spec="llama8b",
    terminal="zsh",
    user="gfaria",
    topology=None,
    devices="machines.yaml",
    state_file=None,
    conda_env="multilora",
):
    """
    Launch SLURM cluster jobs.
    
    Args:
        mode: Operation mode - 'cluster', 'terminate', or 'run'
        account: SLURM account name
        device: Device type for 'run' mode (e.g., 'l40-8')
        spec: Script spec for 'run' mode (e.g., 'llama8b')
        terminal: Terminal to use for 'run' mode
        user: SLURM username
        topology_file: Path to YAML file with cluster topology
        
    Examples:
        # Using a topology file with inline model configs:
        python launch.py --mode=cluster --topology_file=topology_inline.yaml
        
        # Using wildcard device names:
        python launch.py --mode=cluster --topology_file=topology_wildcard.yaml
        
        # Using topology file with script_spec references:
        python launch.py --mode=cluster --topology_file=topology.yaml
        
        # Default behavior (hardcoded topology):
        python launch.py --mode=cluster
    """
        
    if mode == "apply":
        configurations = prep_cluster(account=account, user=user, devices=devices, state_file=state_file, conda_env=conda_env, topology=topology)
        launch_cluster(configurations=configurations)
        
    elif mode == "destroy":
        configurations = prep_cluster(account=account, user=user, devices=devices, state_file=state_file, conda_env=conda_env, topology=topology)
        CLUSTER.terminate()
        
    elif mode =="plan":
        configurations = prep_cluster(account=account, user=user, devices=devices, state_file=state_file, conda_env=conda_env, topology=topology)
        # prints the cluster topology
        print("--" * 20)
        print("Cluster plan:")
        pprint.pprint(configurations)
        print("--" * 20)
    elif mode == "run":
        configurations = prep_cluster(account=account, user=user, devices=devices, state_file=state_file, conda_env=conda_env, topology=topology)

        launch_run(
            device_name=device,
            script_spec=spec,
            terminal=terminal,
        )
    else:
        raise ValueError("Invalid mode")

def main():
    fire.Fire(main_func)

# python launch.py plan --topology ancestral_cluster_config.yaml --devices machines.yaml
if __name__ == "__main__":
    main()
