from slurmcompose.cluster import SlurmCluster
from slurmcompose.clustermonitor import ClusterStateMonitor
from slurmcompose.slurm_utils import get_conda_path_from_env
from slurmcompose.view import (
    DashboardManager,
    GatewayLogCapture,
    LogCaptureContext,
    print_welcome_box,
    print_mode_banner,
    print_section_header,
    is_rich_available
)
import time
import pprint
from termcolor import colored
import asyncio
from literegistry import RegistryClient, FileSystemKVStore, RedisKVStore
import os
import socket
import subprocess
import yaml
import threading
from collections import deque
import sys

from literegistry import redis
from literegistry.gateway import run_in_thread as gateway_run_in_thread
import fire 

CLUSTER = None
GATEWAY_STOP_EVENT = None
GATEWAY_LOGS = deque(maxlen=100)  # Store last 100 gateway log lines
CLUSTER_STATUS = {}  # Store current cluster status

def run_gateway_in_thread(redis_url, gateway_port=8080):
    """Run the gateway in a separate thread using the thread-safe entry point from literegistry.gateway."""
    def gateway_runner():
        try:
            print(f"🌐 Gateway starting...")
            print(f"Registry: {redis_url}")
            print(f"Port: {gateway_port}")
            
            # Use the thread-safe function from literegistry.gateway
            # stdout/stderr are already redirected globally if dashboard is active
            gateway_run_in_thread(registry=redis_url, port=gateway_port)
            
        except KeyboardInterrupt:
            print("⚠️  Gateway received shutdown signal")
        except Exception as e:
            print(f"❌ Gateway error: {e}")
            import traceback
            traceback.print_exc()
    
    return gateway_runner

def launch_cluster(port=6379, configurations=None, gateway_port=8080, use_dashboard=True):
    global GATEWAY_STOP_EVENT, CLUSTER_STATUS

    # Infrastructure setup section
    print_section_header("Infrastructure Setup", "🔧", "cyan")
    
    print(colored(f"  ▸ Starting Redis server on port {port}...", "cyan"))
    redis_url = redis.start_redis_server(port=port)
    print(colored(f"  ✓ Redis server started: {redis_url}", "green"))
    
    for k in CLUSTER.script_specs:
        CLUSTER.script_specs[k]["args"]["registry"] = redis_url

    # Use provided configurations or fall back to default
    if configurations is None:
        raise ValueError(colored("No configurations provided", "red"))

    # Cluster Composition section
    print_section_header("Cluster Composition", "📋", "magenta")
    
    print(colored(f"  Total Configurations: {len(configurations)}", "cyan", attrs=["bold"]))
    for i, config in enumerate(configurations, 1):
        device = config.get('device_name', 'unknown')
        spec = config.get('script_spec', 'unknown')
        count = config.get('count', 1)
        print(colored(f"  [{i}] ", "cyan") + colored(f"{device}", "white", attrs=["bold"]) + 
              colored(f" → {spec}", "yellow") + colored(f" (×{count})", "green"))
    
    # Gateway setup section
    print_section_header("Gateway Service", "🌐", "blue")
    
    print(colored(f"  ▸ Launching gateway server on port {gateway_port}...", "cyan"))
    
    # Set up global log capture BEFORE starting gateway if using dashboard
    global_log_handler = None
    stdout_capture = None
    stderr_capture = None
    
    if use_dashboard and is_rich_available():
        # Set up logging handler globally so gateway thread can use it
        import logging
        
        class GlobalLogHandler(logging.Handler):
            def emit(self, record):
                try:
                    msg = self.format(record)
                    from datetime import datetime
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    GATEWAY_LOGS.append(f"[{timestamp}] {msg}")
                except Exception:
                    self.handleError(record)
        
        global_log_handler = GlobalLogHandler()
        global_log_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)s:%(name)s: %(message)s')
        global_log_handler.setFormatter(formatter)
        logging.root.addHandler(global_log_handler)
        
        # Redirect stdout/stderr globally for the gateway thread
        # Keep also_print=True during setup so we see gateway boot logs
        stdout_capture = GatewayLogCapture(sys.__stdout__, GATEWAY_LOGS, also_print=True)
        stderr_capture = GatewayLogCapture(sys.__stderr__, GATEWAY_LOGS, also_print=True)
        sys.stdout = stdout_capture
        sys.stderr = stderr_capture
    
    gateway_thread = threading.Thread(
        target=run_gateway_in_thread(redis_url, gateway_port),
        daemon=True,
        name="GatewayThread"
    )
    gateway_thread.start()
    
    # Give gateway a moment to start
    print(colored(f"  ⏳ Waiting for gateway to initialize...", "cyan"))
    time.sleep(3)
    print(colored(f"  ✓ Gateway thread running", "green"))

    # Monitoring section
    print_section_header("Cluster Monitor", "👁️", "green")
    
    monitor = ClusterStateMonitor(
        CLUSTER, configs=configurations, check_interval=60  # Check every minute
    )

    print(colored("  ▸ Starting cluster state monitor...", "cyan"))
    monitor.start()
    print(colored("  ✓ Monitor started successfully", "green"))
    
    # Disable printing to terminal now if dashboard mode
    # This prevents monitor output from appearing on terminal
    if use_dashboard and is_rich_available():
        if stdout_capture:
            stdout_capture.also_print = False
        if stderr_capture:
            stderr_capture.also_print = False
    
    # Initialize cluster status
    for config in configurations:
        device = config.get('device_name', 'unknown')
        spec = config.get('script_spec', 'unknown')
        status_key = f"{device}:{spec}"
        CLUSTER_STATUS[status_key] = {'status': '⏳ starting', 'jobs': 0}
    
    # Start dashboard if available and requested
    if use_dashboard and is_rich_available():
        print(colored("\n  ▸ Starting live dashboard...", "cyan"))
        time.sleep(1)
        
        # also_print was already disabled above after monitor started
        dashboard = DashboardManager(configurations, GATEWAY_LOGS, CLUSTER_STATUS)
        live_display = dashboard.start_live_display()
        
        if live_display:
            try:
                # stdout/stderr are already redirected globally above
                # Just run the dashboard
                with live_display:
                    # Background thread to update cluster status
                    def update_status():
                        while True:
                            # Update cluster status from monitor or CLUSTER state
                            for config in configurations:
                                device = config.get('device_name', 'unknown')
                                spec = config.get('script_spec', 'unknown')
                                status_key = f"{device}:{spec}"
                                
                                # You can query actual job status here
                                # For now, simulate with running status
                                if status_key in CLUSTER_STATUS:
                                    CLUSTER_STATUS[status_key]['status'] = '✅ running'
                                    CLUSTER_STATUS[status_key]['jobs'] = config.get('count', 1)
                            
                            dashboard.update_display(live_display.renderable)
                            time.sleep(5)
                    
                    status_thread = threading.Thread(target=update_status, daemon=True)
                    status_thread.start()
                    
                    # Wait for monitor to finish
                    monitor.join()
                    
            except KeyboardInterrupt:
                pass
    else:
        # Fallback to simple mode
        print(colored("\n  ⏳ Monitor running... Press Ctrl+C to stop", "yellow", attrs=["bold"]))
        print()
        
        try:
            monitor.join()
        except KeyboardInterrupt:
            pass
    
    # Shutdown
    print()
    print_section_header("Shutdown", "⚠️", "yellow")
    
    print(colored("  ▸ Stopping monitor...", "yellow"))
    monitor.stop()
    monitor.join()
    print(colored("  ✓ Monitor stopped", "green"))

    # Optionally terminate all jobs when stopping
    print(colored("\n  ▸ Terminating all cluster jobs...", "red"))
    CLUSTER.terminate()
    print(colored("  ✓ All jobs terminated", "green"))

    print(colored("\n  ▸ Shutting down gateway...", "yellow"))
    print(colored("  ✓ Gateway thread will terminate with main process", "green"))
    
    print()
    print(colored("╰" + "─" * 68 + "╯", "cyan"))
    print(colored("  Goodbye! 👋", "cyan", attrs=["bold"]))
    print()


def launch_run(
    device_name="l40-8",
    script_spec="llama8b",
    terminal="zsh",
):

    print_section_header("Script Generation", "🔧", "magenta")
    
    print(colored(f"  Device: ", "cyan") + colored(f"{device_name}", "white", attrs=["bold"]))
    print(colored(f"  Spec: ", "cyan") + colored(f"{script_spec}", "white", attrs=["bold"]))
    print(colored(f"  Terminal: ", "cyan") + colored(f"{terminal}", "white", attrs=["bold"]))
    print()
    
    print(colored(f"  ▸ Generating script...", "cyan"))
    bash_script = CLUSTER.generate_script(device_name, script_spec)
    print(colored(f"  ✓ Script generated", "green"))

    print_section_header("Script Preview", "📜", "blue")
    print()
    print(colored(bash_script, "white"))
    print()
    print(colored("─" * 70, "blue"))
    print()

    print(colored(f"  🚀 Launching in terminal: {terminal}", "green", attrs=["bold"]))
    print()
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
                print(colored(f"    ⚠️  Warning: No devices match pattern '{device_name}', skipping...", "yellow"))
                continue
            
            print(colored(f"    ↳ Expanded '{device_name}' → ", "cyan") + 
                  colored(f"{', '.join(matching_devices)}", "white", attrs=["bold"]))
            
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
            
            if 'name' in config['inline_config'] :#and (config['inline_config']['name'] not in cluster.script_specs):
                config_name = config['inline_config']['name']
            # Register the inline config with the cluster
            cluster.script_specs[config_name] = config['inline_config']
            
            # Create a new config dict with script_spec instead of inline_config
            new_config = {k: v for k, v in config.items() if k != 'inline_config'}
            new_config['script_spec'] = config_name
            
            device_name = config.get('device_name', 'unknown')
            print(colored(f"    ↳ Registered ", "cyan") + 
                  colored(f"{config_name}", "green", attrs=["bold"]) + 
                  colored(f" for ", "cyan") + 
                  colored(f"{device_name}", "white", attrs=["bold"]))
            
            processed_configs.append(new_config)
        else:
            # Config already uses script_spec, just pass through
            processed_configs.append(config)
    
    return processed_configs


def prep_cluster(account="cse", user="gfaria", devices="machines.yaml", state_file=None, conda_env="multilora", topology=None):
    global CLUSTER

    print_section_header("Cluster Initialization", "⚙️", "cyan")
    
    print(colored(f"  User: ", "cyan") + colored(f"{user}", "white", attrs=["bold"]))
    print(colored(f"  Account: ", "cyan") + colored(f"{account}", "white", attrs=["bold"]))
    print(colored(f"  Devices: ", "cyan") + colored(f"{devices}", "white", attrs=["bold"]))
    print(colored(f"  Conda Env: ", "cyan") + colored(f"{conda_env}", "white", attrs=["bold"]))
    print()
    
    print(colored(f"  ▸ Initializing SLURM cluster...", "cyan"))
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
    print(colored("  ✓ Cluster initialized", "green"))
    
    # Load topology if provided
    if topology:
        print(colored(f"\n  ▸ Loading topology from: {topology}", "cyan"))
        configurations = load_topology(topology)
        print(colored(f"  ✓ Topology loaded", "green"))
    else:
        configurations = load_topology(None)
    
    # Expand any device wildcards (e.g., "*-4" -> "a40-4", "l40-4", "l40s-4")
    if configurations:
        print(colored(f"\n  ▸ Processing device wildcards...", "cyan"))
        configurations = expand_device_wildcards(configurations, CLUSTER)
    
    # Process any inline configs and register them with the cluster
    if configurations:
        print(colored(f"  ▸ Processing inline configurations...", "cyan"))
        configurations = process_inline_configs(configurations, CLUSTER)
    
    print()
    print(colored(f"  ✓ Preparation complete: ", "green") + 
          colored(f"{len(configurations) if configurations else 0} configurations ready", "white", attrs=["bold"]))
    
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
    gateway_port=8080,
    redis_port=6379,
    dashboard=True,
):
    """
    Launch SLURM cluster jobs.
    
    Args:
        mode: Operation mode - 'apply', 'destroy', 'plan', or 'run'
        account: SLURM account name
        device: Device type for 'run' mode (e.g., 'l40-8')
        spec: Script spec for 'run' mode (e.g., 'llama8b')
        terminal: Terminal to use for 'run' mode
        user: SLURM username
        topology: Path to YAML file with cluster topology
        devices: Path to devices YAML file
        state_file: Path to cluster state JSON file
        conda_env: Conda environment name
        gateway_port: Port for gateway server (default: 8080)
        redis_port: Port for Redis server (default: 6379)
        dashboard: Enable live terminal dashboard (default: True)
        
    Examples:
        # Apply cluster with live dashboard:
        python launch.py apply --topology=topology.yaml --dashboard=True
        
        # Plan without dashboard:
        python launch.py plan --topology=topology.yaml --dashboard=False
        
        # Destroy cluster:
        python launch.py destroy --topology=topology.yaml
        
        # Run single job:
        python launch.py run --device=l40-8 --spec=llama8b
    """
        
    # Print welcome box
    print_welcome_box()
    
    # Print mode banner
    print_mode_banner(mode)
    
    if mode == "apply":
        configurations = prep_cluster(account=account, user=user, devices=devices, state_file=state_file, conda_env=conda_env, topology=topology)
        launch_cluster(port=redis_port, configurations=configurations, gateway_port=gateway_port, use_dashboard=dashboard)
        
    elif mode == "destroy":
        configurations = prep_cluster(account=account, user=user, devices=devices, state_file=state_file, conda_env=conda_env, topology=topology)
        print_section_header("Terminating Cluster", "🛑", "red")
        print(colored("  ▸ Terminating all jobs...", "red"))
        CLUSTER.terminate()
        print(colored("  ✓ All jobs terminated successfully", "green"))
        print()
        
    elif mode =="plan":
        configurations = prep_cluster(account=account, user=user, devices=devices, state_file=state_file, conda_env=conda_env, topology=topology)
        
        # Print detailed plan
        print_section_header("Detailed Cluster Plan", "📊", "magenta")
        print()
        pprint.pprint(configurations)
        print()
        print(colored("╰" + "─" * 68 + "╯", "cyan"))
        print()
        
    elif mode == "run":
        configurations = prep_cluster(account=account, user=user, devices=devices, state_file=state_file, conda_env=conda_env, topology=topology)

        launch_run(
            device_name=device,
            script_spec=spec,
            terminal=terminal,
        )
    else:
        raise ValueError(colored(f"Invalid mode: {mode}", "red"))

def main():
    fire.Fire(main_func)

# python launch.py plan --topology ancestral_cluster_config.yaml --devices machines.yaml
if __name__ == "__main__":
    main()
