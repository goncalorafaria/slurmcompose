import time
import pprint
from termcolor import colored
import asyncio
from literegistry import RegistryClient, FileSystemKVStore


##
from slurmcompose.cluster import SlurmCluster
from slurmcompose.clustermonitor import ClusterStateMonitor
from slurmcompose.slurm_utils import SlurmScriptGenerator


def check_registry(registry, verbose=False):

    pp = pprint.PrettyPrinter(indent=1, compact=True)

    for k, v in asyncio.run(registry.models()).items():
        print(f"{colored(k, 'red')}")
        for item in v:
            print(colored("--" * 20, "blue"))
            for key, value in item.items():

                print(f"\t{colored(key, 'green')}:{value}")


def check_summary(registry):

    for k, v in asyncio.run(registry.models()).items():
        print(f"{colored(k, 'red')} :{colored(len(v),'green')}")


def terminate_cluster(account="cse", user="graf"):

    cluster = SlurmCluster(
        account=account,
        configs={
            "gsmrm": "example_configs/gsmrm.yaml",
        },
        devices_path="example_configs/machines.yaml",
        state_file="example_configs/cluster_state.json",
        user=user,
        account=account,
    )  # Initialize with your config paths

    cluster.terminate()


#
def launch_cluster(account="cse", user="graf"):

    cluster = SlurmCluster(
        account=account,
        configs={
            "gsmrm": "example_configs/configs/gsmrm.yaml",
        },
        devices_path="example_configs//machines.yaml",
        state_file="example_configs//cluster_state.json",
        user=user,
        account=account,
    )  # Initialize with your config paths

    # Define multiple configurations
    configurations = [
        {"device_name": "a40-4", "script_spec": "gsmrm", "target_instances": 16},
        {"device_name": "l40-4", "script_spec": "gsmrm", "target_instances": 8},
        {"device_name": "l40s-4", "script_spec": "gsmrm", "target_instances": 16},
    ]

    # Create and start the monitor
    monitor = ClusterStateMonitor(
        cluster, configs=configurations, check_interval=60  # Check every minute
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

        print("Monitor stopped successfully")


def main(
    mode: str = "registry",
    account="cse",
    user="graf",
    registry_path="/gscratch/ark/graf/registry",
):
    if mode == "cluster":
        launch_cluster(account=account, user=user)
    elif mode == "registry":
        registry = RegistryClient(FileSystemKVStore(registry_path))
        check_registry(registry)
    elif mode == "summary":
        registry = RegistryClient(FileSystemKVStore(registry_path))
        check_summary(registry)
    elif mode == "terminate":
        terminate_cluster(account=account, user=user)
    else:
        raise ValueError("Invalid mode")


if __name__ == "__main__":
    import fire

    fire.Fire(main)
