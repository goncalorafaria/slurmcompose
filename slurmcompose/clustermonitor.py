import threading
import time
from typing import Dict, List, Optional
from dataclasses import dataclass

##
from slurmcompose.cluster import SlurmCluster


@dataclass(frozen=True)
class ClusterConfig:
    """Configuration for a specific cluster setup"""

    device_name: str
    script_spec: str
    target_instances: int

    def __str__(self):
        return f"{self.device_name}-{self.script_spec}({self.target_instances})"

    def __hash__(self):
        return hash((self.device_name, self.script_spec, self.target_instances))

    def key(self):
        return f"{self.device_name}-{self.script_spec}"

    def __eq__(self, other):
        if not isinstance(other, ClusterConfig):
            return False
        return (
            self.device_name == other.device_name
            and self.script_spec == other.script_spec
            and self.target_instances == other.target_instances
        )


class ClusterStateMonitor(threading.Thread):
    """
    A thread that maintains multiple desired states of running SLURM jobs.
    """

    def __init__(self, cluster, configs: List[Dict], check_interval: int = 60):
        super().__init__()
        self.cluster = cluster
        self.configs = [
            ClusterConfig(
                device_name=cfg["device_name"],
                script_spec=cfg["script_spec"],
                target_instances=cfg["target_instances"],
            )
            for cfg in configs
        ]
        self.check_interval = check_interval
        self._stop_event = threading.Event()

    def stop(self):
        """Signal the thread to stop."""
        self._stop_event.set()

    def run(self):
        """Main thread loop that maintains the desired cluster states."""
        while not self._stop_event.is_set():
            try:
                self._maintain_steady_state()
                time.sleep(self.check_interval)
            except Exception as e:
                print(f"Error in cluster monitor: {e}")
                time.sleep(self.check_interval)

    def _get_job_key(self, job) -> Optional[str]:
        """
        Determine which configuration a job belongs to based on its SLURM metadata.
        Returns a tuple of (device_name, script_spec) or None if not found.
        """
        try:
            # You'll need to implement this based on how you store job metadata
            # This could involve parsing the job name or checking a mapping file
            return f'{job["device"]}-{job["script"]}'
        except Exception as e:
            print(f"Error determining job config: {e}")
            return None

    def _categorize_jobs(self, status_summary: Dict) -> Dict[ClusterConfig, List[str]]:
        """Categorize active jobs by their configurations."""
        active_jobs = {cfg.key(): [] for cfg in self.configs}
        active_jobs["wild"] = []
        for status in ["PENDING", "RUNNING"]:
            for job in status_summary.get(status, []):
                # Here you might want to add logic to determine which config
                # a job belongs to based on its metadata
                # pse
                key = self._get_job_key(job)
                if key in active_jobs:
                    active_jobs[key].append(job.get("id"))
                else:
                    active_jobs["wild"].append(job.get("id"))

        return active_jobs

    def _maintain_steady_state(self):
        """Check current state and launch/terminate jobs as needed for each configuration."""
        status_summary = self.cluster.get_all_job_statuses()
        active_jobs = self._categorize_jobs(status_summary)

        print(active_jobs)
        # Process each configuration
        for cfg in self.configs:
            current_count = (
                len(active_jobs[cfg.key()]) if cfg.key() in active_jobs else 0
            )
            jobs_to_launch = cfg.target_instances - current_count

            print(f"\nConfiguration {cfg}:")
            print(
                f"Current active jobs: {current_count}, Target: {cfg.target_instances}"
            )

            # Launch new jobs if needed
            if jobs_to_launch > 0:
                print(f"Launching {jobs_to_launch} new instances...")
                for _ in range(jobs_to_launch):
                    job_id = self.cluster.invoke(cfg.device_name, cfg.script_spec)
                    if job_id:
                        print(f"Launched new job with ID: {job_id}")
                    else:
                        print("Failed to launch new job")

            if jobs_to_launch < 0:
                print(f"Terminating {abs(jobs_to_launch)} excess instances...")
                for job_id in active_jobs[cfg.key()][: abs(jobs_to_launch)]:
                    self.cluster.terminate_job(job_id)

        for job_id in active_jobs["wild"]:
            print(f"Terminating unknown job {job_id}")
            self.cluster.terminate_job(job_id)

        # Clean up completed/failed jobs
        for status in ["FAILED", "COMPLETED", "TIMEOUT", "CANCELLED"]:
            for job_id in status_summary.get(status, []):
                print(f"Removing finished job {job_id} with status {status}")
                if job_id in self.cluster.my_jobs:
                    self.cluster.my_jobs.remove(job_id)

        # Save updated state
        self.cluster._save_job_state()


if __name__ == "__main__":
    cluster = Cluster()  # Initialize with your config paths
    # cluster.terminate()
    # import pdb

    # pdb.set_trace()
    # Define multiple configurations
    configurations = [
        {"device_name": "l40s-2", "script_spec": "vllm", "target_instances": 2},
        # {"device_name": "l40s-2", "script_spec": "other_script", "target_instances": 2},
        # {"device_name": "a100", "script_spec": "vllm", "target_instances": 1},
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
