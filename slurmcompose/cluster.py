import yaml
from typing import Dict, Any, Optional
from pathlib import Path
import subprocess
import json
import os
import tempfile
import time
import datetime

##
from slurmcompose.slurm_utils import SlurmScriptGenerator


# Slurm job status codes and their meanings
status_dict = {
    "PD": "PENDING",  # Job is queued and waiting for resources
    "R": "RUNNING",  # Job is running
    "CA": "CANCELLED",  # Job was cancelled by user or system
    "F": "FAILED",  # Job completed with non-zero exit code
    "TO": "TIMEOUT",  # Job reached its time limit
    "NF": "NODE_FAIL",  # Job terminated due to node failure
    "CD": "COMPLETED",  # Job completed successfully
    "CG": "COMPLETED",  # Job is in the process of completing
    "PR": "FAILED",  # Job was terminated due to preemption
    "S": "FAILED",  # Job has been suspended
    "ST": "FAILED",  # Job has been stopped
    "BF": "FAILED",  # Job terminated due to node boot failure
    "DL": "FAILED",  # Job terminated due to deadline
    "OOM": "FAILED",  # Job terminated due to memory limit
    "NQ": "NOT_QUEUED",  # Job is not queued
    "SE": "FAILED",  # Job terminated with special exit value
    "RQ": "REQUEUED",  # Job was requeued
}


def load_config(config_path: str) -> None:
    """Load configuration from a YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class SlurmCluster:

    def __init__(
        self,
        configs={
            "vllm": "qflow/serving/configs/vllm.yaml",
            "tulusft": "qflow/serving/configs/tulusft.yaml",
            "tuludpo": "qflow/serving/configs/tuludpo.yaml",
            "tulurm": "qflow/serving/configs/tulurm.yaml",
            "llama8b": "qflow/serving/configs/llama8b.yaml",
            "gsmrm": "qflow/serving/configs/gsmrm.yaml",
            "mathrm": "qflow/serving/configs/mathrm.yaml",
            "xqe": "qflow/serving/configs/xqe.yaml",
            "euro": "qflow/serving/configs/euro.yaml",
            "llama3b": "qflow/serving/configs/llama3b.yaml",
        },
        devices_path="qflow/serving/configs/machines.yaml",
        user="gfaria",
        state_file="qflow/serving/configs/cluster_state.json",  # New parameter for state file
        account="cse",
    ):

        self.devices_specs = load_config(devices_path)
        self.script_specs = {k: load_config(path) for k, path in configs.items()}
        self.user = user
        self.active_jobs = set(self.get_job_ids())
        self.state_file = Path(state_file)
        self.my_jobs = self._load_job_state()
        self.account = account
        print("Currently there are ", len(self.my_jobs), " jobs running")
        print("---" * 20)
        print(self.get_all_job_statuses())
        self._save_job_state()

    def _load_job_state(self) -> list:
        """Load job IDs from state file if it exists."""
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    return json.load(f)

            return []
        except json.JSONDecodeError:
            print(f"Error reading state file: {self.state_file}")
            return []

    def _save_job_state(self):
        """Save current job IDs to state file."""
        try:
            with open(self.state_file, "w") as f:
                json.dump(self.my_jobs, f)
        except Exception as e:
            print(f"Error saving state file: {e}")

    def get_all_job_statuses(self):
        """Get current status for all tracked jobs."""
        status_summary = {status: [] for status in status_dict.values()}

        # Update status for each job
        for job in self.my_jobs:
            job_id = job["id"]
            status = self.check_job_status(job_id)
            if status and status != "UNKNOWN":
                status_summary[status].append(job)
            else:
                # remove from my list
                self.my_jobs.remove(job)
        # self._save_job_state()

        return status_summary

    def generate_script(
        self,
        device_name,
        script_spec,
        **args,
    ):
        device = self.devices_specs[device_name]
        script_config = self.script_specs[script_spec]

        script = SlurmScriptGenerator(
            device, script_config, account=self.account, **args
        ).generate_script()

        return script

    def submit_bash_job(self, script_content):
        try:
            # Create a directory for temporary scripts if it doesn't exist
            script_dir = "temp_scripts"
            if not os.path.exists(script_dir):
                os.makedirs(script_dir)

            # Create a semi-permanent script file
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            script_path = os.path.join(script_dir, f"job_script_{timestamp}.sh")

            # Write the script content
            with open(script_path, "w") as script_file:
                script_file.write(script_content)
                script_file.flush()

            # Make the script executable
            os.chmod(script_path, 0o755)

            # Create log files
            log_filename = f"bash_job_{timestamp}.log"
            err_filename = f"bash_job_{timestamp}.err"

            # Build the command with nohup
            cmd = f"nohup {script_path} > {log_filename} 2> {err_filename} & echo $!"

            # Execute the command and get the PID
            pid = int(os.system(cmd))

            # Optional: Set up a cleanup task for later
            # You might want to keep these scripts for debugging
            # os.unlink(script_path)  # Uncomment if you want to delete the script

            return pid

        except Exception as e:
            print(f"Error submitting job: {str(e)}")
            if "script_path" in locals():
                try:
                    os.unlink(script_path)
                except OSError:
                    pass
            return None

    def submit_slurm_job(self, script_content):
        # Write the script content to a temporary file
        # create a temporary file  with new name

        with tempfile.NamedTemporaryFile(mode="w") as temp:
            temp.write(script_content)
            temp.flush()

            # print(temp.name)
            # Submit the job using sbatch
            try:
                result = subprocess.run(
                    ["sbatch", temp.name], capture_output=True, text=True, check=True
                )
                # Extract job ID from output (typically in format "Submitted batch job 123456")
                job_id = result.stdout.strip().split()[-1]
                return job_id
            except subprocess.CalledProcessError as e:
                print(f"Error submitting job: {e.stderr}")
                return None

    def run(self, device_name, script_spec, **args):

        print(args)

        script = self.generate_script(device_name, script_spec, **args)

        # print(script)
        # print("---" * 40)
        # job_id = 1
        job_id = self.submit_bash_job(script)

        if job_id:
            self.my_jobs.append(
                {"id": job_id, "device": device_name, "script": script_spec}
            )
        self._save_job_state()

        time.sleep(5)
        return job_id

    def invoke(self, device_name, script_spec, **args):
        script = self.generate_script(device_name, script_spec, **args)

        job_id = self.submit_slurm_job(script)

        if job_id:
            self.my_jobs.append(
                {"id": job_id, "device": device_name, "script": script_spec}
            )
            self._save_job_state()

        return job_id

    def check_job_status(self, job_id):
        try:
            # First check if job is currently running using squeue
            result = subprocess.run(
                ["squeue", "-j", str(job_id), "-h", "-o", "%t"],
                capture_output=True,
                text=True,
                check=True,
            )

            # If the job is found in the queue, return its status
            if result.stdout.strip():
                code = result.stdout.strip()
                return status_dict.get(code, "UNKNOWN")

            # If job not in queue, check historical data with sacct
            # Add --starttime to look back further (e.g., 30 days)
            sacct = subprocess.run(
                [
                    "sacct",
                    "-j",
                    str(job_id),
                    "-n",
                    "-o",
                    "State",
                    "--starttime",
                    "now-30days",  # Look back 30 days
                    # Alternatively, use a specific date: "2024-01-01"
                ],
                capture_output=True,
                text=True,
            )

            code = sacct.stdout.strip()
            return status_dict.get(code, "UNKNOWN")

        except subprocess.CalledProcessError as e:
            print(f"Error checking job status: {e.stderr}")
            return None

    def get_job_ids(
        self,
    ):
        """Get job IDs for the current user."""
        try:
            # Run squeue command and capture output
            result = subprocess.run(
                ["squeue", "-u", self.user], capture_output=True, text=True
            )

            # Split output into lines
            lines = result.stdout.strip().split("\n")

            # Skip header line and extract job IDs (first column)
            job_ids = [line.split()[0] for line in lines[1:]]

            return job_ids

        except subprocess.CalledProcessError as e:
            print(f"Error running squeue: {e}")
            return []
        except Exception as e:
            print(f"Error: {e}")
            return []

    def terminate_job(self, job_id):
        # Run scancel for each job ID
        subprocess.run(["scancel", str(job_id)], check=True)
        print(f"Cancelled job {job_id}")

    def terminate(self):

        try:
            for job in self.my_jobs:
                self.terminate_job(job["id"])

            # Clear the state file after successful termination
            self.my_jobs = []
            self._save_job_state()

        except subprocess.CalledProcessError as e:
            print(f"Error cancelling jobs: {e}")
        except Exception as e:
            print(f"Error: {e}")


# Example usage:
if __name__ == "__main__":
    # Create generator instance
    generator = SlurmCluster()

    # Generate and save script
    # script = generator.invoke(device_name="l40s-standard", script_spec="vllm")

    # generator.check_job_status(23513852)

    # print(generator.active_jobs)
    print("Script generated successfully!")


"""
def get_job_ids(
        self,
    ):
        try:
            # Run squeue command and capture output
            result = subprocess.run(
                ["squeue", "-u", self.user], capture_output=True, text=True
            )

            # Split output into lines
            lines = result.stdout.strip().split("\n")

            # Skip header line and extract job IDs (first column)
            job_ids = [line.split()[0] for line in lines[1:]]

            return job_ids

        except subprocess.CalledProcessError as e:
            print(f"Error running squeue: {e}")
            return []
        except Exception as e:
            print(f"Error: {e}")
            return []
"""
