import yaml
from typing import Dict, Any, Optional
from pathlib import Path
import copy


def gpu_slurm(device_spec, script_type, account="cse") -> Dict[str, Any]:
    tag = script_type.split(".")[-1]
    return {
        "job-name": tag,
        "output": "slurm_%j.log",
        "error": "slurm_%j.err",
        "nodes": 1,
        "ntasks": 1,
        "gres": f"gpu:{device_spec['device_count']}",
        "time": f"{device_spec['hours']}:00:00",
        "mem": f"{device_spec['mem']}G",
        "partition": "gpu-" + device_spec["gpu_type"],
        "cpus-per-task": device_spec["cpus"],
        "account": account,
        "qos": "ckpt-gpu",
    }


def cpu_slurm(device_spec, script_type, account="cse") -> Dict[str, Any]:
    tag = script_type.split(".")[-1]
    return {
        "job-name": tag,
        "output": "slurm_%j.log",
        "error": "slurm_%j.err",
        "nodes": 1,
        "ntasks": 1,
        "time": f"{device_spec['hours']}:00:00",
        "mem": f"{device_spec['mem']}G",
        "partition": "ckpt-all",
        "cpus-per-task": device_spec["cpus"],
        "account": account,
        "qos": "ckpt",
    }


SLURM_SPECS = {
    "cpu": cpu_slurm,
    "gpu": gpu_slurm,
}


class SlurmScriptGenerator:
    """A class to generate SLURM batch scripts with customizable configurations."""

    def __init__(
        self,
        device_spec,
        script_config,
        env_defaults={
            "conda_path": "/gscratch/ark/graf/miniconda3/etc/profile.d/conda.sh",
            "conda_env": "quest38",
        },
        account="cse",
        **kwargs,
    ):

        # Default environment configurations
        self.env_defaults = env_defaults

        # self.all_devices = self.load_config(devices_path)
        self.script_config = copy.deepcopy(script_config)

        if "device_count" in device_spec:
            meta_args = {"DEVICE_COUNT": device_spec["device_count"], **kwargs}
        else:
            meta_args = kwargs

        inputs = {
            k: v[2:-1]
            for k, v in self.script_config["args"].items()
            if isinstance(v, str) and "${" in v
        }

        for arg, value in inputs.items():
            if value in meta_args:
                self.script_config["args"][arg] = meta_args[value]

            else:
                raise ValueError(f"Invalid input: {value}")

        script_type = self.script_config.get("script_type")

        # Default SLURM configurations
        self.slurm_defaults = SLURM_SPECS[device_spec["type"]](
            device_spec, script_type, account=account
        )

    def _generate_slurm_headers(self) -> str:
        """Generate SLURM header section."""
        headers = ["#!/bin/bash"]
        for key, value in self.slurm_defaults.items():
            headers.append(f"#SBATCH --{key}={value}")
        return "\n".join(headers)

    def _generate_env_setup(self) -> str:
        """Generate environment setup section."""
        setup = [
            "# Load required modules",
            # f"#module load cuda/{self.env_defaults['cuda_version']}",
            # f"#module load python/{self.env_defaults['python_version']}",
            "",
            "# Activate conda environment",
            f"source {self.env_defaults['conda_path']}",
            f"conda activate {self.env_defaults['conda_env']}",
        ]
        return "\n".join(setup)

    def _generate_command(self) -> str:
        """Generate the command section based on script configuration."""
        if not self.script_config:
            raise ValueError(
                "Script configuration not loaded. Please load a config file first."
            )

        script_type = self.script_config.get("script_type")
        args = self.script_config.get("args", {})

        # Convert args to command line format
        cmd_args = []
        for key, value in args.items():
            formatted_key = key.replace("_", "-")
            cmd_args.append(f"--{formatted_key}={value}")

        command = ["# Run the server", f"python -m {script_type} \\"]
        command.extend(f"{arg} \\" for arg in cmd_args)

        # Remove trailing backslash from last line
        command[-1] = command[-1].rstrip(" \\")
        return "\n".join(command)

    def generate_script(
        self,
    ) -> str:
        """Generate the complete SLURM batch script."""
        script_parts = [
            self._generate_slurm_headers(),
            "",
            self._generate_env_setup(),
            "",
            self._generate_command(),
        ]

        script = "\n".join(script_parts)

        return script


# Example usage:
if __name__ == "__main__":
    # Create generator instance

    # Generate and save script
    # script = generator.invoke(device_name="l40s-standard", script_spec="vllm")

    # generator.check_job_status(23513852)

    # print(generator.active_jobs)
    print("Script generated successfully!")
