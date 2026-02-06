"""
Run function for executing the Mochi Streaming Pipeline benchmark.

This module provides the interface between DeepHyper and the benchmark executable.
"""

import subprocess
import tempfile
import os
import re
from typing import List, Optional

import yaml

from problem import ConfigurableProblem
from launcher import LauncherConfig, create_launcher


# Path to the benchmark executable (relative to repository root)
BENCHMARK_EXE = "./build/pipeline_benchmark"

# Timeout for benchmark execution (seconds)
BENCHMARK_TIMEOUT = 300  # 5 minutes


class BenchmarkRunner:
    """
    Runner class that executes the benchmark with configurations generated
    from a ConfigurableProblem.
    """

    def __init__(self, problem: ConfigurableProblem, benchmark_exe: str = None,
                 timeout: int = None, launcher: LauncherConfig = None,
                 launcher_file: str = None, launcher_name: str = None):
        """
        Initialize the benchmark runner.

        Args:
            problem: ConfigurableProblem instance for generating configs.
            benchmark_exe: Path to benchmark executable.
            timeout: Timeout in seconds for benchmark execution.
            launcher: LauncherConfig instance (takes precedence).
            launcher_file: Path to launcher YAML file.
            launcher_name: Name of default launcher ("mpirun", "srun", etc.).
        """
        self.problem = problem
        self.benchmark_exe = benchmark_exe or BENCHMARK_EXE
        self.timeout = timeout or BENCHMARK_TIMEOUT

        # Set up launcher configuration
        if launcher:
            self.launcher = launcher
        else:
            self.launcher = create_launcher(launcher_file, launcher_name)

    def __call__(self, job):
        """
        Run the benchmark with the given job parameters.

        Args:
            job: DeepHyper job object containing parameters.

        Returns:
            dict: Dictionary with "objective" (throughput in GB/s) and "metadata".
        """
        return self.run(job)

    def run(self, job):
        """
        Run the pipeline benchmark with given hyperparameters.

        Args:
            job: DeepHyper job object containing parameters.
                 May have optional 'dequed' attribute with allocated hostnames.

        Returns:
            dict: Dictionary with "objective" (throughput in GB/s) and "metadata".
        """
        params = job.parameters

        # Check for invalid configurations
        validation_result = self._validate_config(params)
        if validation_result is not None:
            return validation_result

        # Generate configuration from the sample
        config = self.problem.generate_config(params)

        # Ensure output file is unique per job
        if "broker" in config and "output_file" in config["broker"]:
            config["broker"]["output_file"] = f"/tmp/deephyper_bench_{job.id}.dat"

        # Write config to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_path = f.name

        output_file = config.get("broker", {}).get("output_file", "/tmp/deephyper_bench.dat")

        # Extract hosts from dequed if available
        hosts, hostfile_path = self._extract_hosts(job)

        try:
            # Build command using launcher configuration
            cmd = self.launcher.get_command(
                self.benchmark_exe, config_path,
                hosts=hosts, hostfile=hostfile_path
            )
            env = self.launcher.get_environment()
            cwd = self.launcher.working_dir

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                env=env,
                cwd=cwd
            )

            # Parse throughput from output
            throughput = parse_throughput(result.stdout)
            latency_stats = parse_latency(result.stdout)

            if throughput is None:
                return {
                    "objective": -1.0,
                    "metadata": {
                        "status": "parse_error",
                        "stdout": result.stdout[-1000:] if result.stdout else "",
                        "stderr": result.stderr[-500:] if result.stderr else "",
                        "exit_code": result.returncode
                    }
                }

            metadata = {
                "status": "success",
                "exit_code": result.returncode,
                "throughput_gbps": throughput,
            }
            metadata.update(latency_stats)

            return {
                "objective": throughput,
                "metadata": metadata
            }

        except subprocess.TimeoutExpired:
            return {
                "objective": -1.0,
                "metadata": {"status": "timeout", "timeout_seconds": self.timeout}
            }
        except Exception as e:
            return {
                "objective": -1.0,
                "metadata": {"status": "error", "error": str(e)}
            }
        finally:
            # Cleanup temporary files
            if os.path.exists(config_path):
                os.unlink(config_path)
            if os.path.exists(output_file):
                os.unlink(output_file)
            if hostfile_path and os.path.exists(hostfile_path):
                os.unlink(hostfile_path)

    def _extract_hosts(self, job) -> tuple:
        """
        Extract hostnames from job.dequed if available.

        The dequed attribute is set by DeepHyper when using distributed
        evaluation with resource queues. It contains the resources
        (typically hostnames) allocated for this job.

        Args:
            job: DeepHyper job object.

        Returns:
            tuple: (hosts, hostfile_path) where:
                - hosts: List of hostname strings, or None
                - hostfile_path: Path to temporary hostfile, or None
        """
        dequed = getattr(job, 'dequed', None)

        if not dequed:
            return None, None

        # Extract hostnames from dequed
        # dequed can be:
        # - A list of strings (hostnames)
        # - A list of tuples/lists where first element is hostname
        # - A list of dicts with 'hostname' or 'host' key
        hosts = []

        for item in dequed:
            if isinstance(item, str):
                hosts.append(item)
            elif isinstance(item, (list, tuple)) and len(item) > 0:
                hosts.append(str(item[0]))
            elif isinstance(item, dict):
                hostname = item.get('hostname') or item.get('host') or item.get('node')
                if hostname:
                    hosts.append(str(hostname))

        if not hosts:
            return None, None

        # Create a temporary hostfile
        hostfile_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.hostfile',
                                             delete=False) as f:
                for host in hosts:
                    f.write(f"{host}\n")
                hostfile_path = f.name
        except Exception:
            hostfile_path = None

        return hosts, hostfile_path

    def _validate_config(self, params):
        """
        Validate the configuration for known constraints.

        Returns None if valid, or an error result dict if invalid.
        """
        # Constraint: passthrough requires RDMA mode
        forward_strategy = params.get("broker.forward_strategy")
        sender_mode = params.get("sender_to_broker.transfer_mode")

        if forward_strategy == "passthrough" and sender_mode == "rpc_inline":
            return {
                "objective": -1.0,
                "metadata": {
                    "status": "invalid_config",
                    "reason": "passthrough strategy requires RDMA transfer mode"
                }
            }

        return None


def parse_throughput(output):
    """
    Parse throughput from benchmark output.

    Expected format: "Throughput:           X.XX GB/s"

    Args:
        output: Benchmark stdout string.

    Returns:
        float or None: Throughput in GB/s, or None if not found.
    """
    if not output:
        return None
    match = re.search(r"Throughput:\s+([\d.]+)\s+GB/s", output)
    if match:
        return float(match.group(1))
    return None


def parse_latency(output):
    """
    Parse latency statistics from benchmark output.

    Args:
        output: Benchmark stdout string.

    Returns:
        dict: Dictionary with latency statistics (may be empty).
    """
    stats = {}
    if not output:
        return stats

    patterns = [
        (r"Avg batch latency:\s+([\d.]+)\s+ms", "latency_avg_ms"),
        (r"Min batch latency:\s+([\d.]+)\s+ms", "latency_min_ms"),
        (r"Max batch latency:\s+([\d.]+)\s+ms", "latency_max_ms"),
        (r"Stddev:\s+([\d.]+)\s+ms", "latency_stddev_ms"),
        (r"Total time:\s+([\d.]+)\s+(s|ms)", "total_time"),
    ]

    for pattern, key in patterns:
        match = re.search(pattern, output)
        if match:
            value = float(match.group(1))
            # Convert to consistent units if needed
            if key == "total_time" and match.group(2) == "ms":
                value /= 1000.0  # Convert ms to seconds
            stats[key] = value

    return stats


def create_run_function(problem: ConfigurableProblem, benchmark_exe: str = None,
                        timeout: int = None, launcher: LauncherConfig = None,
                        launcher_file: str = None, launcher_name: str = None):
    """
    Create a run function for DeepHyper from a ConfigurableProblem.

    Args:
        problem: ConfigurableProblem instance.
        benchmark_exe: Path to benchmark executable.
        timeout: Timeout in seconds.
        launcher: LauncherConfig instance.
        launcher_file: Path to launcher YAML file.
        launcher_name: Name of default launcher.

    Returns:
        callable: Run function suitable for DeepHyper evaluator.
    """
    runner = BenchmarkRunner(problem, benchmark_exe, timeout,
                             launcher, launcher_file, launcher_name)
    return runner


if __name__ == "__main__":
    # Test the runner with the default problem
    from problem import create_default_problem
    from collections import namedtuple

    problem = create_default_problem()

    # Create a mock job object
    MockJob = namedtuple("MockJob", ["parameters", "id"])
    job = MockJob(
        parameters={
            "pipeline.thallium.rpc_thread_count": 4,
            "sender_to_broker.message_size": 1048576,
            "sender_to_broker.messages_per_batch": 4,
            "sender_to_broker.max_concurrent_batches": 4,
            "sender_to_broker.transfer_mode": "rdma_registered",
            "broker.concurrent_rdma_pulls": 4,
            "broker.ack_timing": "ack_on_receive",
            "broker.forward_strategy": "forward_immediate",
            "broker.abt_io.concurrent_writes": 4,
            "broker_to_receiver.message_size": 1048576,
            "broker_to_receiver.messages_per_batch": 4,
            "broker_to_receiver.max_concurrent_batches": 4,
            "broker_to_receiver.transfer_mode": "rdma_registered",
        },
        id="test_001"
    )

    # Generate and print the config
    config = problem.generate_config(job.parameters)
    print("Generated configuration:")
    print(yaml.dump(config, default_flow_style=False))

    # Test validation
    runner = BenchmarkRunner(problem)

    # Test invalid config (passthrough with rpc_inline)
    invalid_job = MockJob(
        parameters={
            **job.parameters,
            "broker.forward_strategy": "passthrough",
            "sender_to_broker.transfer_mode": "rpc_inline",
        },
        id="test_invalid"
    )
    result = runner._validate_config(invalid_job.parameters)
    print("\nValidation result for invalid config:")
    print(result)
