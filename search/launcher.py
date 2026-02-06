"""
Launcher configuration for running MPI benchmarks on different platforms.

The launcher is configured via a YAML file with the following format:

```yaml
launcher:
  # Command template with placeholders:
  #   {benchmark_exe} - path to benchmark executable
  #   {config_file}   - path to generated config YAML file
  #   {hosts}         - comma-separated list of hostnames (from dequed)
  #   {hostfile}      - path to temporary hostfile (from dequed)
  #   {num_hosts}     - number of hosts
  command_template: "mpirun -np 3 -H {hosts} {benchmark_exe} {config_file}"

  # Optional: template to use when hosts are NOT available (fallback)
  # If not specified, {hosts} and {hostfile} are replaced with empty strings
  command_template_no_hosts: "mpirun -np 3 {benchmark_exe} {config_file}"

  # Optional: environment variables to set
  environment:
    OMP_NUM_THREADS: "1"

  # Optional: working directory (default: current directory)
  working_dir: null
```

Alternative structured format (command_template takes precedence if both present):

```yaml
launcher:
  executable: "mpirun"
  args:
    - "-np"
    - "3"
  environment:
    OMP_NUM_THREADS: "1"
```
"""

import os
import shlex
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import yaml


@dataclass
class LauncherConfig:
    """Configuration for the MPI launcher."""

    # Command template with {benchmark_exe}, {config_file}, {hosts}, {hostfile}, {num_hosts} placeholders
    command_template: Optional[str] = None

    # Fallback template when hosts are not available
    command_template_no_hosts: Optional[str] = None

    # Alternative: structured command specification
    executable: Optional[str] = None
    args: List[str] = field(default_factory=list)

    # Environment variables to set
    environment: Dict[str, str] = field(default_factory=dict)

    # Working directory
    working_dir: Optional[str] = None

    def get_command(self, benchmark_exe: str, config_file: str,
                    hosts: Optional[List[str]] = None,
                    hostfile: Optional[str] = None) -> List[str]:
        """
        Build the command list for subprocess.

        Args:
            benchmark_exe: Path to the benchmark executable.
            config_file: Path to the configuration YAML file.
            hosts: Optional list of hostnames (from dequed).
            hostfile: Optional path to a hostfile.

        Returns:
            List of command arguments suitable for subprocess.run().
        """
        if self.command_template:
            # Determine which template to use
            has_hosts = hosts and len(hosts) > 0

            if has_hosts:
                template = self.command_template
            elif self.command_template_no_hosts:
                template = self.command_template_no_hosts
            else:
                template = self.command_template

            # Build substitution values
            hosts_str = ",".join(hosts) if hosts else ""
            hostfile_str = hostfile if hostfile else ""
            num_hosts = len(hosts) if hosts else 0

            # Use template-based command
            cmd_str = template.format(
                benchmark_exe=benchmark_exe,
                config_file=config_file,
                hosts=hosts_str,
                hostfile=hostfile_str,
                num_hosts=num_hosts
            )
            return shlex.split(cmd_str)
        elif self.executable:
            # Use structured command
            cmd = [self.executable] + self.args + [benchmark_exe, config_file]
            return cmd
        else:
            raise ValueError("Either command_template or executable must be specified")

    def get_environment(self) -> Optional[Dict[str, str]]:
        """
        Get environment variables for the subprocess.

        Returns:
            Dictionary of environment variables, or None if no custom env.
        """
        if not self.environment:
            return None

        # Start with current environment and overlay custom vars
        env = os.environ.copy()
        env.update(self.environment)
        return env

    @classmethod
    def from_dict(cls, data: dict) -> "LauncherConfig":
        """Create LauncherConfig from a dictionary."""
        launcher_data = data.get("launcher", data)

        return cls(
            command_template=launcher_data.get("command_template"),
            command_template_no_hosts=launcher_data.get("command_template_no_hosts"),
            executable=launcher_data.get("executable"),
            args=launcher_data.get("args", []),
            environment=launcher_data.get("environment", {}),
            working_dir=launcher_data.get("working_dir"),
        )

    @classmethod
    def from_yaml(cls, path: str) -> "LauncherConfig":
        """Load LauncherConfig from a YAML file."""
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)


# Default launcher configurations for common platforms
DEFAULT_LAUNCHERS = {
    "mpich": LauncherConfig(
        command_template="mpiexec -n 3 -hosts {hosts} {benchmark_exe} {config_file}",
        command_template_no_hosts="mpiexec -n 3 {benchmark_exe} {config_file}"
    ),
}


def get_default_launcher(name: str = "mpich") -> LauncherConfig:
    """
    Get a default launcher configuration by name.

    Args:
        name: Launcher name ("mpirun", "mpiexec", "srun", "local").

    Returns:
        LauncherConfig for the specified launcher.

    Raises:
        ValueError: If the launcher name is not recognized.
    """
    if name not in DEFAULT_LAUNCHERS:
        available = ", ".join(DEFAULT_LAUNCHERS.keys())
        raise ValueError(f"Unknown launcher '{name}'. Available: {available}")
    return DEFAULT_LAUNCHERS[name]


def create_launcher(
    launcher_file: Optional[str] = None,
    launcher_name: Optional[str] = None
) -> LauncherConfig:
    """
    Create a launcher configuration from file or by name.

    Args:
        launcher_file: Path to launcher YAML file (takes precedence).
        launcher_name: Name of default launcher ("mpirun", "srun", etc.).

    Returns:
        LauncherConfig instance.
    """
    if launcher_file:
        return LauncherConfig.from_yaml(launcher_file)
    elif launcher_name:
        return get_default_launcher(launcher_name)
    else:
        return get_default_launcher("mpich")


if __name__ == "__main__":
    import sys

    # Demo usage
    if len(sys.argv) > 1:
        config = LauncherConfig.from_yaml(sys.argv[1])
    else:
        config = get_default_launcher("mpich")

    print("Launcher Configuration:")
    print(f"  command_template: {config.command_template}")
    print(f"  command_template_no_hosts: {config.command_template_no_hosts}")
    print(f"  executable: {config.executable}")
    print(f"  args: {config.args}")
    print(f"  environment: {config.environment}")
    print(f"  working_dir: {config.working_dir}")
    print()

    # Demo command generation without hosts
    cmd = config.get_command("./build/pipeline_benchmark", "/tmp/config.yaml")
    print(f"Generated command (no hosts): {cmd}")
    print(f"As string: {' '.join(cmd)}")
    print()

    # Demo command generation with hosts
    hosts = ["node1", "node2", "node3"]
    cmd = config.get_command("./build/pipeline_benchmark", "/tmp/config.yaml", hosts=hosts)
    print(f"Generated command (with hosts): {cmd}")
    print(f"As string: {' '.join(cmd)}")
