"""
Configurable search space definition for Mochi Streaming Pipeline parameter optimization.

The search space is defined via a YAML file with special keys:
  - _categorical: [...] - categorical parameter
  - _range: [min, max] - integer range parameter
  - _ordinal: [...] - ordinal parameter (discrete ordered values)
  - Any other value is treated as a constant

Example YAML:
```yaml
pipeline:
  total_data_bytes: 1073741824  # constant
  network_protocol: "tcp"        # constant
  thallium:
    rpc_thread_count:
      _range: [1, 8]

sender_to_broker:
  transfer_mode:
    _categorical: ["rpc_inline", "rdma_direct", "rdma_registered"]
  message_size:
    _ordinal: [65536, 131072, 262144, 524288, 1048576]
```
"""

import copy
from typing import Any

import yaml
from deephyper.hpo import HpProblem


# Special keys that define parameter types
SPECIAL_KEYS = {"_categorical", "_range", "_ordinal"}


class ConfigurableProblem:
    """
    A configurable hyperparameter problem that can be loaded from a YAML template.
    """

    def __init__(self, template_path: str = None, template_dict: dict = None):
        """
        Initialize the problem from a YAML template.

        Args:
            template_path: Path to the YAML template file.
            template_dict: Template as a dictionary (alternative to file).
        """
        if template_path:
            with open(template_path, 'r') as f:
                self.template = yaml.safe_load(f)
        elif template_dict:
            self.template = template_dict
        else:
            raise ValueError("Either template_path or template_dict must be provided")

        self.problem = HpProblem()
        self.parameters = {}  # name -> parameter info
        self.constants = {}   # name -> constant value

        # Parse the template and build the problem
        self._parse_template(self.template, prefix="")

    def _parse_template(self, node: Any, prefix: str):
        """
        Recursively parse the template and register parameters.

        Args:
            node: Current node in the template (dict, list, or scalar).
            prefix: Dot-separated prefix for the current path.
        """
        if not isinstance(node, dict):
            # Scalar value - treat as constant
            self.constants[prefix] = node
            return

        # Check for special parameter definition keys
        keys = set(node.keys())
        special_found = keys & SPECIAL_KEYS

        if special_found:
            if len(special_found) > 1:
                raise ValueError(f"Multiple special keys found at '{prefix}': {special_found}")
            if len(keys) > 1:
                raise ValueError(
                    f"Special key at '{prefix}' must be the only key in its dict, "
                    f"found: {keys}"
                )

            special_key = special_found.pop()
            value = node[special_key]

            if special_key == "_categorical":
                self._add_categorical(prefix, value)
            elif special_key == "_range":
                self._add_range(prefix, value)
            elif special_key == "_ordinal":
                self._add_ordinal(prefix, value)
        else:
            # Regular dict - recurse into children
            for key, child in node.items():
                child_prefix = f"{prefix}.{key}" if prefix else key
                self._parse_template(child, child_prefix)

    def _add_categorical(self, name: str, options: list):
        """Add a categorical parameter."""
        if not isinstance(options, list) or len(options) < 2:
            raise ValueError(f"_categorical at '{name}' must be a list with at least 2 options")
        self.problem.add_hyperparameter(options, name)
        self.parameters[name] = {"type": "categorical", "options": options}

    def _add_range(self, name: str, bounds: list):
        """Add an integer range parameter."""
        if not isinstance(bounds, list) or len(bounds) != 2:
            raise ValueError(f"_range at '{name}' must be a list of [min, max]")
        low, high = bounds
        if not isinstance(low, int) or not isinstance(high, int):
            raise ValueError(f"_range at '{name}' bounds must be integers")
        self.problem.add_hyperparameter((low, high), name)
        self.parameters[name] = {"type": "range", "bounds": (low, high)}

    def _add_ordinal(self, name: str, values: list):
        """Add an ordinal parameter (discrete ordered values)."""
        if not isinstance(values, list) or len(values) < 2:
            raise ValueError(f"_ordinal at '{name}' must be a list with at least 2 values")
        self.problem.add_hyperparameter(values, name)
        self.parameters[name] = {"type": "ordinal", "values": values}

    def get_problem(self) -> HpProblem:
        """Return the DeepHyper HpProblem."""
        return self.problem

    def generate_config(self, sample: dict) -> dict:
        """
        Generate a concrete configuration dictionary from a sample.

        Args:
            sample: Dictionary of parameter values from DeepHyper.
                    Keys are the flattened parameter names.

        Returns:
            dict: Nested configuration dictionary suitable for YAML serialization.
        """
        config = {}

        # First, add all constants
        for name, value in self.constants.items():
            self._set_nested(config, name, value)

        # Then, add all sampled parameter values
        for name, value in sample.items():
            # Convert numpy types to Python types if needed
            if hasattr(value, 'item'):
                value = value.item()
            self._set_nested(config, name, value)

        return config

    def _set_nested(self, config: dict, key: str, value: Any):
        """
        Set a value in a nested dictionary using dot-separated key.

        Args:
            config: The configuration dictionary to modify.
            key: Dot-separated key (e.g., "pipeline.thallium.rpc_thread_count").
            value: The value to set.
        """
        parts = key.split(".")
        current = config

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    def __repr__(self) -> str:
        lines = ["ConfigurableProblem:"]
        lines.append(f"  Parameters ({len(self.parameters)}):")
        for name, info in sorted(self.parameters.items()):
            lines.append(f"    {name}: {info}")
        lines.append(f"  Constants ({len(self.constants)}):")
        for name, value in sorted(self.constants.items()):
            lines.append(f"    {name}: {value}")
        return "\n".join(lines)


def create_problem(template_path: str) -> ConfigurableProblem:
    """
    Create a ConfigurableProblem from a YAML template file.

    Args:
        template_path: Path to the YAML template file.

    Returns:
        ConfigurableProblem: The configured problem instance.
    """
    return ConfigurableProblem(template_path=template_path)


def create_problem_from_dict(template: dict) -> ConfigurableProblem:
    """
    Create a ConfigurableProblem from a template dictionary.

    Args:
        template: The template dictionary.

    Returns:
        ConfigurableProblem: The configured problem instance.
    """
    return ConfigurableProblem(template_dict=template)


# Default template for the pipeline benchmark
DEFAULT_TEMPLATE = {
    "pipeline": {
        "total_data_bytes": 1073741824,  # 1 GB for search
        "network_protocol": "tcp",
        "verify_checksums": False,
        "thallium": {
            "progress_thread": True,
            "rpc_thread_count": {"_range": [1, 8]},
        },
    },
    "sender_to_broker": {
        "message_size": {
            "_ordinal": [65536, 131072, 262144, 524288, 1048576, 2097152, 4194304]
        },
        "messages_per_batch": {"_range": [1, 16]},
        "max_concurrent_batches": {"_range": [1, 16]},
        "transfer_mode": {
            "_categorical": ["rpc_inline", "rdma_direct", "rdma_registered"]
        },
    },
    "broker": {
        "output_file": "/tmp/deephyper_benchmark.dat",
        "concurrent_rdma_pulls": {"_range": [1, 16]},
        "ack_timing": {"_categorical": ["ack_on_persist", "ack_on_receive"]},
        "forward_strategy": {
            "_categorical": [
                "reload_from_file",
                "reuse_after_persist",
                "forward_immediate",
                "passthrough",
            ]
        },
        "passthrough_persist": True,
        "abt_io": {
            "concurrent_writes": {"_range": [1, 16]},
        },
    },
    "broker_to_receiver": {
        "message_size": {
            "_ordinal": [65536, 131072, 262144, 524288, 1048576, 2097152, 4194304]
        },
        "messages_per_batch": {"_range": [1, 16]},
        "max_concurrent_batches": {"_range": [1, 16]},
        "transfer_mode": {
            "_categorical": ["rpc_inline", "rdma_direct", "rdma_registered"]
        },
    },
}


def create_default_problem() -> ConfigurableProblem:
    """
    Create a ConfigurableProblem with the default pipeline benchmark template.

    Returns:
        ConfigurableProblem: The configured problem instance.
    """
    return ConfigurableProblem(template_dict=DEFAULT_TEMPLATE)


if __name__ == "__main__":
    import sys

    # Demo usage
    if len(sys.argv) > 1:
        # Load from file
        problem = create_problem(sys.argv[1])
    else:
        # Use default template
        problem = create_default_problem()

    print(problem)
    print()

    # Show the DeepHyper problem
    print("DeepHyper HpProblem:")
    print("=" * 60)
    for name, hp in problem.get_problem().hyperparameters.items():
        print(f"  {name}: {hp}")
    print()

    # Demo: generate a config from a sample
    print("Example generated config from sample:")
    print("=" * 60)
    sample = {
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
    }
    config = problem.generate_config(sample)
    print(yaml.dump(config, default_flow_style=False))
