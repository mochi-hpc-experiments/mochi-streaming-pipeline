#!/usr/bin/env python3
"""
Unit tests for the DeepHyper parameter search module.

Run with: pytest tests/unit/test_search.py -v
"""

import os
import sys
import tempfile

import pytest
import yaml

# Add search directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'search'))

from problem import (
    ConfigurableProblem,
    create_problem,
    create_problem_from_dict,
    create_default_problem,
)
from launcher import (
    LauncherConfig,
    create_launcher,
    get_default_launcher,
    DEFAULT_LAUNCHERS,
)
from run_function import BenchmarkRunner


class TestConfigurableProblem:
    """Tests for ConfigurableProblem class."""

    def test_create_default_problem(self):
        """Test creating the default problem."""
        problem = create_default_problem()
        assert problem is not None
        assert len(problem.parameters) > 0
        assert len(problem.constants) > 0

    def test_parse_categorical(self):
        """Test parsing categorical parameters."""
        template = {
            "test": {
                "mode": {"_categorical": ["option1", "option2", "option3"]}
            }
        }
        problem = create_problem_from_dict(template)
        assert "test.mode" in problem.parameters
        assert problem.parameters["test.mode"]["type"] == "categorical"
        assert problem.parameters["test.mode"]["options"] == ["option1", "option2", "option3"]

    def test_parse_range(self):
        """Test parsing range parameters."""
        template = {
            "test": {
                "count": {"_range": [1, 10]}
            }
        }
        problem = create_problem_from_dict(template)
        assert "test.count" in problem.parameters
        assert problem.parameters["test.count"]["type"] == "range"
        assert problem.parameters["test.count"]["bounds"] == (1, 10)

    def test_parse_ordinal(self):
        """Test parsing ordinal parameters."""
        template = {
            "test": {
                "size": {"_ordinal": [64, 128, 256, 512]}
            }
        }
        problem = create_problem_from_dict(template)
        assert "test.size" in problem.parameters
        assert problem.parameters["test.size"]["type"] == "ordinal"
        assert problem.parameters["test.size"]["values"] == [64, 128, 256, 512]

    def test_parse_constants(self):
        """Test parsing constant values."""
        template = {
            "pipeline": {
                "total_bytes": 1000000,
                "protocol": "tcp",
                "enabled": True
            }
        }
        problem = create_problem_from_dict(template)
        assert "pipeline.total_bytes" in problem.constants
        assert "pipeline.protocol" in problem.constants
        assert "pipeline.enabled" in problem.constants
        assert problem.constants["pipeline.total_bytes"] == 1000000
        assert problem.constants["pipeline.protocol"] == "tcp"
        assert problem.constants["pipeline.enabled"] is True

    def test_parse_nested(self):
        """Test parsing nested structures."""
        template = {
            "level1": {
                "level2": {
                    "level3": {
                        "param": {"_range": [1, 5]}
                    }
                }
            }
        }
        problem = create_problem_from_dict(template)
        assert "level1.level2.level3.param" in problem.parameters

    def test_generate_config(self):
        """Test generating config from sample."""
        template = {
            "pipeline": {
                "constant_value": 42,
                "param": {"_range": [1, 10]}
            }
        }
        problem = create_problem_from_dict(template)
        sample = {"pipeline.param": 5}
        config = problem.generate_config(sample)

        assert config["pipeline"]["constant_value"] == 42
        assert config["pipeline"]["param"] == 5

    def test_generate_config_nested(self):
        """Test generating config with nested parameters."""
        template = {
            "a": {
                "b": {
                    "c": {"_categorical": ["x", "y"]}
                },
                "d": 100
            }
        }
        problem = create_problem_from_dict(template)
        sample = {"a.b.c": "y"}
        config = problem.generate_config(sample)

        assert config["a"]["b"]["c"] == "y"
        assert config["a"]["d"] == 100

    def test_create_problem_from_yaml_file(self):
        """Test creating problem from YAML file."""
        template = {
            "test": {
                "param": {"_range": [1, 5]},
                "const": "value"
            }
        }
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(template, f)
            temp_path = f.name

        try:
            problem = create_problem(temp_path)
            assert "test.param" in problem.parameters
            assert "test.const" in problem.constants
        finally:
            os.unlink(temp_path)

    def test_invalid_multiple_special_keys(self):
        """Test that multiple special keys raise an error."""
        template = {
            "test": {
                "param": {
                    "_categorical": ["a", "b"],
                    "_range": [1, 10]
                }
            }
        }
        with pytest.raises(ValueError, match="Multiple special keys"):
            create_problem_from_dict(template)

    def test_invalid_categorical_single_option(self):
        """Test that categorical with single option raises error."""
        template = {
            "test": {
                "param": {"_categorical": ["only_one"]}
            }
        }
        with pytest.raises(ValueError, match="at least 2"):
            create_problem_from_dict(template)

    def test_invalid_range_not_list(self):
        """Test that range with non-list raises error."""
        template = {
            "test": {
                "param": {"_range": 10}
            }
        }
        with pytest.raises(ValueError, match="must be a list"):
            create_problem_from_dict(template)

    def test_get_problem_returns_hp_problem(self):
        """Test that get_problem returns a DeepHyper HpProblem."""
        problem = create_default_problem()
        hp_problem = problem.get_problem()
        assert hp_problem is not None
        # Check it has the expected HpProblem interface
        assert hasattr(hp_problem, 'hyperparameter_names')
        assert hasattr(hp_problem, 'space')
        assert len(hp_problem.hyperparameter_names) > 0


class TestLauncherConfig:
    """Tests for LauncherConfig class."""

    def test_default_launcher(self):
        """Test getting default launcher."""
        launcher = get_default_launcher("mpich")
        assert launcher is not None
        assert launcher.command_template is not None

    def test_get_command_basic(self):
        """Test basic command generation."""
        launcher = LauncherConfig(
            command_template="mpiexec -n 3 {benchmark_exe} {config_file}"
        )
        cmd = launcher.get_command("./benchmark", "config.yaml")
        assert cmd == ["mpiexec", "-n", "3", "./benchmark", "config.yaml"]

    def test_get_command_with_hosts(self):
        """Test command generation with hosts."""
        launcher = LauncherConfig(
            command_template="mpiexec -n 3 -hosts {hosts} {benchmark_exe} {config_file}"
        )
        cmd = launcher.get_command("./benchmark", "config.yaml",
                                   hosts=["node1", "node2", "node3"])
        assert cmd == ["mpiexec", "-n", "3", "-hosts", "node1,node2,node3",
                       "./benchmark", "config.yaml"]

    def test_get_command_no_hosts_fallback(self):
        """Test fallback template when no hosts provided."""
        launcher = LauncherConfig(
            command_template="mpiexec -hosts {hosts} {benchmark_exe} {config_file}",
            command_template_no_hosts="mpiexec -n 3 {benchmark_exe} {config_file}"
        )
        # With hosts
        cmd_with = launcher.get_command("./benchmark", "config.yaml",
                                        hosts=["node1", "node2"])
        assert "-hosts" in cmd_with
        assert "node1,node2" in cmd_with

        # Without hosts - should use fallback
        cmd_without = launcher.get_command("./benchmark", "config.yaml")
        assert cmd_without == ["mpiexec", "-n", "3", "./benchmark", "config.yaml"]

    def test_get_command_with_hostfile(self):
        """Test command generation with hostfile."""
        launcher = LauncherConfig(
            command_template="mpiexec --hostfile {hostfile} {benchmark_exe} {config_file}"
        )
        cmd = launcher.get_command("./benchmark", "config.yaml",
                                   hostfile="/tmp/hosts.txt")
        assert cmd == ["mpiexec", "--hostfile", "/tmp/hosts.txt",
                       "./benchmark", "config.yaml"]

    def test_get_command_with_num_hosts(self):
        """Test command generation with num_hosts placeholder."""
        launcher = LauncherConfig(
            command_template="mpiexec -n {num_hosts} {benchmark_exe} {config_file}"
        )
        cmd = launcher.get_command("./benchmark", "config.yaml",
                                   hosts=["n1", "n2", "n3"])
        assert cmd == ["mpiexec", "-n", "3", "./benchmark", "config.yaml"]

    def test_get_environment(self):
        """Test environment variable handling."""
        launcher = LauncherConfig(
            command_template="{benchmark_exe} {config_file}",
            environment={"MY_VAR": "value", "OTHER": "123"}
        )
        env = launcher.get_environment()
        assert env is not None
        assert env["MY_VAR"] == "value"
        assert env["OTHER"] == "123"
        # Should also contain existing environment
        assert "PATH" in env

    def test_get_environment_empty(self):
        """Test that empty environment returns None."""
        launcher = LauncherConfig(
            command_template="{benchmark_exe} {config_file}"
        )
        env = launcher.get_environment()
        assert env is None

    def test_from_dict(self):
        """Test creating launcher from dict."""
        data = {
            "launcher": {
                "command_template": "srun {benchmark_exe} {config_file}",
                "environment": {"VAR": "val"}
            }
        }
        launcher = LauncherConfig.from_dict(data)
        assert "srun" in launcher.command_template
        assert launcher.environment["VAR"] == "val"

    def test_from_yaml_file(self):
        """Test loading launcher from YAML file."""
        data = {
            "launcher": {
                "command_template": "mpirun -np 3 {benchmark_exe} {config_file}",
                "command_template_no_hosts": "mpirun -np 3 {benchmark_exe} {config_file}"
            }
        }
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(data, f)
            temp_path = f.name

        try:
            launcher = LauncherConfig.from_yaml(temp_path)
            assert "mpirun" in launcher.command_template
        finally:
            os.unlink(temp_path)

    def test_create_launcher_from_file(self):
        """Test create_launcher with file path."""
        data = {"launcher": {"command_template": "test {benchmark_exe} {config_file}"}}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(data, f)
            temp_path = f.name

        try:
            launcher = create_launcher(launcher_file=temp_path)
            assert "test" in launcher.command_template
        finally:
            os.unlink(temp_path)

    def test_create_launcher_from_name(self):
        """Test create_launcher with launcher name."""
        launcher = create_launcher(launcher_name="mpich")
        assert launcher is not None

    def test_create_launcher_default(self):
        """Test create_launcher with no arguments."""
        launcher = create_launcher()
        assert launcher is not None
        assert launcher.command_template is not None


class TestBenchmarkRunner:
    """Tests for BenchmarkRunner class."""

    def test_validate_config_valid(self):
        """Test validation passes for valid config."""
        problem = create_default_problem()
        runner = BenchmarkRunner(problem)

        params = {
            "broker.forward_strategy": "forward_immediate",
            "sender_to_broker.transfer_mode": "rpc_inline"
        }
        result = runner._validate_config(params)
        assert result is None  # None means valid

    def test_validate_config_passthrough_rpc_inline(self):
        """Test validation fails for passthrough with rpc_inline."""
        problem = create_default_problem()
        runner = BenchmarkRunner(problem)

        params = {
            "broker.forward_strategy": "passthrough",
            "sender_to_broker.transfer_mode": "rpc_inline"
        }
        result = runner._validate_config(params)
        assert result is not None
        assert result["objective"] == -1.0
        assert result["metadata"]["status"] == "invalid_config"

    def test_validate_config_passthrough_rdma_ok(self):
        """Test validation passes for passthrough with RDMA."""
        problem = create_default_problem()
        runner = BenchmarkRunner(problem)

        params = {
            "broker.forward_strategy": "passthrough",
            "sender_to_broker.transfer_mode": "rdma_direct"
        }
        result = runner._validate_config(params)
        assert result is None  # Valid

    def test_extract_hosts_none(self):
        """Test _extract_hosts with no dequed attribute."""
        problem = create_default_problem()
        runner = BenchmarkRunner(problem)

        class MockJob:
            pass

        job = MockJob()
        hosts, hostfile = runner._extract_hosts(job)
        assert hosts is None
        assert hostfile is None

    def test_extract_hosts_list_of_strings(self):
        """Test _extract_hosts with list of strings."""
        problem = create_default_problem()
        runner = BenchmarkRunner(problem)

        class MockJob:
            dequed = ["node1", "node2", "node3"]

        job = MockJob()
        hosts, hostfile = runner._extract_hosts(job)
        assert hosts == ["node1", "node2", "node3"]
        assert hostfile is not None
        assert os.path.exists(hostfile)

        # Cleanup
        os.unlink(hostfile)

    def test_extract_hosts_list_of_tuples(self):
        """Test _extract_hosts with list of tuples."""
        problem = create_default_problem()
        runner = BenchmarkRunner(problem)

        class MockJob:
            dequed = [("node1", "extra"), ("node2", "data")]

        job = MockJob()
        hosts, hostfile = runner._extract_hosts(job)
        assert hosts == ["node1", "node2"]

        # Cleanup
        if hostfile:
            os.unlink(hostfile)

    def test_extract_hosts_list_of_dicts(self):
        """Test _extract_hosts with list of dicts."""
        problem = create_default_problem()
        runner = BenchmarkRunner(problem)

        class MockJob:
            dequed = [{"hostname": "node1"}, {"host": "node2"}, {"node": "node3"}]

        job = MockJob()
        hosts, hostfile = runner._extract_hosts(job)
        assert hosts == ["node1", "node2", "node3"]

        # Cleanup
        if hostfile:
            os.unlink(hostfile)

    def test_runner_with_custom_launcher(self):
        """Test BenchmarkRunner with custom launcher."""
        problem = create_default_problem()
        launcher = LauncherConfig(
            command_template="custom_mpi {benchmark_exe} {config_file}"
        )
        runner = BenchmarkRunner(problem, launcher=launcher)
        assert runner.launcher.command_template == "custom_mpi {benchmark_exe} {config_file}"


class TestSearchHelpers:
    """Tests for search.py helper functions."""

    def test_parse_hosts_from_string(self):
        """Test parsing hosts from comma-separated string."""
        # Import here to avoid issues if search.py has import errors
        from search import parse_hosts

        hosts = parse_hosts("node1,node2,node3", None)
        assert hosts == ["node1", "node2", "node3"]

    def test_parse_hosts_from_string_with_spaces(self):
        """Test parsing hosts handles whitespace."""
        from search import parse_hosts

        hosts = parse_hosts("node1, node2 , node3", None)
        assert hosts == ["node1", "node2", "node3"]

    def test_parse_hosts_from_file(self):
        """Test parsing hosts from file."""
        from search import parse_hosts

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("node1\nnode2\nnode3\n")
            temp_path = f.name

        try:
            hosts = parse_hosts(None, temp_path)
            assert hosts == ["node1", "node2", "node3"]
        finally:
            os.unlink(temp_path)

    def test_parse_hosts_none(self):
        """Test parse_hosts returns None when no args."""
        from search import parse_hosts

        hosts = parse_hosts(None, None)
        assert hosts is None

    def test_create_host_queue(self):
        """Test creating host queue from list."""
        from search import create_host_queue, HOSTS_PER_JOB

        hosts = ["n1", "n2", "n3", "n4", "n5", "n6"]
        queue = create_host_queue(hosts)

        # Should have 2 groups of 3
        assert queue is not None
        assert len(queue) == 2
        assert len(queue[0]) == HOSTS_PER_JOB
        assert queue[0] == ["n1", "n2", "n3"]
        assert queue[1] == ["n4", "n5", "n6"]

    def test_create_host_queue_partial(self):
        """Test host queue with partial group (should be ignored)."""
        from search import create_host_queue

        # 7 hosts = 2 complete groups, 1 host unused
        hosts = ["n1", "n2", "n3", "n4", "n5", "n6", "n7"]
        queue = create_host_queue(hosts)

        # Should still work, just with 2 groups
        assert len(queue) == 2
        assert queue[0] == ["n1", "n2", "n3"]
        assert queue[1] == ["n4", "n5", "n6"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
