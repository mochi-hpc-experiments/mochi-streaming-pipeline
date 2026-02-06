# DeepHyper Parameter Search

This directory contains scripts for Bayesian optimization-based parameter search
using [DeepHyper](https://deephyper.readthedocs.io/).

## Installation

```bash
pip install deephyper pyyaml pandas
pip install matplotlib  # Optional, for visualizations
```

## Quick Start

```bash
# From the repository root directory
cd /path/to/mochi-streaming-pipeline

# Make sure the benchmark is built
mkdir -p build && cd build && cmake .. && make && cd ..

# Activate spack environment
spack env activate stream-env

# Run a quick test search (20 evaluations, 2 workers)
python search/search.py --max-evals 20 --num-workers 2

# Use a custom search template
python search/search.py --template search/search_template.yaml --max-evals 100

# Use a different MPI launcher (e.g., for SLURM)
python search/search.py --launcher search/launchers/srun.yaml --max-evals 50

# Use built-in launcher presets
python search/search.py --launcher-name srun --max-evals 50

# Analyze results
python search/analyze.py deephyper_results/results.csv
```

## Launcher Configuration

The MPI launcher command is configurable via a YAML file, allowing the search
to run on different platforms (local workstation, SLURM cluster, Cray systems, etc.).

### Built-in Launchers

Use `--launcher-name` to select a built-in launcher:

| Name | Command |
|------|---------|
| `mpich` | `mpiexec -n 3 {benchmark_exe} {config_file}` |

### Custom Launcher File

Create a YAML file with a `command_template` that uses placeholders:

| Placeholder | Description |
|-------------|-------------|
| `{benchmark_exe}` | Path to the benchmark executable |
| `{config_file}` | Path to the generated config YAML file |
| `{hosts}` | Comma-separated list of hostnames (from `job.dequed`) |
| `{hostfile}` | Path to temporary hostfile (from `job.dequed`) |
| `{num_hosts}` | Number of hosts allocated |

```yaml
launcher:
  # Command template with placeholders
  command_template: >-
    srun --nodelist={hosts}
    --ntasks-per-node=1
    --exclusive
    {benchmark_exe} {config_file}

  # Fallback template when no hosts are available (e.g., local testing)
  command_template_no_hosts: >-
    srun -N 3
    --ntasks-per-node=1
    {benchmark_exe} {config_file}

  # Optional: environment variables
  environment:
    OMP_NUM_THREADS: "1"
    FI_PROVIDER: "verbs"

  # Optional: working directory
  working_dir: /scratch/user/project
```

### Distributed Execution on a Cluster

To run the parameter search on a cluster, provide hostnames via `--hosts` or
`--hostfile`. Each benchmark evaluation requires 3 hosts (sender, broker,
receiver), so hosts are allocated in groups of 3.

```bash
# Using comma-separated host list (6 hosts = 2 concurrent evaluations)
python search/search.py \
    --hosts node1,node2,node3,node4,node5,node6 \
    --max-evals 50 \
    --num-workers 2

# Using a hostfile (one hostname per line)
python search/search.py \
    --hostfile /path/to/hosts.txt \
    --max-evals 100 \
    --num-workers 4

# Combined with custom launcher for the platform
python search/search.py \
    --hosts node1,node2,node3,node4,node5,node6 \
    --launcher search/launchers/mpich.yaml \
    --max-evals 50
```

**Example hostfile (`hosts.txt`):**
```
node1
node2
node3
node4
node5
node6
node7
node8
node9
```

With 9 hosts, you can run up to 3 concurrent evaluations (`--num-workers 3`).
The `{hosts}` placeholder in the launcher template is replaced with the
3 hostnames assigned to each job (e.g., `node1,node2,node3`).

Example launcher files are provided in `search/launchers/`:

| File | Platform |
|------|----------|
| `mpich.yaml` | MPICH (supports `{hosts}` for distributed evaluation) |

## Search Template Format

The search space is defined via a YAML template file with special keys:

| Key | Description | Example |
|-----|-------------|---------|
| `_categorical` | Choose from discrete options | `["opt1", "opt2", "opt3"]` |
| `_range` | Integer range (inclusive) | `[1, 16]` |
| `_ordinal` | Ordered discrete values | `[64, 128, 256, 512]` |
| (any other) | Constant value | `"tcp"` or `1073741824` |

### Example Template

```yaml
pipeline:
  total_data_bytes: 1073741824  # constant
  network_protocol: "tcp"        # constant
  thallium:
    rpc_thread_count:
      _range: [1, 8]             # parameter: integer 1-8

sender_to_broker:
  transfer_mode:
    _categorical: ["rpc_inline", "rdma_registered"]  # parameter: choice
  message_size:
    _ordinal: [65536, 262144, 1048576]               # parameter: ordered values
  messages_per_batch:
    _range: [1, 16]              # parameter: integer 1-16
```

This produces flattened parameter names like:
- `pipeline.thallium.rpc_thread_count`
- `sender_to_broker.transfer_mode`
- `sender_to_broker.message_size`
- `sender_to_broker.messages_per_batch`

## Scripts

### `search.py` - Main Search Script

Runs the Bayesian optimization search.

```bash
python search/search.py --help

Options:
  --template, -t FILE   Search template YAML (default: built-in)
  --max-evals N         Maximum evaluations (default: 100)
  --num-workers N       Parallel workers (default: 4)
  --log-dir DIR         Results directory (default: deephyper_results)
  --surrogate MODEL     Surrogate: RF, GP, ET, GBRT (default: RF)
  --initial-points N    Initial random samples (default: 10)
  --benchmark-exe PATH  Benchmark executable path
  --timeout SECS        Per-run timeout (default: 300)
  --launcher, -l FILE   Launcher YAML file for MPI configuration
  --launcher-name NAME  Built-in launcher: mpich
  --hosts HOSTLIST      Comma-separated hostnames for distributed execution
  --hostfile FILE       File with one hostname per line
  --verbose, -v         Verbose output
```

### `analyze.py` - Results Analysis

Analyzes search results and generates visualizations.

```bash
python search/analyze.py deephyper_results/results.csv --output-dir plots --top 20
```

### `problem.py` - Configurable Search Space

Provides `ConfigurableProblem` class for parsing templates:

```python
from search.problem import create_problem, create_default_problem

# From YAML file
problem = create_problem("my_template.yaml")

# Or use built-in default
problem = create_default_problem()

# Get DeepHyper problem
hp_problem = problem.get_problem()

# Generate config from a sample
config = problem.generate_config(sample_dict)
```

### `run_function.py` - Benchmark Execution

Provides `BenchmarkRunner` class for executing benchmarks:

```python
from search.problem import create_default_problem
from search.run_function import BenchmarkRunner

problem = create_default_problem()

# Use default mpirun launcher
runner = BenchmarkRunner(problem, timeout=300)

# Use a custom launcher file
runner = BenchmarkRunner(problem, timeout=300, launcher_file="launchers/srun.yaml")

# Use a built-in launcher by name
runner = BenchmarkRunner(problem, timeout=300, launcher_name="srun")

# Use with DeepHyper evaluator
result = runner(job)  # Returns {"objective": throughput, "metadata": {...}}
```

### `launcher.py` - MPI Launcher Configuration

Provides `LauncherConfig` class for platform-specific MPI commands:

```python
from search.launcher import LauncherConfig, create_launcher

# Load from YAML file
launcher = LauncherConfig.from_yaml("launchers/srun.yaml")

# Or use built-in launcher
launcher = create_launcher(launcher_name="mpirun")

# Get the command for subprocess
cmd = launcher.get_command("./build/pipeline_benchmark", "/tmp/config.yaml")
# Returns: ["mpirun", "-np", "3", "./build/pipeline_benchmark", "/tmp/config.yaml"]

# Get environment variables
env = launcher.get_environment()
```

## Default Search Space

The built-in template includes these parameters:

| Parameter | Type | Range/Values |
|-----------|------|--------------|
| `pipeline.thallium.rpc_thread_count` | range | 1 - 8 |
| `sender_to_broker.transfer_mode` | categorical | rpc_inline, rdma_direct, rdma_registered |
| `sender_to_broker.message_size` | ordinal | 64KB - 4MB |
| `sender_to_broker.messages_per_batch` | range | 1 - 16 |
| `sender_to_broker.max_concurrent_batches` | range | 1 - 16 |
| `broker.ack_timing` | categorical | ack_on_persist, ack_on_receive |
| `broker.forward_strategy` | categorical | reload_from_file, reuse_after_persist, forward_immediate, passthrough |
| `broker.concurrent_rdma_pulls` | range | 1 - 16 |
| `broker.abt_io.concurrent_writes` | range | 1 - 16 |
| `broker_to_receiver.*` | (same as sender_to_broker) | |

## Search Guidelines

| Search Type | Evaluations | Workers | Time Estimate |
|-------------|-------------|---------|---------------|
| Quick test  | 20          | 2       | ~5 min        |
| Standard    | 100         | 4       | ~15 min       |
| Thorough    | 500         | 8       | ~60 min       |

Time estimates assume ~30 second benchmark runs with 1GB data transfer.

## Output

Results are saved to the log directory:

- `results.csv` - All evaluations with parameters and objectives
- `best_config.yaml` - Best configuration as YAML (ready to use)

CSV columns:
- `p:*` - Parameter values (flattened names)
- `objective` - Throughput in GB/s
- `job_id` - Unique identifier
- `m:*` - Metadata (latency stats, status)

## Constraints

The runner automatically handles known constraints:
- **passthrough + rpc_inline**: Invalid (passthrough requires RDMA mode)

Invalid configurations return `objective = -1.0` and DeepHyper learns to avoid them.

## Example Output

```
============================================================
BEST CONFIGURATION
============================================================
Throughput: 0.1234 GB/s

Parameters:
  broker.ack_timing: ack_on_receive
  broker.abt_io.concurrent_writes: 8
  broker.forward_strategy: forward_immediate
  ...
============================================================

Best configuration as YAML:
----------------------------------------
pipeline:
  total_data_bytes: 1073741824
  network_protocol: tcp
  ...
```
