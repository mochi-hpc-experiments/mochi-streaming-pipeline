#!/usr/bin/env python3
"""
DeepHyper parameter search for Mochi Streaming Pipeline.

This script runs Bayesian optimization to find the best parameter configuration
for maximizing throughput of the streaming pipeline.

Usage:
    python search.py --template search_template.yaml --max-evals 100 --num-workers 4

Examples:
    # Quick test run with default template
    python search.py --max-evals 20 --num-workers 2

    # Use custom template
    python search.py --template my_template.yaml --max-evals 100

    # Thorough search
    python search.py --max-evals 500 --num-workers 8
"""

import argparse
import os
import sys
from typing import List, Optional

import yaml
from deephyper.hpo import CBO
from deephyper.evaluator import Evaluator, queued, LokyEvaluator
from deephyper.evaluator.callback import TqdmCallback

from problem import ConfigurableProblem, create_problem, create_default_problem
from run_function import BenchmarkRunner
from launcher import create_launcher, DEFAULT_LAUNCHERS


# Number of hosts required per benchmark run (sender, broker, receiver)
HOSTS_PER_JOB = 3


def parse_hosts(hosts_arg: Optional[str], hostfile_arg: Optional[str]) -> Optional[List[str]]:
    """
    Parse hostnames from command-line arguments.

    Args:
        hosts_arg: Comma-separated list of hostnames.
        hostfile_arg: Path to file with one hostname per line.

    Returns:
        List of hostnames, or None if neither argument provided.
    """
    if hosts_arg:
        return [h.strip() for h in hosts_arg.split(",") if h.strip()]
    elif hostfile_arg:
        if not os.path.exists(hostfile_arg):
            print(f"Error: Hostfile not found: {hostfile_arg}", file=sys.stderr)
            sys.exit(1)
        with open(hostfile_arg, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    return None


def create_host_queue(hosts: List[str], hosts_per_job: int = HOSTS_PER_JOB) -> List[List[str]]:
    """
    Create a list of host groups from a list of hosts.

    The hosts are grouped into chunks of `hosts_per_job` (default 3),
    where each chunk represents the resources for one benchmark evaluation
    (sender, broker, receiver).

    Args:
        hosts: List of all available hostnames.
        hosts_per_job: Number of hosts needed per job (default: 3).

    Returns:
        List of host groups, where each group is a list of hostnames.
    """
    if len(hosts) < hosts_per_job:
        print(f"Error: Need at least {hosts_per_job} hosts, got {len(hosts)}",
              file=sys.stderr)
        sys.exit(1)

    # Group hosts into chunks of hosts_per_job
    # Each chunk is a resource unit that gets assigned to one job
    num_complete_groups = len(hosts) // hosts_per_job
    if num_complete_groups == 0:
        print(f"Error: Need at least {hosts_per_job} hosts for one evaluation",
              file=sys.stderr)
        sys.exit(1)

    resources = []
    for i in range(num_complete_groups):
        start = i * hosts_per_job
        end = start + hosts_per_job
        resources.append(hosts[start:end])

    unused = len(hosts) % hosts_per_job
    if unused > 0:
        print(f"Warning: {unused} hosts not used (need multiples of {hosts_per_job})")

    return resources


def main():
    parser = argparse.ArgumentParser(
        description="DeepHyper parameter search for Mochi Streaming Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Quick test:     python search.py --max-evals 20 --num-workers 2
  Custom template: python search.py --template my_search.yaml --max-evals 100
  Thorough:       python search.py --max-evals 500 --num-workers 8

Results are saved to the log directory as results.csv
        """
    )
    parser.add_argument(
        "--template", "-t", type=str, default=None,
        help="Path to search template YAML file (default: use built-in template)"
    )
    parser.add_argument(
        "--max-evals", type=int, default=100,
        help="Maximum number of evaluations (default: 100)"
    )
    parser.add_argument(
        "--num-workers", type=int, default=4,
        help="Number of parallel workers (default: 4)"
    )
    parser.add_argument(
        "--log-dir", type=str, default="deephyper_results",
        help="Directory for results (default: deephyper_results)"
    )
    parser.add_argument(
        "--random-state", type=int, default=42,
        help="Random seed for reproducibility (default: 42)"
    )
    parser.add_argument(
        "--surrogate", type=str, default="RF",
        choices=["RF", "GP", "ET", "GBRT"],
        help="Surrogate model: RF (Random Forest), GP (Gaussian Process), "
             "ET (Extra Trees), GBRT (Gradient Boosted Trees) (default: RF)"
    )
    parser.add_argument(
        "--initial-points", type=int, default=10,
        help="Number of initial random evaluations before Bayesian optimization (default: 10)"
    )
    parser.add_argument(
        "--benchmark-exe", type=str, default="./build/pipeline_benchmark",
        help="Path to benchmark executable (default: ./build/pipeline_benchmark)"
    )
    parser.add_argument(
        "--timeout", type=int, default=300,
        help="Timeout in seconds for each benchmark run (default: 300)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--launcher", "-l", type=str, default=None,
        help="Path to launcher YAML file for MPI configuration"
    )
    parser.add_argument(
        "--launcher-name", type=str, default="mpich",
        choices=list(DEFAULT_LAUNCHERS.keys()),
        help="Use a built-in launcher (default: mpich). "
             "Ignored if --launcher is specified."
    )
    parser.add_argument(
        "--hosts", type=str, default=None,
        help="Comma-separated list of hostnames for MPI execution "
             "(e.g., 'node1,node2,node3,node4,node5,node6'). "
             "Hosts are allocated in groups of 3 (sender,broker,receiver) per evaluation."
    )
    parser.add_argument(
        "--hostfile", type=str, default=None,
        help="Path to file containing hostnames, one per line. "
             "Alternative to --hosts."
    )
    args = parser.parse_args()

    # Validate arguments
    if args.max_evals < 1:
        print("Error: --max-evals must be at least 1", file=sys.stderr)
        sys.exit(1)
    if args.num_workers < 1:
        print("Error: --num-workers must be at least 1", file=sys.stderr)
        sys.exit(1)

    # Check that benchmark executable exists
    if not os.path.exists(args.benchmark_exe):
        print(f"Error: {args.benchmark_exe} not found.", file=sys.stderr)
        print("Please build the project first: mkdir build && cd build && cmake .. && make",
              file=sys.stderr)
        sys.exit(1)

    # Create the problem
    if args.template:
        if not os.path.exists(args.template):
            print(f"Error: Template file not found: {args.template}", file=sys.stderr)
            sys.exit(1)
        problem = create_problem(args.template)
        template_name = args.template
    else:
        problem = create_default_problem()
        template_name = "(built-in default)"

    print("=" * 60)
    print("Mochi Streaming Pipeline - DeepHyper Parameter Search")
    print("=" * 60)
    print(f"Template:           {template_name}")
    print(f"Max evaluations:    {args.max_evals}")
    print(f"Parallel workers:   {args.num_workers}")
    print(f"Surrogate model:    {args.surrogate}")
    print(f"Initial points:     {args.initial_points}")
    print(f"Log directory:      {args.log_dir}")
    print(f"Random state:       {args.random_state}")
    print(f"Benchmark timeout:  {args.timeout}s")

    # Set up launcher
    launcher = create_launcher(args.launcher, args.launcher_name)
    launcher_info = args.launcher if args.launcher else f"(built-in: {args.launcher_name})"
    print(f"Launcher:           {launcher_info}")

    # Parse hosts if provided
    hosts = parse_hosts(args.hosts, args.hostfile)
    if hosts:
        print(f"Hosts:              {len(hosts)} hosts ({len(hosts) // HOSTS_PER_JOB} concurrent evaluations)")
    else:
        print(f"Hosts:              (local execution)")
    print("=" * 60)

    if args.verbose:
        print("\nLauncher command template:")
        print("-" * 40)
        if launcher.command_template:
            print(f"  {launcher.command_template}")
        else:
            print(f"  {launcher.executable} {' '.join(launcher.args)} {{benchmark_exe}} {{config_file}}")
        if launcher.environment:
            print(f"  Environment: {launcher.environment}")
        print()

        print("Search space:")
        print("-" * 40)
        for name, info in sorted(problem.parameters.items()):
            print(f"  {name}: {info}")
        print()
        print(f"Constants: {len(problem.constants)}")
        print()

    # Create the run function
    runner = BenchmarkRunner(problem, args.benchmark_exe, args.timeout, launcher=launcher)

    # Create evaluator for parallel execution
    # Use "loky" instead of "process" because it uses cloudpickle
    # which can serialize lambda functions in DeepHyper's problem definition
    callbacks = [TqdmCallback()]

    # If hosts are provided, create a queued evaluator for resource allocation
    # Each job gets HOSTS_PER_JOB hosts assigned via job.dequed
    if hosts:
        host_queue = create_host_queue(hosts, HOSTS_PER_JOB)
        # Limit workers to available host groups
        max_workers = len(hosts) // HOSTS_PER_JOB
        num_workers = min(args.num_workers, max_workers)
        if args.num_workers > max_workers:
            print(f"Warning: Reducing num_workers from {args.num_workers} to {max_workers} "
                  f"(limited by available hosts)")

        # Create a queued version of LokyEvaluator
        QueuedLokyEvaluator = queued(LokyEvaluator)
        evaluator = QueuedLokyEvaluator(
            runner,
            num_workers=num_workers,
            callbacks=callbacks,
            queue=host_queue,
            queue_pop_per_task=1,  # Each task gets one group of hosts
        )
    else:
        evaluator = Evaluator.create(
            runner,
            method="loky",
            method_kwargs={
                "num_workers": args.num_workers,
                "callbacks": callbacks,
            },
        )

    # Create the search
    search = CBO(
        problem.get_problem(),
        random_state=args.random_state,
        log_dir=args.log_dir,
        surrogate_model=args.surrogate,
        n_initial_points=args.initial_points,
        verbose=1 if args.verbose else 0,
    )

    # Run the search
    print("\nStarting search...")
    print()
    results = search.search(evaluator, max_evals=args.max_evals)

    # Filter successful runs
    successful = results[results["objective"] > 0]

    if len(successful) == 0:
        print("\nNo successful evaluations!")
        print("Check the log directory for details.")
        sys.exit(1)

    # Find best result
    best_idx = successful["objective"].idxmax()
    best = successful.loc[best_idx]

    # Print summary
    print()
    print("=" * 60)
    print("SEARCH COMPLETE")
    print("=" * 60)
    print(f"Total evaluations:      {len(results)}")
    print(f"Successful evaluations: {len(successful)}")
    print(f"Failed evaluations:     {len(results) - len(successful)}")
    print()
    print("=" * 60)
    print("BEST CONFIGURATION")
    print("=" * 60)
    print(f"Throughput: {best['objective']:.4f} GB/s")
    print()
    print("Parameters:")
    for col in sorted(results.columns):
        if col.startswith("p:"):
            param_name = col[2:]
            print(f"  {param_name}: {best[col]}")
    print("=" * 60)
    print()
    print(f"Full results saved to: {args.log_dir}/results.csv")

    # Generate and print the best config as YAML
    print()
    print("Best configuration as YAML:")
    print("-" * 40)
    best_params = {col[2:]: best[col] for col in results.columns if col.startswith("p:")}
    best_config = problem.generate_config(best_params)
    print(yaml.dump(best_config, default_flow_style=False))

    # Save best config to file
    best_config_path = os.path.join(args.log_dir, "best_config.yaml")
    with open(best_config_path, 'w') as f:
        yaml.dump(best_config, f, default_flow_style=False)
    print(f"Best configuration saved to: {best_config_path}")


if __name__ == "__main__":
    main()
