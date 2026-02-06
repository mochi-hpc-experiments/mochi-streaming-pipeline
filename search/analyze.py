#!/usr/bin/env python3
"""
Analyze DeepHyper search results for Mochi Streaming Pipeline.

This script generates statistics, visualizations, and insights from
the parameter search results.

Usage:
    python analyze.py deephyper_results/results.csv
    python analyze.py deephyper_results/results.csv --output-dir plots
"""

import argparse
import os
import sys

import pandas as pd
import numpy as np

# Optional: matplotlib for visualizations
try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


def main():
    parser = argparse.ArgumentParser(
        description="Analyze DeepHyper search results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analyze.py deephyper_results/results.csv
  python analyze.py results.csv --output-dir plots --top 20
        """
    )
    parser.add_argument(
        "results_csv",
        help="Path to results.csv from DeepHyper search"
    )
    parser.add_argument(
        "--output-dir", "-o", default=".",
        help="Output directory for plots (default: current directory)"
    )
    parser.add_argument(
        "--top", "-n", type=int, default=10,
        help="Number of top configurations to show (default: 10)"
    )
    parser.add_argument(
        "--no-plots", action="store_true",
        help="Skip generating plots"
    )
    args = parser.parse_args()

    # Load results
    if not os.path.exists(args.results_csv):
        print(f"Error: File not found: {args.results_csv}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.results_csv)

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Print basic statistics
    print_statistics(df)

    # Print parameter analysis
    print_parameter_analysis(df)

    # Print top configurations
    print_top_configurations(df, args.top)

    # Generate plots
    if not args.no_plots:
        if HAS_MATPLOTLIB:
            generate_plots(df, args.output_dir)
        else:
            print("\nNote: matplotlib not installed, skipping plots.")
            print("Install with: pip install matplotlib")


def print_statistics(df):
    """Print basic statistics about the search."""
    successful = df[df["objective"] > 0]
    failed = df[df["objective"] <= 0]

    print("=" * 60)
    print("SEARCH STATISTICS")
    print("=" * 60)
    print(f"Total evaluations:      {len(df)}")
    print(f"Successful evaluations: {len(successful)}")
    print(f"Failed evaluations:     {len(failed)}")
    print()

    if len(successful) > 0:
        print("Throughput Statistics (GB/s):")
        print(f"  Best:   {successful['objective'].max():.4f}")
        print(f"  Worst:  {successful['objective'].min():.4f}")
        print(f"  Mean:   {successful['objective'].mean():.4f}")
        print(f"  Median: {successful['objective'].median():.4f}")
        print(f"  Std:    {successful['objective'].std():.4f}")
    print()


def print_parameter_analysis(df):
    """Analyze the impact of each parameter on throughput."""
    successful = df[df["objective"] > 0]

    if len(successful) < 5:
        print("Not enough successful evaluations for parameter analysis.")
        return

    print("=" * 60)
    print("PARAMETER ANALYSIS")
    print("=" * 60)

    param_cols = [c for c in df.columns if c.startswith("p:")]

    # For categorical parameters, show mean throughput per category
    print("\nCategorical Parameters (mean throughput per value):")
    print("-" * 50)
    for col in param_cols:
        if successful[col].dtype == 'object' or len(successful[col].unique()) <= 5:
            print(f"\n{col[2:]}:")
            grouped = successful.groupby(col)["objective"].agg(["mean", "count", "max"])
            grouped = grouped.sort_values("mean", ascending=False)
            for val, row in grouped.iterrows():
                print(f"  {val:25s}: mean={row['mean']:.4f}, max={row['max']:.4f}, n={int(row['count'])}")

    # For numerical parameters, show correlation
    print("\n\nNumerical Parameters (correlation with throughput):")
    print("-" * 50)
    correlations = []
    for col in param_cols:
        if successful[col].dtype in ['int64', 'float64'] and len(successful[col].unique()) > 5:
            corr = successful[col].corr(successful["objective"])
            correlations.append((col[2:], corr))

    correlations.sort(key=lambda x: abs(x[1]), reverse=True)
    for name, corr in correlations:
        direction = "+" if corr > 0 else "-"
        print(f"  {name:25s}: {direction}{abs(corr):.3f}")
    print()


def print_top_configurations(df, n=10):
    """Print the top N configurations."""
    successful = df[df["objective"] > 0]

    if len(successful) == 0:
        print("No successful evaluations to show.")
        return

    print("=" * 60)
    print(f"TOP {min(n, len(successful))} CONFIGURATIONS")
    print("=" * 60)

    top = successful.nlargest(n, "objective")
    param_cols = sorted([c for c in df.columns if c.startswith("p:")])

    for rank, (idx, row) in enumerate(top.iterrows(), 1):
        print(f"\n#{rank} - Throughput: {row['objective']:.4f} GB/s")
        print("-" * 40)
        for col in param_cols:
            param_name = col[2:]
            value = row[col]
            if isinstance(value, float) and value == int(value):
                value = int(value)
            print(f"  {param_name}: {value}")
    print()


def generate_plots(df, output_dir):
    """Generate visualization plots."""
    successful = df[df["objective"] > 0]

    if len(successful) < 2:
        print("Not enough data for plots.")
        return

    print("=" * 60)
    print("GENERATING PLOTS")
    print("=" * 60)

    # 1. Optimization progress
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(range(len(df)), df["objective"].where(df["objective"] > 0),
            'b.', alpha=0.5, label="Evaluations")
    cummax = df["objective"].where(df["objective"] > 0).cummax()
    ax.plot(range(len(df)), cummax, 'r-', linewidth=2, label="Best so far")
    ax.set_xlabel("Evaluation Number")
    ax.set_ylabel("Throughput (GB/s)")
    ax.set_title("Optimization Progress")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/optimization_progress.png", dpi=150)
    plt.close()
    print(f"  Saved: {output_dir}/optimization_progress.png")

    # 2. Throughput distribution
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(successful["objective"], bins=30, edgecolor='black', alpha=0.7)
    ax.axvline(successful["objective"].mean(), color='r', linestyle='--',
               label=f'Mean: {successful["objective"].mean():.3f}')
    ax.axvline(successful["objective"].max(), color='g', linestyle='--',
               label=f'Best: {successful["objective"].max():.3f}')
    ax.set_xlabel("Throughput (GB/s)")
    ax.set_ylabel("Count")
    ax.set_title("Throughput Distribution")
    ax.legend()
    plt.tight_layout()
    plt.savefig(f"{output_dir}/throughput_distribution.png", dpi=150)
    plt.close()
    print(f"  Saved: {output_dir}/throughput_distribution.png")

    # 3. Parameter box plots for categorical parameters
    param_cols = [c for c in df.columns if c.startswith("p:")]
    categorical_params = [c for c in param_cols
                          if successful[c].dtype == 'object' or len(successful[c].unique()) <= 5]

    if categorical_params:
        n_params = len(categorical_params)
        fig, axes = plt.subplots(2, (n_params + 1) // 2, figsize=(14, 10))
        axes = axes.flatten()

        for i, col in enumerate(categorical_params):
            groups = [successful[successful[col] == val]["objective"]
                      for val in sorted(successful[col].unique())]
            labels = [str(v) for v in sorted(successful[col].unique())]

            bp = axes[i].boxplot(groups, labels=labels, patch_artist=True)
            for patch in bp['boxes']:
                patch.set_facecolor('lightblue')
            axes[i].set_title(col[2:])
            axes[i].set_ylabel("Throughput (GB/s)")
            axes[i].tick_params(axis='x', rotation=45)

        # Hide unused subplots
        for j in range(i + 1, len(axes)):
            axes[j].set_visible(False)

        plt.tight_layout()
        plt.savefig(f"{output_dir}/parameter_boxplots.png", dpi=150)
        plt.close()
        print(f"  Saved: {output_dir}/parameter_boxplots.png")

    # 4. Correlation heatmap for numerical parameters
    numerical_params = [c for c in param_cols
                        if successful[c].dtype in ['int64', 'float64']
                        and len(successful[c].unique()) > 5]

    if len(numerical_params) >= 2:
        corr_cols = numerical_params + ["objective"]
        corr_matrix = successful[corr_cols].corr()

        fig, ax = plt.subplots(figsize=(10, 8))
        im = ax.imshow(corr_matrix, cmap='RdBu_r', aspect='auto', vmin=-1, vmax=1)

        # Labels
        labels = [c[2:] if c.startswith("p:") else c for c in corr_cols]
        ax.set_xticks(range(len(labels)))
        ax.set_yticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=45, ha='right')
        ax.set_yticklabels(labels)

        # Add correlation values
        for i in range(len(labels)):
            for j in range(len(labels)):
                text = ax.text(j, i, f'{corr_matrix.iloc[i, j]:.2f}',
                               ha="center", va="center", fontsize=8)

        plt.colorbar(im, label="Correlation")
        ax.set_title("Parameter Correlation Matrix")
        plt.tight_layout()
        plt.savefig(f"{output_dir}/correlation_matrix.png", dpi=150)
        plt.close()
        print(f"  Saved: {output_dir}/correlation_matrix.png")

    print()


if __name__ == "__main__":
    main()
