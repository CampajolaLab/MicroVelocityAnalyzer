# MicroVelocityAnalyzer

MicroVelocityAnalyzer is a Python package for analyzing the velocity and balances of accounts based on transfer and allocation data.
This package is based on the initial inspiration of Carlo Campajola, a inspiration that has been explored in many papers about different cryptocurrencies. This package is based on the work made on Ethereum MicroVelocity on an upcoming paper, and is developed and mantained by Francesco Maria De Collibus and Carlo Campajola

## Features

- Load allocation data from a CSV file.
- Load transfer data from a CSV file.
- Calculate account velocities.
- Calculate account balances.
- Save results to a pickle file.

## Installation

To install the package, run the following command:

```sh
pip install -e .
```

## Usage

After installing the package, you can run the script from the command line using the `micro-velocity-analyzer` command. 

### Basic Usage

```sh
micro-velocity-analyzer --allocated_file path/to/allocated.csv --transfers_file path/to/transfers.csv --output_file path/to/output/general_velocities.pickle --save_every_n 10
```

### Advanced Usage with Parallel Processing

For large datasets, you can leverage parallel processing to speed up the analysis:

```sh
micro-velocity-analyzer \
  --allocated_file path/to/allocated.csv \
  --transfers_file path/to/transfers.csv \
  --output_file path/to/output/general_velocities.pickle \
  --save_every_n 100 \
  --n_cores 8 \
  --n_chunks 64 \
  --batch_size 2
```

### Usage with Split Save (for very large datasets)

When dealing with extremely large datasets that may cause memory issues, use split save to write intermediate results:

```sh
micro-velocity-analyzer \
  --allocated_file path/to/allocated.csv \
  --transfers_file path/to/transfers.csv \
  --output_file path/to/output/general_velocities.pickle \
  --save_every_n 100 \
  --n_cores 16 \
  --n_chunks 128 \
  --batch_size 4 \
  --split_save
```

## Arguments

### Required Arguments

- `--allocated_file`: Path to the CSV file containing initial token allocation data (e.g., minting events, ICO distributions)
  - Default: `sampledata/sample_allocated.csv`
  
- `--transfers_file`: Path to the CSV file containing peer-to-peer transfer data
  - Default: `sampledata/sample_transfers.csv`

### Optional Arguments

- `--output_file`: Path to the output file where results will be saved in pickle format
  - Default: `sampledata/general_velocities.pickle`
  - The saved file contains a tuple: `[backup_accounts, velocities, balances]`

- `--save_every_n`: Block interval for sampling. Controls how often balances and velocities are recorded
  - Default: `1` (every block)
  - Higher values reduce memory usage and file size at the cost of temporal resolution
  - Example: `--save_every_n 100` samples every 100 blocks

- `--n_cores`: Number of CPU cores to use for parallel processing
  - Default: `1` (sequential processing)
  - Recommended: Set to the number of available CPU cores for optimal performance
  - Example: `--n_cores 8`

- `--n_chunks`: Number of chunks to split addresses into for parallel processing
  - Default: `1`
  - Should be >= `n_cores` for effective parallelization
  - Recommended: `n_chunks = n_cores * 4` to `n_cores * 16` for better load balancing
  - Example: `--n_chunks 64` with `--n_cores 8` creates 64 chunks processed by 8 workers

- `--batch_size`: Number of chunks to process before saving/updating results
  - Default: `1`
  - Controls memory usage vs. I/O frequency
  - Higher values use more memory but reduce overhead
  - Formula: batch processes `n_cores * batch_size` chunks at once
  - Example: `--batch_size 2` with `--n_cores 8` processes 16 chunks per batch

- `--split_save`: Enable split save mode for very large datasets
  - Default: `False` (disabled)
  - When enabled, saves intermediate results to separate files instead of keeping everything in memory
  - Use this flag when processing millions of addresses to avoid memory overflow
  - Example: `--split_save`

- `--matching_strategy`: Strategy for matching incoming and outgoing transactions when calculating velocity
  - Default: `lifo` (Last-In-First-Out)
  - Options:
    - `lifo`: Last-In-First-Out - assumes most recently received tokens are spent first. Most economically realistic for modeling savings vs. spending.
    - `fifo`: First-In-First-Out - assumes oldest received tokens are spent first. Useful for comparison or different economic models.
    - `random`: Random - randomly selects which incoming transactions are matched with outgoing ones. Useful for sensitivity analysis and robustness testing.
  - Example: `--matching_strategy fifo` or `--matching_strategy random`
  - Note: Different strategies will produce different velocity results; choose based on your economic assumptions

## Performance Tuning Guide

### Small Datasets (< 100K addresses)
```sh
micro-velocity-analyzer --allocated_file data.csv --transfers_file transfers.csv --save_every_n 1
```

### Medium Datasets (100K - 1M addresses)
```sh
micro-velocity-analyzer \
  --allocated_file data.csv \
  --transfers_file transfers.csv \
  --save_every_n 10 \
  --n_cores 4 \
  --n_chunks 32
```

### Large Datasets (1M - 10M addresses)
```sh
micro-velocity-analyzer \
  --allocated_file data.csv \
  --transfers_file transfers.csv \
  --save_every_n 100 \
  --n_cores 16 \
  --n_chunks 128 \
  --batch_size 4
```

### Very Large Datasets (> 10M addresses)
```sh
micro-velocity-analyzer \
  --allocated_file data.csv \
  --transfers_file transfers.csv \
  --save_every_n 1000 \
  --n_cores 32 \
  --n_chunks 256 \
  --batch_size 8 \
  --split_save
```

## Output Format

To save space, velocities and balances are sampled according to the `save_every_n` parameter. The saved pickle file contains a tuple with three elements:

1. **backup_accounts**: Dictionary of account data with assets and liabilities before velocity calculation
   - Structure: `{address: [{block: amount}, {block: amount}]}` for [assets, liabilities]
   
2. **velocities**: Dictionary mapping addresses to velocity arrays
   - Structure: `{address: numpy.array([velocity_at_interval_0, velocity_at_interval_1, ...])}` 
   - Velocity represents token movement rate (amount/duration) using LIFO accounting
   
3. **balances**: Dictionary mapping addresses to balance arrays
   - Structure: `{address: numpy.array([balance_at_interval_0, balance_at_interval_1, ...])}`
   - Balance at each checkpoint interval

## Example CSV Files

### allocated.csv

```csv
to_address,amount,block_number
address1,100,1
address2,200,2
address1,150,3
```

### transfers.csv

```csv
from_address,to_address,amount,block_number
address1,address2,50,4
address2,address3,100,5
address1,address3,75,6
```

## Velocity Matching Strategies

The package supports three different strategies for matching incoming and outgoing transactions when calculating velocity. Each strategy represents a different economic assumption about which tokens are spent first:

### LIFO (Last-In-First-Out) - Default
```sh
micro-velocity-analyzer \
  --allocated_file data.csv \
  --transfers_file transfers.csv \
  --matching_strategy lifo
```
**Assumptions**: Most recently received tokens are spent first (like a stack). This is the default and most economically realistic for many scenarios, effectively separating "savings" (older tokens held) from "spending" (recently acquired tokens).

### FIFO (First-In-First-Out)
```sh
micro-velocity-analyzer \
  --allocated_file data.csv \
  --transfers_file transfers.csv \
  --matching_strategy fifo
```
**Assumptions**: Oldest received tokens are spent first (queue behavior). Useful for comparison analyses or when specific economic models assume chronological spending patterns.

### RANDOM
```sh
micro-velocity-analyzer \
  --allocated_file data.csv \
  --transfers_file transfers.csv \
  --matching_strategy random
```
**Assumptions**: Incoming transactions are matched randomly with outgoing ones. Useful for:
- Sensitivity analysis to test robustness of results
- Baseline comparison to see impact of matching strategy
- Simulating scenarios with no information about token acquisition order

**Example**: Comparing all three strategies on the same dataset:
```sh
for strategy in lifo fifo random; do
  micro-velocity-analyzer \
    --allocated_file data.csv \
    --transfers_file transfers.csv \
    --output_file results_${strategy}.pickle \
    --matching_strategy $strategy \
    --n_cores 8 \
    --n_chunks 64
done
```

## Post-Processing Analysis

After computing velocities and balances, use the analysis module to extract statistics and cross-sectional snapshots. The analysis script provides three main operations:

### Running the Analysis Script

```sh
python -m micro_velocity_analyzer.analysis \
  --balances path/to/balances.pickle \
  --velocities path/to/velocities.pickle \
  <command> [command-specific-args]
```

### 1. Cross-Section Analysis

Extract velocity/balance statistics at a specific block (snapshot):

```sh
python -m micro_velocity_analyzer.analysis \
  --balances results_balances.pickle \
  --velocities results_velocities.pickle \
  cross-section --block 1000000 --min-denominator 1
```

**Output**: Summary statistics including:
- Count of non-zero addresses
- Mean, median, std, min, max velocity/balance ratios
- Percentile values (25th, 75th, 90th, 99th)

**Arguments**:
- `--block`: Block number for the snapshot (required)
- `--min-denominator`: Minimum balance threshold to include in ratio calculations (default: 1)

### 2. Longitudinal Analysis

Extract sparse velocity/balance ratio time series for a single address:

```sh
python -m micro_velocity_analyzer.analysis \
  --balances results_balances.pickle \
  --velocities results_velocities.pickle \
  longitudinal --address 0xabc123def456 --min-denominator 1
```

**Output**: One line per data point: `block_number ratio_value`

**Use Cases**:
- Track an address's token activity over time
- Identify periods of high/low velocity
- Analyze individual account behavior patterns

**Arguments**:
- `--address`: Ethereum address to analyze (required, lowercase)
- `--min-denominator`: Minimum balance to include in ratio (default: 1)

### 3. Batch Cross-Section Analysis

Extract cross-sectional snapshots at multiple blocks in parallel, with results saved to separate files:

```sh
python -m micro_velocity_analyzer.analysis \
  --balances results_balances.pickle \
  --velocities results_velocities.pickle \
  batch-cross-section \
  --blocks-file blocks.pickle \
  --output-folder xsects/ \
  --n-workers 4 \
  --parallel-mode shared \
  --min-denominator 1
```

**Output**: Three pickle files per block in `output-folder/`:
- `xsect_balance_block_{block}.pickle`: Balance values at block
- `xsect_velocity_block_{block}.pickle`: Velocity values at block
- `xsect_ratio_block_{block}.pickle`: Velocity/balance ratios at block

**Preparation** (create blocks list):
```python
# Create a pickle file with list of block numbers to process
import pickle
blocks = [1000000, 2000000, 3000000]  # Your blocks of interest
with open('blocks.pickle', 'wb') as f:
    pickle.dump(blocks, f)
```

**Arguments**:
- `--blocks-file`: Path to pickle file containing list of block numbers (required)
- `--output-folder`: Folder where results will be saved (required)
- `--min-denominator`: Minimum balance threshold (default: 1)
- `--n-workers`: Number of parallel workers (default: 1)
  - `1`: Sequential processing (minimal memory)
  - `4-8`: Recommended for most datasets
  - `16+`: For very large datasets with many CPU cores
- `--parallel-mode`: Processing strategy (default: `shared`)
  - `shared`: Shared memory mode - faster, requires Python 3.8+, best for medium datasets
  - `batched`: Batch processing - slower but compatible with all Python versions, better for very large datasets
- `--blocks-per-batch`: Blocks per batch in batched mode (default: 10)
  - Higher values reduce overhead but increase memory per batch
  - Tune based on available RAM

**Example: Process 100 blocks with 4 workers**:
```sh
python -m micro_velocity_analyzer.analysis \
  --balances results_balances.pickle \
  --velocities results_velocities.pickle \
  batch-cross-section \
  --blocks-file blocks.pickle \
  --output-folder results/cross_sections/ \
  --n-workers 4 \
  --parallel-mode shared \
  --blocks-per-batch 10
```

**Example: Process very large dataset (>10M addresses) sequentially**:
```sh
python -m micro_velocity_analyzer.analysis \
  --balances results_balances_dir/ \
  --velocities results_velocities_dir/ \
  batch-cross-section \
  --blocks-file blocks.pickle \
  --output-folder results/cross_sections/ \
  --n-workers 1
```

### Loading Analysis Results

After batch cross-section analysis, load results in Python:

```python
import pickle
import os

results_folder = "results/cross_sections/"
block = 1000000

# Load cross-section data at a block
with open(os.path.join(results_folder, f"xsect_balance_block_{block}.pickle"), 'rb') as f:
    balances = pickle.load(f)  # Dict[address, balance]

with open(os.path.join(results_folder, f"xsect_velocity_block_{block}.pickle"), 'rb') as f:
    velocities = pickle.load(f)  # Dict[address, velocity]

with open(os.path.join(results_folder, f"xsect_ratio_block_{block}.pickle"), 'rb') as f:
    ratios = pickle.load(f)  # Dict[address, velocity/balance ratio]

# Example: Find top 10 addresses by velocity/balance ratio
top_addresses = sorted(ratios.items(), key=lambda x: x[1], reverse=True)[:10]
for address, ratio in top_addresses:
    print(f"{address}: {ratio:.4f}")
```

## Contributions

Contributions are welcome! Feel free to open issues or pull requests to improve the project.

## License

This project is licensed under the terms of the MIT license. See the [LICENSE](LICENSE) file for details.

## Author

Francesco Maria De Collibus - [francesco.decollibus@business.uzh.ch](mailto:francesco.decollibus@business.uzh.ch)
Carlo Campajola - [c.campajola@ucl.ac.uk](mailto:c.campajola@ucl.ac.uk)

```

```

```

```
