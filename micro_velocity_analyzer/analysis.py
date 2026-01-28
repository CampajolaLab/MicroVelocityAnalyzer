"""
Sparse analysis utilities for MicroVelocityAnalyzer results.

This module operates on sparse balances and velocities saved by the
MicroVelocityAnalyzer, combining them longitudinally and cross-sectionally
without materializing dense arrays in memory.

Data model:
- Sparse series: list of (block: int, value: int|float) sorted by block ascending.
- Balances: int (Python arbitrary precision) values.
- Velocities: float values.

Key capabilities:
- Value lookup at a block via binary search (O(log n)).
- Merge two sparse series into a sparse ratio stream without densifying.
- Cross-sectional ratio statistics at a block across all addresses (streaming).

CLI (basic): cross-sectional stats at given block.

Note: For very large datasets, prefer using non-split single pickle files for now,
so this tool can stream address entries from one file without building large indices.
Directory (split) mode is supported in a simple sequential manner but may require
more memory if there are many small files.
"""
from __future__ import annotations

import argparse
import os
import pickle
import sys
from bisect import bisect_right
from concurrent.futures import ProcessPoolExecutor, as_completed
from statistics import mean
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

# Try to import shared memory support (Python 3.8+)
try:
    from multiprocessing import shared_memory
    from multiprocessing.managers import SharedMemoryManager
    SHARED_MEMORY_AVAILABLE = True
except ImportError:
    SHARED_MEMORY_AVAILABLE = False

SparseSeries = List[Tuple[int, Union[int, float]]]


def _value_at_block(sparse: SparseSeries, block: int) -> Union[int, float]:
    """Return value at given block using rightmost change <= block; 0 if none.

    Assumes sparse is sorted by block ascending.
    """
    if not sparse:
        return 0
    blocks = [b for b, _ in sparse]
    i = bisect_right(blocks, block) - 1
    if i < 0:
        return 0
    return sparse[i][1]


def _value_at_block_cached(sparse: SparseSeries, block: int, blocks_cache: List[int]) -> Union[int, float]:
    """Return value at given block using pre-extracted block list (performance optimization).
    
    Args:
        sparse: Sparse series list of (block, value) tuples
        block: Block number to query
        blocks_cache: Pre-extracted list of block numbers from sparse series
    
    Returns:
        Value at the given block, or 0 if no prior change
    """
    if not blocks_cache:
        return 0
    i = bisect_right(blocks_cache, block) - 1
    if i < 0:
        return 0
    return sparse[i][1]




def ratio_sparse(balance_sparse: SparseSeries, velocity_sparse: SparseSeries, *, min_denominator: int = 1) -> SparseSeries:
    """Compute sparse ratio series velocity/balance by linearly merging change points.

    - Emits (block, ratio) only when balance >= min_denominator.
    - Linear-time merge over both sparse series; avoids repeated binary searches.
    """
    if not balance_sparse and not velocity_sparse:
        return []

    i = j = 0
    curr_bal: Union[int, float] = 0
    curr_vel: Union[int, float] = 0.0
    out: SparseSeries = []

    # Merge union of change points
    while i < len(balance_sparse) or j < len(velocity_sparse):
        next_blk_bal = balance_sparse[i][0] if i < len(balance_sparse) else None
        next_blk_vel = velocity_sparse[j][0] if j < len(velocity_sparse) else None

        if next_blk_vel is None or (next_blk_bal is not None and next_blk_bal <= next_blk_vel):
            # Advance balances stream
            curr_bal = balance_sparse[i][1]
            blk_i = int(balance_sparse[i][0])
            i += 1
        else:
            # Advance velocities stream
            curr_vel = velocity_sparse[j][1]
            blk_i = int(velocity_sparse[j][0])
            j += 1

        # Emit ratio if denominator ok
        if isinstance(curr_bal, int) and curr_bal >= min_denominator:
            # Instantaneous ratio at latest change point <= blk_i
            vel_val = float(curr_vel)
            out.append((blk_i, float(vel_val) / float(curr_bal)))  # type: ignore

    return out


def _iter_dict_from_pickle(path: str) -> Iterator[Tuple[str, SparseSeries]]:
    """Yield (address, sparse_series) from a single pickle file containing a dict."""
    with open(path, 'rb') as f:
        data = pickle.load(f)
        # data may be a dict or a list/tuple; we expect dict
        if isinstance(data, dict):
            for addr, series in data.items():
                yield addr, series
        else:
            raise ValueError(f"Unexpected pickle content in {path}: {type(data)}")


def _iter_dicts_from_dir(dir_path: str) -> Iterator[Tuple[str, SparseSeries]]:
    """Yield (address, sparse_series) from all pickle files in a directory.

    Files are processed in sorted order by filename to provide determinism.
    """
    for name in sorted(os.listdir(dir_path)):
        if not name.endswith('.pickle'):
            continue
        path = os.path.join(dir_path, name)
        for addr, series in _iter_dict_from_pickle(path):
            yield addr, series


def _iter_balances(path: str) -> Iterator[Tuple[str, SparseSeries]]:
    if os.path.isdir(path):
        return _iter_dicts_from_dir(path)
    return _iter_dict_from_pickle(path)


def _load_balances_map(path: str) -> Dict[str, SparseSeries]:
    # Helper for simple single-file scenario
    with open(path, 'rb') as f:
        data = pickle.load(f)
        if not isinstance(data, dict):
            raise ValueError("Balances pickle must contain a dict")
        return data


def _load_velocities_map(path: str) -> Dict[str, SparseSeries]:
    with open(path, 'rb') as f:
        data = pickle.load(f)
        if not isinstance(data, dict):
            raise ValueError("Velocities pickle must contain a dict")
        return data


def cross_section_ratios(
    balances: Dict[str, SparseSeries],
    velocities: Dict[str, SparseSeries],
    *,
    block: int,
    min_denominator: int = 1,
    addresses: Optional[Iterable[str]] = None,
) -> Iterator[Tuple[str, float]]:
    """Stream (address, ratio) at a given block across addresses.

    - Uses binary search per address; memory proportional to one address at a time.
    - Skips addresses where balance < min_denominator.
    - If an address is missing in either map, treats missing series as empty (value 0).
    """
    addrs = addresses if addresses is not None else balances.keys()
    for addr in addrs:
        bal_series = balances.get(addr, [])
        vel_series = velocities.get(addr, [])
        bal = _value_at_block(bal_series, block)
        if isinstance(bal, int) and bal >= min_denominator:
            vel_val = _value_at_block(vel_series, block)
            yield addr, float(vel_val) / float(bal)


def summarize_cross_section(
    items: Iterable[Tuple[str, float]]
) -> Dict[str, Union[int, float]]:
    """Compute basic stats (count, mean, min, max) without storing all values.

    Returns a dict with keys: count, mean, min, max, sum.
    For more advanced quantiles, consider external streaming quantile algorithms.
    """
    count = 0
    s = 0.0
    min_v = float('inf')
    max_v = float('-inf')
    for _, v in items:
        count += 1
        s += v
        if v < min_v:
            min_v = v
        if v > max_v:
            max_v = v
    if count == 0:
        return {"count": 0, "mean": float('nan'), "min": float('nan'), "max": float('nan'), "sum": 0.0}
    return {"count": count, "mean": s / count, "min": min_v, "max": max_v, "sum": s}


def _global_min_max_block(series_map: Dict[str, SparseSeries]) -> Optional[Tuple[int, int]]:
    """Return global (min_block, max_block) across all non-empty sparse series.

    If all series are empty, return None.
    """
    min_blk: Optional[int] = None
    max_blk: Optional[int] = None
    for s in series_map.values():
        if not s:
            continue
        # series assumed sorted by block ascending
        lo = int(s[0][0])
        hi = int(s[-1][0])
        min_blk = lo if min_blk is None else min(min_blk, lo)
        max_blk = hi if max_blk is None else max(max_blk, hi)
    if min_blk is None or max_blk is None:
        return None
    return (min_blk, max_blk)


def _process_single_block(args) -> Tuple[int, Dict, Dict, Dict]:
    """Worker function to process a single block cross-section (for parallel processing).
    
    Args:
        args: Tuple of (block, bal_map, vel_map, min_denominator, bal_blocks_cache, vel_blocks_cache)
    
    Returns:
        Tuple of (block, balances_xsect, velocities_xsect, ratios_xsect)
    """
    block, bal_map, vel_map, min_denominator, bal_blocks_cache, vel_blocks_cache = args
    
    balances_xsect = {}
    velocities_xsect = {}
    ratios_xsect = {}
    
    # Single pass over all addresses
    for addr in bal_map.keys():
        bal_series = bal_map[addr]
        # Defensive: cache may be empty or missing entries in lazy mode
        bal_blocks = bal_blocks_cache.get(addr) if bal_blocks_cache is not None else None
        if bal_blocks is None:
            bal_blocks = [b for b, _ in bal_series]
        bal = _value_at_block_cached(bal_series, block, bal_blocks)
        
        if bal != 0:
            balances_xsect[addr] = bal
        
        if isinstance(bal, int) and bal >= min_denominator:
            vel_series = vel_map.get(addr, [])
            vel_blocks = vel_blocks_cache.get(addr) if vel_blocks_cache is not None else None
            if vel_blocks is None:
                vel_blocks = [b for b, _ in vel_series]
            vel = _value_at_block_cached(vel_series, block, vel_blocks)
            
            if vel != 0:
                velocities_xsect[addr] = vel
            
            ratios_xsect[addr] = float(vel) / float(bal)
    
    # Check for addresses only in vel_map (edge case)
    for addr in vel_map.keys():
        if addr not in bal_map:
            vel_series = vel_map[addr]
            vel_blocks = vel_blocks_cache.get(addr) if vel_blocks_cache is not None else None
            if vel_blocks is None:
                vel_blocks = [b for b, _ in vel_series]
            vel = _value_at_block_cached(vel_series, block, vel_blocks)
            if vel != 0:
                velocities_xsect[addr] = vel
    
    return block, balances_xsect, velocities_xsect, ratios_xsect


def _process_block_batch(args) -> List[Tuple[int, Dict, Dict, Dict]]:
    """Worker function to process a batch of blocks (reduces pickling overhead).
    
    Args:
        args: Tuple of (block_list, bal_map, vel_map, min_denominator, bal_blocks_cache, vel_blocks_cache, pos, batch_id)
    
    Returns:
        List of tuples: [(block, balances_xsect, velocities_xsect, ratios_xsect), ...]
    """
    from tqdm import tqdm
    block_list, bal_map, vel_map, min_denominator, bal_blocks_cache, vel_blocks_cache, pos, batch_id = args
    
    results = []
    for block in tqdm(block_list, position=pos, leave=False, desc=f"Worker {pos} [Batch {batch_id}]"):
        result = _process_single_block((block, bal_map, vel_map, min_denominator, bal_blocks_cache, vel_blocks_cache))
        results.append(result)
    
    return results


# Global variables for shared memory mode (accessed by worker processes)
_shared_bal_map = None
_shared_vel_map = None
_shared_bal_blocks_cache = None
_shared_vel_blocks_cache = None


def _init_shared_worker(bal_map, vel_map, bal_blocks_cache, vel_blocks_cache):
    """Initialize worker process with shared memory references."""
    global _shared_bal_map, _shared_vel_map, _shared_bal_blocks_cache, _shared_vel_blocks_cache, _lazy_block_cache
    _shared_bal_map = bal_map
    _shared_vel_map = vel_map
    _shared_bal_blocks_cache = bal_blocks_cache
    _shared_vel_blocks_cache = vel_blocks_cache
    _lazy_block_cache = {}  # Per-worker cache to avoid repeated block list extraction


def _get_blocks_lazy(addr: str, series: SparseSeries, is_balance: bool) -> List[int]:
    """Get block list, using lazy cache to avoid re-extracting on every call.
    
    This reduces CPU overhead when block caches are empty (large dataset mode).
    Each worker maintains its own per-call cache.
    """
    global _lazy_block_cache
    cache_key = (addr, 'bal' if is_balance else 'vel')
    if cache_key not in _lazy_block_cache:
        _lazy_block_cache[cache_key] = [b for b, _ in series]
    return _lazy_block_cache[cache_key]


def _process_single_block_shared(args) -> Tuple[int, Dict, Dict, Dict]:
    """Worker function using shared memory (no pickling of data maps).
    
    Args:
        args: Tuple of (block, min_denominator)
    
    Returns:
        Tuple of (block, balances_xsect, velocities_xsect, ratios_xsect)
    """
    block, min_denominator = args
    
    # Access global shared memory references (set by initializer)
    global _shared_bal_map, _shared_vel_map, _shared_bal_blocks_cache, _shared_vel_blocks_cache
    assert _shared_bal_map is not None, "Worker not initialized with shared data"
    assert _shared_vel_map is not None, "Worker not initialized with shared data"
    
    bal_map = _shared_bal_map
    vel_map = _shared_vel_map
    bal_blocks_cache = _shared_bal_blocks_cache
    vel_blocks_cache = _shared_vel_blocks_cache
    
    balances_xsect = {}
    velocities_xsect = {}
    ratios_xsect = {}
    
    # Single pass over all addresses
    for addr in bal_map.keys():
        bal_series = bal_map[addr]
        # Use pre-cached blocks if available; otherwise lazy-extract to save memory
        if bal_blocks_cache and addr in bal_blocks_cache:
            bal_blocks = bal_blocks_cache[addr]
        else:
            bal_blocks = _get_blocks_lazy(addr, bal_series, is_balance=True)
        bal = _value_at_block_cached(bal_series, block, bal_blocks)
        
        if bal != 0:
            balances_xsect[addr] = bal
        
        if isinstance(bal, int) and bal >= min_denominator:
            vel_series = vel_map.get(addr, [])
            # Use pre-cached blocks if available; otherwise lazy-extract
            if vel_blocks_cache and addr in vel_blocks_cache:
                vel_blocks = vel_blocks_cache[addr]
            else:
                vel_blocks = _get_blocks_lazy(addr, vel_series, is_balance=False)
            vel = _value_at_block_cached(vel_series, block, vel_blocks)
            
            if vel != 0:
                velocities_xsect[addr] = vel
            
            ratios_xsect[addr] = float(vel) / float(bal)
    
    # Check for addresses only in vel_map (edge case)
    for addr in vel_map.keys():
        if addr not in bal_map:
            vel_series = vel_map[addr]
            if vel_blocks_cache and addr in vel_blocks_cache:
                vel_blocks = vel_blocks_cache[addr]
            else:
                vel_blocks = _get_blocks_lazy(addr, vel_series, is_balance=False)
            vel = _value_at_block_cached(vel_series, block, vel_blocks)
            if vel != 0:
                velocities_xsect[addr] = vel
    
    return block, balances_xsect, velocities_xsect, ratios_xsect


def _process_block_batch_shared(args) -> List[Tuple[int, Dict, Dict, Dict]]:
    """Worker function to process a batch of blocks using shared memory.
    
    Args:
        args: Tuple of (block_list, min_denominator)
    
    Returns:
        List of tuples: [(block, balances_xsect, velocities_xsect, ratios_xsect), ...]
    """
    block_list, min_denominator = args
    
    results = []
    for block in block_list:
        result = _process_single_block_shared((block, min_denominator))
        results.append(result)
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Sparse analysis for MicroVelocityAnalyzer results")
    parser.add_argument("--balances", required=True, help="Path to balances pickle (single file) or directory")
    parser.add_argument("--velocities", required=True, help="Path to velocities pickle (single file) or directory")
    sub = parser.add_subparsers(dest="cmd", required=True)

    cs = sub.add_parser("cross-section", help="Compute cross-sectional ratio stats at a block")
    cs.add_argument("--block", type=int, required=True, help="Block number for snapshot")
    cs.add_argument("--min-denominator", type=int, default=1, help="Minimum balance to include in ratio")
    # instantaneous latest-change evaluation only

    lg = sub.add_parser("longitudinal", help="Emit sparse ratio series for a single address")
    lg.add_argument("--address", required=True, help="Address to analyze")
    lg.add_argument("--min-denominator", type=int, default=1)
    
    batch = sub.add_parser("batch-cross-section", help="Extract cross-sections at multiple blocks and save to files")
    batch.add_argument("--blocks-file", required=True, help="Path to pickle file containing list of block numbers")
    batch.add_argument("--output-folder", required=True, help="Folder where cross-section pickles will be saved")
    batch.add_argument("--min-denominator", type=int, default=1, help="Minimum balance to include in ratio")
    batch.add_argument("--n-workers", type=int, default=1,
                       help="Number of parallel workers for processing blocks (default: 1). Use 4-8 for significant speedup.")
    batch.add_argument("--parallel-mode", type=str, default="shared",
                       choices=["shared", "batched"],
                       help="Parallel processing mode (default: shared).\n"
                            "  shared: Use shared memory (fast, requires Python 3.8+, Linux/macOS recommended)\n"
                            "  batched: Batch multiple blocks per worker (slower but more compatible)")
    batch.add_argument("--blocks-per-batch", type=int, default=10,
                       help="Number of blocks per batch in 'batched' mode (default: 10). Higher reduces overhead but increases latency.")
    # instantaneous latest-change evaluation only

    args = parser.parse_args()

    if args.cmd == "cross-section":
        # For now, support single-file mode robustly
        if os.path.isdir(args.balances) or os.path.isdir(args.velocities):
            print("Warning: directory mode is experimental; prefer single-file pickles for analysis.")
        bal_map = _load_balances_map(args.balances) if os.path.isfile(args.balances) else dict(_iter_balances(args.balances))
        vel_map = _load_velocities_map(args.velocities) if os.path.isfile(args.velocities) else dict(_iter_balances(args.velocities))

        items = cross_section_ratios(bal_map, vel_map, block=args.block, min_denominator=args.min_denominator)
        stats = summarize_cross_section(items)
        print(stats)
        return

    if args.cmd == "longitudinal":
        bal_map = _load_balances_map(args.balances) if os.path.isfile(args.balances) else dict(_iter_balances(args.balances))
        vel_map = _load_velocities_map(args.velocities) if os.path.isfile(args.velocities) else dict(_iter_balances(args.velocities))
        b = bal_map.get(args.address, [])
        v = vel_map.get(args.address, [])
        sparse_ratio = ratio_sparse(b, v, min_denominator=args.min_denominator)
        for blk, r in sparse_ratio:
            print(blk, r)
    
    if args.cmd == "batch-cross-section":
        # Load block numbers from pickle file
        with open(args.blocks_file, 'rb') as f:
            block_numbers = pickle.load(f)
        
        # Create output folder if it doesn't exist
        os.makedirs(args.output_folder, exist_ok=True)
        
        # Load balances and velocities
        print("Loading balances and velocities...")
        if os.path.isdir(args.balances) or os.path.isdir(args.velocities):
            print("Warning: directory mode is experimental; prefer single-file pickles for analysis.")
        bal_map = _load_balances_map(args.balances) if os.path.isfile(args.balances) else dict(_iter_balances(args.balances))
        vel_map = _load_velocities_map(args.velocities) if os.path.isfile(args.velocities) else dict(_iter_balances(args.velocities))
        # Diagnostics: show global block ranges
        bal_range = _global_min_max_block(bal_map)
        vel_range = _global_min_max_block(vel_map)
        print("Balances range:", bal_range if bal_range is not None else "<empty>")
        print("Velocities range:", vel_range if vel_range is not None else "<empty>")
        if bal_range and vel_range:
            global_min = min(bal_range[0], vel_range[0])
            global_max = max(bal_range[1], vel_range[1])
            print(f"Global block range: [{global_min}, {global_max}]")
        elif bal_range:
            global_min, global_max = bal_range
            print(f"Global block range (balances only): [{global_min}, {global_max}]")
        elif vel_range:
            global_min, global_max = vel_range
            print(f"Global block range (velocities only): [{global_min}, {global_max}]")
        else:
            print("Warning: Both balances and velocities maps are empty. Outputs will be empty.")
            global_min = global_max = None

        # Filter requested blocks to the available global range to avoid wasted work
        if global_min is not None and global_max is not None:
            original_count = len(block_numbers)
            block_numbers = [b for b in block_numbers if global_min <= b <= global_max]
            skipped = original_count - len(block_numbers)
            if skipped > 0:
                print(f"Skipping {skipped} blocks outside global range [{global_min}, {global_max}]")
            if not block_numbers:
                print("No blocks remain after filtering; exiting.")
                return

        # Decide cache strategy based on parallelism
        if args.n_workers == 1:
            # Sequential mode: skip caches to minimize memory usage
            print("Sequential mode detected: skipping block caches to reduce memory.")
            bal_blocks_cache = None
            vel_blocks_cache = None
            cache_on_demand = False
        else:
            # Memory optimization: For large datasets, avoid pre-caching all block lists
            # Instead, cache-on-demand during processing to reduce peak memory
            if len(bal_map) > 1_000_000 or len(vel_map) > 1_000_000:
                print("Large dataset detected. Using lazy block cache to reduce memory peak...")
                bal_blocks_cache = {}
                vel_blocks_cache = {}
                # Pre-populate only as workers request (deferred initialization)
                cache_on_demand = True
            else:
                print("Preparing block caches for fast lookups...")
                bal_blocks_cache = {addr: [b for b, _ in series] for addr, series in bal_map.items()}
                vel_blocks_cache = {addr: [b for b, _ in series] for addr, series in vel_map.items()}
                cache_on_demand = False
        
        # Process blocks
        from tqdm import tqdm
        
        if args.n_workers > 1:
            # Check parallel mode compatibility and auto-select based on dataset size
            if args.parallel_mode == "shared" and not SHARED_MEMORY_AVAILABLE:
                print("Warning: Shared memory not available (requires Python 3.8+). Falling back to 'batched' mode.")
                args.parallel_mode = "batched"
            
            # For very large datasets with many addresses, prefer batched mode to reduce memory peak
            dataset_size = len(bal_map) + len(vel_map)
            if args.parallel_mode == "shared" and dataset_size > 2_000_000 and cache_on_demand:
                print(f"Warning: Dataset very large ({dataset_size:,} addresses). Shared mode with lazy block cache may still be slow.")
                print("Recommend: Use 'batched' mode with more blocks-per-batch for large datasets.")
                print("Proceeding with shared mode (lazy). Monitor memory usage.")
            
            if args.parallel_mode == "shared":
                # Shared memory mode: data loaded once, all workers access same memory
                print(f"Processing {len(block_numbers)} blocks with {args.n_workers} workers (shared memory mode)...")
                print("(Workers access shared data - no serialization overhead)")
                
                # Use ProcessPoolExecutor with initializer to set up shared references
                with ProcessPoolExecutor(max_workers=args.n_workers, 
                                        initializer=_init_shared_worker,
                                        initargs=(bal_map, vel_map, bal_blocks_cache, vel_blocks_cache)) as executor:
                    with tqdm(total=len(block_numbers), desc="Processing blocks", position=0) as pbar:
                        # Submit initial batch of work
                        futures = {}
                        block_idx = 0
                        
                        # Fill the worker pool with initial jobs
                        while block_idx < min(args.n_workers, len(block_numbers)):
                            block = block_numbers[block_idx]
                            future = executor.submit(_process_single_block_shared,
                                                    (block, args.min_denominator))
                            futures[future] = block
                            block_idx += 1
                        
                        # Process results and submit new work as workers complete
                        while futures:
                            # Wait for at least one future to complete
                            done, _ = as_completed(futures).__next__(), None
                            
                            # Get result and clean up
                            block, balances_xsect, velocities_xsect, ratios_xsect = done.result()
                            futures.pop(done)
                            pbar.update(1)
                            
                            # Save to files
                            balance_file = os.path.join(args.output_folder, f"xsect_balance_block_{block}.pickle")
                            velocity_file = os.path.join(args.output_folder, f"xsect_velocity_block_{block}.pickle")
                            ratio_file = os.path.join(args.output_folder, f"xsect_ratio_block_{block}.pickle")
                            
                            with open(balance_file, 'wb') as f:
                                pickle.dump(balances_xsect, f)
                            with open(velocity_file, 'wb') as f:
                                pickle.dump(velocities_xsect, f)
                            with open(ratio_file, 'wb') as f:
                                pickle.dump(ratios_xsect, f)
                            
                            if not balances_xsect and not velocities_xsect and not ratios_xsect:
                                print(f"Warning: All cross-sections empty at block {block}.")
                            
                            # Submit new work if blocks remain
                            if block_idx < len(block_numbers):
                                block = block_numbers[block_idx]
                                future = executor.submit(_process_single_block_shared,
                                                        (block, args.min_denominator))
                                futures[future] = block
                                block_idx += 1
            
            else:  # batched mode
                # Batched mode: group blocks to reduce pickling overhead
                batch_size = args.blocks_per_batch
                block_batches = [block_numbers[i:i+batch_size] for i in range(0, len(block_numbers), batch_size)]
                
                print(f"Processing {len(block_numbers)} blocks in {len(block_batches)} batches with {args.n_workers} workers...")
                print(f"({batch_size} blocks per batch to reduce serialization overhead)")
                
                with ProcessPoolExecutor(max_workers=args.n_workers) as executor:
                    with tqdm(total=len(block_batches), desc="Processing batches", position=0) as pbar:
                        # Submit initial batch of work
                        futures = {}
                        batch_idx = 0
                        
                        # Fill the worker pool with initial jobs
                        while batch_idx < min(args.n_workers, len(block_batches)):
                            block_batch = block_batches[batch_idx]
                            # Position starts from 1 (position 0 is main progress bar)
                            worker_position = (batch_idx % args.n_workers) + 1
                            future = executor.submit(_process_block_batch,
                                                    (block_batch, bal_map, vel_map, args.min_denominator,
                                                     bal_blocks_cache, vel_blocks_cache, worker_position, batch_idx))
                            futures[future] = (block_batch, worker_position)
                            batch_idx += 1
                        
                        # Process results and submit new work as workers complete
                        while futures:
                            # Wait for at least one future to complete
                            done, _ = as_completed(futures).__next__(), None
                            
                            # Get batch results and clean up
                            batch_results = done.result()
                            completed_batch, worker_position = futures.pop(done)
                            pbar.update(1)
                            
                            # Process each block in the batch
                            for block, balances_xsect, velocities_xsect, ratios_xsect in batch_results:
                                # Save to files
                                balance_file = os.path.join(args.output_folder, f"xsect_balance_block_{block}.pickle")
                                velocity_file = os.path.join(args.output_folder, f"xsect_velocity_block_{block}.pickle")
                                ratio_file = os.path.join(args.output_folder, f"xsect_ratio_block_{block}.pickle")
                                
                                with open(balance_file, 'wb') as f:
                                    pickle.dump(balances_xsect, f)
                                with open(velocity_file, 'wb') as f:
                                    pickle.dump(velocities_xsect, f)
                                with open(ratio_file, 'wb') as f:
                                    pickle.dump(ratios_xsect, f)
                                
                                if not balances_xsect and not velocities_xsect and not ratios_xsect:
                                    print(f"Warning: All cross-sections empty at block {block}.")
                            
                            # Submit new work to the same worker position if batches remain
                            if batch_idx < len(block_batches):
                                block_batch = block_batches[batch_idx]
                                future = executor.submit(_process_block_batch,
                                                        (block_batch, bal_map, vel_map, args.min_denominator,
                                                         bal_blocks_cache, vel_blocks_cache, worker_position, batch_idx))
                                futures[future] = (block_batch, worker_position)
                                batch_idx += 1
        else:
            # Sequential processing mode (optimized with single-pass and cached lookups)
            print(f"Processing {len(block_numbers)} blocks sequentially...")
            for block in tqdm(block_numbers, desc="Processing blocks"):
                block, balances_xsect, velocities_xsect, ratios_xsect = _process_single_block(
                    (block, bal_map, vel_map, args.min_denominator, bal_blocks_cache, vel_blocks_cache)
                )
                
                # Save to files
                balance_file = os.path.join(args.output_folder, f"xsect_balance_block_{block}.pickle")
                velocity_file = os.path.join(args.output_folder, f"xsect_velocity_block_{block}.pickle")
                ratio_file = os.path.join(args.output_folder, f"xsect_ratio_block_{block}.pickle")
                
                with open(balance_file, 'wb') as f:
                    pickle.dump(balances_xsect, f)
                with open(velocity_file, 'wb') as f:
                    pickle.dump(velocities_xsect, f)
                with open(ratio_file, 'wb') as f:
                    pickle.dump(ratios_xsect, f)
                
                if not balances_xsect and not velocities_xsect and not ratios_xsect:
                    print(f"Warning: All cross-sections empty at block {block}.\n"
                          f"This often indicates the requested block is outside the data range\n"
                          f"or there are no non-zero values at that snapshot.")
        
        print(f"Cross-sections saved to {args.output_folder}")



if __name__ == "__main__":
    main()
