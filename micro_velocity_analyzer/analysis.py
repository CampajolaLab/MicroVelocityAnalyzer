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
from bisect import bisect_right
from statistics import mean
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

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
            blk = next_blk_bal  # type: ignore
            curr_bal = balance_sparse[i][1]
            i += 1
        else:
            blk = next_blk_vel  # type: ignore
            curr_vel = velocity_sparse[j][1]
            j += 1

        # Emit ratio if denominator ok
        if isinstance(curr_bal, int) and curr_bal >= min_denominator:
            out.append((int(blk), float(curr_vel) / float(curr_bal)))  # type: ignore

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
            vel = _value_at_block(vel_series, block)
            yield addr, float(vel) / float(bal)


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


def main():
    parser = argparse.ArgumentParser(description="Sparse analysis for MicroVelocityAnalyzer results")
    parser.add_argument("--balances", required=True, help="Path to balances pickle (single file) or directory")
    parser.add_argument("--velocities", required=True, help="Path to velocities pickle (single file) or directory")
    sub = parser.add_subparsers(dest="cmd", required=True)

    cs = sub.add_parser("cross-section", help="Compute cross-sectional ratio stats at a block")
    cs.add_argument("--block", type=int, required=True, help="Block number for snapshot")
    cs.add_argument("--min-denominator", type=int, default=1, help="Minimum balance to include in ratio")

    lg = sub.add_parser("longitudinal", help="Emit sparse ratio series for a single address")
    lg.add_argument("--address", required=True, help="Address to analyze")
    lg.add_argument("--min-denominator", type=int, default=1)
    
    batch = sub.add_parser("batch-cross-section", help="Extract cross-sections at multiple blocks and save to files")
    batch.add_argument("--blocks-file", required=True, help="Path to pickle file containing list of block numbers")
    batch.add_argument("--output-folder", required=True, help="Folder where cross-section pickles will be saved")
    batch.add_argument("--min-denominator", type=int, default=1, help="Minimum balance to include in ratio")

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
        
        # Process each block
        from tqdm import tqdm
        for block in tqdm(block_numbers, desc="Processing blocks"):
            # Extract cross-sectional balances
            balances_xsect = {}
            for addr in bal_map.keys():
                bal_series = bal_map.get(addr, [])
                bal = _value_at_block(bal_series, block)
                if bal != 0:  # Only store non-zero balances
                    balances_xsect[addr] = bal
            
            # Extract cross-sectional velocities
            velocities_xsect = {}
            for addr in vel_map.keys():
                vel_series = vel_map.get(addr, [])
                vel = _value_at_block(vel_series, block)
                if vel != 0:  # Only store non-zero velocities
                    velocities_xsect[addr] = vel
            
            # Extract cross-sectional ratios (velocity/balance)
            ratios_xsect = {}
            for addr in bal_map.keys():
                bal_series = bal_map.get(addr, [])
                vel_series = vel_map.get(addr, [])
                bal = _value_at_block(bal_series, block)
                if isinstance(bal, int) and bal >= args.min_denominator:
                    vel = _value_at_block(vel_series, block)
                    ratios_xsect[addr] = float(vel) / float(bal)
            
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
        
        print(f"Cross-sections saved to {args.output_folder}")



if __name__ == "__main__":
    main()
