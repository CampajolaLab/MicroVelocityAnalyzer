"""
MicroVelocityAnalyzer - Blockchain Token Velocity Analysis Tool

This module analyzes blockchain token transactions to calculate:
1. Account balances over time
2. Token velocity (rate of token movement) using LIFO (Last-In-First-Out) accounting

The analyzer processes token allocations and transfers to understand how quickly
tokens move through the economy, which is a key metric for token economics analysis.
"""

import argparse
import os
import pickle
import numpy as np
import csv
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed



def process_chunk_balances_v2(args):
    """
    Calculate account balances for a chunk of addresses (parallel worker function).
    
    This function processes a subset of addresses to compute their balance at regular
    checkpoint blocks. For each checkpoint, it applies all balance changes that
    occurred up to and including that block, ensuring balances are correctly carried
    forward between transactions.
    
    Args:
        args: Tuple containing (addresses, accounts_chunk, min_block_number,
              max_block_number, save_every_n, LIMIT, pos)
    

    Returns:
        dict: Maps address -> list of (block, balance) tuples (sparse storage, only when balance changes)
    """
    addresses, accounts_chunk, min_block_number, max_block_number, save_every_n, LIMIT, pos = args

    # Precompute checkpoint block numbers where balances will be recorded
    save_block_numbers = [min_block_number + i * save_every_n for i in range(LIMIT)]

    results = {}

    for address in tqdm(addresses, position=pos, leave=False):
        current_balance = 0
        sparse_balances = []  # List of (block, balance) tuples

        # Collect all balance changes as (block_number, delta_amount)
        balance_changes = []
        for block_number, amount in accounts_chunk[address][0].items():
            balance_changes.append((int(block_number), int(amount)))  # assets: +amount
        for block_number, amount in accounts_chunk[address][1].items():
            balance_changes.append((int(block_number), -int(amount)))  # liabilities: -amount

        # Sort changes chronologically
        balance_changes.sort(key=lambda x: x[0])

        change_idx = 0
        for i, checkpoint_block in enumerate(save_block_numbers):
            old_balance = current_balance
            while change_idx < len(balance_changes) and balance_changes[change_idx][0] <= checkpoint_block:
                current_balance += balance_changes[change_idx][1]
                change_idx += 1
            # Only store if balance changed or first checkpoint
            if current_balance != old_balance or i == 0:
                sparse_balances.append((checkpoint_block, current_balance))

        results[address] = sparse_balances

    return results



def process_chunk_velocities(args):
    """
    Calculate token velocities for a chunk of addresses using LIFO matching (parallel worker function).
    
    Token velocity measures how fast tokens move through addresses. This function uses
    LIFO (Last-In-First-Out) accounting to match outgoing transactions with incoming ones,
    calculating velocity as: amount / duration (where duration is blocks between receipt and send).
    
    The LIFO approach assumes that the most recently received tokens are spent first,
    which makes economic sense by effectively separating savings from spending.
    
    This version stores velocities ONLY at transaction blocks (incoming/outgoing), making
    save_every_n irrelevant for velocity calculation. Velocity changes only when transactions
    occur, so this approach maximally compresses storage without losing information.
    
    Args:
        args: Tuple containing (addresses, accounts_chunk, min_block_number, 
              save_every_n, LIMIT, pos)
              Note: save_every_n is not used for velocity calculation
    
    Returns:
        dict: Maps address -> list of (block, velocity) tuples at transaction blocks only
    """
    addresses, accounts_chunk, min_block_number, save_every_n, LIMIT, pos = args
    results = {}
    
    for address in tqdm(addresses, position=pos, leave=False):
        # Only calculate velocity if address has both incoming and outgoing transactions
        if len(accounts_chunk[address][0]) > 0 and len(accounts_chunk[address][1]) > 0:
            # Get sorted lists of liability (outgoing) block numbers
            sorted_out_blocks = sorted(list(accounts_chunk[address][1].keys()))

            # Track velocity contributions at each transaction block
            # We'll accumulate velocity changes and then compute running totals
            velocity_changes = {}  # block -> delta_velocity
            
            # Collect all transaction blocks where velocity might change
            all_tx_blocks = set(accounts_chunk[address][0].keys()) | set(accounts_chunk[address][1].keys())

            # Process each outgoing transaction (liability)
            for outgoing_block in sorted_out_blocks:
                # Refresh list of available incoming transactions (assets)
                sorted_in_blocks = sorted(list(accounts_chunk[address][0].keys()))

                # Find all incoming transactions that occurred before this outgoing transaction (LIFO order)
                # Iterate from most recent to oldest
                for incoming_block in reversed(sorted_in_blocks):
                    if incoming_block >= outgoing_block:
                        continue  # Skip future incoming transactions

                    # Keep as integers for exact arithmetic (Python arbitrary precision)
                    incoming_amount = int(accounts_chunk[address][0][incoming_block])
                    outgoing_amount = int(accounts_chunk[address][1][outgoing_block])

                    # Case 1: Incoming amount covers (or exceeds) the outgoing amount
                    if (incoming_amount - outgoing_amount) >= 0:
                        if incoming_block == outgoing_block:
                            # Through transaction - just reduce the net incoming amount
                            accounts_chunk[address][0][incoming_block] -= outgoing_amount
                            accounts_chunk[address][1].pop(outgoing_block)
                            break
                        else:
                            # Calculate velocity: amount / time duration
                            duration = outgoing_block - incoming_block
                            if duration > 0:
                                velocity_contrib = float(outgoing_amount) / float(duration)
                                # Add velocity at incoming block (when tokens start moving)
                                velocity_changes[incoming_block] = velocity_changes.get(incoming_block, 0.0) + velocity_contrib
                                # Subtract velocity at outgoing block (when tokens stop moving)
                                velocity_changes[outgoing_block] = velocity_changes.get(outgoing_block, 0.0) - velocity_contrib

                            # Update remaining amounts
                            accounts_chunk[address][0][incoming_block] -= outgoing_amount
                            accounts_chunk[address][1].pop(outgoing_block)
                            break

                    # Case 2: Incoming amount is less than outgoing - partial match
                    else:
                        if incoming_block == outgoing_block:
                            # Through transaction - reduce liability and consume entire asset
                            accounts_chunk[address][1][outgoing_block] -= incoming_amount
                            accounts_chunk[address][0].pop(incoming_block)
                        else:
                            # Calculate velocity for the partial amount
                            duration = outgoing_block - incoming_block
                            if duration > 0:
                                velocity_contrib = float(incoming_amount) / float(duration)
                                # Add velocity at incoming block (when tokens start moving)
                                velocity_changes[incoming_block] = velocity_changes.get(incoming_block, 0.0) + velocity_contrib
                                # Subtract velocity at outgoing block (when tokens stop moving)
                                velocity_changes[outgoing_block] = velocity_changes.get(outgoing_block, 0.0) - velocity_contrib

                            # Update remaining amounts and continue to next incoming transaction
                            accounts_chunk[address][1][outgoing_block] -= incoming_amount
                            accounts_chunk[address][0].pop(incoming_block)
            
            # Convert velocity_changes to running total sparse list
            if velocity_changes:
                sorted_blocks = sorted(velocity_changes.keys())
                sparse_velocities = []
                running_velocity = 0.0
                
                for block in sorted_blocks:
                    running_velocity += velocity_changes[block]
                    # Only store if velocity is non-zero (or if it just became zero)
                    if running_velocity != 0.0 or sparse_velocities:
                        sparse_velocities.append((block, running_velocity))
                
                results[address] = sparse_velocities
            else:
                results[address] = []
    
    return results



class MicroVelocityAnalyzer:
    """
    Analyzes blockchain token transactions to calculate balances and velocities.
    
    This class processes token allocation and transfer data to compute:
    1. Account balances over time at regular block intervals
    2. Token velocity using LIFO (Last-In-First-Out) accounting
    
    Data Structure:
        accounts[address] = [assets_dict, liabilities_dict]
        - assets_dict: {block_number: amount} for tokens received
        - liabilities_dict: {block_number: amount} for tokens sent
    
    Attributes:
        allocated_file (str): Path to CSV file with initial token allocations
        transfers_file (str): Path to CSV file with token transfers
        output_file (str): Path where results will be saved (pickle format)
        save_every_n (int): Block interval for saving checkpoints (e.g., 1 = every block)
        n_cores (int): Number of CPU cores for parallel processing
        n_chunks (int): Number of chunks to split addresses into
        split_save (bool): If True, save intermediate results to separate files
        batch_size (int): Number of chunks to process per batch
    """
    
    def __init__(self, allocated_file, transfers_file, output_file='temp/general_velocities.pickle', 
                 save_every_n=1, n_cores=1, n_chunks=1, split_save=False, batch_size=1):
        self.allocated_file = allocated_file
        self.transfers_file = transfers_file
        self.output_file = output_file
        self.save_every_n = save_every_n
        self.n_cores = n_cores
        self.n_chunks = n_chunks
        self.split_save = split_save
        self.batch_size = batch_size
        
        # Main data structures
        self.accounts = {}  # {address: [{block: amount}, {block: amount}]} for assets/liabilities
        self.backup_accounts = {}  # Copy of accounts before velocity calculation modifies it
        
        # Block range tracking
        self.min_block_number = float('inf')
        self.max_block_number = float('-inf')
        
        # Results
        self.velocities = {}  # {address: numpy array of velocities}
        self.balances = {}  # {address: numpy array of balances}
        self.LIMIT = 0  # Number of time intervals (checkpoints)
        
        self._create_output_folder()

    def _create_output_folder(self):
        """Create output directory if it doesn't exist."""
        output_folder = os.path.dirname(self.output_file)
        if output_folder and not os.path.exists(output_folder):
            os.makedirs(output_folder)

    
    def load_allocated_data(self):
        """
        Load initial token allocation data from CSV file.
        
        Processes the allocated_file CSV which contains initial token distributions
        (e.g., minting events, ICO allocations). Updates the accounts dictionary
        and tracks the block number range.
        
        Expected CSV columns: to_address, amount, block_number
        """
        with open(self.allocated_file, 'r') as file:
            reader = csv.DictReader(file)
            for line in tqdm(reader):
                self._process_allocation(line)

    def _process_allocation(self, line):
        """
        Process a single allocation record from CSV.
        
        Args:
            line (dict): CSV row with keys: to_address, amount, block_number
        """
        to_address = line['to_address'].lower()
        try:
            # Parse amount as integer to maintain exact precision for large values
            amount = int(line['amount'])
            block_number = int(line['block_number'])
        except ValueError:
            print(f"Invalid data in allocated_file: {line}")
            return  # Skip invalid records

        # Initialize account structure if new address
        if to_address not in self.accounts:
            self.accounts[to_address] = [{}, {}]  # [assets, liabilities]
        
        # Add to assets (index 0) for this address at this block
        if block_number not in self.accounts[to_address][0]:
            self.accounts[to_address][0][block_number] = amount
        else:
            self.accounts[to_address][0][block_number] += amount

        # Update block range
        self.min_block_number = min(self.min_block_number, block_number)
        self.max_block_number = max(self.max_block_number, block_number)

    def load_transfer_data(self):
        """
        Load token transfer data from CSV file.
        
        Processes the transfers_file CSV which contains peer-to-peer token transfers.
        Each transfer creates an asset for the recipient and a liability for the sender.
        Updates the accounts dictionary and tracks the block number range.
        
        Expected CSV columns: from_address, to_address, amount, block_number
        """
        with open(self.transfers_file, 'r') as file:
            reader = csv.DictReader(file)
            for line in tqdm(reader):
                self._process_transfer(line)

    def _process_transfer(self, line):
        """
        Process a single transfer record from CSV.
        
        Args:
            line (dict): CSV row with keys: from_address, to_address, amount, block_number
        """
        from_address = line['from_address'].lower()
        to_address = line['to_address'].lower()
        try:
            # Parse amount as integer to maintain exact precision for large values
            amount = int(line['amount'])
            block_number = int(line['block_number'])
        except ValueError:
            print(f"Invalid data in transfers_file: {line}")
            return  # Skip invalid records

        # Record asset for recipient (to_address)
        if to_address not in self.accounts:
            self.accounts[to_address] = [{}, {}]
        if block_number not in self.accounts[to_address][0]:
            self.accounts[to_address][0][block_number] = amount
        else:
            self.accounts[to_address][0][block_number] += amount

        # Record liability for sender (from_address)
        if from_address not in self.accounts:
            self.accounts[from_address] = [{}, {}]
        if block_number not in self.accounts[from_address][1]:
            self.accounts[from_address][1][block_number] = amount
        else:
            self.accounts[from_address][1][block_number] += amount

        # Update block range
        self.min_block_number = min(self.min_block_number, block_number)
        self.max_block_number = max(self.max_block_number, block_number)



    def _get_split_filename(self, base_type, last_address):
        """
        Generate filename for split save functionality.
        
        Args:
            base_type (str): Type of data ('balances' or 'velocities')
            last_address (str): Last address in the chunk being saved
            
        Returns:
            str: Full path for the split file
        """
        dirname = os.path.dirname(self.output_file)
        basename = os.path.splitext(os.path.basename(self.output_file))[0]
        return os.path.join(dirname, f"{basename}_{base_type}_{last_address}.pickle")

    def _save_split_results(self, results, result_type, last_address):
        """
        Save intermediate results to a split file.
        
        Used when processing large datasets to avoid memory issues by saving
        results incrementally rather than holding everything in memory.
        
        Args:
            results (dict): Results to save
            result_type (str): Type of data ('balances' or 'velocities')
            last_address (str): Last address in this batch
        """
        filename = self._get_split_filename(result_type, last_address)
        print(f'Saving {result_type} to {filename}')
        with open(filename, 'wb') as f:
            pickle.dump(results, f)

    def calculate_balances_parallel(self):
        """
        Calculate account balances using parallel processing with rolling queue.
        
        Distributes addresses across multiple worker processes to calculate balances
        at regular block intervals. Uses a rolling queue where new chunks are submitted
        as soon as a worker finishes, eliminating idle time from waiting for slow workers.
        
        The parallel execution model:
        1. Split addresses into n_chunks
        2. Keep up to n_cores workers busy at all times
        3. Submit new work as soon as a worker completes
        4. Optionally save intermediate results to avoid memory overflow
        """
        addresses = list(self.accounts.keys())
        
        # Shuffle addresses unless using split_save (which needs deterministic order)
        if not self.split_save:
            np.random.shuffle(addresses)
            
        # Divide addresses into chunks for parallel processing
        chunk_size = max(1, len(addresses) // self.n_chunks)
        chunks = [addresses[i:(i + chunk_size)] for i in range(0, len(addresses), chunk_size)]
        
        total_chunks = len(chunks)
        batch_results = {}
        
        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
                # Submit initial batch of work (up to n_cores chunks)
                futures = {}
                chunk_idx = 0
                
                # Fill the worker pool with initial jobs
                while chunk_idx < min(self.n_cores, total_chunks):
                    chunk = chunks[chunk_idx]
                    accounts_chunk = {address: self.accounts[address] for address in chunk}
                    args = (chunk, accounts_chunk, self.min_block_number, 
                           self.max_block_number, self.save_every_n, 
                           self.LIMIT, (chunk_idx % self.n_cores) + 1)
                    future = executor.submit(process_chunk_balances_v2, args)
                    futures[future] = chunk
                    chunk_idx += 1
                
                # Process results and submit new work as workers complete
                while futures:
                    # Wait for at least one future to complete
                    done, _ = as_completed(futures).__next__(), None
                    
                    # Get result and clean up
                    chunk_results = done.result()
                    completed_chunk = futures.pop(done)
                    batch_results.update(chunk_results)
                    pbar.update(1)
                    
                    # Handle split_save if needed
                    if self.split_save and len(batch_results) >= self.batch_size * len(completed_chunk):
                        last_address = list(batch_results.keys())[-1]
                        self._save_split_results(batch_results, 'balances', last_address)
                        batch_results = {}
                    
                    # Submit new work if chunks remain
                    if chunk_idx < total_chunks:
                        chunk = chunks[chunk_idx]
                        accounts_chunk = {address: self.accounts[address] for address in chunk}
                        args = (chunk, accounts_chunk, self.min_block_number, 
                               self.max_block_number, self.save_every_n, 
                               self.LIMIT, (chunk_idx % self.n_cores) + 1)
                        future = executor.submit(process_chunk_balances_v2, args)
                        futures[future] = chunk
                        chunk_idx += 1
                
                # Save remaining results
                if not self.split_save and batch_results:
                    self.balances.update(batch_results)

    def calculate_velocities_parallel(self):
        """
        Calculate token velocities using parallel processing with LIFO matching and rolling queue.
        
        Distributes addresses across multiple worker processes to calculate velocities
        using LIFO (Last-In-First-Out) accounting. Uses a rolling queue where new chunks 
        are submitted as soon as a worker finishes, eliminating idle time from waiting 
        for slow workers.
        
        Note: This operation modifies the accounts dictionary as it matches and
        consumes assets/liabilities. The original accounts are preserved in
        backup_accounts for reference.
        """
        addresses = list(self.accounts.keys())
        
        # Shuffle addresses unless using split_save (which needs deterministic order)
        if not self.split_save:
            np.random.shuffle(addresses)
            
        # Divide addresses into chunks for parallel processing
        chunk_size = max(1, len(addresses) // self.n_chunks)
        chunks = [addresses[i:(i + chunk_size)] for i in range(0, len(addresses), chunk_size)]
        
        total_chunks = len(chunks)
        batch_results = {}
        
        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
                # Submit initial batch of work (up to n_cores chunks)
                futures = {}
                chunk_idx = 0
                
                # Fill the worker pool with initial jobs
                while chunk_idx < min(self.n_cores, total_chunks):
                    chunk = chunks[chunk_idx]
                    accounts_chunk = {address: self.accounts[address] for address in chunk}
                    args = (chunk, accounts_chunk, self.min_block_number, 
                           self.save_every_n, self.LIMIT, (chunk_idx % self.n_cores) + 1)
                    future = executor.submit(process_chunk_velocities, args)
                    futures[future] = chunk
                    chunk_idx += 1
                
                # Process results and submit new work as workers complete
                while futures:
                    # Wait for at least one future to complete
                    done, _ = as_completed(futures).__next__(), None
                    
                    # Get result and clean up
                    chunk_results = done.result()
                    completed_chunk = futures.pop(done)
                    batch_results.update(chunk_results)
                    pbar.update(1)
                    
                    # Handle split_save if needed
                    if self.split_save and len(batch_results) >= self.batch_size * len(completed_chunk):
                        last_address = list(batch_results.keys())[-1]
                        self._save_split_results(batch_results, 'velocities', last_address)
                        batch_results = {}
                    
                    # Submit new work if chunks remain
                    if chunk_idx < total_chunks:
                        chunk = chunks[chunk_idx]
                        accounts_chunk = {address: self.accounts[address] for address in chunk}
                        args = (chunk, accounts_chunk, self.min_block_number, 
                               self.save_every_n, self.LIMIT, (chunk_idx % self.n_cores) + 1)
                        future = executor.submit(process_chunk_velocities, args)
                        futures[future] = chunk
                        chunk_idx += 1
                
                # Save remaining results
                if not self.split_save and batch_results:
                    self.velocities.update(batch_results)


    def save_balances(self):
        """
        Save balances to pickle file and clear from memory.
        """
        if self.split_save:
            return
        else:
            balances_file = self.output_file.replace('.pickle', '_balances.pickle')
            with open(balances_file, 'wb') as file:
                pickle.dump(self.balances, file)
            print(f"Balances saved to {balances_file}. Clearing balances from memory.")
            self.balances = {}  # Free memory

    def save_velocities(self):
        """
        Save velocities to pickle file.
        """
        if self.split_save:
            return
        else:
            velocities_file = self.output_file.replace('.pickle', '_velocities.pickle')
            with open(velocities_file, 'wb') as file:
                pickle.dump(self.velocities, file)
            print(f"Velocities saved to {velocities_file}.")

    def run_analysis(self, balances_only=False, velocities_only=False):
        """
        Execute the complete analysis pipeline.
        
        Pipeline steps:
        1. Load allocated data (initial distributions)
        2. Load transfer data (peer-to-peer transfers)
        3. Calculate block range and number of checkpoints
        4. Backup accounts (velocity calculation modifies them)
        5. Calculate balances at checkpoints (unless velocities_only)
        6. Save balances and clear from memory (unless velocities_only)
        7. Calculate velocities using LIFO matching (unless balances_only)
        8. Save velocities to file(s) (unless balances_only)
        
        Args:
            balances_only (bool): If True, calculate and save only balances
            velocities_only (bool): If True, calculate and save only velocities
        """
        print("Loading allocated data...", self.allocated_file)
        self.load_allocated_data()
        
        print("Loading transfer data...", self.transfers_file)
        self.load_transfer_data()
        
        print("Computing interval of", self.save_every_n, "blocks")
        print(f"Min block number: {self.min_block_number}")
        print(f"Max block number: {self.max_block_number}")
        
        # Calculate number of checkpoints in the analysis
        self.LIMIT = (self.max_block_number - self.min_block_number) // self.save_every_n + 1
        
        # Create backup before velocity calculation (which modifies accounts)
        self.backup_accounts = self.accounts.copy()
        
        print(f"Number of blocks considered: {self.LIMIT}")
        
        if not velocities_only:
            print("Calculating balances...")
            self.calculate_balances_parallel()
            
            print("Saving balances and clearing from memory...")
            self.save_balances()
        
        if not balances_only:
            print("Calculating velocities...")
            self.calculate_velocities_parallel()
            
            print("Saving velocities...")
            self.save_velocities()
        
        print("Done!")


def main():
    """
    Command-line interface for MicroVelocityAnalyzer.
    
    Parse arguments and run the analysis with specified parameters.
    """
    parser = argparse.ArgumentParser(description='Micro Velocity Analyzer')
    parser.add_argument('--allocated_file', type=str, default='sampledata/sample_allocated.csv', 
                       help='Path to the allocated CSV file')
    parser.add_argument('--transfers_file', type=str, default='sampledata/sample_transfers.csv', 
                       help='Path to the transfers CSV file')
    parser.add_argument('--output_file', type=str, default='sampledata/general_velocities.pickle', 
                       help='Path to the output file')
    parser.add_argument('--save_every_n', type=int, default=1, 
                       help='Save every Nth position of the velocity array')
    parser.add_argument('--n_cores', type=int, default=1, 
                       help='Number of cores to use')
    parser.add_argument('--n_chunks', type=int, default=1, 
                       help='Number of chunks to split the data into (must be >= n_cores)')
    parser.add_argument('--split_save', action='store_true', default=False, 
                       help='Split the save into different files')
    parser.add_argument('--batch_size', type=int, default=1, 
                       help='Number of chunks to process in a single batch')
    parser.add_argument('--balances_only', action='store_true', default=False,
                       help='Calculate and save only balances (skip velocities)')
    parser.add_argument('--velocities_only', action='store_true', default=False,
                       help='Calculate and save only velocities (skip balances)')
    args = parser.parse_args()
    
    # Validate mutually exclusive options
    if args.balances_only and args.velocities_only:
        parser.error("--balances_only and --velocities_only are mutually exclusive")

    analyzer = MicroVelocityAnalyzer(
        allocated_file=args.allocated_file,
        transfers_file=args.transfers_file,
        output_file=args.output_file,
        save_every_n=args.save_every_n,
        n_cores=args.n_cores,
        n_chunks=args.n_chunks,
        split_save=args.split_save,
        batch_size=args.batch_size
    )
    
    # Run analysis with optional flags
    analyzer.run_analysis(
        balances_only=args.balances_only,
        velocities_only=args.velocities_only
    )


if __name__ == "__main__":
    main()

