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
    block intervals. It iterates through all blocks where balance changes occur and
    updates the running balance accordingly.
    
    Args:
        args: Tuple containing (addresses, accounts_chunk, min_block_number, 
              max_block_number, save_every_n, LIMIT, pos)
    
    Returns:
        dict: Maps address -> numpy array of balances at each checkpoint
    """
    addresses, accounts_chunk, min_block_number, max_block_number, save_every_n, LIMIT, pos = args
    
    # Calculate checkpoint block numbers where balances will be saved
    save_block_numbers = [min_block_number + i * save_every_n for i in range(LIMIT)]

    results = {}

    for address in tqdm(addresses, position=pos, leave=False):
        current_balance = 0.0
        
        # Get all unique block numbers where this address had transactions (assets or liabilities)
        block_numbers = set(accounts_chunk[address][0].keys()).union(set(accounts_chunk[address][1].keys()))
        block_numbers = sorted(block_numbers)

        # Initialize balance array for all checkpoints
        balances = np.zeros(len(save_block_numbers), dtype=np.uint256)
        
        # Process each block where transactions occurred
        for block in block_numbers:
            # Add incoming tokens (assets)
            if block in accounts_chunk[address][0]:
                current_balance += accounts_chunk[address][0][block]
            
            # Subtract outgoing tokens (liabilities)
            if block in accounts_chunk[address][1]:
                current_balance -= accounts_chunk[address][1][block]
            
            # Calculate which checkpoint index this block corresponds to
            idx = (block - min_block_number) // save_every_n + 1
            if idx < len(balances):
                balances[idx] = current_balance

        results[address] = balances
        del balances
    
    return results



def process_chunk_velocities(args):
    """
    Calculate token velocities for a chunk of addresses using LIFO matching (parallel worker function).
    
    Token velocity measures how fast tokens move through addresses. This function uses
    LIFO (Last-In-First-Out) accounting to match outgoing transactions with incoming ones,
    calculating velocity as: amount / duration (where duration is blocks between receipt and send).
    
    The LIFO approach assumes that the most recently received tokens are spent first,
    which makes economic sense by effectively separating savings from spending.
    
    Args:
        args: Tuple containing (addresses, accounts_chunk, min_block_number, 
              save_every_n, LIMIT, pos)
    
    Returns:
        dict: Maps address -> numpy array of velocities at each checkpoint
    """
    addresses, accounts_chunk, min_block_number, save_every_n, LIMIT, pos = args
    results = {}
    
    for address in tqdm(addresses, position=pos, leave=False):
        # Only calculate velocity if address has both incoming and outgoing transactions
        if len(accounts_chunk[address][0]) > 0 and len(accounts_chunk[address][1]) > 0:
            # Get sorted lists of asset (incoming) and liability (outgoing) block numbers
            block_numbers_by_type = [list(accounts_chunk[address][0].keys()), list(accounts_chunk[address][1].keys())]
            block_numbers_by_type[0].sort()
            block_numbers_by_type[1].sort()

            # Initialize velocity array for all time intervals
            ind_velocity = np.zeros(LIMIT, dtype=np.float64)

            # Process each outgoing transaction (liability)
            for outgoing_block in block_numbers_by_type[1]:
                # Refresh list of available incoming transactions (assets)
                block_numbers_by_type[0] = list(accounts_chunk[address][0].keys())
                incoming_blocks = np.array(block_numbers_by_type[0], dtype=int)

                # Find all incoming transactions that occurred before this outgoing transaction
                # Iterate through them in LIFO order (most recent first)
                for i in range(0, len(incoming_blocks[incoming_blocks < outgoing_block])):
                    # LIFO: Select from end of array (most recent incoming transaction)
                    incoming_block = incoming_blocks[incoming_blocks < outgoing_block][(len(incoming_blocks[incoming_blocks < outgoing_block]) - 1) - i]

                    incoming_amount = float(accounts_chunk[address][0][incoming_block])
                    outgoing_amount = float(accounts_chunk[address][1][outgoing_block])

                    # Case 1: Incoming amount covers (or exceeds) the outgoing amount
                    if (incoming_amount - outgoing_amount) >= 0:
                        # Calculate which time intervals this token movement spans
                        idx_range = np.unique(np.arange(incoming_block - min_block_number, outgoing_block - min_block_number)//save_every_n)

                        if len(idx_range) == 1:
                            # Same interval - just reduce the asset
                            accounts_chunk[address][0][incoming_block] -= outgoing_amount
                            accounts_chunk[address][1].pop(outgoing_block)
                            break
                        else:
                            # Calculate velocity: amount / time duration
                            duration = outgoing_block - incoming_block
                            if duration > 0:
                                ind_velocity[idx_range] += outgoing_amount / duration

                            # Update remaining amounts
                            accounts_chunk[address][0][incoming_block] -= outgoing_amount
                            accounts_chunk[address][1].pop(outgoing_block)
                            break

                    # Case 2: Incoming amount is less than outgoing - partial match
                    else:
                        # Calculate which time intervals this token movement spans
                        idx_range = np.unique(np.arange(incoming_block - min_block_number, outgoing_block - min_block_number)//save_every_n)

                        if len(idx_range) == 1:
                            # Same interval - reduce liability and consume entire asset
                            accounts_chunk[address][1][outgoing_block] -= incoming_amount
                            accounts_chunk[address][0].pop(incoming_block)
                        else:
                            # Calculate velocity for the partial amount
                            duration = outgoing_block - incoming_block
                            if duration > 0:
                                ind_velocity[idx_range] += incoming_amount / duration

                            # Update remaining amounts and continue to next incoming transaction
                            accounts_chunk[address][1][outgoing_block] -= incoming_amount
                            accounts_chunk[address][0].pop(incoming_block)
            results[address] = ind_velocity
    
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
            amount = float(line['amount'])
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
            amount = float(line['amount'])
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
        Calculate account balances using parallel processing.
        
        Distributes addresses across multiple worker processes to calculate balances
        at regular block intervals. Can optionally save results incrementally if
        split_save is enabled.
        
        The parallel execution model:
        1. Split addresses into n_chunks
        2. Process chunks in batches of (n_cores * batch_size)
        3. Optionally save intermediate results to avoid memory overflow
        """
        addresses = list(self.accounts.keys())
        
        # Shuffle addresses unless using split_save (which needs deterministic order)
        if not self.split_save:
            np.random.shuffle(addresses)
            
        # Divide addresses into chunks for parallel processing
        chunk_size = max(1, len(addresses) // self.n_chunks)
        chunks = [addresses[i:(i + chunk_size)] for i in range(0, len(addresses), chunk_size)]
        
        total_chunks = len(chunks)
        processed_chunks = 0
        batch_results = {}
        
        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            with tqdm(total=total_chunks, desc="Processing chunks") as pbar:
                while processed_chunks < total_chunks:
                    # Process batches to control memory usage
                    current_batch = chunks[processed_chunks:processed_chunks + self.n_cores*self.batch_size]
                    futures = []
                    
                    # Submit worker jobs for this batch
                    for i, chunk in enumerate(current_batch):
                        accounts_chunk = {address: self.accounts[address] for address in chunk}
                        args = (chunk, accounts_chunk, self.min_block_number, 
                               self.max_block_number, self.save_every_n, 
                               self.LIMIT, i + 1)
                        futures.append(executor.submit(process_chunk_balances_v2, args))
                    
                    # Collect results as they complete
                    for future in as_completed(futures):
                        chunk_results = future.result()
                        batch_results.update(chunk_results)
                        processed_chunks += 1
                        pbar.update(1)
                    
                    # Handle results based on split_save setting
                    if self.split_save:
                        last_address = current_batch[-1][-1]
                        self._save_split_results(batch_results, 'balances', last_address)
                        batch_results = {}  # Clear to free memory
                    else:
                        self.balances.update(batch_results)
                    
                    del futures

    def calculate_velocities_parallel(self):
        """
        Calculate token velocities using parallel processing with LIFO matching.
        
        Distributes addresses across multiple worker processes to calculate velocities
        using LIFO (Last-In-First-Out) accounting. Can optionally save results 
        incrementally if split_save is enabled.
        
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
        
        batch_results = {}
        processed_chunks = 0
        
        with ProcessPoolExecutor(max_workers=self.n_cores) as executor:
            with tqdm(total=len(chunks), desc="Processing chunks") as pbar:
                while processed_chunks < len(chunks):
                    # Process batches to control memory usage
                    current_batch = chunks[processed_chunks:processed_chunks + self.n_cores*self.batch_size]
                    futures = []
                    
                    # Submit worker jobs for this batch
                    for i, chunk in enumerate(current_batch):
                        accounts_chunk = {address: self.accounts[address] for address in chunk}
                        args = (chunk, accounts_chunk, self.min_block_number, 
                               self.save_every_n, self.LIMIT, i + 1)
                        futures.append(executor.submit(process_chunk_velocities, args))
                    
                    # Collect results as they complete
                    for future in as_completed(futures):
                        chunk_results = future.result()
                        batch_results.update(chunk_results)
                        processed_chunks += 1
                        pbar.update(1)
                    
                    # Handle results based on split_save setting
                    if self.split_save:
                        last_address = current_batch[-1][-1]
                        self._save_split_results(batch_results, 'velocities', last_address)
                        batch_results = {}  # Clear to free memory
                    else:
                        self.velocities.update(batch_results)
                    
                    del futures

    def save_results(self):
        """
        Save final results to pickle file.
        
        Saves a tuple containing:
        - backup_accounts: Original account data before velocity calculation
        - velocities: Calculated velocity arrays per address
        - balances: Calculated balance arrays per address
        
        Note: If split_save is enabled, this method does nothing as results
        are already saved incrementally.
        """
        if self.split_save:
            return
        else:
            with open(self.output_file, 'wb') as file:
                pickle.dump([self.backup_accounts, self.velocities, self.balances], file)

    def run_analysis(self):
        """
        Execute the complete analysis pipeline.
        
        Pipeline steps:
        1. Load allocated data (initial distributions)
        2. Load transfer data (peer-to-peer transfers)
        3. Calculate block range and number of checkpoints
        4. Backup accounts (velocity calculation modifies them)
        5. Calculate balances at checkpoints
        6. Calculate velocities using LIFO matching
        7. Save results to file(s)
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
        print("Calculating balances...")
        self.calculate_balances_parallel()
        
        print("Calculating velocities...")
        self.calculate_velocities_parallel()
        
        print("Saving results...")
        self.save_results()
        
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
    args = parser.parse_args()

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
    analyzer.run_analysis()


if __name__ == "__main__":
    main()

