"""
Deprecated functions from MicroVelocityAnalyzer.

These functions are kept for reference but are no longer used in the main codebase.
They have been replaced by more efficient parallel implementations.
"""

import numpy as np
from tqdm import tqdm


def process_chunk_balances(args):
    """
    DEPRECATED: Use process_chunk_balances_v2 instead.
    
    Original version of balance calculation that used float64 instead of uint256.
    This version is less memory efficient for large datasets.
    """
    addresses, accounts_chunk, min_block_number, max_block_number, save_every_n, LIMIT, pos = args
    results = {}
    save_block_numbers = [min_block_number + i * save_every_n for i in range(LIMIT)]
    for address in tqdm(addresses, position=pos, leave=False):
        # Collect all balance changes
        balance_changes = []
        for block_number, amount in accounts_chunk[address][0].items():
            balance_changes.append((int(block_number), float(amount)))
        for block_number, amount in accounts_chunk[address][1].items():
            balance_changes.append((int(block_number), -float(amount)))
        # Sort the balance changes by block number
        balance_changes.sort()
        # Initialize balance and index for balance changes
        balance = 0.0
        change_idx = 0
        balances = []
        # Iterate over save_block_numbers
        for block_number in save_block_numbers:
            # Apply all balance changes up to the current block_number
            while change_idx < len(balance_changes) and balance_changes[change_idx][0] <= block_number:
                balance += balance_changes[change_idx][1]
                change_idx += 1
            balances.append(balance)
        results[address] = np.array(balances, dtype=np.float64)
    
    del balance_changes, balances
    return results


def calculate_balances(self):
    """
    DEPRECATED: Use calculate_balances_parallel instead.
    
    Sequential (single-threaded) version of balance calculation.
    Kept for reference but parallel version is preferred even for single-core execution.
    """
    save_block_numbers = [self.min_block_number + i * self.save_every_n for i in range(self.LIMIT)]
    for address in tqdm(self.accounts.keys()):
        # Collect all balance changes
        balance_changes = []
        for block_number, amount in self.accounts[address][0].items():
            balance_changes.append((int(block_number), float(amount)))
        for block_number, amount in self.accounts[address][1].items():
            balance_changes.append((int(block_number), -float(amount)))
        # Sort the balance changes by block number
        balance_changes.sort()
        # Initialize balance and index for balance changes
        balance = 0.0
        change_idx = 0
        balances = []
        # Iterate over save_block_numbers
        for block_number in save_block_numbers:
            # Apply all balance changes up to the current block_number
            while change_idx < len(balance_changes) and balance_changes[change_idx][0] <= block_number:
                balance += balance_changes[change_idx][1]
                change_idx += 1
            balances.append(balance)
        self.balances[address] = np.array(balances, dtype=np.float64)


def calculate_velocities(self):
    """
    DEPRECATED: Use calculate_velocities_parallel instead.
    
    Sequential (single-threaded) version of velocity calculation using LIFO matching.
    Kept for reference but parallel version is preferred even for single-core execution.
    """
    for address in tqdm(self.accounts.keys()):
        if len(self.accounts[address][0]) > 0 and len(self.accounts[address][1]) > 0:
            self._calculate_individual_velocity(address)


def _calculate_individual_velocity(self, address):
    """
    DEPRECATED: Logic moved into process_chunk_velocities for parallel processing.
    
    Calculate velocity for a single address using LIFO (Last-In-First-Out) matching.
    Matches each outgoing transaction with the most recent incoming transactions.
    """
    arranged_keys = [list(self.accounts[address][0].keys()), list(self.accounts[address][1].keys())]
    arranged_keys[0].sort()
    arranged_keys[1].sort()
    ind_velocity = np.zeros(self.LIMIT, dtype=np.float64)

    for border in tqdm(arranged_keys[1], leave=False):
        arranged_keys[0] = list(self.accounts[address][0].keys())
        test = np.array(arranged_keys[0], dtype=int)

        for i in range(0, len(test[test < border])):
            counter = test[test < border][(len(test[test < border]) - 1) - i]
            asset_amount = float(self.accounts[address][0][counter])
            liability_amount = float(self.accounts[address][1][border])
            if (asset_amount - liability_amount) >= 0:
                idx_range = np.unique(np.arange(counter - self.min_block_number, border - self.min_block_number)//self.save_every_n)
                if len(idx_range) == 1:
                    self.accounts[address][0][counter] -= liability_amount
                    self.accounts[address][1].pop(border)
                    break
                else:
                    duration = border - counter
                    if duration > 0:
                        ind_velocity[idx_range] += liability_amount / duration
                    self.accounts[address][0][counter] -= liability_amount
                    self.accounts[address][1].pop(border)
                    break
            else:
                idx_range = np.unique(np.arange(counter - self.min_block_number, border - self.min_block_number)//self.save_every_n)
                if len(idx_range) == 1:
                    self.accounts[address][1][border] -= asset_amount
                    self.accounts[address][0].pop(counter)
                else:
                    duration = border - counter
                    if duration > 0:
                        ind_velocity[idx_range] += asset_amount / duration
                    self.accounts[address][1][border] -= asset_amount
                    self.accounts[address][0].pop(counter)
    self.velocities[address] = ind_velocity
