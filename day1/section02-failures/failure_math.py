#!/usr/bin/env python3
"""
Failure Probability Mathematics - Why Distribution Requires Failure Handling

This demonstrates the fundamental problem: as you scale out (add more disks/servers),
the probability of *something* failing increases dramatically.
"""

def disk_failure_probability():
    """
    Example: If probability of 1 disk failing is once in 3 years,
    what is the probability of that disk failing on any given day?
    If there are 10,000 disks, what is the probability of one or more disks failing in any given day?
    """
    
    print("=" * 70)
    print("DISK FAILURE PROBABILITY ANALYSIS")
    print("=" * 70)
    
    # Single disk failure rate
    mttf_years = 3  # Mean Time To Failure: 3 years
    days_per_year = 365
    mttf_days = mttf_years * days_per_year
    
    print(f"\n1. Single Disk Analysis:")
    print(f"   - Mean Time To Failure (MTTF): {mttf_years} years = {mttf_days} days")
    
    # Daily failure probability for a single disk
    p_fail_single = 1 / mttf_days
    p_survive_single = 1 - p_fail_single
    
    print(f"   - Probability disk fails on any given day: {p_fail_single:.6f} ({p_fail_single*100:.4f}%)")
    print(f"   - Probability disk survives any given day: {p_survive_single:.6f} ({p_survive_single*100:.4f}%)")
    
    # Multiple disks scenario
    num_disks = 10000
    
    print(f"\n2. Large-Scale System ({num_disks:,} disks):")
    
    # Probability ALL disks survive a day
    p_all_survive = p_survive_single ** num_disks
    
    # Probability at least ONE disk fails
    p_any_fail = 1 - p_all_survive
    
    print(f"   - Probability ALL disks survive the day: {p_all_survive:.6f} ({p_all_survive*100:.4f}%)")
    print(f"   - Probability at least ONE disk fails: {p_any_fail:.6f} ({p_any_fail*100:.2f}%)")
    
    print("\n" + "=" * 70)
    print("KEY INSIGHT: Failures are NOT rare events at scale!")
    print("=" * 70)
    print("\nAt small scale (1 disk):  Failure once every 3 years (rare)")
    print(f"At large scale ({num_disks:,} disks): At least one failure per day is almost certain (99.99%)")
    print("\n→ This is why distributed systems MUST handle failures as normal operations")
    print("→ Replication, redundancy, and fault tolerance are NOT optional at scale")
    print("=" * 70)


if __name__ == '__main__':
    # Main disk failure probability demonstration
    disk_failure_probability()
