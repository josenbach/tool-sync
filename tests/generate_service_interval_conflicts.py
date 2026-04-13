#!/usr/bin/env python3
"""
Generate service interval conflicts CSV, excluding inactive tools.

Connects to TipQA, fetches all tools, filters out inactive/non-active tools,
and identifies parts where active tools have conflicting service intervals.

Usage:
    python tests/generate_service_interval_conflicts.py
    python tests/generate_service_interval_conflicts.py --include-lost  # include L/OS/OC/TO/QAHD statuses
"""

import os
import sys
import argparse
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utilities.database_utils import get_tipqa_connection, get_all_tipqa_tools
from utilities.shared_sync_utils import log_and_print, load_config


def main():
    parser = argparse.ArgumentParser(description='Generate service interval conflicts CSV without inactive tools')
    parser.add_argument('--include-lost', action='store_true',
                        help='Include Lost/Offsite/QA Hold tools (L, OS, OC, TO, QAHD). '
                             'By default only truly inactive (I) tools are excluded.')
    parser.add_argument('--output', default='tests/service_interval_conflicts.csv',
                        help='Output CSV path (default: tests/service_interval_conflicts.csv)')
    args = parser.parse_args()

    load_dotenv()
    config = load_config()

    log_and_print("Connecting to TipQA (via Databricks)...")
    conn = get_tipqa_connection(config)

    try:
        log_and_print("Fetching all TipQA tools...")
        df = get_all_tipqa_tools(conn, config)
        log_and_print(f"Total tools fetched: {len(df)}")

        # Filter out inactive tools (maintenance_status='I' or revision_status='I')
        inactive_mask = (
            (df['maintenance_status'].astype(str).str.strip() == 'I') |
            (df['revision_status'].astype(str).str.strip() == 'I')
        )
        inactive_count = inactive_mask.sum()

        if not args.include_lost:
            # Also exclude lost/offsite/QA hold
            non_active_mask = inactive_mask | (
                df['maintenance_status'].astype(str).str.strip().isin(['L', 'OS', 'OC', 'TO', 'QAHD'])
            )
            excluded_count = non_active_mask.sum()
            active_df = df[~non_active_mask].copy()
            log_and_print(f"Excluded {excluded_count} non-active tools "
                          f"({inactive_count} inactive + {excluded_count - inactive_count} lost/offsite/QA hold)")
        else:
            active_df = df[~inactive_mask].copy()
            log_and_print(f"Excluded {inactive_count} inactive (I-status) tools")

        log_and_print(f"Active tools remaining: {len(active_df)}")

        # Clean up service interval: convert to numeric, drop nulls/zeros
        active_df['si_numeric'] = pd.to_numeric(active_df['service_interval_seconds'], errors='coerce')
        has_si = active_df['si_numeric'].notna() & (active_df['si_numeric'] > 0)
        si_df = active_df[has_si].copy()
        si_df['si_int'] = si_df['si_numeric'].astype(int)
        log_and_print(f"Tools with positive service intervals: {len(si_df)}")

        # Clean up part numbers
        si_df['part_clean'] = si_df['part_number'].astype(str).str.strip()
        si_df = si_df[si_df['part_clean'].notna() & (si_df['part_clean'] != '') & (si_df['part_clean'] != 'nan')]

        # Group by part number, find parts with conflicting intervals
        conflicts = []
        for part, group in si_df.groupby('part_clean'):
            unique_intervals = group['si_int'].unique()
            if len(unique_intervals) > 1:
                for _, row in group.sort_values('serial_number').iterrows():
                    conflicts.append({
                        'Part Number': row['part_clean'],
                        'Asset ID': row['serial_number'],
                        'Service Interval (seconds)': row['si_int'],
                    })

        conflicts_df = pd.DataFrame(conflicts)
        log_and_print(f"Found {len(conflicts_df)} tool entries across "
                      f"{conflicts_df['Part Number'].nunique() if len(conflicts_df) > 0 else 0} parts with conflicting intervals")

        conflicts_df.to_csv(args.output, index=False)
        log_and_print(f"Written to {args.output}")

        # Summary of conflict groups
        if len(conflicts_df) > 0:
            log_and_print("\n=== CONFLICT SUMMARY ===")
            for part in conflicts_df['Part Number'].unique():
                part_rows = conflicts_df[conflicts_df['Part Number'] == part]
                intervals = part_rows['Service Interval (seconds)'].unique()
                counts = part_rows.groupby('Service Interval (seconds)').size()
                summary = ', '.join(f"{int(si)}s x{cnt}" for si, cnt in counts.items())
                log_and_print(f"  {part}: {len(part_rows)} tools, intervals: {summary}")

    finally:
        conn.close()
        log_and_print("Database connection closed.")


if __name__ == '__main__':
    main()
