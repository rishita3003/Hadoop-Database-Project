#!/usr/bin/env python3

import sys
from collections import defaultdict

def projection_reducer():
    print("Projection Reducer called")
    # line 
    for line in sys.stdin:
        print(line)

def filter_reducer():
    print("Filter Reducer called")
    # line 
    for line in sys.stdin:
        print(line)

import sys

def group_by_reducer():
    """ Reduces aggregated data based on group keys and outputs final results. """
    current_key = None
    current_aggregates = None

    for line in sys.stdin:
        # Split the line into key and value parts
        parts = line.strip().split(',')
        key = tuple(parts[:-1])  # Group by key columns
        values = parts[-1]  # Aggregated values

        # Parse aggregated values, e.g., SUM=10, COUNT=2
        agg_data = {}
        for value in values.split(';'):
            func, val = value.split('=')
            agg_data[func] = float(val)

        # If the key changes, output the current aggregates and reset
        if key != current_key:
            if current_key is not None:
                output_results(current_key, current_aggregates)
            current_key = key
            current_aggregates = agg_data
        else:
            # Update the aggregates with the new data
            update_final_aggregates(current_aggregates, agg_data)

    # Output the final group's result
    if current_key is not None:
        output_results(current_key, current_aggregates)

def output_results(key, aggregates):
    """ Formats and prints the results for a group. """
    results = ','.join(map(str, key)) + ',' + ','.join(f"{func}={val}" for func, val in aggregates.items())
    print(results)

def update_final_aggregates(current_aggregates, new_aggregates):
    """ Combines the current and new aggregate values. """
    for func, value in new_aggregates.items():
        if func in current_aggregates:
            if func == 'COUNT':
                current_aggregates[func] += value
            elif func in ['SUM', 'AVG_SUM']:  # Assume AVG is split into SUM and COUNT
                current_aggregates[func] += value
            elif func == 'MAX':
                current_aggregates[func] = max(current_aggregates[func], value)
            elif func == 'MIN':
                current_aggregates[func] = min(current_aggregates[func], value)
            elif func == 'AVG_COUNT':  # Separate handling if AVG was split
                current_aggregates[func] += value
        else:
            current_aggregates[func] = value



def main():
    current_word = None
    current_count = 0
    word = None

    for line in sys.stdin:
        line = line.strip()
        word, count = line.split(",", 1) # count set as 1 by mapper
        try:
            count = int(count) # converting count from string to int
        except ValueError:
            continue
        if current_word == word:
            current_count += count
        else:
            if current_word:
                print(f'{current_word}\t{current_count}')
            current_count = count
            current_word = word

    if current_word == word:
        print(f'{current_word}\t{current_count}')
        
        
if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: reducer.py <projection_columns>")
        sys.exit(1)

    elif sys.argv[1] == "projection":
        projection_reducer()
    elif sys.argv[1] == "filter":
        filter_reducer()
    elif sys.argv[1] == "group_by":
        group_by_reducer()
    else:
        print("Invalid argument")
        sys.exit(1)
            
