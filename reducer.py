#!/usr/bin/env python3

import sys
from collections import defaultdict
import json

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
    

def apply_aggregation(values, func):
    """ Apply the specified aggregation function to a list of values. """
    if func == 'SUM':
        return sum(values)
    elif func == 'COUNT':
        return len(values)
    elif func == 'MAX':
        return max(values)
    elif func == 'MIN':
        return min(values)
    elif func == 'AVG':
        return sum(values) / len(values) if values else 0
    else:
        raise ValueError(f"Unknown function: {func}")
    
def get_agg_functions():
    with open('agg_functions.json', 'r') as f:
        agg_functions = json.load(f)
    return agg_functions


def group_by_reducer():
    input_line = sys.stdin.readline().strip()
    agg_functions = get_agg_functions()
    
    aggregated_results = defaultdict(list)
    #print("input_line: ", input_line)
    #print("agg_functions: ", agg_functions)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split('\t')
        key = tuple(parts[:1])  # Change this if you have multiple grouping keys
        values = list(map(float, parts[1:]))

        # Ensure values list has enough elements
        if len(values) < len(agg_functions):
            print(f"Skipping line due to insufficient values: {line}")
            continue

        # Group values by key, and accumulate in lists
        for idx, (column, func) in enumerate(agg_functions.items(), start=1):  # assuming values align with dict ordering
            aggregated_results[(key, column)].append(values[idx - 1])

    # Apply aggregate functions and print results
    for (key, column), values in aggregated_results.items():
        func = agg_functions[column]
        result = apply_aggregation(values, "SUM")
        print(key[0],result)

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
    elif sys.argv[1] == "group":
        group_by_reducer()
    else:
        print("Invalid argument")
        sys.exit(1)
            
