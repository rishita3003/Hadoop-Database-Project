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
    else:
        print("Invalid argument")
        sys.exit(1)
            