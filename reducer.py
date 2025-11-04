#!/usr/bin/env python3
import sys

current_word = None
current_count = 0
word = None

# Input comes from standard input (stdin)
for line in sys.stdin:
    # Parse the input we got from mapper.py
    line = line.strip()
    word, count = line.split("\t", 1)
    try:
        count = int(count)
    except ValueError:
        # Ignore lines where count is not a number
        continue

    # Aggregate counts for the same word
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # Write the result to stdout
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

# Output the last word if needed
if current_word == word:
    print(f"{current_word}\t{current_count}")