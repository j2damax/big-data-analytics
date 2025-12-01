#!/usr/bin/env python

# make sure to install these packages before running:
# pip install sodapy

import json
from sodapy import Socrata

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.austintexas.gov", None)

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("sh59-i6y9", limit=100000, order="read_date DESC")

# Print first few records (equivalent to head())
print("First 5 records:")
for i, record in enumerate(results[:5]):
    print(f"Record {i+1}: {record}")

# Write to JSONL file using built-in json module
with open('data/traffic.jsonl', 'w') as f:
    for record in reversed(results):
        f.write(json.dumps(record) + '\n')

print(f"Dataset written to data/traffic.jsonl with {len(results)} records")