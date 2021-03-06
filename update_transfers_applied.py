#! /usr/bin/env python3
""" Record history of changes to credits-transferred.
      Student, sending school, courses that changed
"""

import csv
with open('./downloads/CB_LEH_TRNS_DTL_SRC_CLASS_ALL-2020-02-21.csv',
          lineend= '', errors='ignore') as csv_file:
  csv_reader = csv.reader(csv_file)
print("Hello, World!")
