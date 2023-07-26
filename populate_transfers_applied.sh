#! /usr/local/bin/bash

# Run initialize_transfers_applied.py to create the transfers_applied table and to populate it with
# events prior to March 4, 2021. Then run this to bring the history of transfer evaluations up to
# date. Thereafter, just run update_transfers_applied.py daily.

# For each daily file in downloads, in calendar order, update the transfers_applied table.

for file in `ls -rt downloads/*ALL*`
do
  ls -l $file
  update_transfers_applied.py $file
done