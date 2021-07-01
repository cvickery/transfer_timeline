#! /usr/local/bin/bash

# Generate timelines with repeated events in separate rows for data integrity validation.

# Institutions
colleges='BAR BCC BKL BMC CSI CTY HOS HTR JJC KCC LAG LEH MEC NCC NYT QCC QNS SPS YRK'

# The admit term list has to be updated manually.
start=$SECONDS
for admit_term in 1192 1199 1202 1209 1212 1219
do echo $admit_term
   ./grouped_timelines.py -i $colleges -a $admit_term > grouped_timelines.log
done
echo That took $(( SECONDS - start )) seconds
