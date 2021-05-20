#! /usr/local/bin/bash
time ./generate_baseline_stats.py -i bcc hos ncc qcc bkl leh qns -t 1192 1199 1202 1209 1212 1219 \
 -e apply:admit admit:commit commit:matric admit:matric admit:first_eval admit:latest_eval \
 commit:first_eval commit:latest_eval matric:first_eval matric:latest_eval first_eval:start_reg \
 latest_eval:start_reg
mv debug.xlsx Baseline_Intervals_`date +%Y-%m-%d`.xlsx