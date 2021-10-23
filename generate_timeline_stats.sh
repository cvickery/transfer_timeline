#! /usr/local/bin/bash

# Generate/Update a standardized set of event interval data.

# Institution order will be preserved in the generated Excel spreadsheet
institutions='bcc bmc hos kcc lag qcc bar bkl csi cty htr jjc leh mec nyt qns sps yrk'

# Admit terms list should be updated manually as new terms "of interest" become relevant.
terms='1199 1202 1209 1212 1219 1222'

# There are 45 possible event pairs; these seem potentially interesting. Edit to select others.
# The order within the pairs should be adjusted so that positive/negative interval values have a
# consistent meaning of "goodness".
event_pairs='apply:admit admit:commit commit:matric admit:matric admit:first_eval admit:latest_eval
 admit:start_reg commit:first_eval commit:latest_eval matric:first_eval matric:latest_eval
 first_eval:start_reg latest_eval:start_reg first_eval:first_cls latest_eval:first_cls
 first_eval:census latest_eval:census'

# Be sure all the query data, except for evaluations, is up to date.
echo Check Query Data 2>&1
./check_query_data.py
if [[ $? != 0 ]]
then exit
fi

# Rebuild tables
echo Rebuild Timeline Tables 2>&1
./build_timeline_tables.py

# Run the process
echo Generate Timeline Statistics 2>&1
./generate_timeline_stats.py \
 -i $institutions \
 -t $terms \
 -e $event_pairs

# Rename the Excel "debug" workbook for archival purposes
mv debug.xlsx Transfer_Timeline_Intervals_`date +%Y-%m-%d`.xlsx
