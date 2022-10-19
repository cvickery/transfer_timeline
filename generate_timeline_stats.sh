#! /usr/local/bin/bash

# Generate/Update a standardized set of event interval data.

# Command line arguments
for arg in $@
do
  case $arg in
    'skip_updates') SKIP_UPDATES=True
                    ;;
     *) echo "Unrecognized argument: $arg"
        exit 1
        ;;
  esac
done

# Institution order will be preserved in the generated Excel spreadsheet (community colleges first)
institutions='bcc bmc hos kcc lag qcc bar bkl csi cty htr jjc leh mec nyt qns sps yrk'

# Admit terms list should be updated manually as new terms "of interest" become relevant.
terms='1199 1202 1209 1212 1219 1222 1229 1232'

# There are 12C2 = 66 possible event pairs; these seem potentially interesting. Edit to select
# others.

# The order within the pairs should be adjusted so that positive/negative interval values have a
# consistent meaning of "goodness".

# Suppress "uninteresting pairs"
event_pairs='apply:admit
             #admit:commit
             commit:matric
             admit:matric
             admit:first_eval
             admit:latest_eval
             admit:start_open_enr
             #commit:first_eval
             #commit:latest_eval
             matric:first_eval
             matric:latest_eval
             #first_eval:start_open_enr
             #latest_eval:start_open_enr
             #first_eval:start_classes
             #latest_eval:start_classes
             #first_eval:census_date
             #latest_eval:census_date'
# Interesting event pairs
event_pairs='apply:admit
             admit:commit
             commit:matric
             admit:matric
             admit:first_eval
             admit:latest_eval
             admit:start_open_enr
             commit:first_eval
             commit:latest_eval
             matric:first_eval
             matric:latest_eval
             first_eval:start_open_enr
             latest_eval:start_open_enr
             first_eval:start_classes
             latest_eval:start_classes
             first_eval:census_date
             latest_eval:census_date'

stats='n mean median'

# Skip table management if testing generator
if [[ ! $SKIP_UPDATES ]]
then
  # Be sure all the query data, except for evaluations, is up to date.
  echo Check Query Data 2>&1
  ./check_query_data.py

  case $? in
    0)    # Queries up to date: rebuild tables
          echo Rebuild Timeline Tables 2>&1
          ./build_timeline_tables.py
          ;;
    1)    # Queries not up to date, but user chose to continue
          ;;
    255)  # Queries not up to date and no user override
          echo Download new query files from PeopleSoft
          exit
          ;;
    *)    echo "Unexpected response ($?) from check_queiry_data.py"
          exit 1
          ;;
  esac
else echo Skip checking queries and updating timeline tables
fi

# Run the process
echo Generate Timeline Statistics 2>&1
./generate_timeline_stats.py \
 -i $institutions \
 -t $terms \
 -e $event_pairs \
 -s $stats

# Rename the Excel "debug" workbook for archival purposes
mv debug.xlsx Transfer_Timeline_Intervals_`date +%Y-%m-%d`.xlsx
