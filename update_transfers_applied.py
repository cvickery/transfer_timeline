#! /usr/local/bin/python3
""" Add new rows to transfers_applied.
    Using query data that covers the past week, so skip rows that already exist.
    Since the posted_date is part of the primary key, there is no longer a need to check the posted
    date for redundancy, only for info in the update_history table.
"""

import csv
import datetime
import sys
import argparse

from collections import namedtuple, defaultdict
from pathlib import Path
from pgconnection import PgConnection

parser = argparse.ArgumentParser('Update Transfers')
parser.add_argument('-np', '--no_progress', action='store_true')
parser.add_argument('file', nargs='?')
args = parser.parse_args()
progress = not args.no_progress

curric_conn = PgConnection()
curric_cursor = curric_conn.cursor()
trans_conn = PgConnection('cuny_transfers')
trans_cursor = trans_conn.cursor()

# If a file was specified on the command line, use that. Otherwise use the latest one found in
# downloads. The idea is to allow history from previous snapshots to be captured during development,
# then to use the latest snapshot on a daily basis.
the_file = args.file
if the_file is None:
  # No snapshot specified; use latest available.
  files = Path('./downloads').glob('CV*ALL*')
  for file in files:
    if the_file is None or file.stat().st_mtime > the_file.stat().st_mtime:
      the_file = file
else:
  the_file = Path(the_file)

if the_file is None:
  sys.exit('No input file.')

# Using the date the file was transferred to Tumbleweed as a proxy for CF SYSDATE
file_date = datetime.date.fromtimestamp(the_file.stat().st_mtime)
file_name = the_file.name

print('Using:', file_name, file_date.strftime('%B %d, %Y'), file=sys.stderr)
iso_file_date = file_date.isoformat()
debug = open(f'./debugs/{iso_file_date}', 'w')
print(file_name, file=debug)

# # Record types, to be indexed by dst_institution
# num_new = defaultdict(int)      # never before seen
# num_alt = defaultdict(int)      # real changes
# num_old = defaultdict(int)      # posted date <= max already in db
# num_already = defaultdict(int)  # new data, but duplicates existing

# # Counts of multiple existing records, to be indexed by src_institution
# num_mult = defaultdict(int)

# # Miscellaneous anomalies, indexed by src_institution, dst_institution
# num_debug = defaultdict(int)

# Latest posted date, and counts for the update_history table
last_post = None
num_added = 0
# num_changed = 0
num_skipped = 0

# Caches of all repeatable, message, and blanket credit courses
curric_cursor.execute("select course_id, offer_nbr from cuny_courses where repeatable = 'Y'")
repeatables = [(int(row.course_id), int(row.offer_nbr)) for row in curric_cursor.fetchall()]
curric_cursor.execute("""select course_id, offer_nbr
                       from cuny_courses
                       where designation in ('MLA', 'MNL')""")
messages = [(int(row.course_id), int(row.offer_nbr)) for row in curric_cursor.fetchall()]
curric_cursor.execute("""select course_id, offer_nbr
                       from cuny_courses
                       where attributes ~* 'BKCR'""")
blankets = [(int(row.course_id), int(row.offer_nbr)) for row in curric_cursor.fetchall()]
curric_conn.close()

max_newly_posted_date = None
# Progress indicators
m = 0
num_records = len(open(the_file, encoding='ascii', errors='backslashreplace').readlines()) - 1
with open(f'./Logs/update_{iso_file_date}.log', 'w') as logfile:
  with open(the_file, encoding='ascii', errors='backslashreplace') as csv_file:
    reader = csv.reader(csv_file)
    for line in reader:
      if reader.line_num == 1:
        headers = [h.lower().replace(' ', '_') for h in line]
        cols = [h for h in headers]

        # Adjust for columns not present in queries prior to 2021-03-18
        if 'comment' not in cols:
          cols += ['user_id', 'reject_reason', 'transfer_overridden', 'override_reason', 'comment']
          values_added = ('00000000', '', False, '', '')
        else:
          cols.remove('sysdate')
          values_added = None

        # columns {src_is_repeatable, dst_is_message, dst_is_blanket} from CF catalog (cached above).
        cols.insert(1 + cols.index('src_offer_nbr'), 'src_is_repeatable')
        cols.insert(1 + cols.index('dst_gpa'), 'dst_is_message')
        cols.insert(1 + cols.index('dst_is_message'), 'dst_is_blanket')

        placeholders = ((len(cols)) * '%s,').strip(', ')
        cols = ', '.join([c for c in cols])
        Row = namedtuple('Row', headers)
      else:
        # If SYSDATE is available, substitute it for file_date
        if reader.line_num == 2 and values_added is None:
          mo, da, yr = [int(x) for x in line[-1].split('/')]
          file_date = datetime.datetime(yr, mo, da)

        # if m != num_added + num_changed + num_skipped and not miscount:
        #   miscount = True
        #   print(f'Line {reader.line_num}: {m} != {num_added}+{num_changed}+'f'{num_skipped}',
        #         file=debug)
        m += 1
        if progress:
          print(f'  {m:06,}/{num_records:06,}\r', end='', file=sys.stderr)

        row = Row._make(line)

        if '/' in row.posted_date:
          mo, da, yr = row.posted_date.split('/')
          posted_date = datetime.date(int(yr), int(mo), int(da))
        else:
          posted_date = datetime.date(1901, 1, 1)

        # # Ignore old records and ones with no posted_date
        # if not posted_date or (posted_date < min_new_posted_date):
        #   num_old[row.dst_institution] += 1
        #   num_skipped += 1
        #   continue

        src_course_id = int(row.src_course_id)
        src_offer_nbr = int(row.src_offer_nbr)
        src_catalog_nbr = row.src_catalog_nbr.strip()
        dst_course_id = int(row.dst_course_id)
        dst_offer_nbr = int(row.dst_offer_nbr)
        dst_catalog_nbr = row.dst_catalog_nbr.strip()

        # Is the src course is repeatable; is dst course is MESG or BKCR
        src_is_repeatable = (src_course_id, src_offer_nbr) in repeatables
        dst_is_message = (dst_course_id, dst_offer_nbr) in messages
        dst_is_blanket = (dst_course_id, dst_offer_nbr) in blankets

        values_tuple = (row.student_id, row.src_institution, row.transfer_model_nbr,
                        row.enrollment_term, row.enrollment_session, row.articulation_term,
                        row.model_status, posted_date, row.src_subject, src_catalog_nbr,
                        row.src_designation, row.src_grade, row.src_gpa, row.src_course_id,
                        row.src_offer_nbr, src_is_repeatable, row.src_description,
                        row.academic_program, row.units_taken, row.dst_institution,
                        row.dst_designation, row.dst_course_id, row.dst_offer_nbr, row.dst_subject,
                        dst_catalog_nbr, row.dst_grade, row.dst_gpa, dst_is_message, dst_is_blanket)
        if values_added is None:
          values_tuple += (row.user_id, row.reject_reason, row.transfer_overridden == 'Y',
                           row.override_reason, row.comment)
        else:
          values_tuple += values_added

        trans_cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders}) '
                             f'on conflict do nothing',
                             values_tuple)

        if trans_cursor.rowcount == 1:
          num_added += 1
          if (max_newly_posted_date is None) or (posted_date > max_newly_posted_date):
            max_newly_posted_date = posted_date
        else:
          print(f'Skipped {values_tuple}', file=logfile)
          num_skipped += 1


# Prepare summary info
if max_newly_posted_date is None:
  max_newly_posted_date = 'NULL'
else:
  max_newly_posted_date = f"'{max_newly_posted_date}'"

# uncounted = num_records - num_added - num_changed - num_skipped
# print(f'{file_name[-16:-4]}: {m=} {num_records=} {num_added=} {num_changed=} {num_skipped=} '
#       f'{uncounted=}', file=debug)

trans_cursor.execute(f"""
    insert into update_history values(
          '{file_name}', '{file_date}', {max_newly_posted_date},
          {num_records}, {num_added}, {num_skipped})
          on conflict do nothing
  """)
if trans_cursor.rowcount == 0:
  print(f"""Update History conflict\n new:
        '{file_name}', '{file_date}', {max_newly_posted_date},
        {num_records}, {num_added}, {num_changed}, {num_skipped})
        """, file=sys.stderr)

trans_conn.commit()
trans_conn.close()
