#! /usr/local/bin/python3
""" Replace rows in transferred_courses where the posted_date is newer; archive replaced rows.
    Add new rows.
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
file_date = datetime.datetime.fromtimestamp(the_file.stat().st_mtime)
file_name = the_file.name

print('Using:', file_name, file_date.strftime('%B %d, %Y'), file=sys.stderr)
iso_file_date = file_date.isoformat()
debug = open(f'./debugs/{iso_file_date}', 'w')
print(file_name, file=debug)

# Record types, to be indexed by dst_institution
num_new = defaultdict(int)      # never before seen
num_alt = defaultdict(int)      # real changes
num_old = defaultdict(int)      # posted date <= max already in db
num_already = defaultdict(int)  # new data, but duplicates existing

# Counts of multiple existing records, to be indexed by src_institution
num_mult = defaultdict(int)

# Miscellaneous anomalies, indexed by src_institution, dst_institution
num_debug = defaultdict(int)

# Latest posted date, and counts for the update_history table
last_post = None
num_added = 0
num_changed = 0
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

# Skip anything already in the db, based on posted_dates
max_newly_posted_date = None
trans_cursor.execute('select max(last_post) from update_history')
min_new_posted_date = trans_cursor.fetchone().max
if min_new_posted_date is None:
  sys.exit('No max posted date in history table')
min_new_posted_date = min_new_posted_date + datetime.timedelta(days=1)
min_new_posted_date_str = min_new_posted_date.isoformat()

print(f"Skipping transactions posted before {min_new_posted_date_str}.")

# Progress indicators
m = 0
num_records = len(open(the_file, encoding='ascii', errors='backslashreplace').readlines()) - 1

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

      # columns {src_repeatable, dst_is_message, dst_is_blanket} from CF catalog (cached above).
      cols.insert(1 + cols.index('src_offer_nbr'), 'src_repeatable')
      cols.insert(1 + cols.index('dst_gpa'), 'dst_is_message')
      cols.insert(1 + cols.index('dst_is_message'), 'dst_is_blanket')

      placeholders = ((len(cols)) * '%s,').strip(', ')
      cols = ', '.join([c for c in cols])
      Row = namedtuple('Row', headers)
    else:
      if reader.line_num == 2 and values_added is None:
        # SYSDATE is available: substitute it for file_date
        mo, da, yr = [int(x) for x in line[-1].split('/')]
        file_date = datetime.datetime(yr, mo, da)

      assert m == num_added + num_changed + num_skipped, (f'{m} != {num_added}+{num_changed}+'
                                                          f'{num_skipped}')
      m += 1
      if progress:
        print(f'  {m:06,}/{num_records:06,}\r', end='', file=sys.stderr)

      row = Row._make(line)

      if '/' in row.posted_date:
        mo, da, yr = row.posted_date.split('/')
        posted_date = datetime.date(int(yr), int(mo), int(da))
      else:
        posted_date = None

      # Ignore old records and ones with no posted_date
      if not posted_date or (posted_date < min_new_posted_date):
        num_old[row.dst_institution] += 1
        num_skipped += 1
        continue

      src_course_id = int(row.src_course_id)
      src_offer_nbr = int(row.src_offer_nbr)
      src_catalog_nbr = row.src_catalog_nbr.strip()
      dst_course_id = int(row.dst_course_id)
      dst_offer_nbr = int(row.dst_offer_nbr)
      dst_catalog_nbr = row.dst_catalog_nbr.strip()

      # Is the src course is repeatable; is dst course is MESG or BKCR
      src_repeatable = (src_course_id, src_offer_nbr) in repeatables
      dst_is_mesg = (dst_course_id, dst_offer_nbr) in messages
      dst_is_bkcr = (dst_course_id, dst_offer_nbr) in blankets

      # Look up existing transfers_applied record(s)
      trans_cursor.execute(f"""
select * from transfers_applied
 where student_id = {row.student_id}
   and src_course_id = {src_course_id}
   and src_offer_nbr = {src_offer_nbr}
   and dst_institution = '{row.dst_institution}'
""")
      if trans_cursor.rowcount == 0:
        # Not in transfers_applied yet: add new record
        # --------------------------------------------
        values_tuple = (row.student_id, row.src_institution, row.transfer_model_nbr,
                        row.enrollment_term, row.enrollment_session, row.articulation_term,
                        row.model_status, posted_date, row.src_subject, src_catalog_nbr,
                        row.src_designation, row.src_grade, row.src_gpa, row.src_course_id,
                        row.src_offer_nbr, src_repeatable, row.src_description,
                        row.academic_program, row.units_taken, row.dst_institution,
                        row.dst_designation, row.dst_course_id, row.dst_offer_nbr, row.dst_subject,
                        dst_catalog_nbr, row.dst_grade, row.dst_gpa, dst_is_mesg, dst_is_bkcr)
        if values_added is None:
          values_tuple += (row.user_id, row.reject_reason, row.transfer_overridden == 'Y',
                           row.override_reason, row.comment)
        else:
          values_tuple += values_added

        trans_cursor.execute(f'insert into transfers_applied ({cols}) values ({placeholders}) ',
                             values_tuple)

        num_new[row.dst_institution] += 1
        num_added += 1

      else:
        # A single sending course can transfer as multiple receiving courses, for example when a
        # BKCR course is used to fill out credits transferred. So we really want to coalesce the
        # cases into a single record in the db.

        # Debug: look up credit info for the courses in this record
        curric_cursor.execute(f"""
    select course_id, offer_nbr, min_credits, max_credits
      from cuny_courses
     where course_id in {src_course_id, dst_course_id}
  """)
        credit_info = {(c.course_id, c.offer_nbr): (c.min_credits, c.max_credits)
                       for c in curric_cursor.fetchall()}
        try:
          new_src_cr = credit_info[(src_course_id, src_offer_nbr)]
          new_dst_cr = credit_info[(dst_course_id, dst_offer_nbr)]
        except KeyError as ke:
          print(ke, 'Credit lookup failure')
        print(f'src: {src_course_id:06}:{src_offer_nbr} {new_src_cr} {src_repeatable} '
              f'dst: {dst_course_id:06}:{dst_offer_nbr} {new_dst_cr} {dst_is_mesg} {dst_is_bkcr}')

        for record in trans_cursor.fetchall():
          if record.posted_date and not (record.posted_date < posted_date):
            # Existing record is not older: skip this one, and add to debug file for verification
            # Maybe the record was backdated for some reason?
            num_debug[(row.src_institution, row.dst_institution)] += 1
            print(f'*** CF query {the_file}: posted date is not newer than existing '
                  f'tranfers_applied posted_date\n',
                  f'CF row: {line}\nDB record: {[v for v in record]}\n', file=debug)

            num_already[row.dst_institution] += 1
            num_skipped += 1
            continue

          # Debug: show credits , repeatable, is_message, is_bkcr for existing and new src/dst
          old_dst_course_id = int(row.dst_course_id)
          old_dst_offer_nbr = int(row.dst_offer_nbr)
          curric_cursor.execute(f"""
    select course_id, offer_nbr, min_credits, max_credits
      from cuny_courses
     where course_id = {old_dst_course_id}
       and offer_nbr = {old_dst_offer_nbr}
""")
          old_cr = [(c.min_credits, c.max_credits) for c in curric_cursor.fetchall()]
          old_mesg = (old_dst_course_id, old_dst_offer_nbr) in messages
          old_bkcr = (old_dst_course_id, old_dst_offer_nbr) in blankets
          print(f'old: {old_dst_course_id:06}:{old_dst_offer_nbr} {old_cr} {old_mesg} {old_bkcr}')

          # Has destination course changed?
          if int(record.dst_course_id) == dst_course_id \
             and int(record.dst_offer_nbr) == dst_offer_nbr:
             # Most common case: nothing more to do
             num_already[row.dst_institution] += 1
             num_skipped += 1

          else:
            # Different destination course: Write the new record to the transfers_changed table
            values_tuple = (row.student_id, row.src_institution, row.transfer_model_nbr,
                            row.enrollment_term, row.enrollment_session, row.articulation_term,
                            row.model_status, posted_date, row.src_subject, src_catalog_nbr,
                            row.src_designation, row.src_grade, row.src_gpa, row.src_course_id,
                            row.src_offer_nbr, src_repeatable, row.src_description,
                            row.academic_program, row.units_taken, row.dst_institution,
                            row.dst_designation, row.dst_course_id, row.dst_offer_nbr,
                            row.dst_subject, dst_catalog_nbr, row.dst_grade, row.dst_gpa,
                            dst_is_mesg, dst_is_bkcr)
            if values_added is None:
              values_tuple += (row.user_id, row.reject_reason,
                               row.transfer_overridden == 'Y', row.override_reason,
                               row.comment)
            else:
              values_tuple += values_added

            trans_cursor.execute(f'insert into transfers_changed ({cols}) values ({placeholders}) ',
                                 values_tuple)

            num_alt[row.dst_institution] += 1
            num_changed += 1

        if (max_newly_posted_date is None) or (posted_date > max_newly_posted_date):
          max_newly_posted_date = posted_date

# Prepare summary info
if max_newly_posted_date is None:
  max_newly_posted_date = 'NULL'
else:
  max_newly_posted_date = f"'{max_newly_posted_date}'"

uncounted = num_records - num_added - num_changed - num_skipped
print(f'{file_name[-16:-4]}: {m=} {num_records=} {num_added=} {num_changed=} {num_skipped=} '
      f'{uncounted=}', file=debug)

if (num_added + num_changed + num_skipped) != 0:
  trans_cursor.execute(f"""
  insert into update_history values(
            '{file_name}', '{file_date}', {max_newly_posted_date},
            {num_records}, {num_added}, {num_changed}, {num_skipped})
            on conflict do nothing
  """)
  if trans_cursor.rowcount == 0:
    print(f"""Update History conflict\n new:
          '{file_name}', '{file_date}', {max_newly_posted_date},
          {num_records}, {num_added}, {num_changed}, {num_skipped})
          """, file=sys.stderr)

trans_conn.commit()
trans_conn.close()

# Generate log of actions, by college
with open('./logs/' + iso_file_date + '.log', 'w') as report:
  for key in sorted(num_old.keys()):
    print(f'{iso_file_date}       old {key[0:3]} {num_old[key]:7,}', file=report)

  for key in sorted(num_already.keys()):
    print(f'{iso_file_date} unchanged {key[0:3]} {num_already[key]:7,}', file=report)

  for key in sorted(num_new.keys()):
    print(f'{iso_file_date}       new {key[0:3]} {num_new[key]:7,}', file=report)

  for key in sorted(num_alt.keys()):
    print(f'{iso_file_date}   altered {key[0:3]} {num_alt[key]:7,}', file=report)

  for key in sorted(num_mult.keys()):
    print(f'{iso_file_date}  multiple {key[0:3]} {num_mult[key]:7,}', file=report)

  for key in sorted(num_debug.keys()):
    key_str = f'{key[0][0:3]}:{key[1][0:3]}'
    print(f'{iso_file_date} *** debug {key_str} {num_debug[key]:7,}', file=report)
