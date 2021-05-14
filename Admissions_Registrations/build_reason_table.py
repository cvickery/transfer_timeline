#! /usr/local/bin/python3
""" Add the program reason descriptions to the db.
"""

import csv
import sys
from collections import namedtuple
from pgconnection import PgConnection

conn = PgConnection('cuny_transfers')
cursor = conn.cursor()
cursor.execute("""
    drop table if exists program_reasons;
    create table program_reasons (
    institution text,
    program_action text,
    action_reason text,
    description text,
    primary key (institution, program_action, action_reason)
    );
    """)
with open('./prog_reason_table.csv') as infile:
  reader = csv.reader(infile)
  for line in reader:
    if reader.line_num == 1:
      cols = [col.lower().replace(' ', '_') for col in line]
      Row = namedtuple('Row', cols)
    else:
      row = Row._make(line)
      if row.setid.startswith('GRD') or row.setid.startswith('UAC') or row.status != 'A':
        continue
      description = row.short_description
      if len(row.description) > len(description):
        description = row.description
      if len(row.long_description) > len(description):
        description = row.long_description
      institution = row.setid[0:3]
      cursor.execute(f"""
    insert into program_reasons values('{institution}',
                                       '{row.program_action}',
                                       '{row.action_reason}',
                                       '{description}')
    """)

conn.commit()
conn.close()
