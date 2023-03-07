#! /usr/local/bin/python3
"""The admit_actions table explains the admit_action codes in the admissions table."""
import csv
import psycopg

from collections import namedtuple
from psycopg.rows import namedtuple_row

with psycopg.connect('dbname=cuny_transfers') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:

    cursor.execute("""
    drop table if exists admit_actions;
    create table admit_actions (
    action text primary key,
    description text
    )
    """)

    with open('./queries/ADMIT_ACTION_TBL.csv') as act:
      reader = csv.reader(act)
      for line in reader:
        if reader.line_num == 1:
          cols = [c.lower().replace(' ', '_') for c in line]
          Row = namedtuple('Row', cols)
        else:
          row = Row._make(line)
          cursor.execute(f"""
    insert into admit_actions values ('{row.program_action}', '{row.description}')
    """)
