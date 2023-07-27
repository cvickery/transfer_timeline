#! /usr/local/bin/python3
"""The admit_types table explains the admit_type codes in the admissions table.

In the CF query, this information is repeated for each college:
Row Institution Admit Type  Eff Date  Status  Descr                   Short Desc  Career  Readmit
1   BAR01       2           05/28/2019  I     Freshman                Freshman            N
2   BAR01       3           05/28/2019  I     Undergraduate Transfer  Transfer            N
3   BAR01       4           05/28/2019  I     Re-Admit                Re-Admit            N
4   BAR01       FRD         01/01/1901  A     Freshman                Freshman     UGRD   N
5   BAR01       FRS         01/01/1901  A     Freshman                Freshman     UGRD   N
6   BAR01       GDS         01/01/1901  A     Graduate Degree         Grad Degr           N
7   BAR01       GND         01/01/1901  A     Graduate Non Degree     Grad NDeg           N
8   BAR01       GRA         01/01/1901  A     Graduate Readmit        Grad Radm           Y
9   BAR01       MCH         01/01/1901  A     Macaulay                Macaulay     UGRD   N
10  BAR01       NON         01/01/1901  A     Non Degree              Non Degree          N
11  BAR01       PCL         01/01/1901  A     Pre College             PreCollege   UGRD   N
12  BAR01       PRE         01/01/1901  A     Pre-Readmit             PreReadmit          N
13  BAR01       RAD         01/01/1901  A     Re-Admit                Re-Admit            Y
14  BAR01       TRD         01/01/1901  A     Transfer                Transfer     UGRD   N
15  BAR01       TRN         01/01/1901  A     Transfer                Transfer     UGRD   N
16  BAR01       VST         10/01/2021  I     Visiting                Visiting     UGRD   N
17  ...

 Additional Note:
  TRN = “Transfer Normal(?)”
  TRD = “Transfer Direct-Admit“

In the admissions CF query, the Inactive values 2,3,4 appear for admit terms prior to 1182 (about).
So here, we accept Inactive Admit Types.

"""
import csv
import psycopg

from collections import namedtuple
from psycopg.rows import namedtuple_row

with psycopg.connect('dbname=cuny_transfers') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:

    cursor.execute("""
    drop table if exists admit_types;
    create table admit_types (
    institution text,
    admit_type text,
    description text,
    primary key (institution, admit_type)
    )
    """)

    with open('./queries/ADMIT_TYPE_TBL.csv') as att:
      reader = csv.reader(att)
      for line in reader:
        if reader.line_num == 1:
          cols = [c.lower().replace(' ', '_') for c in line]
          Row = namedtuple('Row', cols)
        else:
          row = Row._make(line)
          cursor.execute(f"""
    insert into admit_types values ('{row.institution}', '{row.admit_type}', '{row.descr}')
    """)
