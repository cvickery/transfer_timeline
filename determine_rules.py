#! /usr/bin/env python3
"""  Generate report of courses transferred keyed by sending and receiving colleges, course,
     articulation term, and rule_key.
"""
import sys
from pgconnection import PgConnection

conn = PgConnection()
cursor = conn.cursor()
rule_cursor = conn.cursor()


def get_rule(row):
  """ See what rule(s) might have been used to determine the receiving course.
  """
  rule_cursor.execute(f"""
select string_agg(r.rule_key, ', ') as rule_keys
  from transfer_rules r, source_courses s, destination_courses d
 where s.rule_id = r.id
   and d.rule_id = r.id
   and s.course_id = {row.src_course_id}
   and s.offer_nbr = {row.src_offer_nbr}
   and d.course_id = {row.dst_course_id}
   and d.offer_nbr = {row.dst_offer_nbr}
""")
  if rule_cursor.rowcount == 0:
    return 'None'
  elif rule_cursor.rowcount == 1:
    return rule_cursor.fetchone().rule_keys
  return f'Unexpected rowcount: {rule_cursor.rowcount}'


cursor.execute("""
select student_id,
       src_institution,
       src_course_id,
       src_offer_nbr,
       src_grade,
       src_gpa,
       src_subject||' '||src_catalog_nbr as sending_course,
       dst_institution,
       dst_course_id,
       dst_offer_nbr,
       dst_subject||' '||dst_catalog_nbr as receiving_course,
       dst_designation
  from transfers_applied
""")
print(f'Sending, Course, Receiving, Course, Rule Key')
m = 0
n = cursor.rowcount
for row in cursor.fetchall():
  m += 1
  print(f'  {m:,}/{n:,}\r', end='', file=sys.stderr)
  rule_key_str = get_rule(row)
  print(f'{row.src_institution}, {row.sending_course}, '
        f'{row.dst_institution}, {row.receiving_course}, {rule_key_str}')
print(file=sys.stderr)
