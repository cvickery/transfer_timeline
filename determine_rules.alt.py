#! /usr/bin/env python3
"""  Generate report of courses transferred keyed by sending and receiving colleges, course,
     articulation term, and rule_key.

     Alternate version: cache all rules keyed by sending course and destination college.
     Then go through the transfers_applied table again based on those results.
"""
import sys
from collections import defaultdict, namedtuple
from pgconnection import PgConnection

conn = PgConnection()
cursor = conn.cursor()
rule_cursor = conn.cursor()

Rule = namedtuple('Rule', 'rule_key, min_gpa max_gpa destination_courses')
course_to_college = defaultdict(list)


def get_rule(row):
  """ See what rule(s) might have been used to determine the receiving course.
  """
  try:
    return ','.join([r.rule_key
                    for r in course_to_college[int(row.src_course_id),
                                               int(row.src_offer_nbr),
                                               row.dst_institution]])
  except AttributeError as ae:
    print(f'{ae}: {row=}', file=sys.stderr)
    exit()
  except ValueError as ve:
    print(f'{ve}: {row=}', file=sys.stderr)
    exit()


print('Look up all rules', file=sys.stderr)
cursor.execute("""
select id, rule_key, sending_courses, destination_institution, receiving_courses
       from transfer_rules
""")
print('Cache all rules', file=sys.stderr)
m = 0
n = cursor.rowcount
for row in cursor.fetchall():
  m += 1
  print(f'  {m:,}/{n:,}\r', end='', file=sys.stderr)
  for sending_course in row.sending_courses.split(':'):
    course_id, offer_nbr = sending_course.split('.')
    rule_cursor.execute(f"""
select min_gpa, max_gpa
  from source_courses
 where rule_id = {row.id}
   and course_id = {course_id}
   and offer_nbr = {offer_nbr}
""")

    for course in rule_cursor.fetchall():
      course_to_college[int(course_id),
                        int(offer_nbr),
                        row.destination_institution].append(
                            Rule._make([row.rule_key,
                                        course.min_gpa,
                                        course.max_gpa,
                                        row.receiving_courses.split(':')]))

# for key in course_to_college.keys():
#   print(f'{key}: {course_to_college[key]}')

print('\nDetermine rule_keys for transferred courses', file=sys.stderr)
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
